//! The `scaling` experiment mode: one continuous read/write load across a
//! membership change.
//!
//! The load has four key cohorts. Reads use an immutable pool. Writes rewrite
//! an existing key, create a fresh key, or delete an existing key exactly once,
//! so the expected state of every key is known without a read/write race. Each
//! transition stages its change, starts the load, executes the change, waits
//! for the target epoch to reach full replication, and verifies the cohorts it
//! touched before the next transition starts.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, ensure};
use futures::{StreamExt, stream};
use hashring_client::{ClientError, HashringClient, MutationOutcome, RetryReason};
use hashring_core::{
    migration::MigrationPhase,
    proto::ErrorCode,
    topology::{Member, TopologySnapshot},
};
use rand::{SeedableRng, rngs::StdRng};
use serde_json::json;
use tokio::sync::mpsc;

use super::{
    EventLog, ExperimentConfig, ProcessGroup, ScalingLoadMode, ScalingScenario,
    deterministic_value, latency::LatencyHistogram, measurement, spawn_node, wait_for_listener,
};

/// How often the runner polls the coordinator's phase and the client's write
/// counters. It bounds the resolution of every phase boundary and of the
/// client-observed write-stop window.
const PHASE_POLL_INTERVAL: Duration = Duration::from_millis(5);

/// The phase a request started in. Requests are attributed by start, because a
/// write that starts in the pause and returns after publication did not become
/// slow because of publication.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    Pre,
    CopyingSnapshot,
    ReplayingChangelog,
    PausingWrites,
    Verifying,
    ReadyToPublish,
    Published,
    Post,
}

impl Phase {
    fn from_byte(byte: u8) -> Self {
        match byte {
            1 => Self::CopyingSnapshot,
            2 => Self::ReplayingChangelog,
            3 => Self::PausingWrites,
            4 => Self::Verifying,
            5 => Self::ReadyToPublish,
            6 => Self::Published,
            7 => Self::Post,
            _ => Self::Pre,
        }
    }

    fn byte(self) -> u8 {
        match self {
            Self::Pre => 0,
            Self::CopyingSnapshot => 1,
            Self::ReplayingChangelog => 2,
            Self::PausingWrites => 3,
            Self::Verifying => 4,
            Self::ReadyToPublish => 5,
            Self::Published => 6,
            Self::Post => 7,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Pre => "pre",
            Self::CopyingSnapshot => "copying_snapshot",
            Self::ReplayingChangelog => "replaying_changelog",
            Self::PausingWrites => "pausing_writes",
            Self::Verifying => "verifying",
            Self::ReadyToPublish => "ready_to_publish",
            Self::Published => "published",
            Self::Post => "post",
        }
    }
}

fn phase_of(phase: MigrationPhase) -> Phase {
    match phase {
        MigrationPhase::Planned | MigrationPhase::Resetting => Phase::Pre,
        MigrationPhase::CopyingSnapshot => Phase::CopyingSnapshot,
        MigrationPhase::ReplayingChangelog => Phase::ReplayingChangelog,
        MigrationPhase::PausingWrites => Phase::PausingWrites,
        MigrationPhase::Verifying => Phase::Verifying,
        MigrationPhase::ReadyToPublish => Phase::ReadyToPublish,
        MigrationPhase::Published | MigrationPhase::CleaningUp | MigrationPhase::Complete => {
            Phase::Published
        }
        MigrationPhase::Aborting | MigrationPhase::Aborted => Phase::Published,
    }
}

/// The primary stratum of a key for one transition. The variants overlap in
/// the topology, so a key gets the first one that applies; every key of a moved
/// range is still counted as moved by the `owner_changed` tag in the plan, but
/// the buckets here are disjoint so their counts add up.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Stratum {
    RemovedSource,
    NewTarget,
    OwnerChanged,
    ReplicasChanged,
    Unchanged,
}

impl Stratum {
    fn name(self) -> &'static str {
        match self {
            Self::RemovedSource => "removed_source",
            Self::NewTarget => "new_target",
            Self::OwnerChanged => "owner_changed",
            Self::ReplicasChanged => "replicas_changed",
            Self::Unchanged => "unchanged",
        }
    }
}

fn applicable_strata(scenario: ScalingScenario, label: &str) -> BTreeSet<Stratum> {
    let grow = [
        Stratum::NewTarget,
        Stratum::OwnerChanged,
        Stratum::ReplicasChanged,
        Stratum::Unchanged,
    ];
    let shrink = [
        Stratum::RemovedSource,
        Stratum::OwnerChanged,
        Stratum::ReplicasChanged,
        Stratum::Unchanged,
    ];
    let replace = [
        Stratum::RemovedSource,
        Stratum::NewTarget,
        Stratum::OwnerChanged,
        Stratum::ReplicasChanged,
        Stratum::Unchanged,
    ];
    let control = [Stratum::Unchanged];
    let chosen: &[Stratum] = match (scenario, label) {
        (ScalingScenario::Grow, _) => &grow,
        (ScalingScenario::Shrink, _) => &shrink,
        (ScalingScenario::Replace, _) => &replace,
        (ScalingScenario::Churn, "grow") => &grow,
        (ScalingScenario::Churn, "shrink") => &shrink,
        (ScalingScenario::Control, _) => &control,
        (ScalingScenario::RoundTrip, "grow") => &grow,
        (ScalingScenario::RoundTrip, "shrink") => &shrink,
        _ => &grow,
    };
    chosen.iter().copied().collect()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Cohort {
    Rewrite,
    WriteNew,
    Delete,
}

impl Cohort {
    fn metric_name(self) -> &'static str {
        match self {
            Self::Rewrite => "put_rewrite",
            Self::WriteNew => "put_new",
            Self::Delete => "delete",
        }
    }
}

#[derive(Clone, Debug)]
enum Allowed {
    /// The key must hold this value.
    Value(Vec<u8>),
    /// The key may be absent or hold this value (a fresh write with an unknown
    /// outcome).
    ValueOrMissing(Vec<u8>),
    /// A rewrite with an unknown outcome: either the old or the new value.
    Rewrite { before: Vec<u8>, after: Vec<u8> },
    /// The key must be absent.
    Missing,
    /// A delete with an unknown outcome: either the old value or absent.
    MissingOrValue(Vec<u8>),
}

impl Allowed {
    fn check(&self, value: Option<&[u8]>) -> bool {
        match self {
            Self::Value(expected) => value == Some(expected.as_slice()),
            Self::ValueOrMissing(expected) => value.is_none() || value == Some(expected.as_slice()),
            Self::Rewrite { before, after } => {
                value == Some(before.as_slice()) || value == Some(after.as_slice())
            }
            Self::Missing => value.is_none(),
            Self::MissingOrValue(expected) => value.is_none() || value == Some(expected.as_slice()),
        }
    }

    fn describe(&self) -> String {
        match self {
            Self::Value(_) => "value".into(),
            Self::ValueOrMissing(_) => "value-or-missing".into(),
            Self::Rewrite { .. } => "old-or-new-value".into(),
            Self::Missing => "missing".into(),
            Self::MissingOrValue(_) => "value-or-missing".into(),
        }
    }
}

fn is_not_found(error: &ClientError) -> bool {
    matches!(
        error,
        ClientError::Operation(failure) if failure.code == ErrorCode::NotFound
    )
}

fn mutation_outcome(error: &ClientError) -> MutationOutcome {
    match error {
        ClientError::Operation(failure) => failure.outcome,
        ClientError::DeadlineExceeded { outcome }
        | ClientError::TopologyRefresh { outcome, .. }
        | ClientError::Rpc { outcome, .. } => *outcome,
        ClientError::MissingVersion => MutationOutcome::MayHaveApplied,
        ClientError::Topology(_) => MutationOutcome::KnownNotApplied,
    }
}

fn error_reason(error: &ClientError) -> &'static str {
    match error {
        ClientError::Operation(failure) => match failure.code {
            ErrorCode::NotFound => "not_found",
            ErrorCode::Moved => "moved",
            ErrorCode::RangeBusy => "range_busy",
            ErrorCode::Unavailable => "unavailable",
            ErrorCode::DeadlineExceeded => "deadline",
            ErrorCode::TooLarge => "too_large",
            ErrorCode::ResourceExhausted => "resource_exhausted",
            ErrorCode::InvalidArgument => "invalid_argument",
            ErrorCode::MutationIdConflict => "mutation_id_conflict",
            ErrorCode::TemporarilyUnavailable => "temporarily_unavailable",
            ErrorCode::OutcomeUnknown => "outcome_unknown",
            ErrorCode::ReplicaNotReady => "replica_not_ready",
            ErrorCode::LeaseExpired => "lease_expired",
            ErrorCode::Unspecified => "operation",
        },
        ClientError::DeadlineExceeded { .. } => "deadline",
        ClientError::Topology(_) => "topology",
        ClientError::TopologyRefresh { .. } => "topology_refresh",
        ClientError::Rpc { .. } => "rpc",
        ClientError::MissingVersion => "protocol",
    }
}

struct Bucket {
    histogram: LatencyHistogram,
    count: u64,
}

struct MetricState {
    window: BTreeMap<String, Bucket>,
    total: BTreeMap<String, Bucket>,
    window_errors: BTreeMap<String, u64>,
    total_errors: BTreeMap<String, u64>,
    violations: BTreeMap<String, u64>,
    violation_examples: Vec<String>,
}

struct Metrics {
    phase: AtomicU8,
    highest_micros: u64,
    open: bool,
    issued: AtomicU64,
    late: AtomicU64,
    total_issued: AtomicU64,
    total_late: AtomicU64,
    in_flight: AtomicU64,
    /// In-flight open-loop writes. The local drain after a transition waits
    /// for these only; the reads continue into the next transition.
    in_flight_writes: AtomicU64,
    /// Successful PUT and DELETE completions. The write-stop window uses it
    /// to find the first write that went through after the last rejection.
    writes_ok: AtomicU64,
    max_in_flight: usize,
    state: Mutex<MetricState>,
}

impl Metrics {
    fn new(operation_timeout: Duration, open: bool, max_in_flight: usize) -> Result<Self> {
        Ok(Self {
            phase: AtomicU8::new(Phase::Pre.byte()),
            highest_micros: (operation_timeout.as_micros() as u64).saturating_add(1_000_000),
            open,
            issued: AtomicU64::new(0),
            late: AtomicU64::new(0),
            total_issued: AtomicU64::new(0),
            total_late: AtomicU64::new(0),
            in_flight: AtomicU64::new(0),
            in_flight_writes: AtomicU64::new(0),
            writes_ok: AtomicU64::new(0),
            max_in_flight,
            state: Mutex::new(MetricState {
                window: BTreeMap::new(),
                total: BTreeMap::new(),
                window_errors: BTreeMap::new(),
                total_errors: BTreeMap::new(),
                violations: BTreeMap::new(),
                violation_examples: Vec::new(),
            }),
        })
    }

    /// Reserves one in-flight slot. Returns false when the offered load is
    /// already at the limit; the caller drops the request and records it late.
    fn try_begin(&self) -> bool {
        if self.in_flight.fetch_add(1, Ordering::AcqRel) < self.max_in_flight as u64 {
            self.issued.fetch_add(1, Ordering::Relaxed);
            true
        } else {
            self.in_flight.fetch_sub(1, Ordering::AcqRel);
            false
        }
    }

    fn late(&self) {
        self.late.fetch_add(1, Ordering::Relaxed);
    }

    /// Reserves an in-flight slot for a write and counts it separately, so a
    /// transition can drain its writes while the reads keep going.
    fn begin_write(&self) -> bool {
        if self.try_begin() {
            self.in_flight_writes.fetch_add(1, Ordering::AcqRel);
            true
        } else {
            false
        }
    }

    fn finish_write(&self) {
        self.in_flight_writes.fetch_sub(1, Ordering::AcqRel);
        self.finish();
    }

    fn finish(&self) {
        self.in_flight.fetch_sub(1, Ordering::AcqRel);
    }

    fn phase(&self) -> Phase {
        Phase::from_byte(self.phase.load(Ordering::Acquire))
    }

    fn set_phase(&self, phase: Phase) {
        self.phase.store(phase.byte(), Ordering::Release);
    }

    fn record(
        &self,
        kind: &'static str,
        phase: Phase,
        elapsed: Duration,
        result: &Result<(), ClientError>,
    ) {
        let key = format!("{}|{kind}", phase.name());
        let mut state = self.state.lock().expect("the metrics lock is not poisoned");
        let bucket = state.window.entry(key.clone()).or_insert_with(|| Bucket {
            histogram: LatencyHistogram::new(self.highest_micros)
                .expect("the latency histogram is valid"),
            count: 0,
        });
        bucket.histogram.record(elapsed.as_micros() as u64);
        bucket.count += 1;
        if let Err(error) = result {
            let reason = error_reason(error);
            *state
                .window_errors
                .entry(format!("{key}|{reason}"))
                .or_insert(0) += 1;
        }
        drop(state);
        if result.is_ok() && matches!(kind, "put_rewrite" | "put_new" | "delete") {
            self.writes_ok.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn writes_ok(&self) -> u64 {
        self.writes_ok.load(Ordering::Acquire)
    }

    fn violation(&self, kind: &str, detail: String) {
        let mut state = self.state.lock().expect("the metrics lock is not poisoned");
        *state.violations.entry(kind.to_owned()).or_insert(0) += 1;
        if state.violation_examples.len() < 32 {
            state.violation_examples.push(format!("{kind}: {detail}"));
        }
    }

    fn sample(&self, events: &EventLog) -> Result<()> {
        let mut state = self.state.lock().expect("the metrics lock is not poisoned");
        let window = std::mem::take(&mut state.window);
        let errors = std::mem::take(&mut state.window_errors);
        let issued = self.issued.swap(0, Ordering::AcqRel);
        let late = self.late.swap(0, Ordering::AcqRel);
        self.total_issued.fetch_add(issued, Ordering::Relaxed);
        self.total_late.fetch_add(late, Ordering::Relaxed);
        let in_flight = self.in_flight.load(Ordering::Acquire);
        let open = self.open;
        let mut seconds = BTreeMap::new();
        for (key, bucket) in window {
            seconds.insert(
                key.clone(),
                json!({
                    "operations": bucket.count,
                    "latency_us": bucket.histogram.summary(),
                }),
            );
            let total = state.total.entry(key).or_insert_with(|| Bucket {
                histogram: LatencyHistogram::new(self.highest_micros)
                    .expect("the latency histogram is valid"),
                count: 0,
            });
            total.count += bucket.count;
            total.histogram.merge(&bucket.histogram);
        }
        for (key, count) in &errors {
            *state.total_errors.entry(key.clone()).or_insert(0) += count;
        }
        let violations = state.violations.clone();
        let violation_examples = state.violation_examples.clone();
        drop(state);
        events.record(
            "metrics_sampled",
            json!({
                "mode": "scaling",
                "load_mode": if open { "open" } else { "closed" },
                "phase": self.phase().name(),
                "issued": issued,
                "late": late,
                "achieved_rate": issued as f64,
                "in_flight": in_flight,
                "metrics": seconds,
                "errors": errors,
                "violations": violations,
                "violation_examples": violation_examples,
            }),
        )
    }

    fn summary(&self) -> serde_json::Value {
        let state = self.state.lock().expect("the metrics lock is not poisoned");
        let metrics: BTreeMap<_, _> = state
            .total
            .iter()
            .map(|(key, bucket)| {
                (
                    key.clone(),
                    json!({
                        "operations": bucket.count,
                        "latency_us": bucket.histogram.summary(),
                    }),
                )
            })
            .collect();
        json!({
            "phase": self.phase().name(),
            "load_mode": if self.open { "open" } else { "closed" },
            "metrics": metrics,
            "errors": state.total_errors,
            "violations": state.violations,
            "violation_examples": state.violation_examples,
            "issued": self.total_issued.load(Ordering::Acquire),
            "late": self.total_late.load(Ordering::Acquire),
            "in_flight": self.in_flight.load(Ordering::Acquire),
        })
    }

    fn violation_count(&self) -> u64 {
        self.state
            .lock()
            .expect("the metrics lock is not poisoned")
            .violations
            .values()
            .sum()
    }

    fn total_operations(&self) -> u64 {
        let state = self.state.lock().expect("the metrics lock is not poisoned");
        // The buckets the sampler has not flushed yet are part of the count.
        // Leaving them out makes two snapshots up to a second stale, and the
        // transition's operations then depend on where the sampler's tick
        // happened to fall.
        let flushed: u64 = state.total.values().map(|bucket| bucket.count).sum();
        let pending: u64 = state.window.values().map(|bucket| bucket.count).sum();
        flushed + pending
    }
}

/// The write-stop window as the client observed it: the first and last
/// `RangeBusy` rejection and the first successful write after them.
///
/// The coordinator's phase boundaries are not the fence: a node only fences a
/// range when it receives the command, and a client only notices through a
/// rejected attempt. The three timestamps are therefore sampled from the
/// client's cumulative counters, at [`PHASE_POLL_INTERVAL`] resolution.
#[derive(Default)]
struct StopWindow {
    previous_busy: u64,
    previous_writes: u64,
    first_busy_ms: Option<f64>,
    last_busy_ms: Option<f64>,
    first_ok_ms: Option<f64>,
}

impl StopWindow {
    /// A window whose baseline is the counters as they are now.
    ///
    /// Without it a later transition sees the earlier transitions' rejections
    /// as new ones and pins its first rejection to zero.
    fn starting(busy: u64, writes_ok: u64) -> Self {
        Self {
            previous_busy: busy,
            previous_writes: writes_ok,
            ..Self::default()
        }
    }

    /// Samples cumulative counters. `now_ms` is measured from the start of the
    /// change.
    fn observe(&mut self, now_ms: f64, busy: u64, writes_ok: u64) {
        if busy > self.previous_busy {
            self.first_busy_ms.get_or_insert(now_ms);
            self.last_busy_ms = Some(now_ms);
            // A new rejection means the window is open again, so a write that
            // completed before it is not the first write after the stop.
            self.first_ok_ms = None;
        } else if self.last_busy_ms.is_some()
            && self.first_ok_ms.is_none()
            && writes_ok > self.previous_writes
        {
            self.first_ok_ms = Some(now_ms);
        }
        self.previous_busy = busy;
        self.previous_writes = writes_ok;
    }

    /// Whether more sampling cannot change the summary.
    fn closed(&self) -> bool {
        self.last_busy_ms.is_none() || self.first_ok_ms.is_some()
    }

    fn summary(&self) -> serde_json::Value {
        json!({
            "first_range_busy_ms": self.first_busy_ms,
            "last_range_busy_ms": self.last_busy_ms,
            "first_write_ok_ms": self.first_ok_ms,
            "observed_ms": self
                .first_busy_ms
                .zip(self.first_ok_ms)
                .map(|(first, ok)| ok - first),
            "poll_ms": PHASE_POLL_INTERVAL.as_millis() as u64,
        })
    }
}

struct KeyPool {
    keys: Vec<u64>,
    next: AtomicUsize,
    exhausted: AtomicBool,
}

impl KeyPool {
    fn new(keys: Vec<u64>) -> Self {
        Self {
            keys,
            next: AtomicUsize::new(0),
            exhausted: AtomicBool::new(false),
        }
    }

    fn next(&self) -> Option<u64> {
        let index = self.next.fetch_add(1, Ordering::AcqRel);
        let key = self.keys.get(index).copied();
        if key.is_none() {
            self.exhausted.store(true, Ordering::Release);
        }
        key
    }

    fn len(&self) -> usize {
        self.keys.len()
    }

    fn exhausted(&self) -> bool {
        self.exhausted.load(Ordering::Acquire)
    }
}

struct TransitionPlan {
    rewrite: Arc<KeyPool>,
    write_new: Arc<KeyPool>,
    delete: Arc<KeyPool>,
    strata: Arc<BTreeMap<u64, Stratum>>,
    expected_strata: BTreeSet<Stratum>,
    present_strata: BTreeSet<Stratum>,
}

/// How much of the key space one change moves: the keys whose owner differs
/// between the base and target topologies. This is the plan's migration
/// volume; the coordinator's copy path reports the same movement as range
/// snapshot records, which a direct merge does not produce.
struct MigrationStats {
    keys: u64,
    keys_moved: u64,
}

impl MigrationStats {
    fn rate(&self) -> f64 {
        if self.keys == 0 {
            0.0
        } else {
            self.keys_moved as f64 / self.keys as f64
        }
    }
}

fn migration_stats(
    base: &TopologySnapshot,
    target: &TopologySnapshot,
    keys: u64,
) -> Result<MigrationStats> {
    let mut keys_moved = 0;
    for key in 0..keys {
        let encoded = key.to_be_bytes();
        let token = base.key_token(&encoded);
        let base_owner = base.owner_for_token(token)?.node_id.as_str();
        let target_owner = target.owner_for_token(token)?.node_id.as_str();
        if base_owner != target_owner {
            keys_moved += 1;
        }
    }
    Ok(MigrationStats { keys, keys_moved })
}

struct Ledger {
    entries: Mutex<BTreeMap<u64, Allowed>>,
    strata: Arc<BTreeMap<u64, Stratum>>,
    touched: Mutex<BTreeSet<Stratum>>,
    publish_window: AtomicBool,
    publish_expected: Mutex<BTreeMap<u64, Allowed>>,
    value_bytes: usize,
}

impl Ledger {
    fn new(strata: Arc<BTreeMap<u64, Stratum>>, value_bytes: usize) -> Self {
        Self {
            entries: Mutex::new(BTreeMap::new()),
            strata,
            touched: Mutex::new(BTreeSet::new()),
            publish_window: AtomicBool::new(false),
            publish_expected: Mutex::new(BTreeMap::new()),
            value_bytes,
        }
    }

    fn begin_publish_window(&self) {
        self.publish_window.store(true, Ordering::Release);
    }

    fn end_publish_window(&self) {
        self.publish_window.store(false, Ordering::Release);
    }

    fn record(&self, key: u64, cohort: Cohort, result: &Result<(), ClientError>) {
        let before = deterministic_value(key, 0, self.value_bytes);
        let after = deterministic_value(key, 1, self.value_bytes);
        let allowed = match (cohort, result) {
            (Cohort::Rewrite, Ok(())) => Allowed::Value(after),
            (Cohort::Rewrite, Err(error)) => match mutation_outcome(error) {
                MutationOutcome::KnownNotApplied => Allowed::Value(before),
                MutationOutcome::MayHaveApplied => Allowed::Rewrite { before, after },
            },
            (Cohort::WriteNew, Ok(())) => Allowed::Value(after),
            (Cohort::WriteNew, Err(error)) => match mutation_outcome(error) {
                MutationOutcome::KnownNotApplied => Allowed::Missing,
                MutationOutcome::MayHaveApplied => Allowed::ValueOrMissing(after),
            },
            (Cohort::Delete, Ok(())) => Allowed::Missing,
            (Cohort::Delete, Err(error)) => match mutation_outcome(error) {
                MutationOutcome::KnownNotApplied => Allowed::Value(before),
                MutationOutcome::MayHaveApplied => Allowed::MissingOrValue(before),
            },
        };
        if let Some(stratum) = self.strata.get(&key).copied() {
            self.touched
                .lock()
                .expect("the touched-strata lock is not poisoned")
                .insert(stratum);
        }
        if result.is_ok()
            && self.publish_window.load(Ordering::Acquire)
            && let Some(stratum) = self.strata.get(&key).copied()
            && matches!(
                stratum,
                Stratum::RemovedSource | Stratum::NewTarget | Stratum::OwnerChanged
            )
        {
            self.publish_expected
                .lock()
                .expect("the publish-expected lock is not poisoned")
                .insert(key, allowed.clone());
        }
        self.entries
            .lock()
            .expect("the ledger lock is not poisoned")
            .insert(key, allowed);
    }

    fn entries(&self) -> BTreeMap<u64, Allowed> {
        self.entries
            .lock()
            .expect("the ledger lock is not poisoned")
            .clone()
    }

    fn publish_expected(&self) -> BTreeMap<u64, Allowed> {
        self.publish_expected
            .lock()
            .expect("the publish-expected lock is not poisoned")
            .clone()
    }

    fn touched_strata(&self) -> BTreeSet<Stratum> {
        self.touched
            .lock()
            .expect("the touched-strata lock is not poisoned")
            .clone()
    }
}

struct TaskHandles {
    stop: Arc<AtomicBool>,
    handles: Vec<tokio::task::JoinHandle<Result<()>>>,
}

impl TaskHandles {
    fn new(stop: Arc<AtomicBool>) -> Self {
        Self {
            stop,
            handles: Vec::new(),
        }
    }

    fn push(&mut self, handle: tokio::task::JoinHandle<Result<()>>) {
        self.handles.push(handle);
    }

    async fn stop(mut self, timeout: Duration) -> Result<()> {
        self.stop.store(true, Ordering::Release);
        for handle in std::mem::take(&mut self.handles) {
            tokio::time::timeout(timeout, handle)
                .await
                .context("a scaling load task did not stop")???;
        }
        Ok(())
    }
}

impl Drop for TaskHandles {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        for handle in &self.handles {
            handle.abort();
        }
    }
}

/// A fixed-rate sender thread. The thread measures the due time against the
/// wall clock; a Tokio timer would add about a millisecond to every request,
/// which is larger than the latencies a migration window shows.
struct OpenSender {
    stop: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<Result<()>>>,
}

impl OpenSender {
    fn join(mut self) -> Result<()> {
        self.stop.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            thread
                .join()
                .map_err(|_| anyhow!("an open-loop sender thread panicked"))??;
        }
        Ok(())
    }
}

impl Drop for OpenSender {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
    }
}

fn start_closed_reads(
    config: &ExperimentConfig,
    client: &HashringClient,
    metrics: Arc<Metrics>,
    read_pool: Arc<Vec<u64>>,
    distribution: crate::latency::KeyDistribution,
    stop: Arc<AtomicBool>,
    tasks: &mut TaskHandles,
) {
    for task in 0..config.concurrency {
        let client = client.clone();
        let metrics = Arc::clone(&metrics);
        let stop = Arc::clone(&stop);
        let read_pool = Arc::clone(&read_pool);
        let distribution = distribution.clone();
        let value_bytes = config.value_bytes;
        let seed = config.hash_seed ^ (task as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15);
        tasks.push(tokio::spawn(async move {
            let mut rng = StdRng::seed_from_u64(seed);
            while !stop.load(Ordering::Acquire) {
                let index = distribution.sample(&mut rng, read_pool.len() as u64) as usize;
                let key = read_pool[index];
                let phase = metrics.phase();
                metrics.issued.fetch_add(1, Ordering::Relaxed);
                let started = Instant::now();
                let result = client.get(key.to_be_bytes().to_vec()).await;
                let elapsed = started.elapsed();
                record_read(&metrics, key, value_bytes, phase, elapsed, result);
            }
            Ok(())
        }));
    }
}

fn record_read(
    metrics: &Metrics,
    key: u64,
    value_bytes: usize,
    phase: Phase,
    elapsed: Duration,
    result: Result<hashring_client::GetOutput, ClientError>,
) {
    match result {
        Ok(output) if output.value == deterministic_value(key, 0, value_bytes) => {
            metrics.record("get", phase, elapsed, &Ok(()));
        }
        Ok(_) => {
            metrics.record("get", phase, elapsed, &Err(ClientError::MissingVersion));
            metrics.violation(
                "read_wrong_value",
                format!("key {key} returned a value that was never written"),
            );
        }
        Err(error) if is_not_found(&error) => {
            metrics.violation("read_missing", format!("key {key} was not found"));
            metrics.record("get", phase, elapsed, &Err(error));
        }
        Err(error) => {
            metrics.record("get", phase, elapsed, &Err(error));
        }
    }
}

fn start_open_reads(
    config: &ExperimentConfig,
    client: &HashringClient,
    metrics: Arc<Metrics>,
    read_pool: Arc<Vec<u64>>,
    distribution: crate::latency::KeyDistribution,
    stop: Arc<AtomicBool>,
    rate: u64,
) -> Result<OpenSender> {
    let handle = tokio::runtime::Handle::current();
    let client = client.clone();
    let thread_stop = Arc::clone(&stop);
    let seed = config.hash_seed;
    let value_bytes = config.value_bytes;
    let thread = std::thread::Builder::new()
        .name("scaling-open-reads".to_owned())
        .spawn(move || {
            let mut rng = StdRng::seed_from_u64(seed);
            let mut index = 0_u64;
            let mut margin = Duration::from_micros(100);
            let start = Instant::now();
            while !thread_stop.load(Ordering::Acquire) {
                let due = start + Duration::from_secs_f64(index as f64 / rate as f64);
                super::latency::wait_until(due, &mut margin);
                if thread_stop.load(Ordering::Acquire) {
                    break;
                }
                if metrics.try_begin() {
                    let key =
                        read_pool[distribution.sample(&mut rng, read_pool.len() as u64) as usize];
                    let client = client.clone();
                    let metrics = Arc::clone(&metrics);
                    handle.spawn(async move {
                        let phase = metrics.phase();
                        let result = client.get(key.to_be_bytes().to_vec()).await;
                        let elapsed = Instant::now().saturating_duration_since(due);
                        record_read(&metrics, key, value_bytes, phase, elapsed, result);
                        metrics.finish();
                    });
                } else {
                    metrics.late();
                }
                index += 1;
            }
            Ok(())
        })?;
    Ok(OpenSender {
        stop,
        thread: Some(thread),
    })
}

struct OpenWriteContext<'a> {
    config: &'a ExperimentConfig,
    client: &'a HashringClient,
    plan: &'a TransitionPlan,
    ledger: &'a Arc<Ledger>,
    metrics: &'a Arc<Metrics>,
    stop: &'a Arc<AtomicBool>,
    probe_tx: mpsc::UnboundedSender<(u64, Option<Vec<u8>>)>,
}

fn start_open_writes(context: OpenWriteContext<'_>) -> Result<Vec<OpenSender>> {
    let OpenWriteContext {
        config,
        client,
        plan,
        ledger,
        metrics,
        stop,
        probe_tx,
    } = context;
    let rates = cohort_rates(config);
    let cohorts = [
        (Cohort::Rewrite, Arc::clone(&plan.rewrite), rates.rewrite),
        (
            Cohort::WriteNew,
            Arc::clone(&plan.write_new),
            rates.write_new,
        ),
        (Cohort::Delete, Arc::clone(&plan.delete), rates.delete),
    ];
    let mut senders = Vec::new();
    let sender_context = OpenSenderContext {
        client,
        ledger,
        metrics,
        stop,
        probe_tx,
        value_bytes: config.value_bytes,
    };
    for (cohort, pool, rate) in cohorts {
        ensure!(rate > 0, "an open-loop write share rounds to zero");
        senders.push(start_open_writes_for(cohort, rate, pool, &sender_context)?);
    }
    Ok(senders)
}

struct OpenSenderContext<'a> {
    client: &'a HashringClient,
    ledger: &'a Arc<Ledger>,
    metrics: &'a Arc<Metrics>,
    stop: &'a Arc<AtomicBool>,
    probe_tx: mpsc::UnboundedSender<(u64, Option<Vec<u8>>)>,
    value_bytes: usize,
}

fn start_open_writes_for(
    cohort: Cohort,
    rate: u64,
    pool: Arc<KeyPool>,
    context: &OpenSenderContext<'_>,
) -> Result<OpenSender> {
    let value_bytes = context.value_bytes;
    let probe_tx = context.probe_tx.clone();
    let handle = tokio::runtime::Handle::current();
    let client = context.client.clone();
    let ledger = Arc::clone(context.ledger);
    let metrics = Arc::clone(context.metrics);
    let stop = Arc::clone(context.stop);
    let thread_stop = Arc::clone(&stop);
    let thread = std::thread::Builder::new()
        .name(format!("scaling-open-{}", cohort.metric_name()))
        .spawn(move || {
            let mut index = 0_u64;
            let mut margin = Duration::from_micros(100);
            let start = Instant::now();
            while !thread_stop.load(Ordering::Acquire) {
                let due = start + Duration::from_secs_f64(index as f64 / rate as f64);
                super::latency::wait_until(due, &mut margin);
                if thread_stop.load(Ordering::Acquire) {
                    break;
                }
                if !metrics.begin_write() {
                    metrics.late();
                    index += 1;
                    continue;
                }
                let Some(key) = pool.next() else {
                    metrics.finish_write();
                    break;
                };
                let client = client.clone();
                let ledger = Arc::clone(&ledger);
                let metrics = Arc::clone(&metrics);
                let probe_tx = probe_tx.clone();
                handle.spawn(async move {
                    let phase = metrics.phase();
                    let result = match cohort {
                        Cohort::Rewrite | Cohort::WriteNew => client
                            .put(
                                key.to_be_bytes().to_vec(),
                                deterministic_value(key, 1, value_bytes),
                            )
                            .await
                            .map(|_| ()),
                        Cohort::Delete => {
                            client.delete(key.to_be_bytes().to_vec()).await.map(|_| ())
                        }
                    };
                    let elapsed = Instant::now().saturating_duration_since(due);
                    ledger.record(key, cohort, &result);
                    metrics.record(cohort.metric_name(), phase, elapsed, &result);
                    if result.is_ok() {
                        let expected = match cohort {
                            Cohort::Delete => None,
                            Cohort::Rewrite | Cohort::WriteNew => {
                                Some(deterministic_value(key, 1, value_bytes))
                            }
                        };
                        let _ = probe_tx.send((key, expected));
                    }
                    metrics.finish_write();
                });
                index += 1;
            }
            Ok(())
        })?;
    Ok(OpenSender {
        stop: Arc::clone(&stop),
        thread: Some(thread),
    })
}

async fn drain_in_flight(metrics: &Metrics, config: &ExperimentConfig) -> Result<()> {
    if metrics.in_flight.load(Ordering::Acquire) == 0 {
        return Ok(());
    }
    let deadline = Instant::now()
        + Duration::from_millis(config.operation_timeout_ms)
            .saturating_add(Duration::from_secs(10));
    while metrics.in_flight.load(Ordering::Acquire) > 0 {
        ensure!(
            Instant::now() < deadline,
            "{} open-loop requests were still in flight after the drain deadline",
            metrics.in_flight.load(Ordering::Acquire)
        );
        tokio::time::sleep(PHASE_POLL_INTERVAL).await;
    }
    Ok(())
}

/// Waits for one transition's writes only. The reads belong to the next
/// transition, so they are not part of this drain.
async fn drain_writes(metrics: &Metrics, config: &ExperimentConfig) -> Result<()> {
    if metrics.in_flight_writes.load(Ordering::Acquire) == 0 {
        return Ok(());
    }
    let deadline = Instant::now()
        + Duration::from_millis(config.operation_timeout_ms)
            .saturating_add(Duration::from_secs(10));
    while metrics.in_flight_writes.load(Ordering::Acquire) > 0 {
        ensure!(
            Instant::now() < deadline,
            "{} open-loop write requests were still in flight after the drain deadline",
            metrics.in_flight_writes.load(Ordering::Acquire)
        );
        tokio::time::sleep(PHASE_POLL_INTERVAL).await;
    }
    Ok(())
}

pub(crate) async fn run(context: crate::RunContext<'_>) -> Result<u64> {
    let crate::RunContext {
        config,
        members: peak_members,
        initial_count,
        ports,
        coordinator_endpoint,
        admin_client,
        workload_client,
        processes,
        events,
        measurements,
        observations,
    } = context;
    let initial_members = peak_members[..initial_count].to_vec();
    let transitions = transitions(config, peak_members, &initial_members);
    ensure!(!transitions.is_empty(), "a scaling run needs a transition");

    // The change itself may take longer than one data operation, so the admin
    // client gets the migration timeout while the workload keeps the operation
    // deadline. The observer polls with a short timeout of its own so a busy
    // coordinator cannot stall the phase timeline.
    let control_timeout =
        Duration::from_millis(config.migration_timeout_ms.max(config.operation_timeout_ms));
    let control = HashringClient::connect(coordinator_endpoint.to_owned(), control_timeout).await?;
    let observer =
        HashringClient::connect(coordinator_endpoint.to_owned(), Duration::from_secs(2)).await?;

    let metrics = Arc::new(Metrics::new(
        Duration::from_millis(config.operation_timeout_ms),
        config.scaling_load_mode == ScalingLoadMode::Open,
        config.max_in_flight,
    )?);
    let sampler = tokio::spawn({
        let metrics = Arc::clone(&metrics);
        let events = events.clone();
        async move {
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;
                if let Err(error) = metrics.sample(&events) {
                    eprintln!("scaling metrics sample failed: {error:#}");
                }
            }
        }
    });

    let pool_plan = plan_keys(config, &transitions)?;
    let read_pool = Arc::new(pool_plan.read.clone());
    let distribution =
        crate::latency::KeyDistribution::parse(&config.key_distribution, read_pool.len() as u64)?;
    let read_stop = Arc::new(AtomicBool::new(false));
    let mut read_tasks = TaskHandles::new(Arc::clone(&read_stop));
    let mut open_reads: Option<OpenSender> = None;
    if config.scaling_load_mode == ScalingLoadMode::Open {
        let rate = config.scaling_open_rate * 80 / 100;
        ensure!(rate > 0, "the open-loop read share rounds to zero");
        open_reads = Some(start_open_reads(
            config,
            workload_client,
            Arc::clone(&metrics),
            Arc::clone(&read_pool),
            distribution.clone(),
            Arc::clone(&read_stop),
            rate,
        )?);
    } else {
        start_closed_reads(
            config,
            workload_client,
            Arc::clone(&metrics),
            Arc::clone(&read_pool),
            distribution.clone(),
            Arc::clone(&read_stop),
            &mut read_tasks,
        );
    }

    let mut all_entries = BTreeMap::new();
    let mut all_fresh = BTreeSet::new();
    let mut final_epoch = admin_client.refresh_topology().await?.epoch;
    let mut re_read_handles: Vec<tokio::task::JoinHandle<Result<()>>> = Vec::new();
    let mut pending_verification: Option<(&'static str, BTreeMap<u64, Allowed>)> = None;

    for (index, transition) in transitions.iter().enumerate() {
        let control_run = is_control(config);
        let staged = if control_run {
            None
        } else {
            let change = control
                .begin_topology_change(transition.target_members.clone())
                .await
                .with_context(|| format!("staging the {} change", transition.label))?;
            ensure!(
                change.base_topology.is_some(),
                "the staged {} change has no base topology",
                transition.label
            );
            Some(change)
        };
        let (base_topology, target_topology) = match &staged {
            Some(change) => (
                change
                    .base_topology
                    .clone()
                    .expect("the staged change has a base topology"),
                change.target_topology.clone(),
            ),
            None => {
                let current = admin_client.refresh_topology().await?;
                (current.clone(), current)
            }
        };
        let plan = build_transition_plan(
            config,
            transition,
            &base_topology,
            &target_topology,
            &pool_plan,
            index,
            transitions.len(),
        )?;
        let migration = migration_stats(&base_topology, &target_topology, config.key_count)?;
        let ledger = Arc::new(Ledger::new(Arc::clone(&plan.strata), config.value_bytes));

        // The new members are in the pending topology already, so they can be
        // started now. The removed members are told to stop by the coordinator
        // once their ranges have been handed off.
        let base_members = base_topology.members.clone();
        if !control_run {
            for member in &transition.target_members {
                if !base_members
                    .iter()
                    .any(|base| base.node_id == member.node_id)
                {
                    let peak_index = peak_members
                        .iter()
                        .position(|peak| peak.node_id == member.node_id)
                        .with_context(|| format!("{} is not a peak member", member.node_id))?;
                    let port = ports[peak_index + 1];
                    spawn_node(
                        processes,
                        member.node_id.clone(),
                        port,
                        coordinator_endpoint,
                    )?;
                    wait_for_listener(port, processes).await?;
                }
            }
        }

        events.record(
            "scaling_transition_started",
            json!({
                "label": transition.label,
                "change_id": staged.as_ref().map(|change| change.change_id.clone()).unwrap_or_default(),
                "base_epoch": base_topology.epoch,
                "target_epoch": target_topology.epoch,
                "direct_merge": staged.as_ref().is_some_and(|change| change.direct_merge),
                "control": control_run,
                "rewrite_keys": plan.rewrite.len(),
                "new_keys": plan.write_new.len(),
                "delete_keys": plan.delete.len(),
                "migration_rate": migration.rate(),
                "keys_moved": migration.keys_moved,
                "ranges": staged.as_ref().map_or(0, |change| change.ranges.len()),
                "expected_strata": plan.expected_strata.iter().map(|s| s.name()).collect::<Vec<_>>(),
                "present_strata": plan.present_strata.iter().map(|s| s.name()).collect::<Vec<_>>(),
                "strata_counts": stratum_counts(&plan.strata),
            }),
        )?;

        let write_stop = Arc::new(AtomicBool::new(false));
        let mut write_tasks = TaskHandles::new(Arc::clone(&write_stop));
        let (probe_tx, mut probe_rx) = mpsc::unbounded_channel::<(u64, Option<Vec<u8>>)>();
        let probe_metrics = Arc::clone(&metrics);
        let probe_client = workload_client.clone();
        let probe_handle = tokio::spawn(async move {
            while let Some((key, expected)) = probe_rx.recv().await {
                let phase = probe_metrics.phase();
                let started = Instant::now();
                let result = probe_client.get(key.to_be_bytes().to_vec()).await;
                let elapsed = started.elapsed();
                match result {
                    Ok(output) if expected.as_deref() == Some(output.value.as_slice()) => {
                        probe_metrics.record("probe_read_after_write", phase, elapsed, &Ok(()));
                    }
                    Err(error) if expected.is_none() && is_not_found(&error) => {
                        probe_metrics.record("probe_read_after_write", phase, elapsed, &Ok(()));
                    }
                    Ok(_) if expected.is_none() => {
                        probe_metrics.violation(
                            "probe_deleted_key_present",
                            format!("key {key} was deleted but read back"),
                        );
                    }
                    Ok(_) => {
                        probe_metrics.record(
                            "probe_read_after_write",
                            phase,
                            elapsed,
                            &Err(ClientError::MissingVersion),
                        );
                        probe_metrics.violation(
                            "probe_wrong_value",
                            format!("key {key} did not read back the value that was written"),
                        );
                    }
                    Err(error) if expected.is_some() && is_not_found(&error) => {
                        probe_metrics.violation(
                            "probe_missing",
                            format!("key {key} was written but not found"),
                        );
                        probe_metrics.record("probe_read_after_write", phase, elapsed, &Err(error));
                    }
                    Err(error) => {
                        probe_metrics.record("probe_read_after_write", phase, elapsed, &Err(error));
                    }
                }
            }
            Ok::<_, anyhow::Error>(())
        });

        let mut open_writes: Vec<OpenSender> = Vec::new();
        if config.scaling_load_mode == ScalingLoadMode::Open {
            open_writes = start_open_writes(OpenWriteContext {
                config,
                client: workload_client,
                plan: &plan,
                ledger: &ledger,
                metrics: &metrics,
                stop: &write_stop,
                probe_tx: probe_tx.clone(),
            })?;
        } else {
            start_writes(WriteContext {
                config,
                client: workload_client,
                plan: &plan,
                ledger: &ledger,
                metrics: &metrics,
                stop: &write_stop,
                handles: &mut write_tasks,
                probe_tx: probe_tx.clone(),
            });
        }

        // The previous transition's cohorts are verified while this
        // transition's write batches are already running, so the load never
        // goes through a write-only pause between the two changes.
        if let Some((label, entries)) = pending_verification.take() {
            let violations = verify_entries(workload_client, &entries, config.concurrency).await?;
            ensure!(
                violations == 0,
                "{label} cohort verification found {violations} violations"
            );
            events.record(
                "scaling_cohorts_verified",
                json!({ "label": label, "keys": entries.len(), "violations": violations }),
            )?;
        }

        if index == 0 && config.warmup_seconds > 0 {
            events.record(
                "scaling_warmup_started",
                json!({ "seconds": config.warmup_seconds }),
            )?;
            tokio::time::sleep(Duration::from_secs(config.warmup_seconds)).await;
        }

        let retries_before = workload_client.retry_reasons();
        let operations_before = metrics.total_operations();
        let started = Instant::now();
        metrics.set_phase(Phase::Pre);
        let mut stop_window: Option<StopWindow> = None;
        let mut published_ms: Option<f64> = None;
        let mut complete_ms: Option<f64> = None;
        // RF repair starts at completion, so the convergence clock starts
        // there too. The removed-node waits below must not be hidden from it.
        let mut rf_started: Option<Instant> = None;
        let mut after_publish_probe: Option<tokio::task::JoinHandle<Result<()>>> = None;
        let completed = if let Some(staged) = &staged {
            ledger.begin_publish_window();
            let mut last_phase = Phase::Pre;
            let execution_client = control.clone();
            let change_id = staged.change_id.clone();
            let base_epoch = staged.base_epoch;
            let target_epoch = staged.target_topology.epoch;
            let execution = tokio::spawn(async move {
                execution_client
                    .execute_topology_change(change_id, base_epoch, target_epoch)
                    .await
            });
            let mut window = StopWindow::starting(
                workload_client.retry_reason_count(RetryReason::RangeBusy),
                metrics.writes_ok(),
            );
            loop {
                if execution.is_finished() {
                    break;
                }
                if let Ok(Some(current)) = observer.topology_change().await {
                    let phase = phase_of(current.phase);
                    if phase != last_phase {
                        if phase == Phase::Published {
                            published_ms = Some(started.elapsed().as_secs_f64() * 1_000.0);
                            // Re-read the keys written before the switch right
                            // away. A stale read after publication heals
                            // quickly, so a probe that starts after the whole
                            // transition would never see it.
                            if after_publish_probe.is_none() {
                                after_publish_probe =
                                    Some(tokio::spawn(spawn_after_publish_probe(
                                        workload_client.clone(),
                                        Arc::clone(&metrics),
                                        ledger.publish_expected(),
                                        write_operations_per_second(config),
                                    )));
                            }
                        }
                        metrics.set_phase(phase);
                        events.record(
                            "migration_phase_changed",
                            json!({
                                "label": transition.label,
                                "phase": format!("{:?}", current.phase),
                                "epoch": current.target_topology.epoch,
                            }),
                        )?;
                        last_phase = phase;
                    }
                }
                tokio::time::sleep(PHASE_POLL_INTERVAL).await;
                window.observe(
                    started.elapsed().as_secs_f64() * 1_000.0,
                    workload_client.retry_reason_count(RetryReason::RangeBusy),
                    metrics.writes_ok(),
                );
            }
            let completed = execution
                .await
                .context("joining the topology change execution")??;
            ensure!(
                completed.phase == MigrationPhase::Complete,
                "the {} change ended in {:?}",
                transition.label,
                completed.phase
            );
            complete_ms = Some(started.elapsed().as_secs_f64() * 1_000.0);
            rf_started = Some(Instant::now());
            // The first write after the last rejection can complete after
            // Complete, so keep sampling until the window closes.
            let window_deadline = Instant::now() + Duration::from_secs(2);
            while !window.closed() && Instant::now() < window_deadline {
                tokio::time::sleep(PHASE_POLL_INTERVAL).await;
                window.observe(
                    started.elapsed().as_secs_f64() * 1_000.0,
                    workload_client.retry_reason_count(RetryReason::RangeBusy),
                    metrics.writes_ok(),
                );
            }
            stop_window = Some(window);
            // The removed nodes are told to stop once their ranges are handed
            // off. Wait for them before the process liveness checks, or a
            // normal removal looks like an unexpected exit.
            for member in &base_members {
                if !transition
                    .target_members
                    .iter()
                    .any(|target| target.node_id == member.node_id)
                {
                    let status = processes
                        .wait_for_successful_exit(&member.node_id, Duration::from_secs(10))
                        .await?;
                    events.record(
                        "removed_node_exited",
                        json!({ "node_id": member.node_id, "status": status }),
                    )?;
                }
            }
            ledger.end_publish_window();
            Some(completed)
        } else {
            // S0 keeps the topology unchanged. The measured baseline is the
            // post-change window with the same load.
            events.record(
                "scaling_control_window_started",
                json!({ "seconds": config.scaling_post_seconds }),
            )?;
            tokio::time::sleep(Duration::from_secs(config.scaling_post_seconds)).await;
            None
        };
        let change_seconds = started.elapsed();
        metrics.set_phase(Phase::Post);
        let retries_after = workload_client.retry_reasons();
        let retry_delta: BTreeMap<_, _> = retries_after
            .iter()
            .map(|(reason, count)| {
                (
                    (*reason).to_owned(),
                    count.saturating_sub(*retries_before.get(*reason).unwrap_or(&0)),
                )
            })
            .collect();
        if completed.is_some() {
            let range_busy = retry_delta.get("range_busy").copied().unwrap_or(0);
            ensure!(
                range_busy > 0,
                "the {} change produced no client-observed RangeBusy write rejection, so the run has no evidence that writes were stopped; it is invalid",
                transition.label
            );
        }

        let target_epoch = target_topology.epoch;
        let mut full_rf_seconds: Option<f64> = None;
        if completed.is_some() {
            let rf_started = rf_started.expect("a completed change records its completion time");
            wait_for_full_rf_at_epoch(
                &observer,
                config.desired_replication_factor,
                target_epoch,
                Duration::from_millis(config.migration_timeout_ms),
                processes,
            )
            .await?;
            full_rf_seconds = Some(rf_started.elapsed().as_secs_f64());

            // Keep the load running while the post-change window is measured.
            if config.scaling_post_seconds > 0 {
                tokio::time::sleep(Duration::from_secs(config.scaling_post_seconds)).await;
            }
        } else if config.scaling_post_seconds == 0 {
            ensure!(false, "a control run needs a positive post window");
        }

        // Open-loop senders only stop here; their requests are drained by the
        // in-flight accounting rather than by a task join.
        for sender in open_writes.drain(..) {
            sender.join()?;
        }

        write_tasks
            .stop(
                Duration::from_millis(config.operation_timeout_ms)
                    .saturating_add(Duration::from_secs(10)),
            )
            .await?;
        // The open senders hold a probe sender of their own, so the probe only
        // sees the channel close after the writes are drained. Draining first
        // keeps an in-flight write from racing the probe's timeout and
        // failing the run with the wrong error.
        drain_writes(&metrics, config).await?;
        drop(probe_tx);
        tokio::time::timeout(Duration::from_secs(30), probe_handle)
            .await
            .context("the read-after-write probe did not stop")???;

        ensure!(
            !plan.rewrite.exhausted() && !plan.write_new.exhausted() && !plan.delete.exhausted(),
            "a {} cohort was exhausted before the change completed",
            transition.label
        );
        let touched = ledger.touched_strata();
        let missing: Vec<_> = plan
            .expected_strata
            .difference(&touched)
            .map(|stratum| stratum.name())
            .collect();
        ensure!(
            missing.is_empty(),
            "the {} change had no operations in strata: {}",
            transition.label,
            missing.join(", ")
        );

        let entries = ledger.entries();
        let last_transition = index + 1 == transitions.len();
        let verification_status = if last_transition {
            let violations = verify_entries(workload_client, &entries, config.concurrency).await?;
            ensure!(
                violations == 0,
                "{} cohort verification found {violations} violations",
                transition.label
            );
            events.record(
                "scaling_cohorts_verified",
                json!({ "label": transition.label, "keys": entries.len(), "violations": violations }),
            )?;
            "passed"
        } else {
            pending_verification = Some((transition.label, entries.clone()));
            "deferred_to_next_transition"
        };

        // The probe started when the publication was observed; keep its
        // handle with the other probes.
        if let Some(probe) = after_publish_probe.take() {
            re_read_handles.push(probe);
        }

        let elapsed = started.elapsed();
        let operations = metrics.total_operations().saturating_sub(operations_before);
        measurements.insert(
            format!("{}_transition", transition.label),
            measurement(operations, elapsed),
        );
        observations.insert(
            format!("{}_transition", transition.label),
            json!({
                "change_seconds": change_seconds.as_secs_f64(),
                "published_ms": published_ms,
                "complete_ms": complete_ms,
                "full_rf_seconds": full_rf_seconds,
                "post_seconds": config.scaling_post_seconds,
                "control": completed.is_none(),
                "direct_merge": completed.as_ref().is_some_and(|change| change.direct_merge),
                "base_epoch": base_topology.epoch,
                "target_epoch": target_epoch,
                "work_steps_ms": completed.as_ref().map(|change| json!(change.timings)),
                "stop_window": stop_window.as_ref().map(StopWindow::summary),
                "migration": {
                    "keys": migration.keys,
                    "keys_moved": migration.keys_moved,
                    "rate": migration.rate(),
                    "ranges": completed.as_ref().map_or(0, |change| change.ranges.len()),
                    "snapshot_records": completed
                        .as_ref()
                        .map_or(0, |change| change.ranges.iter().map(|range| range.snapshot_records).sum::<u64>()),
                },
                "retries_by_reason": retry_delta,
                "rewrite_pool": plan.rewrite.len(),
                "new_pool": plan.write_new.len(),
                "delete_pool": plan.delete.len(),
                "touched_strata": touched.iter().map(|s| s.name()).collect::<Vec<_>>(),
                "verification": verification_status,
            }),
        );
        events.record(
            "scaling_transition_complete",
            json!({
                "label": transition.label,
                "seconds": change_seconds.as_secs_f64(),
                "published_ms": published_ms,
                "complete_ms": complete_ms,
                "full_rf_seconds": full_rf_seconds,
                "migration_rate": migration.rate(),
                "keys_moved": migration.keys_moved,
                "target_epoch": target_epoch,
                "retries_by_reason": retry_delta,
                "work_steps_ms": completed.as_ref().map(|change| json!(change.timings)),
            }),
        )?;

        for (key, allowed) in entries {
            all_entries.insert(key, allowed);
        }
        all_fresh.extend(plan.write_new.keys.iter().copied());
        final_epoch = target_epoch;
    }

    ensure!(
        pending_verification.is_none(),
        "the last transition's cohorts were not verified"
    );
    for handle in re_read_handles {
        tokio::time::timeout(Duration::from_secs(120), handle)
            .await
            .context("the read-after-publish probe did not stop")???;
    }
    if let Some(sender) = open_reads.take() {
        sender.join()?;
    }
    drain_in_flight(&metrics, config).await?;
    read_tasks
        .stop(
            Duration::from_millis(config.operation_timeout_ms)
                .saturating_add(Duration::from_secs(10)),
        )
        .await?;
    sampler.abort();
    let _ = sampler.await;
    let _ = metrics.sample(events);

    // The whole preloaded key space must still be what it was, with the
    // touched cohorts overlaid. Fresh keys are checked as well.
    let mut expected: BTreeMap<u64, Allowed> = (0..config.key_count)
        .map(|key| {
            (
                key,
                Allowed::Value(deterministic_value(key, 0, config.value_bytes)),
            )
        })
        .collect();
    for (key, allowed) in all_entries {
        expected.insert(key, allowed);
    }
    for key in all_fresh {
        expected.entry(key).or_insert_with(|| Allowed::Missing);
    }
    let violations = verify_entries(workload_client, &expected, config.concurrency).await?;
    ensure!(
        violations == 0,
        "final verification found {violations} violations"
    );
    let observed = metrics.violation_count();
    ensure!(
        observed == 0,
        "the load observed {observed} read or probe violations"
    );

    observations.insert("scaling_metrics".into(), metrics.summary());
    observations.insert(
        "scaling_pool_plan".into(),
        json!({
            "read_keys": read_pool.len(),
            "read_distribution": distribution.name(),
            "load_mode": config.scaling_load_mode.name(),
            "open_rate": config.scaling_open_rate,
            "write_rate": config.scaling_write_rate,
            "pool_seconds": config.scaling_pool_seconds,
            "transitions": transitions.iter().map(|t| t.label).collect::<Vec<_>>(),
        }),
    );
    events.record(
        "scaling_complete",
        json!({ "epoch": final_epoch, "metrics": metrics.summary() }),
    )?;
    super::record_topology(events, admin_client).await?;
    Ok(final_epoch)
}

fn transitions(
    config: &ExperimentConfig,
    peak_members: &[Member],
    initial_members: &[Member],
) -> Vec<Transition> {
    match config.scaling_scenario {
        ScalingScenario::Grow => vec![Transition {
            label: "grow",
            target_members: peak_members[..config.node_count].to_vec(),
        }],
        ScalingScenario::Shrink => vec![Transition {
            label: "shrink",
            target_members: initial_members[..initial_members.len().saturating_sub(1)].to_vec(),
        }],
        ScalingScenario::RoundTrip | ScalingScenario::Churn => vec![
            Transition {
                label: "grow",
                target_members: peak_members[..config.node_count].to_vec(),
            },
            Transition {
                label: "shrink",
                target_members: initial_members.to_vec(),
            },
        ],
        ScalingScenario::Replace => {
            let mut target_members = initial_members.to_vec();
            target_members.pop();
            target_members.push(
                peak_members
                    .last()
                    .expect("a replace run has a spare peak member")
                    .clone(),
            );
            vec![Transition {
                label: "replace",
                target_members,
            }]
        }
        ScalingScenario::Control => vec![Transition {
            label: "control",
            target_members: initial_members.to_vec(),
        }],
    }
}

fn is_control(config: &ExperimentConfig) -> bool {
    config.scaling_scenario == ScalingScenario::Control
}

/// The write operations per second each cohort is offered.
///
/// The pools and the senders must share one definition. Deriving an open-loop
/// total from 20% of the offered rate and then splitting it again rounded a
/// small delete share down to zero in the pool while the sender still offered
/// deletes.
#[derive(Clone, Copy, Debug)]
struct CohortRates {
    rewrite: u64,
    write_new: u64,
    delete: u64,
}

/// The per-cohort write rates of the run's load mode.
fn cohort_rates(config: &ExperimentConfig) -> CohortRates {
    let (total, rewrite, write_new, delete) = match config.scaling_load_mode {
        ScalingLoadMode::Closed => (config.scaling_write_rate, 60, 25, 15),
        ScalingLoadMode::Open => (config.scaling_open_rate, 12, 5, 3),
    };
    CohortRates {
        rewrite: total * rewrite / 100,
        write_new: total * write_new / 100,
        delete: total * delete / 100,
    }
}

/// The write operations per second of every cohort together.
fn write_operations_per_second(config: &ExperimentConfig) -> u64 {
    let rates = cohort_rates(config);
    rates.rewrite + rates.write_new + rates.delete
}

struct Transition {
    label: &'static str,
    target_members: Vec<Member>,
}

struct BuildPlan {
    read: Vec<u64>,
}

/// Splits the preloaded key space into an immutable read pool and one chunk
/// per transition. The chunks are disjoint, so a key touched in one transition
/// is never a `v0` input to the next one.
fn plan_keys(config: &ExperimentConfig, transitions: &[Transition]) -> Result<BuildPlan> {
    let read_share = if transitions.len() == 1 {
        60_u64
    } else {
        50_u64
    };
    let read_count = config.key_count * read_share / 100;
    ensure!(read_count > 0, "the read pool is empty");
    let writable = config.key_count.saturating_sub(read_count);
    let per_transition = writable / transitions.len() as u64;
    ensure!(
        per_transition >= 512,
        "the key space leaves only {per_transition} keys per transition; raise --keys or lower --scaling-write-rate"
    );
    let read = (0..read_count).collect();
    Ok(BuildPlan { read })
}

fn build_transition_plan(
    config: &ExperimentConfig,
    transition: &Transition,
    base: &TopologySnapshot,
    target: &TopologySnapshot,
    pool_plan: &BuildPlan,
    index: usize,
    transitions: usize,
) -> Result<TransitionPlan> {
    let base_ids: BTreeSet<&str> = base
        .members
        .iter()
        .map(|member| member.node_id.as_str())
        .collect();
    let target_ids: BTreeSet<&str> = target
        .members
        .iter()
        .map(|member| member.node_id.as_str())
        .collect();
    let removed: BTreeSet<String> = base_ids
        .difference(&target_ids)
        .map(|id| (*id).to_owned())
        .collect();
    let added: BTreeSet<String> = target_ids
        .difference(&base_ids)
        .map(|id| (*id).to_owned())
        .collect();

    let writable = config.key_count - pool_plan.read.len() as u64;
    let per_transition = writable / transitions as u64;
    let chunk_start = pool_plan.read.len() as u64 + index as u64 * per_transition;
    let chunk_end = if index + 1 == transitions {
        config.key_count
    } else {
        chunk_start + per_transition
    };

    let write_rate = cohort_rates(config);
    let pool_seconds = config.scaling_pool_seconds;
    let rewrite_quota = (write_rate.rewrite * pool_seconds) as usize;
    let new_quota = (write_rate.write_new * pool_seconds) as usize;
    let delete_quota = (write_rate.delete * pool_seconds) as usize;
    ensure!(
        rewrite_quota > 0 && new_quota > 0 && delete_quota > 0,
        "the scaling write rate is too low to give every cohort a key; raise it or raise --scaling-pool-seconds"
    );
    ensure!(
        rewrite_quota + delete_quota <= (chunk_end - chunk_start) as usize,
        "the {} transition needs {} rewrite/delete keys but its chunk has {}; raise --keys or lower --scaling-write-rate/--scaling-pool-seconds",
        transition.label,
        rewrite_quota + delete_quota,
        chunk_end - chunk_start
    );

    let mut by_stratum = classify_range(base, target, chunk_start..chunk_end, &removed, &added);
    let present_strata: BTreeSet<Stratum> = by_stratum.keys().copied().collect();
    let rewrite_keys = take_stratified(&mut by_stratum, rewrite_quota);
    let delete_keys = take_stratified(&mut by_stratum, delete_quota);
    ensure!(
        !rewrite_keys.is_empty() && !delete_keys.is_empty(),
        "the {} transition has no rewrite or delete keys",
        transition.label
    );

    let fresh_start = config.key_count + index as u64 * new_quota as u64;
    let fresh_end = fresh_start + new_quota as u64;
    let mut fresh_by_stratum =
        classify_range(base, target, fresh_start..fresh_end, &removed, &added);
    let fresh_present: BTreeSet<Stratum> = fresh_by_stratum.keys().copied().collect();
    let write_new_keys = take_stratified(&mut fresh_by_stratum, new_quota);
    ensure!(
        !write_new_keys.is_empty(),
        "the {} transition has no fresh keys",
        transition.label
    );

    let mut present_strata = present_strata;
    present_strata.extend(fresh_present);
    let applicable = applicable_strata(config.scaling_scenario, transition.label);
    let expected_strata: BTreeSet<_> = present_strata.intersection(&applicable).copied().collect();

    let mut strata = BTreeMap::new();
    for key in &rewrite_keys {
        if let Some(stratum) = classify_key(base, target, *key, &removed, &added) {
            strata.insert(*key, stratum);
        }
    }
    for key in &delete_keys {
        if let Some(stratum) = classify_key(base, target, *key, &removed, &added) {
            strata.insert(*key, stratum);
        }
    }
    for key in &write_new_keys {
        if let Some(stratum) = classify_key(base, target, *key, &removed, &added) {
            strata.insert(*key, stratum);
        }
    }

    for stratum in &expected_strata {
        let covered = rewrite_keys
            .iter()
            .chain(&delete_keys)
            .chain(&write_new_keys)
            .any(|key| strata.get(key) == Some(stratum));
        ensure!(
            covered,
            "the {} transition has no write or delete key in the {} stratum",
            transition.label,
            stratum.name()
        );
    }

    Ok(TransitionPlan {
        rewrite: Arc::new(KeyPool::new(rewrite_keys)),
        write_new: Arc::new(KeyPool::new(write_new_keys)),
        delete: Arc::new(KeyPool::new(delete_keys)),
        strata: Arc::new(strata),
        expected_strata,
        present_strata,
    })
}

fn classify_range(
    base: &TopologySnapshot,
    target: &TopologySnapshot,
    range: std::ops::Range<u64>,
    removed: &BTreeSet<String>,
    added: &BTreeSet<String>,
) -> BTreeMap<Stratum, Vec<u64>> {
    let mut by_stratum: BTreeMap<Stratum, Vec<u64>> = BTreeMap::new();
    for key in range {
        if let Some(stratum) = classify_key(base, target, key, removed, added) {
            by_stratum.entry(stratum).or_default().push(key);
        }
    }
    by_stratum
}

fn classify_key(
    base: &TopologySnapshot,
    target: &TopologySnapshot,
    key: u64,
    removed: &BTreeSet<String>,
    added: &BTreeSet<String>,
) -> Option<Stratum> {
    let encoded = key.to_be_bytes();
    let token = base.key_token(&encoded);
    let base_owner = base.owner_for_token(token).ok()?.node_id.as_str();
    let target_owner = target.owner_for_token(token).ok()?.node_id.as_str();
    if base_owner != target_owner {
        if removed.contains(base_owner) {
            Some(Stratum::RemovedSource)
        } else if added.contains(target_owner) {
            Some(Stratum::NewTarget)
        } else {
            Some(Stratum::OwnerChanged)
        }
    } else {
        let base_replicas = base.replica_node_ids_for_token(token).ok()?;
        let target_replicas = target.replica_node_ids_for_token(token).ok()?;
        if base_replicas != target_replicas {
            Some(Stratum::ReplicasChanged)
        } else {
            Some(Stratum::Unchanged)
        }
    }
}

fn take_stratified(by_stratum: &mut BTreeMap<Stratum, Vec<u64>>, quota: usize) -> Vec<u64> {
    let mut selected = Vec::with_capacity(quota);
    while selected.len() < quota {
        let mut progressed = false;
        for keys in by_stratum.values_mut() {
            if selected.len() == quota {
                break;
            }
            if let Some(key) = keys.pop() {
                selected.push(key);
                progressed = true;
            }
        }
        if !progressed {
            break;
        }
    }
    selected
}

fn stratum_counts(strata: &BTreeMap<u64, Stratum>) -> BTreeMap<&'static str, u64> {
    let mut counts = BTreeMap::new();
    for stratum in strata.values() {
        *counts.entry(stratum.name()).or_insert(0) += 1;
    }
    counts
}

struct WriteContext<'a> {
    config: &'a ExperimentConfig,
    client: &'a HashringClient,
    plan: &'a TransitionPlan,
    ledger: &'a Arc<Ledger>,
    metrics: &'a Arc<Metrics>,
    stop: &'a Arc<AtomicBool>,
    handles: &'a mut TaskHandles,
    probe_tx: mpsc::UnboundedSender<(u64, Option<Vec<u8>>)>,
}

fn start_writes(context: WriteContext<'_>) {
    let WriteContext {
        config,
        client,
        plan,
        ledger,
        metrics,
        stop,
        handles,
        probe_tx,
    } = context;
    let rate = cohort_rates(config);
    let cohorts = [
        (Cohort::Rewrite, Arc::clone(&plan.rewrite), rate.rewrite),
        (
            Cohort::WriteNew,
            Arc::clone(&plan.write_new),
            rate.write_new,
        ),
        (Cohort::Delete, Arc::clone(&plan.delete), rate.delete),
    ];
    for (cohort, pool, cohort_rate) in cohorts {
        if cohort_rate == 0 {
            continue;
        }
        let tasks = (cohort_rate / 50).clamp(1, config.concurrency as u64) as usize;
        let interval = Duration::from_secs_f64(tasks as f64 / cohort_rate as f64);
        for _ in 0..tasks {
            let client = client.clone();
            let pool = Arc::clone(&pool);
            let ledger = Arc::clone(ledger);
            let metrics = Arc::clone(metrics);
            let stop = Arc::clone(stop);
            let probe_tx = probe_tx.clone();
            let value_bytes = config.value_bytes;
            handles.push(tokio::spawn(async move {
                while !stop.load(Ordering::Acquire) {
                    let Some(key) = pool.next() else {
                        break;
                    };
                    if !interval.is_zero() {
                        tokio::time::sleep(interval).await;
                    }
                    let phase = metrics.phase();
                    metrics.issued.fetch_add(1, Ordering::Relaxed);
                    let started = Instant::now();
                    let result = match cohort {
                        Cohort::Rewrite | Cohort::WriteNew => client
                            .put(
                                key.to_be_bytes().to_vec(),
                                deterministic_value(key, 1, value_bytes),
                            )
                            .await
                            .map(|_| ()),
                        Cohort::Delete => {
                            client.delete(key.to_be_bytes().to_vec()).await.map(|_| ())
                        }
                    };
                    let elapsed = started.elapsed();
                    ledger.record(key, cohort, &result);
                    metrics.record(cohort.metric_name(), phase, elapsed, &result);
                    if result.is_ok() {
                        let expected = match cohort {
                            Cohort::Delete => None,
                            Cohort::Rewrite | Cohort::WriteNew => {
                                Some(deterministic_value(key, 1, value_bytes))
                            }
                        };
                        let _ = probe_tx.send((key, expected));
                    }
                }
                Ok(())
            }));
        }
    }
}

async fn spawn_after_publish_probe(
    client: HashringClient,
    metrics: Arc<Metrics>,
    expected: BTreeMap<u64, Allowed>,
    write_rate: u64,
) -> Result<()> {
    let rate = (write_rate / 2).clamp(20, 500);
    let interval = Duration::from_secs_f64(1.0 / rate as f64);
    for (key, allowed) in expected {
        tokio::time::sleep(interval).await;
        let phase = metrics.phase();
        let started = Instant::now();
        let result = client.get(key.to_be_bytes().to_vec()).await;
        let elapsed = started.elapsed();
        match result {
            Ok(output) if allowed.check(Some(&output.value)) => {
                metrics.record("probe_read_after_publish", phase, elapsed, &Ok(()));
            }
            Err(error) if is_not_found(&error) && allowed.check(None) => {
                metrics.record("probe_read_after_publish", phase, elapsed, &Ok(()));
            }
            Ok(_) => {
                metrics.record(
                    "probe_read_after_publish",
                    phase,
                    elapsed,
                    &Err(ClientError::MissingVersion),
                );
                metrics.violation(
                    "probe_after_publish_wrong_value",
                    format!(
                        "key {key} expected {} after publication",
                        allowed.describe()
                    ),
                );
            }
            Err(error) if is_not_found(&error) => {
                metrics.violation(
                    "probe_after_publish_missing",
                    format!(
                        "key {key} expected {} after publication",
                        allowed.describe()
                    ),
                );
                metrics.record("probe_read_after_publish", phase, elapsed, &Err(error));
            }
            Err(error) => {
                metrics.record("probe_read_after_publish", phase, elapsed, &Err(error));
            }
        }
    }
    Ok(())
}

async fn verify_entries(
    client: &HashringClient,
    entries: &BTreeMap<u64, Allowed>,
    concurrency: usize,
) -> Result<u64> {
    let violations = Arc::new(AtomicU64::new(0));
    let examples = Arc::new(Mutex::new(Vec::new()));
    let client = client.clone();
    stream::iter(entries.iter())
        .for_each_concurrent(Some(concurrency), |(key, allowed)| {
            let client = client.clone();
            let violations = Arc::clone(&violations);
            let examples = Arc::clone(&examples);
            let key = *key;
            let allowed = allowed.clone();
            async move {
                let result = client.get(key.to_be_bytes().to_vec()).await;
                let (ok, detail) = match &result {
                    Ok(output) => (
                        allowed.check(Some(&output.value)),
                        format!("key {key} expected {}", allowed.describe()),
                    ),
                    Err(ClientError::Operation(failure))
                        if failure.code == ErrorCode::NotFound && allowed.check(None) =>
                    {
                        (true, String::new())
                    }
                    Err(error) => (
                        false,
                        format!("key {key} expected {}, got {error}", allowed.describe()),
                    ),
                };
                if !ok {
                    violations.fetch_add(1, Ordering::Relaxed);
                    let mut examples = examples.lock().expect("the example lock is not poisoned");
                    if examples.len() < 32 {
                        examples.push(detail);
                    }
                }
            }
        })
        .await;
    let examples = examples
        .lock()
        .expect("the example lock is not poisoned")
        .clone();
    ensure!(
        examples.is_empty(),
        "verification examples: {}",
        examples.join("; ")
    );
    Ok(violations.load(Ordering::Acquire))
}

async fn wait_for_full_rf_at_epoch(
    client: &HashringClient,
    desired_rf: u32,
    target_epoch: u64,
    timeout: Duration,
    processes: &mut ProcessGroup,
) -> Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        processes.ensure_running()?;
        if let Ok(Ok(topology)) =
            tokio::time::timeout(Duration::from_secs(3), client.refresh_topology()).await
            && topology.epoch >= target_epoch
            && let Ok(Ok(status)) =
                tokio::time::timeout(Duration::from_secs(3), client.replica_status()).await
            && !status.ranges.is_empty()
            && status.ranges.iter().all(|range| {
                range.live_rf == desired_rf
                    && !range.repairing
                    && range.followers.iter().all(|follower| follower.healthy)
            })
        {
            return Ok(());
        }
        ensure!(
            Instant::now() < deadline,
            "full RF at epoch {target_epoch} did not converge within {timeout:?}"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allowed_states_cover_the_unknown_outcomes() {
        let v0 = vec![0_u8];
        let v1 = vec![1_u8];
        let missing = Allowed::Missing;
        let present = Allowed::Value(v1.clone());
        let value_or_missing = Allowed::ValueOrMissing(v1.clone());
        let rewrite = Allowed::Rewrite {
            before: v0.clone(),
            after: v1.clone(),
        };
        let missing_or_value = Allowed::MissingOrValue(v0.clone());

        assert!(present.check(Some(&v1)));
        assert!(!present.check(None));
        assert!(value_or_missing.check(None));
        assert!(value_or_missing.check(Some(&v1)));
        assert!(rewrite.check(Some(&v0)));
        assert!(rewrite.check(Some(&v1)));
        assert!(!rewrite.check(None));
        assert!(missing.check(None));
        assert!(!missing.check(Some(&v0)));
        assert!(missing_or_value.check(None));
        assert!(missing_or_value.check(Some(&v0)));
        assert!(!missing_or_value.check(Some(&v1)));
    }

    #[test]
    fn stratified_selection_takes_from_every_stratum_first() {
        let mut by_stratum = BTreeMap::from([
            (Stratum::RemovedSource, vec![1_u64, 2, 3, 4, 5, 6]),
            (Stratum::OwnerChanged, vec![11_u64, 12]),
            (Stratum::Unchanged, vec![21_u64]),
        ]);
        let selected = take_stratified(&mut by_stratum, 3);
        assert_eq!(selected.len(), 3);
        assert_eq!(by_stratum[&Stratum::RemovedSource].len(), 5);
        assert_eq!(by_stratum[&Stratum::OwnerChanged].len(), 1);
        assert_eq!(by_stratum[&Stratum::Unchanged].len(), 0);
    }

    #[test]
    fn every_scenario_has_the_strata_its_topology_can_show() {
        let grow = applicable_strata(ScalingScenario::Grow, "grow");
        assert!(grow.contains(&Stratum::NewTarget));
        assert!(!grow.contains(&Stratum::RemovedSource));

        let shrink = applicable_strata(ScalingScenario::Shrink, "shrink");
        assert!(shrink.contains(&Stratum::RemovedSource));
        assert!(!shrink.contains(&Stratum::NewTarget));

        let round_trip = applicable_strata(ScalingScenario::RoundTrip, "grow");
        assert!(round_trip.contains(&Stratum::NewTarget));
        assert!(!round_trip.contains(&Stratum::RemovedSource));
        let round_trip = applicable_strata(ScalingScenario::RoundTrip, "shrink");
        assert!(round_trip.contains(&Stratum::RemovedSource));
        assert!(!round_trip.contains(&Stratum::NewTarget));
    }
}
