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
use std::future::Future;
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
    Complete,
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
            8 => Self::Complete,
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
            Self::Complete => 8,
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
            Self::Complete => "complete",
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
        // `Complete` is a distinct observation: publication happened some
        // polls earlier, so folding it into `Published` would record the
        // completion time as the publication time.
        MigrationPhase::Published | MigrationPhase::CleaningUp => Phase::Published,
        MigrationPhase::Complete => Phase::Complete,
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
    /// Highest RangeBusy count observed at a successful write completion.
    /// Unlike two sampled counters, this establishes the event order.
    write_ok_after_busy: AtomicU64,
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
            write_ok_after_busy: AtomicU64::new(0),
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
    }

    /// Records a successful write and the rejection count the client knew when
    /// that write started.
    ///
    /// The count is read before the request is sent, so it is a lower bound on
    /// what the write saw: a write that was already in flight when a rejection
    /// happened does not claim it. Reading it after the write would make the
    /// count an upper bound and let a success that started before the stop
    /// close the stop window too early.
    fn record_write_success(&self, busy_before: u64) {
        self.write_ok_after_busy
            .fetch_max(busy_before, Ordering::Release);
        self.writes_ok.fetch_add(1, Ordering::Release);
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
    initial_writes: u64,
    first_busy_ms: Option<f64>,
    last_busy_ms: Option<f64>,
    first_ok_ms: Option<f64>,
    /// The rejections the client knew when the write that closed the window
    /// started.
    first_ok_busy: Option<u64>,
}

impl StopWindow {
    /// A window whose baseline is the counters as they are now.
    ///
    /// Without it a later transition sees the earlier transitions' rejections
    /// as new ones and pins its first rejection to zero.
    fn starting(busy: u64, writes_ok: u64) -> Self {
        Self {
            previous_busy: busy,
            initial_writes: writes_ok,
            ..Self::default()
        }
    }

    /// Samples cumulative counters. `now_ms` is measured from the start of the
    /// change.
    fn observe(&mut self, now_ms: f64, busy: u64, writes_ok: u64, write_ok_after_busy: u64) {
        if busy > self.previous_busy {
            self.first_busy_ms.get_or_insert(now_ms);
            self.last_busy_ms = Some(now_ms);
            // A new rejection means the window is open again, so a write that
            // completed before it is not the first write after the stop.
            self.first_ok_ms = None;
        }
        // A success in the same sample may have preceded the rejection. Only
        // close when a completion observed at least this rejection count.
        // Keep that evidence across polls: the success can be observed before
        // the poll catches up with the rejection counter.
        if self.last_busy_ms.is_some()
            && self.first_ok_ms.is_none()
            && writes_ok > self.initial_writes
            && write_ok_after_busy >= busy
        {
            self.first_ok_ms = Some(now_ms);
            self.first_ok_busy = Some(write_ok_after_busy);
        }
        self.previous_busy = busy;
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
            // The evidence behind `first_write_ok_ms`: the client had seen
            // this many rejections when that write started, so the write
            // cannot have been the one that was already in flight when the
            // stop began.
            "busy_before_first_ok": self.first_ok_busy,
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

/// The periodic metrics sampler.
///
/// Every early return after the sampler starts must stop it, or it keeps
/// appending `metrics_sampled` events to `events.jsonl` after the run has
/// reported its failure. Owning the task in a value with a `Drop` makes the
/// cleanup independent of which `?` returned.
struct MetricsSampler {
    handle: Option<tokio::task::JoinHandle<()>>,
}

impl MetricsSampler {
    fn spawn(metrics: Arc<Metrics>, events: EventLog) -> Self {
        let handle = tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;
                if let Err(error) = metrics.sample(&events) {
                    eprintln!("scaling metrics sample failed: {error:#}");
                }
            }
        });
        Self {
            handle: Some(handle),
        }
    }

    async fn stop(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.abort();
            let _ = handle.await;
        }
    }
}

impl Drop for MetricsSampler {
    fn drop(&mut self) {
        if let Some(handle) = &self.handle {
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
    async fn join(mut self) -> Result<()> {
        self.stop.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            // The sender is an OS thread because a Tokio timer would add
            // roughly a millisecond per request. Waiting for it therefore has
            // to leave the async worker.
            tokio::task::spawn_blocking(move || thread.join())
                .await
                .context("joining an open-loop sender thread")?
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
                    // Read the rejection count before the write for the same
                    // reason the closed loop does: only a rejection the client
                    // already knew when the write started is evidence that the
                    // write happened after the stop.
                    let busy_before = client.retry_reason_count(RetryReason::RangeBusy);
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
                    if result.is_ok() {
                        metrics.record_write_success(busy_before);
                    }
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
    let mut sampler = MetricsSampler::spawn(Arc::clone(&metrics), events.clone());

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
    let mut re_read_handles: Vec<AfterPublishProbe> = Vec::new();
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
        // Where the publication time came from: the phase was observed, or
        // the change was only seen at completion and its completion time is
        // the earliest moment that can be named.
        let mut published_ms_source: Option<&'static str> = None;
        let mut complete_ms: Option<f64> = None;
        // RF repair starts at completion, so the convergence clock starts
        // there too. The removed-node waits below must not be hidden from it.
        let mut rf_started: Option<Instant> = None;
        let mut after_publish_probe: Option<AfterPublishProbe> = None;
        // The change's own throughput: the numerator and the denominator both
        // stop when the change completes.
        let (completed, change_seconds, change_operations) = if let Some(staged) = &staged {
            ledger.begin_publish_window();
            let mut last_phase = Phase::Pre;
            let execution_client = control.clone();
            let change_id = staged.change_id.clone();
            let base_epoch = staged.base_epoch;
            let target_epoch = staged.target_topology.epoch;
            // Capture the baseline before execution can produce a rejection.
            let mut window = StopWindow::starting(
                workload_client.retry_reason_count(RetryReason::RangeBusy),
                metrics.writes_ok(),
            );
            let execution = tokio::spawn(async move {
                execution_client
                    .execute_topology_change(change_id, base_epoch, target_epoch)
                    .await
            });
            loop {
                if execution.is_finished() {
                    break;
                }
                if let Ok(Some(current)) = observer.topology_change().await {
                    let phase = phase_of(current.phase);
                    if phase != last_phase {
                        if matches!(phase, Phase::Published | Phase::Complete) {
                            if published_ms.is_none() {
                                published_ms = Some(started.elapsed().as_secs_f64() * 1_000.0);
                                published_ms_source = Some(if phase == Phase::Published {
                                    "observed"
                                } else {
                                    "completion_fallback"
                                });
                            }
                            // Re-read the keys written before the switch right
                            // away. A stale read after publication heals
                            // quickly, so a probe that starts after the whole
                            // transition would never see it. A change that is
                            // first observed as `Complete` still gets a probe;
                            // starting it late is better than silently having
                            // no read-after-publish coverage.
                            if after_publish_probe.is_none() {
                                after_publish_probe = spawn_after_publish_probe_task(
                                    config,
                                    workload_client,
                                    &metrics,
                                    &ledger,
                                    events,
                                    transition.label,
                                )?;
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
                    metrics.write_ok_after_busy.load(Ordering::Acquire),
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
            // The change's own throughput: both the numerator and the
            // denominator stop here, so the stop window, the removed-node
            // waits and the post window cannot dilute it.
            let change_seconds = started.elapsed();
            let change_operations = metrics.total_operations().saturating_sub(operations_before);
            // A short change can go from `Verifying` to `Complete` between two
            // polls, so the loop above may exit without ever observing a
            // published phase. Start the probe here rather than silently
            // dropping the read-after-publish coverage; the earliest time we
            // can name is completion, and the source says so.
            if after_publish_probe.is_none() {
                published_ms.get_or_insert(complete_ms.expect("just recorded"));
                published_ms_source.get_or_insert("completion_fallback");
                after_publish_probe = spawn_after_publish_probe_task(
                    config,
                    workload_client,
                    &metrics,
                    &ledger,
                    events,
                    transition.label,
                )?;
            }
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
                    metrics.write_ok_after_busy.load(Ordering::Acquire),
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
            (Some(completed), change_seconds, change_operations)
        } else {
            // S0 keeps the topology unchanged. The measured baseline is the
            // post-change window with the same load.
            events.record(
                "scaling_control_window_started",
                json!({ "seconds": config.scaling_post_seconds }),
            )?;
            tokio::time::sleep(Duration::from_secs(config.scaling_post_seconds)).await;
            (
                None,
                started.elapsed(),
                metrics.total_operations().saturating_sub(operations_before),
            )
        };
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
            sender.join().await?;
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
        // The throughput is the change's own: the numerator and denominator
        // both stop when the change completes, so the post window and the
        // verification reads cannot dilute it.
        measurements.insert(
            format!("{}_transition", transition.label),
            measurement(change_operations, change_seconds),
        );
        observations.insert(
            format!("{}_transition", transition.label),
            json!({
                "change_seconds": change_seconds.as_secs_f64(),
                "published_ms": published_ms,
                "published_ms_source": published_ms_source,
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
                "published_ms_source": published_ms_source,
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
    // Every change has to show that its keys could be read after the
    // publication. A probe that read nothing leaves the run without that
    // evidence, so it fails the run instead of passing it quietly.
    let mut probe_results = Vec::new();
    for probe in re_read_handles {
        let read = join_after_publish_probe(probe).await?;
        probe_results.push(read);
    }
    ensure!(
        probe_results.len() == transitions.len() - usize::from(is_control(config)),
        "the run has {} read-after-publish probes for {} changes; a change whose publish \
         window held no write to a moved stratum has no read-after-publish evidence",
        probe_results.len(),
        transitions.len() - usize::from(is_control(config))
    );
    observations.insert(
        "scaling_after_publish_probes".into(),
        json!({ "probes": probe_results.len(), "keys_read": probe_results }),
    );
    if let Some(sender) = open_reads.take() {
        sender.join().await?;
    }
    drain_in_flight(&metrics, config).await?;
    read_tasks
        .stop(
            Duration::from_millis(config.operation_timeout_ms)
                .saturating_add(Duration::from_secs(10)),
        )
        .await?;
    sampler.stop().await;
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
    // An open-loop run takes its write rates from its open rate, so the
    // closed-loop rate it was also given is not what it wrote at. The plan
    // reports the rates the run actually used.
    let rates = cohort_rates(config);
    observations.insert(
        "scaling_pool_plan".into(),
        json!({
            "read_keys": read_pool.len(),
            "read_distribution": distribution.name(),
            "load_mode": config.scaling_load_mode.name(),
            "open_rate": config.scaling_open_rate,
            "write_rate": if config.scaling_load_mode == ScalingLoadMode::Open {
                serde_json::Value::Null
            } else {
                json!(config.scaling_write_rate)
            },
            "cohort_write_rates": {
                "rewrite": rates.rewrite,
                "write_new": rates.write_new,
                "delete": rates.delete,
                "total": rates.rewrite + rates.write_new + rates.delete,
            },
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
///
/// The shares are whole operations per second, so the cohorts together offer
/// slightly less than the configured rate when the shares do not divide it.
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

/// The paced rate of the read-after-publish probe. It reads at half the write
/// rate so it trails the writes instead of competing with them, bounded on
/// both ends so a tiny or enormous write rate still yields a usable pace.
fn after_publish_probe_rate(config: &ExperimentConfig) -> u64 {
    (write_operations_per_second(config) / 2).clamp(20, 500)
}

/// How long the probe may go without reading a key before it is stuck.
///
/// The probe reads its keys one after another, and every read is bounded by
/// the client's operation timeout, so a probe that is making progress always
/// finishes a key within that timeout plus the pause between two reads. A
/// probe that goes longer than that without a key is stuck, and the run must
/// not wait for it forever.
///
/// A budget for the whole probe would have to cover the service time of every
/// read as well as the pause between them, and the service time is not known
/// in advance: the probe runs right after a publication, while the cluster is
/// still repairing replicas. A fixed budget fails a run that is only slow.
fn after_publish_probe_idle_limit(operation_timeout: Duration, rate: u64) -> Duration {
    let pause = Duration::from_secs_f64(1.0 / rate as f64);
    operation_timeout + pause + Duration::from_secs(10)
}

/// The read-after-publish probe of one transition and what it has to show.
struct AfterPublishProbe {
    label: &'static str,
    /// The keys the probe was given to read.
    expected: usize,
    /// How long the probe may go without reading a key.
    idle_limit: Duration,
    /// What the watchdog saw and how it ended.
    report: Arc<ProbeReport>,
    /// The probe's task. It is aborted when the probe is dropped.
    handle: Option<tokio::task::JoinHandle<Result<usize>>>,
    /// The watchdog that gives up on a probe which stops reading keys.
    watchdog: Option<tokio::task::JoinHandle<()>>,
}

impl Drop for AfterPublishProbe {
    fn drop(&mut self) {
        // A probe that is dropped before it finished would keep reading keys
        // and writing metrics after the run moved on, so both of its tasks
        // are aborted here. The joined path consumes the handle first, so an
        // aborted handle is only ever a task that is still running.
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
        if let Some(watchdog) = self.watchdog.take() {
            watchdog.abort();
        }
    }
}

/// Starts the read-after-publish probe of one transition.
///
/// The probe reads the keys that were written to a moved stratum while the
/// change was publishing, so a stale read after the publication is caught. A
/// change whose publish window held no such write has nothing to read, and
/// that is recorded rather than left to look like a probe that read nothing.
fn spawn_after_publish_probe_task(
    config: &ExperimentConfig,
    workload_client: &HashringClient,
    metrics: &Arc<Metrics>,
    ledger: &Ledger,
    events: &EventLog,
    label: &'static str,
) -> Result<Option<AfterPublishProbe>> {
    let expected = ledger.publish_expected();
    if expected.is_empty() {
        events.record(
            "scaling_after_publish_probe_skipped",
            json!({
                "label": label,
                "reason": "no successful write to a moved stratum landed in the publish window",
            }),
        )?;
        return Ok(None);
    }
    let rate = after_publish_probe_rate(config);
    let idle_limit =
        after_publish_probe_idle_limit(Duration::from_millis(config.operation_timeout_ms), rate);
    // Record the start before the tasks exist. An event that cannot be written
    // must not leave a probe running with no owner left to stop it.
    events.record(
        "scaling_after_publish_probe_started",
        json!({
            "label": label,
            "keys": expected.len(),
            "rate": rate,
            "idle_limit_ms": idle_limit.as_millis() as u64,
        }),
    )?;
    // The probe reports every key it reads, so the watchdog can tell a probe
    // that is still working from one that stopped.
    let (progress, progress_updates) = tokio::sync::watch::channel(ProbeState::Reading(0));
    let report = Arc::new(ProbeReport::default());
    let client = workload_client.clone();
    let handle = tokio::spawn(spawn_after_publish_probe(
        move |key: u64| {
            let client = client.clone();
            async move {
                client
                    .get(key.to_be_bytes().to_vec())
                    .await
                    .map(|output| output.value)
            }
        },
        Arc::clone(metrics),
        expected.clone(),
        rate,
        progress,
    ));
    let watchdog = tokio::spawn(watch_after_publish_probe(
        label,
        progress_updates,
        handle.abort_handle(),
        Arc::clone(&report),
        idle_limit,
        events.clone(),
    ));
    Ok(Some(AfterPublishProbe {
        label,
        expected: expected.len(),
        idle_limit,
        report,
        handle: Some(handle),
        watchdog: Some(watchdog),
    }))
}

/// What a probe's watchdog saw and how it ended.
///
/// The run reads it when the probe is joined: a probe that was given up on can
/// then say how many keys it verified before it stopped, instead of claiming
/// that it never read one.
#[derive(Default)]
struct ProbeReport {
    /// The most keys the watchdog saw verified.
    verified: AtomicU64,
    /// Whether the watchdog gave up on the probe.
    stuck: AtomicBool,
}

/// What a read-after-publish probe is doing.
///
/// The probe publishes every key it reads, so the watchdog can tell a probe
/// that is still working from one that stopped. The last state is the one the
/// probe leaves behind when it read every key it was given.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ProbeState {
    /// The probe read this many keys and is still reading.
    Reading(u64),
    /// The probe read every key it was given.
    Finished,
}

/// Gives up on a probe that stops reading keys.
///
/// The clock starts with the probe rather than with the moment the run waits
/// for it, so a probe that stopped early is given up on while the run is
/// still going. Every key the probe reads moves the deadline, so the limit
/// covers the time between two keys instead of the whole probe.
async fn watch_after_publish_probe(
    label: &'static str,
    mut progress: tokio::sync::watch::Receiver<ProbeState>,
    abort: tokio::task::AbortHandle,
    report: Arc<ProbeReport>,
    idle_limit: Duration,
    events: EventLog,
) {
    let mut read = 0_u64;
    let mut deadline = tokio::time::Instant::now() + idle_limit;
    loop {
        tokio::select! {
            update = progress.changed() => {
                if update.is_err() {
                    // The probe ended without saying so, so there is nothing
                    // left to watch.
                    return;
                }
                match *progress.borrow() {
                    ProbeState::Finished => return,
                    ProbeState::Reading(count) => {
                        read = count;
                        report.verified.store(count, Ordering::Release);
                        deadline = tokio::time::Instant::now() + idle_limit;
                    }
                }
            }
            _ = tokio::time::sleep_until(deadline) => {
                match *progress.borrow() {
                    ProbeState::Finished => return,
                    ProbeState::Reading(count) if count != read => {
                        // The probe read a key after the deadline was set, so
                        // it is making progress and the deadline was stale.
                        read = count;
                        report.verified.store(count, Ordering::Release);
                        deadline = tokio::time::Instant::now() + idle_limit;
                        continue;
                    }
                    ProbeState::Reading(_) => {}
                }
                // The store of the count must precede the store of the verdict,
                // so a reader of the verdict also sees the final count.
                report.verified.store(read, Ordering::Release);
                report.stuck.store(true, Ordering::Release);
                if let Err(error) = events.record(
                    "scaling_after_publish_probe_stuck",
                    json!({
                        "label": label,
                        "idle_limit_ms": idle_limit.as_millis() as u64,
                        "verified": read,
                    }),
                ) {
                    eprintln!("scaling after-publish probe event failed: {error:#}");
                }
                abort.abort();
                return;
            }
        }
    }
}

/// Waits for the read-after-publish probe and reports how many keys it read.
///
/// The probe is given as long as it keeps reading keys. It is given up on
/// when it stops making progress, because a probe that is not reading will
/// never finish. A change whose probe read every key it was given passes;
/// anything else fails the run.
async fn join_after_publish_probe(mut probe: AfterPublishProbe) -> Result<usize> {
    let label = probe.label;
    let expected = probe.expected;
    let idle_limit = probe.idle_limit;
    let report = Arc::clone(&probe.report);
    let handle = probe
        .handle
        .take()
        .expect("a probe always has the task it was started with");
    let read = match handle.await {
        Ok(Ok(read)) => read,
        Ok(Err(error)) => return Err(error.context("the read-after-publish probe failed")),
        Err(error) if error.is_cancelled() && report.stuck.load(Ordering::Acquire) => {
            let verified = report.verified.load(Ordering::Acquire);
            return Err(anyhow::anyhow!(
                "the {label} read-after-publish probe made no progress for {idle_limit:?} \
                 after verifying {verified} of its {expected} keys"
            ));
        }
        Err(error) => {
            return Err(anyhow::Error::new(error).context("joining the read-after-publish probe"));
        }
    };
    ensure!(
        read == expected && read > 0,
        "the {label} read-after-publish probe verified {read} of its {expected} keys"
    );
    Ok(read)
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
    // The configuration bounds the rate and the window, so these products fit
    // a u64. Converting them to a usize is still fallible on a platform with a
    // smaller address space, and a truncated quota would be a wrong plan.
    let rewrite_quota = usize::try_from(write_rate.rewrite * pool_seconds)
        .context("the rewrite pool does not fit the platform address space")?;
    let new_quota = usize::try_from(write_rate.write_new * pool_seconds)
        .context("the fresh-write pool does not fit the platform address space")?;
    let delete_quota = usize::try_from(write_rate.delete * pool_seconds)
        .context("the delete pool does not fit the platform address space")?;
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
                    // The rejection count is read before the write, so the
                    // success can only claim a rejection the client already
                    // knew about when it started.
                    let busy_before = client.retry_reason_count(RetryReason::RangeBusy);
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
                    if result.is_ok() {
                        metrics.record_write_success(busy_before);
                    }
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

async fn spawn_after_publish_probe<F, Fut>(
    mut get: F,
    metrics: Arc<Metrics>,
    expected: BTreeMap<u64, Allowed>,
    rate: u64,
    progress: tokio::sync::watch::Sender<ProbeState>,
) -> Result<usize>
where
    F: FnMut(u64) -> Fut,
    Fut: Future<Output = Result<Vec<u8>, ClientError>>,
{
    let interval = Duration::from_secs_f64(1.0 / rate as f64);
    let mut read = 0_usize;
    for (key, allowed) in expected {
        tokio::time::sleep(interval).await;
        let phase = metrics.phase();
        let started = Instant::now();
        let result = get(key).await;
        let elapsed = started.elapsed();
        match result {
            Ok(value) if allowed.check(Some(&value)) => {
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
                return Err(anyhow!("key {key} had the wrong value after publication"));
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
                return Err(anyhow!("key {key} was missing after publication"));
            }
            Err(error) => {
                let detail = format!("key {key} could not be verified after publication: {error}");
                metrics.record("probe_read_after_publish", phase, elapsed, &Err(error));
                return Err(anyhow!(detail));
            }
        }
        // Only a checked value or an expected absence is evidence of a read.
        read += 1;
        progress.send_replace(ProbeState::Reading(read as u64));
    }
    // The probe is done, so the watchdog must not give up on it.
    progress.send_replace(ProbeState::Finished);
    Ok(read)
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

    #[test]
    fn a_rejection_and_a_success_in_one_sample_still_close_the_stop_window() {
        let mut window = StopWindow::starting(0, 0);
        window.observe(10.0, 1, 1, 1);
        assert_eq!(window.first_busy_ms, Some(10.0));
        assert_eq!(window.first_ok_ms, Some(10.0));
        assert!(window.closed());
    }

    #[test]
    fn a_success_before_the_rejection_does_not_close_the_stop_window() {
        let mut window = StopWindow::starting(7, 12);
        // Both counters advance in one poll, but the success saw only the
        // previous rejection count: it happened before the new rejection.
        window.observe(10.0, 8, 13, 7);
        assert_eq!(window.first_busy_ms, Some(10.0));
        assert_eq!(window.first_ok_ms, None);
        assert!(!window.closed());
        window.observe(20.0, 8, 13, 7);
        assert!(!window.closed());
        window.observe(30.0, 8, 14, 8);
        assert_eq!(window.first_ok_ms, Some(30.0));
        assert_eq!(window.summary()["observed_ms"], 20.0);
        assert_eq!(window.summary()["busy_before_first_ok"], 8);
        // A later rejection invalidates that success, too.
        window.observe(40.0, 9, 14, 8);
        assert!(!window.closed());
    }

    #[test]
    fn a_success_is_kept_when_the_busy_sample_catches_up_later() {
        let mut window = StopWindow::starting(0, 0);
        window.observe(10.0, 1, 1, 2);
        window.observe(20.0, 2, 1, 2);
        assert_eq!(window.last_busy_ms, Some(20.0));
        assert_eq!(window.summary()["busy_before_first_ok"], 2);
        assert!(window.closed());
    }

    #[tokio::test]
    async fn publication_probe_counts_only_verified_keys_and_rejects_read_failures() {
        for failure in [
            Ok(vec![99]),
            Err(ClientError::DeadlineExceeded {
                outcome: MutationOutcome::KnownNotApplied,
            }),
            Err(hashring_client::OperationFailure {
                code: ErrorCode::NotFound,
                message: "missing".into(),
                retryable: false,
                outcome: MutationOutcome::KnownNotApplied,
            }
            .into()),
        ] {
            let metrics = Arc::new(Metrics::new(Duration::from_secs(1), false, 1).unwrap());
            let expected =
                BTreeMap::from([(1, Allowed::Value(vec![1])), (2, Allowed::Value(vec![2]))]);
            let mut responses = [Ok(vec![1]), failure].into_iter();
            let (progress, updates) = tokio::sync::watch::channel(ProbeState::Reading(0));
            let result = spawn_after_publish_probe(
                |_| std::future::ready(responses.next().unwrap()),
                metrics,
                expected,
                500,
                progress,
            )
            .await;
            assert!(result.is_err(), "an unverified key must fail the probe");
            assert_eq!(*updates.borrow(), ProbeState::Reading(1));
        }
    }

    #[tokio::test]
    async fn publication_probe_accepts_a_checked_value_and_expected_absence() {
        let metrics = Arc::new(Metrics::new(Duration::from_secs(1), false, 1).unwrap());
        let expected = BTreeMap::from([(1, Allowed::Value(vec![1])), (2, Allowed::Missing)]);
        let (progress, updates) = tokio::sync::watch::channel(ProbeState::Reading(0));
        let result = spawn_after_publish_probe(
            |key| {
                std::future::ready(if key == 1 {
                    Ok(vec![1])
                } else {
                    Err(hashring_client::OperationFailure {
                        code: ErrorCode::NotFound,
                        message: "deleted".into(),
                        retryable: false,
                        outcome: MutationOutcome::KnownNotApplied,
                    }
                    .into())
                })
            },
            metrics,
            expected,
            500,
            progress,
        )
        .await;
        assert_eq!(result.unwrap(), 2);
        assert_eq!(*updates.borrow(), ProbeState::Finished);
    }

    #[test]
    fn completion_is_not_recorded_as_publication() {
        assert_eq!(phase_of(MigrationPhase::Published), Phase::Published);
        assert_eq!(phase_of(MigrationPhase::CleaningUp), Phase::Published);
        assert_eq!(phase_of(MigrationPhase::Complete), Phase::Complete);
    }

    #[test]
    fn the_probe_is_given_as_long_as_it_keeps_reading_keys() {
        // The limit covers one read's timeout and the pause between two reads,
        // so a probe that is making progress always finishes a key within it.
        let limit = after_publish_probe_idle_limit(Duration::from_secs(30), 20);
        assert!(limit > Duration::from_secs(30));
        assert!(limit < Duration::from_secs(60));
        // A faster probe waits less between two keys, so it may go idle for
        // less time.
        let fast = after_publish_probe_idle_limit(Duration::from_secs(30), 500);
        assert!(fast < limit);
    }

    #[tokio::test]
    async fn a_probe_that_stops_reading_keys_is_given_up_on() {
        let directory = tempfile::tempdir().unwrap();
        let events = EventLog::open(&directory.path().join("events.jsonl")).unwrap();
        let (progress, updates) = tokio::sync::watch::channel(ProbeState::Reading(0));
        // A probe that never finishes a key keeps its channel open and sends
        // nothing, so the watchdog sees no progress at all.
        let probe = tokio::spawn(async move {
            let _progress = progress;
            std::future::pending::<()>().await;
        });
        let report = Arc::new(ProbeReport::default());
        let watchdog = tokio::spawn(watch_after_publish_probe(
            "grow",
            updates,
            probe.abort_handle(),
            Arc::clone(&report),
            Duration::from_millis(50),
            events.clone(),
        ));
        watchdog.await.unwrap();
        assert!(report.stuck.load(Ordering::Acquire));
        assert_eq!(report.verified.load(Ordering::Acquire), 0);
        // The probe was aborted, so joining it reports the cancellation.
        let error = probe.await.unwrap_err();
        assert!(error.is_cancelled());
    }

    #[tokio::test]
    async fn a_probe_that_keeps_reading_keys_is_left_alone() {
        let directory = tempfile::tempdir().unwrap();
        let events = EventLog::open(&directory.path().join("events.jsonl")).unwrap();
        let (progress, updates) = tokio::sync::watch::channel(ProbeState::Reading(0));
        let sender = progress.clone();
        // The probe reads a key every 10 ms, which is well inside the limit,
        // and then says that it is done.
        let probe = tokio::spawn(async move {
            for read in 1..=5_u64 {
                tokio::time::sleep(Duration::from_millis(10)).await;
                sender.send_replace(ProbeState::Reading(read));
            }
            sender.send_replace(ProbeState::Finished);
            5_usize
        });
        let report = Arc::new(ProbeReport::default());
        let watchdog = tokio::spawn(watch_after_publish_probe(
            "grow",
            updates,
            probe.abort_handle(),
            Arc::clone(&report),
            Duration::from_millis(50),
            events.clone(),
        ));
        watchdog.await.unwrap();
        assert!(!report.stuck.load(Ordering::Acquire));
        // The watchdog sees every state the probe publishes, but a burst of
        // updates may already have moved on to `Finished` when it wakes, so
        // the count it reports is a lower bound.
        assert!(report.verified.load(Ordering::Acquire) > 0);
        assert_eq!(probe.await.unwrap(), 5);
    }

    #[tokio::test]
    async fn a_stuck_probe_reports_the_keys_it_had_verified() {
        let directory = tempfile::tempdir().unwrap();
        let events = EventLog::open(&directory.path().join("events.jsonl")).unwrap();
        let (progress, updates) = tokio::sync::watch::channel(ProbeState::Reading(0));
        // The probe verifies two keys and then stops without finishing.
        let sender = progress.clone();
        let probe = tokio::spawn(async move {
            for read in 1..=2_u64 {
                sender.send_replace(ProbeState::Reading(read));
            }
            std::future::pending::<()>().await;
        });
        let report = Arc::new(ProbeReport::default());
        let watchdog = tokio::spawn(watch_after_publish_probe(
            "grow",
            updates,
            probe.abort_handle(),
            Arc::clone(&report),
            Duration::from_millis(50),
            events.clone(),
        ));
        watchdog.await.unwrap();
        assert!(report.stuck.load(Ordering::Acquire));
        assert_eq!(report.verified.load(Ordering::Acquire), 2);
        assert!(probe.await.unwrap_err().is_cancelled());
    }
}
