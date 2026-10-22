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

use anyhow::{Context, Result, ensure};
use futures::{StreamExt, stream};
use hashring_client::{ClientError, HashringClient, MutationOutcome};
use hashring_core::{
    migration::{MigrationPhase, TopologyChange},
    proto::ErrorCode,
    topology::{Member, TopologySnapshot},
};
use rand::{SeedableRng, rngs::StdRng};
use serde_json::json;
use tokio::sync::mpsc;

use super::{
    EventLog, ExperimentConfig, Measurement, ProcessGroup, ScalingScenario, deterministic_value,
    latency::LatencyHistogram, measurement, spawn_node, wait_for_listener,
};

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
    let chosen: &[Stratum] = match (scenario, label) {
        (ScalingScenario::Grow, _) => &grow,
        (ScalingScenario::Shrink, _) => &shrink,
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
    state: Mutex<MetricState>,
}

impl Metrics {
    fn new(operation_timeout: Duration) -> Result<Self> {
        Ok(Self {
            phase: AtomicU8::new(Phase::Pre.byte()),
            highest_micros: (operation_timeout.as_micros() as u64).saturating_add(1_000_000),
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
                "phase": self.phase().name(),
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
            "metrics": metrics,
            "errors": state.total_errors,
            "violations": state.violations,
            "violation_examples": state.violation_examples,
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
        self.state
            .lock()
            .expect("the metrics lock is not poisoned")
            .total
            .values()
            .map(|bucket| bucket.count)
            .sum()
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

pub(crate) struct RunContext<'a> {
    pub config: &'a ExperimentConfig,
    pub peak_members: &'a [Member],
    pub initial_count: usize,
    pub ports: &'a [u16],
    pub coordinator_endpoint: &'a str,
    pub admin_client: &'a HashringClient,
    pub workload_client: &'a HashringClient,
    pub processes: &'a mut ProcessGroup,
    pub events: &'a EventLog,
    pub measurements: &'a mut BTreeMap<String, Measurement>,
    pub observations: &'a mut BTreeMap<String, serde_json::Value>,
}

pub(crate) async fn run(context: RunContext<'_>) -> Result<u64> {
    let RunContext {
        config,
        peak_members,
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

    let metrics = Arc::new(Metrics::new(Duration::from_millis(
        config.operation_timeout_ms,
    ))?);
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
    for task in 0..config.concurrency {
        let client = workload_client.clone();
        let metrics = Arc::clone(&metrics);
        let stop = Arc::clone(&read_stop);
        let read_pool = Arc::clone(&read_pool);
        let distribution = distribution.clone();
        let value_bytes = config.value_bytes;
        let seed = config.hash_seed ^ (task as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15);
        read_tasks.push(tokio::spawn(async move {
            let mut rng = StdRng::seed_from_u64(seed);
            while !stop.load(Ordering::Acquire) {
                let index = distribution.sample(&mut rng, read_pool.len() as u64) as usize;
                let key = read_pool[index];
                let phase = metrics.phase();
                let started = Instant::now();
                let result = client.get(key.to_be_bytes().to_vec()).await;
                let elapsed = started.elapsed();
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
            Ok(())
        }));
    }

    let mut all_entries = BTreeMap::new();
    let mut all_fresh = BTreeSet::new();
    let mut final_epoch = admin_client.refresh_topology().await?.epoch;
    let mut re_read_handles: Vec<tokio::task::JoinHandle<Result<()>>> = Vec::new();
    let mut pending_verification: Option<(&'static str, BTreeMap<u64, Allowed>)> = None;

    for (index, transition) in transitions.iter().enumerate() {
        let change = control
            .begin_topology_change(transition.target_members.clone())
            .await
            .with_context(|| format!("staging the {} change", transition.label))?;
        ensure!(
            change.base_topology.is_some(),
            "the staged {} change has no base topology",
            transition.label
        );
        let plan = build_transition_plan(
            config,
            transition,
            &change,
            &pool_plan,
            index,
            transitions.len(),
        )?;
        let ledger = Arc::new(Ledger::new(Arc::clone(&plan.strata), config.value_bytes));

        // The new members are in the pending topology already, so they can be
        // started now. The removed members are told to stop by the coordinator
        // once their ranges have been handed off.
        let base_members = change
            .base_topology
            .as_ref()
            .expect("the staged change has a base topology")
            .members
            .clone();
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

        events.record(
            "scaling_transition_started",
            json!({
                "label": transition.label,
                "change_id": change.change_id,
                "base_epoch": change.base_epoch,
                "target_epoch": change.target_topology.epoch,
                "direct_merge": change.direct_merge,
                "rewrite_keys": plan.rewrite.len(),
                "new_keys": plan.write_new.len(),
                "delete_keys": plan.delete.len(),
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
        ledger.begin_publish_window();
        metrics.set_phase(Phase::Pre);
        let mut last_phase = Phase::Pre;
        let execution_client = control.clone();
        let change_id = change.change_id.clone();
        let base_epoch = change.base_epoch;
        let target_epoch = change.target_topology.epoch;
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
            tokio::time::sleep(Duration::from_millis(5)).await;
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
        // The removed nodes are told to stop once their ranges are handed
        // off. Wait for them before the process liveness checks, or a normal
        // removal looks like an unexpected exit.
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
        let range_busy = retry_delta.get("range_busy").copied().unwrap_or(0);
        ensure!(
            range_busy > 0,
            "the {} change produced no client-observed RangeBusy write rejection, so the run has no evidence that writes were stopped; it is invalid",
            transition.label
        );

        let target_epoch = completed.target_topology.epoch;
        wait_for_full_rf_at_epoch(
            &observer,
            config.desired_replication_factor,
            target_epoch,
            Duration::from_millis(config.migration_timeout_ms),
            processes,
        )
        .await?;

        // Keep the load running while the post-change window is measured.
        if config.scaling_post_seconds > 0 {
            tokio::time::sleep(Duration::from_secs(config.scaling_post_seconds)).await;
        }

        write_tasks
            .stop(
                Duration::from_millis(config.operation_timeout_ms)
                    .saturating_add(Duration::from_secs(10)),
            )
            .await?;
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

        // Read after publication, with a separate client and a bounded rate,
        // so the probe does not become a burst inside the measured window.
        let publish_expected = ledger.publish_expected();
        re_read_handles.push(tokio::spawn(spawn_after_publish_probe(
            workload_client.clone(),
            Arc::clone(&metrics),
            publish_expected,
            config.scaling_write_rate,
        )));

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
                "post_seconds": config.scaling_post_seconds,
                "direct_merge": completed.direct_merge,
                "base_epoch": completed.base_epoch,
                "target_epoch": target_epoch,
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
                "target_epoch": target_epoch,
                "retries_by_reason": retry_delta,
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
        ScalingScenario::RoundTrip => vec![
            Transition {
                label: "grow",
                target_members: peak_members[..config.node_count].to_vec(),
            },
            Transition {
                label: "shrink",
                target_members: initial_members.to_vec(),
            },
        ],
    }
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
    change: &TopologyChange,
    pool_plan: &BuildPlan,
    index: usize,
    transitions: usize,
) -> Result<TransitionPlan> {
    let base = change
        .base_topology
        .as_ref()
        .context("a planned change has a base topology")?;
    let target = &change.target_topology;
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

    let write_rate = config.scaling_write_rate;
    let rewrite_quota = (write_rate * 60 / 100 * config.scaling_pool_seconds) as usize;
    let new_quota = (write_rate * 25 / 100 * config.scaling_pool_seconds) as usize;
    let delete_quota = (write_rate * 15 / 100 * config.scaling_pool_seconds) as usize;
    ensure!(
        rewrite_quota > 0 && new_quota > 0 && delete_quota > 0,
        "scaling_write_rate is too low to give every cohort a key; raise it or raise --scaling-pool-seconds"
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
    let rate = config.scaling_write_rate;
    let cohorts = [
        (Cohort::Rewrite, Arc::clone(&plan.rewrite), rate * 60 / 100),
        (
            Cohort::WriteNew,
            Arc::clone(&plan.write_new),
            rate * 25 / 100,
        ),
        (Cohort::Delete, Arc::clone(&plan.delete), rate * 15 / 100),
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
