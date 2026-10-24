//! The `fault` experiment mode: continuous mixed load across the loss of one
//! node.
//!
//! `FaultScenario::Follower` kills a node that is only a follower for the
//! selected keys and checks that the surviving owner keeps serving them.
//! `FaultScenario::Owner` kills the owner and checks the failover, the ledger,
//! and the permitted `OwnerOnly` rollbacks. Both scenarios keep the load
//! running through the automatic removal, the replacement join, and RF repair,
//! then verify every key the run touched.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    io::{BufWriter, Write},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::{Context, Result, ensure};
use hashring_client::{ClientError, HashringClient, MutationOutcome};
use hashring_core::{
    proto::{ErrorCode, RecordVersion, ReplicaStatusResponse},
    topology::{Member, TopologySnapshot, WriteAckPolicy},
};
use serde::Serialize;
use serde_json::{Value, json};

use super::{
    ExperimentConfig, FaultScenario, RunContext, deterministic_value, measurement, spawn_node,
    unix_ms, wait_for_full_rf, wait_for_listener,
};

/// A worker's request ledger is flushed this often, so a failed run still has
/// nearly all of its completed requests.
const LEDGER_FLUSH_INTERVAL: u64 = 64;
const NEVER: u64 = u64::MAX;
const MIN_COHORT: usize = 8;

#[derive(Clone, Default)]
struct OperationMix {
    get: u64,
    put: u64,
    delete: u64,
}

impl OperationMix {
    fn parse(text: &str) -> Result<Self> {
        let parts: Vec<u64> = text
            .split(':')
            .map(|part| {
                part.trim()
                    .parse::<u64>()
                    .with_context(|| format!("parsing the operation mix {text:?}"))
            })
            .collect::<Result<_>>()?;
        ensure!(
            parts.len() == 3,
            "the operation mix must be GET:PUT:DELETE, not {text:?}"
        );
        ensure!(parts.iter().sum::<u64>() > 0, "the operation mix is empty");
        Ok(Self {
            get: parts[0],
            put: parts[1],
            delete: parts[2],
        })
    }

    fn choose(&self, step: u64, worker: usize) -> Operation {
        let total = self.get + self.put + self.delete;
        // A fixed coprime walk spreads the operations over workers without a
        // random number generator, so a rerun with the same inputs reproduces
        // the same shape of load.
        let slot = (step.wrapping_mul(37).wrapping_add(worker as u64 * 11)) % total;
        if slot < self.get {
            Operation::Get
        } else if slot < self.get + self.put {
            Operation::Put
        } else {
            Operation::Delete
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Operation {
    Get,
    Put,
    Delete,
}

impl Operation {
    fn name(self) -> &'static str {
        match self {
            Self::Get => "get",
            Self::Put => "put",
            Self::Delete => "delete",
        }
    }

    fn success_metric(self) -> &'static str {
        match self {
            Self::Get => "get_ok",
            Self::Put => "put_ok",
            Self::Delete => "delete_ok",
        }
    }
}

#[derive(Clone, Debug, Serialize)]
struct VersionView {
    topology_epoch: u64,
    owner_sequence: u64,
    owner_node_id: String,
}

impl From<&RecordVersion> for VersionView {
    fn from(version: &RecordVersion) -> Self {
        Self {
            topology_epoch: version.topology_epoch,
            owner_sequence: version.owner_sequence,
            owner_node_id: version.owner_node_id.clone(),
        }
    }
}

#[derive(Serialize)]
struct RequestEntry {
    id: u64,
    worker: usize,
    key: u64,
    operation: &'static str,
    value_hex: Option<String>,
    invoked_unix_ms: u128,
    completed_unix_ms: u128,
    elapsed_us: u128,
    start_epoch: u64,
    end_epoch: u64,
    result: &'static str,
    error: Option<String>,
    error_code: Option<String>,
    outcome: Option<&'static str>,
    version: Option<VersionView>,
    topology_epoch: Option<u64>,
    retries: u64,
    retry_reasons: BTreeMap<String, u64>,
}

struct RequestLedger {
    file: Mutex<BufWriter<File>>,
}

impl RequestLedger {
    fn open(path: &std::path::Path) -> Result<Self> {
        Ok(Self {
            file: Mutex::new(BufWriter::new(File::create(path)?)),
        })
    }

    fn record(&self, entry: &RequestEntry) -> Result<()> {
        let mut file = self
            .file
            .lock()
            .map_err(|_| anyhow::anyhow!("request ledger lock was poisoned"))?;
        serde_json::to_writer(&mut *file, entry)?;
        file.write_all(b"\n")?;
        if entry.id.is_multiple_of(LEDGER_FLUSH_INTERVAL) {
            file.flush()?;
        }
        Ok(())
    }

    fn flush(&self) -> Result<()> {
        self.file
            .lock()
            .map_err(|_| anyhow::anyhow!("request ledger lock was poisoned"))?
            .flush()?;
        Ok(())
    }
}

/// One state the key's history allows.
#[derive(Clone, Debug, PartialEq)]
struct PossibleState {
    value: Option<Vec<u8>>,
    /// `None` means any version: an ambiguous mutation may have applied, but
    /// the caller never saw the version it received.
    version: Option<RecordVersion>,
}

impl PossibleState {
    fn present(value: Vec<u8>, version: RecordVersion) -> Self {
        Self {
            value: Some(value),
            version: Some(version),
        }
    }

    fn absent(version: Option<RecordVersion>) -> Self {
        Self {
            value: None,
            version,
        }
    }

    fn matches(&self, value: Option<&[u8]>, version: Option<&RecordVersion>) -> bool {
        match (&self.value, value) {
            (None, None) => true,
            (Some(expected), Some(observed)) => {
                expected.as_slice() == observed
                    && self.version.as_ref().is_none_or(|expected| {
                        expected == version.expect("present value has a version")
                    })
            }
            _ => false,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Observation {
    Matches,
    /// The observation is an older acknowledged state. Under `OwnerOnly` an
    /// owner failure may allow this; the caller decides whether it is a
    /// failure.
    Rollback {
        first: bool,
        kind: &'static str,
    },
    /// The observation is not explainable by the recorded history.
    Impossible,
}

#[derive(Clone, Debug)]
struct KeyModel {
    possible: Vec<PossibleState>,
    /// Every state a definite mutation or a pre-read established. A later read
    /// that matches one of these but not `possible` is a rollback rather than
    /// an arbitrary value.
    acknowledged: Vec<PossibleState>,
    rolled_back: bool,
}

impl KeyModel {
    fn new(state: PossibleState) -> Self {
        Self {
            possible: vec![state.clone()],
            acknowledged: vec![state],
            rolled_back: false,
        }
    }

    fn resync(&mut self, state: PossibleState) {
        self.possible = vec![state.clone()];
        if !self.acknowledged.contains(&state) {
            self.acknowledged.push(state);
        }
    }

    fn on_put(&mut self, value: Vec<u8>, version: RecordVersion) {
        self.resync(PossibleState::present(value, version));
    }

    fn on_delete(&mut self, version: RecordVersion) {
        self.resync(PossibleState::absent(Some(version)));
    }

    fn on_ambiguous_put(&mut self, value: Vec<u8>) {
        self.possible.push(PossibleState {
            value: Some(value),
            version: None,
        });
    }

    fn on_ambiguous_delete(&mut self) {
        self.possible.push(PossibleState::absent(None));
    }

    fn observe(&mut self, value: Option<&[u8]>, version: Option<&RecordVersion>) -> Observation {
        if self
            .possible
            .iter()
            .any(|state| state.matches(value, version))
        {
            return Observation::Matches;
        }
        if self
            .acknowledged
            .iter()
            .any(|state| state.matches(value, version))
        {
            let first = !self.rolled_back;
            self.rolled_back = true;
            let expected_absent = self.possible.iter().any(|state| state.value.is_none());
            let kind = match (value, expected_absent) {
                (Some(_), true) => "acknowledged_delete_lost",
                (Some(_), false) => "older_value_reappeared",
                (None, _) => "acknowledged_value_lost",
            };
            // Keep the observed state as possible so one rollback does not
            // make every later read of the same old state impossible. The
            // history still remembers the newer acknowledged state.
            let state = match value {
                Some(value) => PossibleState {
                    value: Some(value.to_vec()),
                    version: version.cloned(),
                },
                None => PossibleState::absent(None),
            };
            self.possible.push(state);
            return Observation::Rollback { first, kind };
        }
        Observation::Impossible
    }
}

#[derive(Default)]
struct Progress {
    kill_ms: AtomicU64,
    first_failure_after_kill_ms: AtomicU64,
    last_failure_after_kill_ms: AtomicU64,
    failures_after_kill: AtomicU64,
    first_read_after_kill_ms: AtomicU64,
    first_write_after_kill_ms: AtomicU64,
    first_delete_after_kill_ms: AtomicU64,
}

impl Progress {
    fn new() -> Self {
        Self {
            kill_ms: AtomicU64::new(NEVER),
            first_failure_after_kill_ms: AtomicU64::new(NEVER),
            last_failure_after_kill_ms: AtomicU64::new(0),
            failures_after_kill: AtomicU64::new(0),
            first_read_after_kill_ms: AtomicU64::new(NEVER),
            first_write_after_kill_ms: AtomicU64::new(NEVER),
            first_delete_after_kill_ms: AtomicU64::new(NEVER),
        }
    }

    fn record_failure(&self, invoked_ms: u64, completed_ms: u64) {
        let kill_ms = self.kill_ms.load(Ordering::Acquire);
        if kill_ms == NEVER || invoked_ms < kill_ms {
            return;
        }
        self.first_failure_after_kill_ms
            .fetch_min(completed_ms, Ordering::Relaxed);
        self.last_failure_after_kill_ms
            .fetch_max(completed_ms, Ordering::Relaxed);
        self.failures_after_kill.fetch_add(1, Ordering::Relaxed);
    }

    fn record_success(&self, operation: Operation, invoked_ms: u64, completed_ms: u64) {
        let kill_ms = self.kill_ms.load(Ordering::Acquire);
        if kill_ms == NEVER || invoked_ms < kill_ms {
            return;
        }
        let field = match operation {
            Operation::Get => &self.first_read_after_kill_ms,
            Operation::Put => &self.first_write_after_kill_ms,
            Operation::Delete => &self.first_delete_after_kill_ms,
        };
        let _ = field.compare_exchange(NEVER, completed_ms, Ordering::AcqRel, Ordering::Relaxed);
    }

    fn value(value: &AtomicU64) -> Option<f64> {
        let value = value.load(Ordering::Acquire);
        (value != NEVER).then_some(value as f64 / 1_000.0)
    }
}

#[derive(Default)]
struct WorkerStats {
    ops: BTreeMap<String, u64>,
    errors: BTreeMap<String, u64>,
    retries: BTreeMap<String, u64>,
    retry_total: u64,
    capacity_backpressure: u64,
    may_have_applied: u64,
    rollbacks: u64,
    rollback_kinds: BTreeMap<String, u64>,
    impossible: u64,
    violations: Vec<String>,
    models: BTreeMap<u64, KeyModel>,
}

impl WorkerStats {
    fn merge(&mut self, other: WorkerStats) {
        for (key, value) in other.ops {
            *self.ops.entry(key).or_default() += value;
        }
        for (key, value) in other.errors {
            *self.errors.entry(key).or_default() += value;
        }
        for (key, value) in other.retries {
            *self.retries.entry(key).or_default() += value;
        }
        self.retry_total += other.retry_total;
        self.capacity_backpressure += other.capacity_backpressure;
        self.may_have_applied += other.may_have_applied;
        self.rollbacks += other.rollbacks;
        for (kind, count) in other.rollback_kinds {
            *self.rollback_kinds.entry(kind).or_default() += count;
        }
        self.impossible += other.impossible;
        if self.violations.len() < 100 {
            let room = 100 - self.violations.len();
            self.violations
                .extend(other.violations.into_iter().take(room));
        }
    }

    fn requests(&self) -> u64 {
        self.ops.values().sum::<u64>() + self.errors.values().sum::<u64>()
    }
}

fn error_label(error: &ClientError) -> String {
    match error {
        ClientError::Operation(failure) => format!("operation:{:?}", failure.code),
        ClientError::DeadlineExceeded { .. } => "deadline".to_owned(),
        ClientError::Rpc { code, .. } => format!("rpc:{code:?}"),
        ClientError::TopologyRefresh { .. } => "topology_refresh".to_owned(),
        ClientError::Topology(_) => "topology".to_owned(),
        ClientError::MissingVersion => "missing_version".to_owned(),
    }
}

fn error_code(error: &ClientError) -> Option<ErrorCode> {
    match error {
        ClientError::Operation(failure) => Some(failure.code),
        _ => None,
    }
}

fn mutation_outcome(error: &ClientError) -> MutationOutcome {
    match error {
        ClientError::Operation(failure) => failure.outcome,
        ClientError::DeadlineExceeded { outcome } => *outcome,
        ClientError::TopologyRefresh { outcome, .. } => *outcome,
        ClientError::Rpc { outcome, .. } => *outcome,
        ClientError::Topology(_) | ClientError::MissingVersion => MutationOutcome::KnownNotApplied,
    }
}

fn is_not_found(error: &ClientError) -> bool {
    matches!(
        error,
        ClientError::Operation(failure) if failure.code == ErrorCode::NotFound
    )
}

fn retry_delta(
    before: &BTreeMap<&'static str, u64>,
    after: &BTreeMap<&'static str, u64>,
) -> BTreeMap<String, u64> {
    after
        .iter()
        .filter_map(|(reason, count)| {
            let previous = before.get(reason).copied().unwrap_or(0);
            (*count > previous).then(|| ((*reason).to_owned(), count - previous))
        })
        .collect()
}

fn hex(value: &[u8]) -> String {
    value.iter().map(|byte| format!("{byte:02x}")).collect()
}

struct WorkerContext {
    worker: usize,
    keys: Vec<u64>,
    models: BTreeMap<u64, KeyModel>,
    endpoint: String,
    operation_timeout: Duration,
    value_bytes: usize,
    mix: OperationMix,
    started_at: Instant,
    stop: Arc<AtomicBool>,
    progress: Arc<Progress>,
    ledger: Arc<RequestLedger>,
    next_id: Arc<AtomicU64>,
}

async fn run_worker(context: WorkerContext) -> Result<WorkerStats> {
    let WorkerContext {
        worker,
        keys,
        mut models,
        endpoint,
        operation_timeout,
        value_bytes,
        mix,
        started_at,
        stop,
        progress,
        ledger,
        next_id,
    } = context;
    ensure!(!keys.is_empty(), "a load worker was started without keys");
    let client = HashringClient::connect(endpoint, operation_timeout).await?;
    let mut stats = WorkerStats::default();
    let mut step = 0_u64;
    while !stop.load(Ordering::Acquire) {
        let key = keys[(step as usize + worker) % keys.len()];
        let operation = mix.choose(step, worker);
        let value =
            (operation == Operation::Put).then(|| deterministic_value(key, step, value_bytes));
        let retry_before = client.retry_reasons();
        let retry_total_before = client.retry_count();
        let start_epoch = client.epoch().await;
        let invoked_unix_ms = unix_ms();
        let invoked = Instant::now();
        let invoked_ms = started_at.elapsed().as_millis() as u64;
        let mut entry = RequestEntry {
            id: next_id.fetch_add(1, Ordering::Relaxed),
            worker,
            key,
            operation: operation.name(),
            value_hex: value.as_deref().map(hex),
            invoked_unix_ms,
            completed_unix_ms: 0,
            elapsed_us: 0,
            start_epoch,
            end_epoch: start_epoch,
            result: "ok",
            error: None,
            error_code: None,
            outcome: None,
            version: None,
            topology_epoch: None,
            retries: 0,
            retry_reasons: BTreeMap::new(),
        };
        let model = models
            .get_mut(&key)
            .with_context(|| format!("worker {worker} has no model for key {key}"))?;
        match operation {
            Operation::Get => match client.get(key.to_be_bytes().to_vec()).await {
                Ok(output) => {
                    entry.end_epoch = output.topology_epoch;
                    entry.topology_epoch = Some(output.topology_epoch);
                    entry.value_hex = Some(hex(&output.value));
                    entry.version = Some(VersionView::from(&output.version));
                    *stats
                        .ops
                        .entry(operation.success_metric().to_owned())
                        .or_default() += 1;
                    progress.record_success(
                        operation,
                        invoked_ms,
                        started_at.elapsed().as_millis() as u64,
                    );
                    match model.observe(Some(&output.value), Some(&output.version)) {
                        Observation::Matches => {}
                        Observation::Rollback { first, kind } => {
                            if first {
                                stats.rollbacks += 1;
                                *stats.rollback_kinds.entry(kind.to_owned()).or_default() += 1;
                            }
                        }
                        Observation::Impossible => {
                            stats.impossible += 1;
                            if stats.violations.len() < 100 {
                                stats.violations.push(format!(
                                    "GET key {key} returned an impossible value at epoch {}",
                                    output.topology_epoch
                                ));
                            }
                        }
                    }
                }
                Err(error) if is_not_found(&error) => {
                    entry.result = "not_found";
                    *stats.ops.entry("get_not_found".to_owned()).or_default() += 1;
                    progress.record_success(
                        operation,
                        invoked_ms,
                        started_at.elapsed().as_millis() as u64,
                    );
                    match model.observe(None, None) {
                        Observation::Matches => {}
                        Observation::Rollback { first, kind } => {
                            if first {
                                stats.rollbacks += 1;
                                *stats.rollback_kinds.entry(kind.to_owned()).or_default() += 1;
                            }
                        }
                        Observation::Impossible => {
                            stats.impossible += 1;
                            if stats.violations.len() < 100 {
                                stats.violations.push(format!(
                                    "GET key {key} found no record where the history required one"
                                ));
                            }
                        }
                    }
                }
                Err(error) => {
                    entry.result = "error";
                    entry.error = Some(error.to_string().chars().take(256).collect());
                    entry.error_code = error_code(&error).map(|code| format!("{code:?}"));
                    entry.outcome = Some(match mutation_outcome(&error) {
                        MutationOutcome::KnownNotApplied => "known_not_applied",
                        MutationOutcome::MayHaveApplied => "may_have_applied",
                    });
                    *stats.errors.entry(error_label(&error)).or_default() += 1;
                    progress.record_failure(invoked_ms, started_at.elapsed().as_millis() as u64);
                }
            },
            Operation::Put => {
                let value = value.expect("a PUT carries a value");
                match client.put(key.to_be_bytes().to_vec(), value.clone()).await {
                    Ok(output) => {
                        entry.end_epoch = output.topology_epoch;
                        entry.topology_epoch = Some(output.topology_epoch);
                        entry.version = Some(VersionView::from(&output.version));
                        *stats
                            .ops
                            .entry(operation.success_metric().to_owned())
                            .or_default() += 1;
                        progress.record_success(
                            operation,
                            invoked_ms,
                            started_at.elapsed().as_millis() as u64,
                        );
                        model.on_put(value, output.version);
                    }
                    Err(error) => {
                        entry.result = "error";
                        entry.error = Some(error.to_string().chars().take(256).collect());
                        entry.error_code = error_code(&error).map(|code| format!("{code:?}"));
                        let outcome = mutation_outcome(&error);
                        entry.outcome = Some(match outcome {
                            MutationOutcome::KnownNotApplied => "known_not_applied",
                            MutationOutcome::MayHaveApplied => "may_have_applied",
                        });
                        *stats.errors.entry(error_label(&error)).or_default() += 1;
                        if outcome == MutationOutcome::MayHaveApplied {
                            stats.may_have_applied += 1;
                            model.on_ambiguous_put(value);
                        }
                        if error_code(&error) == Some(ErrorCode::ResourceExhausted) {
                            stats.capacity_backpressure += 1;
                        }
                        progress
                            .record_failure(invoked_ms, started_at.elapsed().as_millis() as u64);
                    }
                }
            }
            Operation::Delete => match client.delete(key.to_be_bytes().to_vec()).await {
                Ok(output) => {
                    entry.end_epoch = output.topology_epoch;
                    entry.topology_epoch = Some(output.topology_epoch);
                    entry.version = Some(VersionView::from(&output.version));
                    *stats
                        .ops
                        .entry(operation.success_metric().to_owned())
                        .or_default() += 1;
                    progress.record_success(
                        operation,
                        invoked_ms,
                        started_at.elapsed().as_millis() as u64,
                    );
                    model.on_delete(output.version);
                }
                Err(error) => {
                    entry.result = "error";
                    entry.error = Some(error.to_string().chars().take(256).collect());
                    entry.error_code = error_code(&error).map(|code| format!("{code:?}"));
                    let outcome = mutation_outcome(&error);
                    entry.outcome = Some(match outcome {
                        MutationOutcome::KnownNotApplied => "known_not_applied",
                        MutationOutcome::MayHaveApplied => "may_have_applied",
                    });
                    *stats.errors.entry(error_label(&error)).or_default() += 1;
                    if outcome == MutationOutcome::MayHaveApplied {
                        stats.may_have_applied += 1;
                        model.on_ambiguous_delete();
                    }
                    if error_code(&error) == Some(ErrorCode::ResourceExhausted) {
                        stats.capacity_backpressure += 1;
                    }
                    progress.record_failure(invoked_ms, started_at.elapsed().as_millis() as u64);
                }
            },
        }
        let completed = Instant::now();
        let retry_after = client.retry_reasons();
        entry.completed_unix_ms = unix_ms();
        entry.elapsed_us = invoked.elapsed().as_micros();
        entry.end_epoch = client.epoch().await;
        entry.retries = client.retry_count().saturating_sub(retry_total_before);
        entry.retry_reasons = retry_delta(&retry_before, &retry_after);
        for (reason, count) in &entry.retry_reasons {
            *stats.retries.entry(reason.clone()).or_default() += *count;
            if reason == "resource_exhausted" {
                stats.capacity_backpressure += *count;
            }
        }
        stats.retry_total += entry.retries;
        ledger.record(&entry)?;
        let _ = completed;
        step += 1;
    }
    stats.models = models;
    Ok(stats)
}

#[derive(Serialize)]
struct FaultRound {
    round: u32,
    scenario: &'static str,
    victim: String,
    victim_epoch: u64,
    cohort_keys: usize,
    affected_ranges: usize,
    owner_unchanged: bool,
    pre_failure_replica_progress: Value,
    replica_status_after_failure: Value,
    kill_to_publish_seconds: f64,
    read_recovery_seconds: Option<f64>,
    write_recovery_seconds: Option<f64>,
    delete_recovery_seconds: Option<f64>,
    first_failure_after_kill_ms: Option<u64>,
    last_failure_after_kill_ms: Option<u64>,
    failures_after_kill: u64,
    replacement_to_full_rf_seconds: f64,
    failure_to_full_rf_seconds: f64,
    final_epoch: u64,
    collateral_victim_owned_keys: u64,
    collateral_rollbacks: u64,
    load: Value,
    rollbacks: u64,
    impossible_reads: u64,
    violations: Vec<String>,
    new_owner_counts: BTreeMap<String, u64>,
}

struct Victim {
    node_id: String,
    cohort: Vec<u64>,
    owner_by_key: BTreeMap<u64, String>,
}

fn select_victim(
    config: &ExperimentConfig,
    topology: &TopologySnapshot,
    round: u32,
    killed: &BTreeSet<String>,
) -> Result<Victim> {
    let candidates: Vec<&Member> = topology
        .members
        .iter()
        .filter(|member| !killed.contains(&member.node_id))
        .collect();
    ensure!(!candidates.is_empty(), "fault run has no live candidate");
    let minimum = (config.key_count as usize).clamp(1, MIN_COHORT);
    let start = (round as usize - 1) % candidates.len();
    for offset in 0..candidates.len() {
        let candidate = candidates[(start + offset) % candidates.len()];
        let mut cohort = Vec::new();
        let mut owner_by_key = BTreeMap::new();
        for key in 0..config.key_count {
            let token = topology.key_token(&key.to_be_bytes());
            let replicas = topology.replica_node_ids_for_token(token)?;
            let owner = replicas[0];
            let is_victim = match config.fault_scenario {
                FaultScenario::Owner => owner == candidate.node_id,
                FaultScenario::Follower => {
                    owner != candidate.node_id
                        && replicas[1..].contains(&candidate.node_id.as_str())
                }
            };
            if is_victim {
                cohort.push(key);
                owner_by_key.insert(key, owner.to_owned());
            }
        }
        if cohort.len() >= minimum {
            return Ok(Victim {
                node_id: candidate.node_id.clone(),
                cohort,
                owner_by_key,
            });
        }
    }
    anyhow::bail!(
        "no member is a {} for at least {minimum} keys",
        match config.fault_scenario {
            FaultScenario::Owner => "owner",
            FaultScenario::Follower => "follower",
        }
    )
}

fn ranges_for_keys(
    status: &ReplicaStatusResponse,
    topology: &TopologySnapshot,
    keys: &[u64],
) -> (Value, usize) {
    let mut selected = BTreeMap::new();
    for &key in keys {
        let token = topology.key_token(&key.to_be_bytes());
        if let Some(range) = status.ranges.iter().find(|range| {
            if range.start_exclusive < range.end_inclusive {
                token > range.start_exclusive && token <= range.end_inclusive
            } else {
                token > range.start_exclusive || token <= range.end_inclusive
            }
        }) {
            selected.insert(
                (
                    range.start_exclusive,
                    range.end_inclusive,
                    range.owner_node_id.clone(),
                ),
                json!({
                    "start_exclusive": range.start_exclusive,
                    "end_inclusive": range.end_inclusive,
                    "owner_node_id": range.owner_node_id,
                    "current_rf": range.current_rf,
                    "live_rf": range.live_rf,
                    "writable": range.writable,
                    "under_replicated": range.under_replicated,
                    "write_block_reason": range.write_block_reason,
                    "followers": range
                        .followers
                        .iter()
                        .map(|follower| {
                            json!({
                                "node_id": follower.node_id,
                                "admitted": follower.admitted,
                                "lag_millis": follower.lag_millis,
                                "lag_known": follower.lag_known,
                                "stream_head": follower.stream_head,
                                "stream_cursor": follower.stream_cursor,
                                "last_ack_sequence": follower.last_ack_sequence,
                                "last_ack_known": follower.last_ack_known,
                                "healthy": follower.healthy,
                            })
                        })
                        .collect::<Vec<_>>(),
                }),
            );
        }
    }
    let ranges: Vec<Value> = selected.into_values().collect();
    let count = ranges.len();
    (json!({ "ranges": ranges }), count)
}

fn cohort_replica_stats(
    status: &ReplicaStatusResponse,
    topology: &TopologySnapshot,
    keys: &[u64],
) -> (Option<usize>, usize, usize) {
    let mut live_rf = usize::MAX;
    let mut under_replicated = 0;
    let mut not_writable = 0;
    let mut seen = BTreeMap::new();
    for &key in keys {
        let token = topology.key_token(&key.to_be_bytes());
        let Some(range) = status.ranges.iter().find(|range| {
            if range.start_exclusive < range.end_inclusive {
                token > range.start_exclusive && token <= range.end_inclusive
            } else {
                token > range.start_exclusive || token <= range.end_inclusive
            }
        }) else {
            continue;
        };
        if seen
            .insert((range.start_exclusive, range.end_inclusive), range.live_rf)
            .is_some()
        {
            continue;
        }
        live_rf = live_rf.min(range.live_rf as usize);
        under_replicated += usize::from(range.under_replicated);
        not_writable += usize::from(!range.writable);
    }
    (
        (live_rf != usize::MAX).then_some(live_rf),
        under_replicated,
        not_writable,
    )
}

fn load_json(stats: &WorkerStats, elapsed: Duration) -> Value {
    json!({
        "requests": stats.requests(),
        "seconds": elapsed.as_secs_f64(),
        "operations_per_second": stats.requests() as f64 / elapsed.as_secs_f64().max(f64::EPSILON),
        "ops": stats.ops,
        "errors": stats.errors,
        "retries": stats.retries,
        "retry_total": stats.retry_total,
        "capacity_backpressure": stats.capacity_backpressure,
        "may_have_applied": stats.may_have_applied,
        "rollback_kinds": stats.rollback_kinds,
    })
}

pub(crate) async fn run(context: RunContext<'_>) -> Result<u64> {
    let RunContext {
        config,
        members,
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
    let scenario = config.fault_scenario;
    let scenario_name = match scenario {
        FaultScenario::Owner => "owner",
        FaultScenario::Follower => "follower",
    };
    let mix = OperationMix::parse(&config.mix)?;
    let operation_timeout = Duration::from_millis(config.operation_timeout_ms);
    let observer =
        HashringClient::connect(coordinator_endpoint.to_owned(), Duration::from_secs(2)).await?;
    let ledger = Arc::new(RequestLedger::open(
        &config.output_dir.join("requests.jsonl"),
    )?);
    let next_id = Arc::new(AtomicU64::new(1));

    // A key's expected state starts at the value the initial dataset wrote.
    // The version is unknown until the first cohort read, so it matches any
    // version; the value is exact, which is enough to catch an initial value
    // that changed without a mutation in the ledger.
    let mut models: BTreeMap<u64, KeyModel> = (0..config.key_count)
        .map(|key| {
            (
                key,
                KeyModel::new(PossibleState {
                    value: Some(deterministic_value(key, 0, config.value_bytes)),
                    version: None,
                }),
            )
        })
        .collect();
    let mut rounds = Vec::new();
    let mut killed = BTreeSet::new();
    let mut total_rollbacks = 0_u64;
    let mut total_impossible = 0_u64;
    let mut total_capacity = 0_u64;
    let mut total_requests = 0_u64;
    let started = Instant::now();

    for round in 1..=config.fault_rounds {
        let topology = admin_client.refresh_topology().await?;
        let victim = select_victim(config, &topology, round, &killed)?;
        if round == 1 {
            events.record(
                "fault_cohort_selected",
                json!({
                    "scenario": scenario_name,
                    "victim": victim.node_id,
                    "cohort_keys": victim.cohort.len(),
                }),
            )?;
        }
        // The ledger's model must start from the value this run sees now, not
        // from a previous round's expectation. A read also gives the pre-failure
        // version the rollback check can recognize.
        for &key in &victim.cohort {
            let state = match workload_client.get(key.to_be_bytes().to_vec()).await {
                Ok(output) => PossibleState::present(output.value, output.version),
                Err(error) if is_not_found(&error) => PossibleState::absent(None),
                Err(error) => {
                    return Err(error)
                        .with_context(|| format!("reading cohort key {key} before round {round}"));
                }
            };
            models
                .get_mut(&key)
                .expect("every dataset key has a model")
                .resync(state);
        }
        let status = observer.replica_status().await?;
        let (pre_progress, affected_ranges) = ranges_for_keys(&status, &topology, &victim.cohort);
        events.record(
            "fault_pre_failure_replica_progress",
            json!({ "round": round, "ranges": affected_ranges }),
        )?;

        let workers = config.concurrency.min(victim.cohort.len()).max(1);
        let stop = Arc::new(AtomicBool::new(false));
        let progress = Arc::new(Progress::new());
        let started_at = Instant::now();
        let mut handles = Vec::new();
        for worker in 0..workers {
            let keys: Vec<u64> = victim
                .cohort
                .iter()
                .copied()
                .skip(worker)
                .step_by(workers)
                .collect();
            let mut worker_models = BTreeMap::new();
            for key in &keys {
                worker_models.insert(*key, models.remove(key).expect("cohort key has a model"));
            }
            handles.push(tokio::spawn(run_worker(WorkerContext {
                worker,
                keys,
                models: worker_models,
                endpoint: coordinator_endpoint.to_owned(),
                operation_timeout,
                value_bytes: config.value_bytes,
                mix: mix.clone(),
                started_at,
                stop: Arc::clone(&stop),
                progress: Arc::clone(&progress),
                ledger: Arc::clone(&ledger),
                next_id: Arc::clone(&next_id),
            })));
        }
        events.record(
            "fault_load_started",
            json!({
                "round": round,
                "scenario": scenario_name,
                "workers": workers,
                "cohort_keys": victim.cohort.len(),
                "pre_seconds": config.fault_pre_seconds,
            }),
        )?;
        tokio::time::sleep(Duration::from_secs(config.fault_pre_seconds)).await;
        processes.ensure_running()?;

        let kill_at = Instant::now();
        let (exit, _) = processes.kill(&victim.node_id)?;
        let kill_elapsed = (kill_at - started_at).as_millis() as u64;
        progress.kill_ms.store(kill_elapsed, Ordering::Release);
        events.record(
            "fault_injected",
            json!({
                "round": round,
                "scenario": scenario_name,
                "victim": victim.node_id,
                "exit": exit,
                "epoch": topology.epoch,
                "cohort_keys": victim.cohort.len(),
            }),
        )?;

        let publish_deadline = Instant::now() + Duration::from_secs(60);
        let mut min_live_rf = usize::MAX;
        let mut max_under_replicated = 0_usize;
        let mut writable_false = 0_usize;
        let published = loop {
            processes.ensure_running()?;
            let current = observer.refresh_topology().await?;
            if let Ok(Ok(status)) =
                tokio::time::timeout(Duration::from_secs(2), observer.replica_status()).await
            {
                let (live_rf, under_replicated, not_writable) =
                    cohort_replica_stats(&status, &current, &victim.cohort);
                if let Some(live_rf) = live_rf {
                    min_live_rf = min_live_rf.min(live_rf);
                }
                max_under_replicated = max_under_replicated.max(under_replicated);
                writable_false = writable_false.max(not_writable);
            }
            if current.epoch > topology.epoch
                && !current
                    .members
                    .iter()
                    .any(|member| member.node_id == victim.node_id)
            {
                break current;
            }
            ensure!(
                Instant::now() < publish_deadline,
                "the coordinator did not publish the failure of {} within 60 seconds",
                victim.node_id
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        };
        let publish_at = Instant::now();
        let kill_to_publish_seconds = (publish_at - kill_at).as_secs_f64();
        // A follower loss must not move the owner of a selected key. The
        // replacement that follows is a normal membership change and may move
        // keys, so this is checked on the post-failure topology, not the final
        // one.
        let owner_unchanged = match config.fault_scenario {
            FaultScenario::Owner => true,
            FaultScenario::Follower => victim.cohort.iter().all(|key| {
                published.owner(&key.to_be_bytes()).is_ok_and(|owner| {
                    victim
                        .owner_by_key
                        .get(key)
                        .is_some_and(|expected| expected == &owner.node_id)
                })
            }),
        };
        events.record(
            "fault_published",
            json!({
                "round": round,
                "epoch": published.epoch,
                "seconds": kill_to_publish_seconds,
                "members": published.members.iter().map(|member| member.node_id.clone()).collect::<Vec<_>>(),
            }),
        )?;

        // The failover publishes before its cleanup finishes; a replacement
        // cannot be staged until the automatic change is terminal.
        let change_deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let active = admin_client.topology_change().await?;
            if active.is_none_or(|active| active.phase.is_terminal()) {
                break;
            }
            ensure!(
                Instant::now() < change_deadline,
                "the automatic failover change did not finish within 30 seconds"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let (post_failure_min_live_rf, post_failure_under_replicated, post_failure_not_writable) =
            match observer.replica_status().await {
                Ok(status) => cohort_replica_stats(&status, &published, &victim.cohort),
                Err(error) => {
                    events.record(
                        "fault_post_failure_status_failed",
                        json!({ "round": round, "error": error.to_string() }),
                    )?;
                    (None, 0, 0)
                }
            };

        // Keep the load running through the replacement and RF repair. The
        // new member is staged first so the coordinator can copy into it.
        let spare = &members[initial_count + round as usize - 1];
        let spare_port = ports[initial_count + round as usize];
        let mut target = published.members.clone();
        target.push(spare.clone());
        let change = admin_client.begin_topology_change(target).await?;
        let replacement_started = Instant::now();
        spawn_node(
            processes,
            spare.node_id.clone(),
            spare_port,
            coordinator_endpoint,
        )?;
        wait_for_listener(spare_port, processes).await?;
        let completed = admin_client
            .execute_topology_change(
                change.change_id,
                change.base_epoch,
                change.target_topology.epoch,
            )
            .await?;
        wait_for_full_rf(
            admin_client,
            config.desired_replication_factor,
            Duration::from_millis(config.migration_timeout_ms),
            processes,
        )
        .await?;
        let replacement_to_full_rf_seconds = replacement_started.elapsed().as_secs_f64();
        let failure_to_full_rf_seconds = kill_at.elapsed().as_secs_f64();
        events.record(
            "fault_full_rf_restored",
            json!({
                "round": round,
                "epoch": completed.target_topology.epoch,
                "replacement": spare.node_id,
                "replacement_to_full_rf_seconds": replacement_to_full_rf_seconds,
                "failure_to_full_rf_seconds": failure_to_full_rf_seconds,
            }),
        )?;

        tokio::time::sleep(Duration::from_secs(config.fault_post_seconds)).await;
        stop.store(true, Ordering::Release);
        let load_elapsed = started_at.elapsed();
        let mut stats = WorkerStats::default();
        let mut all_models = std::mem::take(&mut models);
        for handle in handles {
            let mut worker = handle.await.context("a fault load worker panicked")??;
            all_models.append(&mut worker.models);
            stats.merge(worker);
        }
        models = all_models;
        ledger.flush()?;
        let read_recovery_seconds = Progress::value(&progress.first_read_after_kill_ms);
        let write_recovery_seconds = Progress::value(&progress.first_write_after_kill_ms);
        let delete_recovery_seconds = Progress::value(&progress.first_delete_after_kill_ms);
        let first_failure_after_kill_ms = {
            let value = progress.first_failure_after_kill_ms.load(Ordering::Acquire);
            (value != NEVER).then_some(value)
        };
        let failures_after_kill = progress.failures_after_kill.load(Ordering::Acquire);
        let last_failure_after_kill_ms = (failures_after_kill > 0)
            .then(|| progress.last_failure_after_kill_ms.load(Ordering::Acquire));

        // Verify every key after repair. Keys the follower scenario's victim
        // owned are collateral owner failures; they are recorded separately,
        // because this scenario's claim is about keys whose owner stayed alive.
        let collateral: BTreeSet<u64> = match config.fault_scenario {
            FaultScenario::Follower => (0..config.key_count)
                .filter(|key| {
                    topology
                        .owner(&key.to_be_bytes())
                        .is_ok_and(|owner| owner.node_id == victim.node_id)
                })
                .collect(),
            FaultScenario::Owner => BTreeSet::new(),
        };
        let mut collateral_rollbacks = 0_u64;
        for key in 0..config.key_count {
            let result = workload_client.get(key.to_be_bytes().to_vec()).await;
            let (value, version) = match result {
                Ok(output) => (Some(output.value), Some(output.version)),
                Err(error) if is_not_found(&error) => (None, None),
                Err(error) => {
                    return Err(error)
                        .with_context(|| format!("verifying key {key} after round {round}"));
                }
            };
            let model = models.get_mut(&key).expect("every dataset key has a model");
            let observation = model.observe(value.as_deref(), version.as_ref());
            if collateral.contains(&key) {
                if matches!(observation, Observation::Rollback { first: true, .. }) {
                    collateral_rollbacks += 1;
                }
                let state = match value {
                    Some(value) => PossibleState::present(value, version.expect("present value")),
                    None => PossibleState::absent(None),
                };
                model.resync(state);
                continue;
            }
            match observation {
                Observation::Matches => {}
                Observation::Rollback { first, kind } => {
                    if first {
                        stats.rollbacks += 1;
                        *stats.rollback_kinds.entry(kind.to_owned()).or_default() += 1;
                    }
                }
                Observation::Impossible => {
                    stats.impossible += 1;
                    if stats.violations.len() < 100 {
                        stats.violations.push(format!(
                            "final GET key {key} returned a value the history does not allow"
                        ));
                    }
                }
            }
        }

        let final_topology = admin_client.refresh_topology().await?;
        let final_epoch = final_topology.epoch;
        let mut new_owner_counts = BTreeMap::new();
        for &key in &victim.cohort {
            if let Ok(owner) = final_topology.owner(&key.to_be_bytes()) {
                *new_owner_counts.entry(owner.node_id.clone()).or_default() += 1;
            }
        }
        total_rollbacks += stats.rollbacks;
        total_impossible += stats.impossible;
        total_capacity += stats.capacity_backpressure;
        total_requests += stats.requests();
        let load = load_json(&stats, load_elapsed);
        let record = FaultRound {
            round,
            scenario: scenario_name,
            victim: victim.node_id.clone(),
            victim_epoch: topology.epoch,
            cohort_keys: victim.cohort.len(),
            affected_ranges,
            owner_unchanged,
            pre_failure_replica_progress: pre_progress,
            replica_status_after_failure: json!({
                "min_live_rf_during_failure": (min_live_rf != usize::MAX).then_some(min_live_rf),
                "max_under_replicated_ranges_during_failure": max_under_replicated,
                "max_not_writable_ranges_during_failure": writable_false,
                "min_live_rf_after_publish": post_failure_min_live_rf,
                "under_replicated_ranges_after_publish": post_failure_under_replicated,
                "not_writable_ranges_after_publish": post_failure_not_writable,
            }),
            kill_to_publish_seconds,
            read_recovery_seconds,
            write_recovery_seconds,
            delete_recovery_seconds,
            first_failure_after_kill_ms,
            last_failure_after_kill_ms,
            failures_after_kill,
            replacement_to_full_rf_seconds,
            failure_to_full_rf_seconds,
            final_epoch,
            collateral_victim_owned_keys: collateral.len() as u64,
            collateral_rollbacks,
            load,
            rollbacks: stats.rollbacks,
            impossible_reads: stats.impossible,
            violations: stats.violations.clone(),
            new_owner_counts,
        };
        events.record("fault_round_complete", serde_json::to_value(&record)?)?;
        measurements.insert(
            format!("fault_round_{round}_load"),
            measurement(stats.requests(), load_elapsed),
        );
        rounds.push(record);
        killed.insert(victim.node_id.clone());

        ensure!(
            stats.impossible == 0,
            "round {round} observed {} values the history does not allow: {:?}",
            stats.impossible,
            stats.violations
        );
        ensure!(
            stats.requests() > 0,
            "round {round} completed no requests under load"
        );
        if config.fault_scenario == FaultScenario::Follower {
            ensure!(
                stats.ops.get("put_ok").copied().unwrap_or(0) > 0
                    && stats
                        .ops
                        .get("get_ok")
                        .copied()
                        .unwrap_or(0)
                        .saturating_add(stats.ops.get("get_not_found").copied().unwrap_or(0))
                        > 0,
                "round {round}: follower loss stopped the load's writes or reads"
            );
        }
        ensure!(
            owner_unchanged,
            "round {round}: killing a follower changed the owner of a selected key"
        );
        if config.fault_scenario == FaultScenario::Follower
            || config.write_ack_policy != WriteAckPolicy::OwnerOnly
        {
            ensure!(
                stats.rollbacks == 0,
                "round {round} rolled back {} acknowledged changes",
                stats.rollbacks
            );
        }
    }

    observations.insert("fault_scenario".into(), json!(scenario_name));
    observations.insert("fault_round_count".into(), json!(config.fault_rounds));
    observations.insert("fault_rounds".into(), serde_json::to_value(&rounds)?);
    observations.insert(
        "fault_acknowledged_rollbacks".into(),
        json!(total_rollbacks),
    );
    observations.insert("fault_impossible_reads".into(), json!(total_impossible));
    observations.insert("fault_capacity_backpressure".into(), json!(total_capacity));
    observations.insert("fault_requests".into(), json!(total_requests));
    observations.insert(
        "fault_total_seconds".into(),
        json!(started.elapsed().as_secs_f64()),
    );
    events.record(
        "fault_run_complete",
        json!({
            "rounds": config.fault_rounds,
            "rollbacks": total_rollbacks,
            "impossible_reads": total_impossible,
            "requests": total_requests,
            "seconds": started.elapsed().as_secs_f64(),
        }),
    )?;
    Ok(rounds.last().map_or(0, |round| round.final_epoch))
}
