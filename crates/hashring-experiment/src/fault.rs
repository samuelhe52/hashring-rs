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
    collections::{BTreeMap, BTreeSet, HashMap, VecDeque},
    fs::File,
    hash::{DefaultHasher, Hash, Hasher},
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
    /// The number of slots that carry a GET.
    get: u64,
    /// The number of slots that carry a PUT.
    put: u64,
    /// The number of slots that carry a DELETE.
    delete: u64,
    /// The number of slots the mix divides the operations into.
    total: u64,
    /// The step of the walk over the slots.
    stride: u64,
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
        let total = parts
            .iter()
            .fold(0_u64, |sum, part| sum.saturating_add(*part));
        ensure!(total > 0, "the operation mix is empty");
        Ok(Self {
            get: parts[0],
            put: parts[1],
            delete: parts[2],
            total,
            stride: coprime_stride(total),
        })
    }

    fn choose(&self, step: u64, worker: usize) -> Operation {
        // A walk whose step shares no factor with the total reaches every
        // slot, so every worker offers every operation instead of only the
        // ones its own step happens to land on.
        let slot = (step
            .wrapping_mul(self.stride)
            .wrapping_add(worker as u64 * 11))
            % self.total;
        if slot < self.get {
            Operation::Get
        } else if slot < self.get.saturating_add(self.put) {
            Operation::Put
        } else {
            Operation::Delete
        }
    }

    /// Whether the mix offers the operation at all.
    fn offers(&self, operation: Operation) -> bool {
        match operation {
            Operation::Get => self.get > 0,
            Operation::Put => self.put > 0,
            Operation::Delete => self.delete > 0,
        }
    }
}

/// The smallest step at or above 37 that shares no factor with the total.
///
/// A fixed step only reaches every slot when it is coprime with the total, so
/// the step is searched for instead of assumed: a step of 37 leaves a worker
/// without one of the three operations whenever the total is a multiple of 37.
fn coprime_stride(total: u64) -> u64 {
    if total <= 1 {
        return 1;
    }
    let mut stride = 37;
    while gcd(stride, total) != 1 {
        stride += 1;
    }
    stride
}

fn gcd(mut left: u64, mut right: u64) -> u64 {
    while right != 0 {
        let remainder = left % right;
        left = right;
        right = remainder;
    }
    left
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
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

/// The digest of a value.
///
/// The acknowledged history keeps one digest per state instead of the whole
/// value, so a long run does not grow the driver's memory with the size of
/// the data it wrote.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct ValueDigest([u8; 32]);

impl ValueDigest {
    fn of(value: &[u8]) -> Self {
        Self(*blake3::hash(value).as_bytes())
    }
}

/// The digest of a record version.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct VersionDigest([u8; 32]);

impl VersionDigest {
    fn of(version: &RecordVersion) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(&version.topology_epoch.to_be_bytes());
        hasher.update(&version.owner_sequence.to_be_bytes());
        hasher.update(version.owner_node_id.as_bytes());
        Self(*hasher.finalize().as_bytes())
    }
}

/// One state the run acknowledged, kept as digests.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct AcknowledgedState {
    /// `None` when the key was absent.
    value: Option<ValueDigest>,
    /// `None` when the run never saw a version for this state.
    version: Option<VersionDigest>,
}

impl AcknowledgedState {
    fn of(state: &PossibleState) -> Self {
        Self {
            value: state.value.as_deref().map(ValueDigest::of),
            version: state.version.as_ref().map(VersionDigest::of),
        }
    }

    /// The state an observation describes.
    ///
    /// The digests are taken once per observation: the history holds up to
    /// [`KeyModel::window`] states, and hashing the value once per state would
    /// cost a hash per state per read.
    fn of_observation(value: Option<&[u8]>, version: Option<&RecordVersion>) -> Self {
        Self {
            value: value.map(ValueDigest::of),
            version: version.map(VersionDigest::of),
        }
    }

    /// Whether the observation is this acknowledged state.
    fn matches(&self, observation: &AcknowledgedState) -> bool {
        match (&self.value, &observation.value) {
            (None, None) => true,
            (Some(expected), Some(observed)) => {
                expected == observed
                    && self
                        .version
                        .as_ref()
                        .is_none_or(|expected| Some(expected) == observation.version.as_ref())
            }
            _ => false,
        }
    }

    /// A fingerprint for the window's membership index.
    ///
    /// The index holds a 64-bit hash rather than a second copy of the state,
    /// which keeps the window's memory close to the digests themselves. Two
    /// states can share a fingerprint: the index counts it, so a hit that
    /// finds no matching state is a collision, and the state is new after all.
    fn fingerprint(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
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
        eligible: bool,
    },
    /// The observation is not explainable by the recorded history, and the
    /// history is too short to say whether it is an allowed rollback.
    Undecidable,
    /// The observation is not explainable by the recorded history.
    Impossible,
}

#[derive(Clone, Debug)]
struct KeyModel {
    possible: Vec<PossibleState>,
    /// Definite and uncertain mutation candidates and concretely observed
    /// states, as digests, in the order each was last recorded. A later read
    /// that matches one of these but not `possible` is a rollback rather than an arbitrary
    /// value. The list is bounded by `window`, and the states it dropped are
    /// counted.
    acknowledged: VecDeque<AcknowledgedState>,
    /// The states `acknowledged` holds, by fingerprint and count, so
    /// acknowledging a state the window has never held does not scan the
    /// window: only a state that is already in it needs its position. A
    /// fingerprint is one hash instead of a second copy of the state, and two
    /// states can share one: the count keeps the index right when one of them
    /// leaves the window.
    known: HashMap<u64, usize>,
    /// How many acknowledged states the bound dropped.
    dropped_acknowledged: u64,
    /// How many acknowledged states the key keeps. It has to cover the writes
    /// the key takes during the load: a rollback to a state that left the
    /// window cannot be told apart from a value the history never held.
    window: usize,
    rolled_back: bool,
    /// The newest topology in which a mutation or read established this key.
    /// A failure of an older owner cannot undo a state established after it.
    established_epoch: u64,
}

impl KeyModel {
    fn new(state: PossibleState, window: usize) -> Self {
        let mut model = Self {
            possible: Vec::new(),
            acknowledged: VecDeque::new(),
            known: HashMap::new(),
            dropped_acknowledged: 0,
            window,
            rolled_back: false,
            established_epoch: 0,
        };
        model.resync(state);
        model
    }

    fn begin_round(&mut self) {
        self.rolled_back = false;
    }

    fn resync(&mut self, state: PossibleState) {
        if let Some(version) = &state.version {
            self.established_epoch = self.established_epoch.max(version.topology_epoch);
        }
        self.possible = vec![state.clone()];
        self.remember(&state);
    }

    /// Keeps an acknowledged state, and forgets the oldest one when the key's
    /// history is full.
    ///
    /// The window holds the most recently acknowledged states: a state that is
    /// acknowledged again moves to the back, so a rollback to it is still
    /// recognized after newer writes pushed it out of the window.
    fn remember(&mut self, state: &PossibleState) {
        let acknowledged = AcknowledgedState::of(state);
        let fingerprint = acknowledged.fingerprint();
        if self.acknowledged.back() == Some(&acknowledged) {
            return;
        }
        if self.known.contains_key(&fingerprint) {
            // A state the window already holds is acknowledged again, which is
            // rare: find its place in the order and move it to the back.
            if let Some(position) = self
                .acknowledged
                .iter()
                .position(|entry| *entry == acknowledged)
            {
                if position + 1 != self.acknowledged.len() {
                    let entry = self
                        .acknowledged
                        .remove(position)
                        .expect("the position is inside the window");
                    self.acknowledged.push_back(entry);
                }
                return;
            }
            // A fingerprint hit that finds no state is a collision: another
            // state in the window has the same fingerprint. This state is new,
            // and the count it adds keeps the other one indexed when it leaves.
        }
        if self.acknowledged.len() >= self.window
            && let Some(evicted) = self.acknowledged.pop_front()
        {
            self.forget(evicted.fingerprint());
            self.dropped_acknowledged += 1;
        }
        *self.known.entry(fingerprint).or_insert(0) += 1;
        self.acknowledged.push_back(acknowledged);
    }

    /// Drops one state with this fingerprint from the index.
    fn forget(&mut self, fingerprint: u64) {
        let remaining = match self.known.get_mut(&fingerprint) {
            Some(count) => {
                *count -= 1;
                *count
            }
            None => return,
        };
        if remaining == 0 {
            self.known.remove(&fingerprint);
        }
    }

    fn on_put(&mut self, value: Vec<u8>, version: RecordVersion) {
        self.resync(PossibleState::present(value, version));
    }

    fn on_delete(&mut self, version: RecordVersion) {
        self.resync(PossibleState::absent(Some(version)));
    }

    fn on_ambiguous_put(&mut self, value: Vec<u8>) {
        let state = PossibleState {
            value: Some(value),
            version: None,
        };
        self.remember(&state);
        self.possible.push(state);
    }

    fn on_ambiguous_delete(&mut self) {
        let state = PossibleState::absent(None);
        self.remember(&state);
        self.possible.push(state);
    }

    fn observe_at(
        &mut self,
        value: Option<&[u8]>,
        version: Option<&RecordVersion>,
        response_epoch: u64,
        failure_epoch: u64,
        failure_injected: bool,
    ) -> Observation {
        let eligible = failure_injected
            && response_epoch > failure_epoch
            && self.established_epoch <= failure_epoch
            && version.is_none_or(|version| version.topology_epoch <= failure_epoch);
        let observation = self.observe(value, version);
        // A successful read from the promoted owner establishes its state,
        // even when the record itself still carries the old owner's version.
        self.established_epoch = self.established_epoch.max(response_epoch);
        match observation {
            Observation::Rollback { first, kind, .. } => Observation::Rollback {
                first,
                kind,
                eligible,
            },
            other => other,
        }
    }

    fn observe(&mut self, value: Option<&[u8]>, version: Option<&RecordVersion>) -> Observation {
        if let Some(uncertain) = self
            .possible
            .iter()
            .find(|state| state.matches(value, version))
            .map(|state| state.version.is_none())
        {
            let state = PossibleState {
                value: value.map(<[u8]>::to_vec),
                version: version.cloned(),
            };
            // Once a GET identifies an ambiguous PUT's version, that concrete
            // state replaces its wildcard. Keep other historical candidates.
            let concrete = AcknowledgedState::of(&state);
            if uncertain
                && concrete.value.is_some()
                && concrete.version.is_some()
                && let Some(index) = self
                    .acknowledged
                    .iter()
                    .position(|entry| entry.value == concrete.value && entry.version.is_none())
            {
                let removed = self.acknowledged.remove(index).expect("known index");
                self.forget(removed.fingerprint());
            }
            self.resync(state);
            return Observation::Matches;
        }
        let observation = AcknowledgedState::of_observation(value, version);
        if self
            .acknowledged
            .iter()
            .any(|state| state.matches(&observation))
        {
            let first = !self.rolled_back;
            self.rolled_back = true;
            let expected_absent = self.possible.iter().any(|state| state.value.is_none());
            let kind = match (value, expected_absent) {
                (Some(_), true) => "acknowledged_delete_lost",
                (Some(_), false) => "older_value_reappeared",
                (None, _) => "acknowledged_value_lost",
            };
            // The read establishes the current state. Newer and uncertain
            // states remain historical candidates, rather than live branches.
            let state = match value {
                Some(value) => PossibleState {
                    value: Some(value.to_vec()),
                    version: version.cloned(),
                },
                None => PossibleState::absent(None),
            };
            self.resync(state);
            return Observation::Rollback {
                first,
                kind,
                eligible: false,
            };
        }
        if self.dropped_acknowledged > 0 {
            // The history no longer holds every acknowledged state, so this
            // read cannot be told apart from a rollback to a state that was
            // dropped. It is undecidable, not impossible.
            return Observation::Undecidable;
        }
        Observation::Impossible
    }
}

/// Requests issued after the follower was killed in its original placement.
/// A successful mutation counts only if its returned version still belongs
/// to that placement: later success after reconfiguration cannot prove that
/// the surviving owner was writable with its follower missing.
#[derive(Clone, Default, Serialize)]
struct ReducedRfProgress {
    puts: u64,
    deletes: u64,
    admission_rejections: u64,
    capacity_errors: u64,
    capacity_retries: u64,
}

/// What the load did after the node was killed.
///
/// Every figure in here is counted from the kill, not from the start of the
/// load: the load runs before the failure too, and a figure that included the
/// pre-failure window would report the window as the recovery time.
#[derive(Default)]
struct Progress {
    /// The milliseconds from the start of the load to the kill.
    kill_ms: AtomicU64,
    published_ms: AtomicU64,
    reduced_rf: Mutex<ReducedRfProgress>,
    /// The milliseconds from the kill to the first failure, and to the last.
    first_failure_after_kill_ms: AtomicU64,
    last_failure_after_kill_ms: AtomicU64,
    failures_after_kill: AtomicU64,
    /// The milliseconds from the kill to the first successful read, write and
    /// delete.
    first_read_after_kill_ms: AtomicU64,
    first_write_after_kill_ms: AtomicU64,
    first_delete_after_kill_ms: AtomicU64,
    /// The successes the load completed after the kill, by operation.
    reads_after_kill: AtomicU64,
    writes_after_kill: AtomicU64,
    deletes_after_kill: AtomicU64,
}

impl Progress {
    fn new() -> Self {
        Self {
            kill_ms: AtomicU64::new(NEVER),
            published_ms: AtomicU64::new(NEVER),
            reduced_rf: Mutex::new(ReducedRfProgress::default()),
            first_failure_after_kill_ms: AtomicU64::new(NEVER),
            last_failure_after_kill_ms: AtomicU64::new(0),
            failures_after_kill: AtomicU64::new(0),
            first_read_after_kill_ms: AtomicU64::new(NEVER),
            first_write_after_kill_ms: AtomicU64::new(NEVER),
            first_delete_after_kill_ms: AtomicU64::new(NEVER),
            reads_after_kill: AtomicU64::new(0),
            writes_after_kill: AtomicU64::new(0),
            deletes_after_kill: AtomicU64::new(0),
        }
    }

    fn record_reduced_rf(&self, entry: &RequestEntry, invoked_ms: u64, victim_epoch: u64) {
        let kill = self.kill_ms.load(Ordering::Acquire);
        let published = self.published_ms.load(Ordering::Acquire);
        if kill == NEVER
            || invoked_ms <= kill
            || invoked_ms >= published
            || entry.start_epoch != victim_epoch
            || entry.operation == "get"
        {
            return;
        }
        let mut reduced = self.reduced_rf.lock().expect("progress lock");
        if entry.result == "ok"
            && entry
                .version
                .as_ref()
                .is_some_and(|v| v.topology_epoch == victim_epoch)
        {
            match entry.operation {
                "put" => reduced.puts += 1,
                "delete" => reduced.deletes += 1,
                _ => {}
            }
        }
        for reason in ["temporarily_unavailable", "replica_not_ready"] {
            reduced.admission_rejections += entry.retry_reasons.get(reason).copied().unwrap_or(0);
        }
        if matches!(
            entry.error_code.as_deref(),
            Some("TemporarilyUnavailable" | "ReplicaNotReady")
        ) {
            reduced.admission_rejections += 1;
        }
        reduced.capacity_errors +=
            u64::from(entry.error_code.as_deref() == Some("ResourceExhausted"));
        reduced.capacity_retries += entry
            .retry_reasons
            .get("resource_exhausted")
            .copied()
            .unwrap_or(0);
    }

    fn record_failure(&self, invoked_ms: u64, completed_ms: u64) {
        let kill_ms = self.kill_ms.load(Ordering::Acquire);
        if kill_ms == NEVER || invoked_ms < kill_ms {
            return;
        }
        let after_kill = completed_ms.saturating_sub(kill_ms);
        self.first_failure_after_kill_ms
            .fetch_min(after_kill, Ordering::Relaxed);
        self.last_failure_after_kill_ms
            .fetch_max(after_kill, Ordering::Relaxed);
        self.failures_after_kill.fetch_add(1, Ordering::Relaxed);
    }

    fn record_success(&self, operation: Operation, invoked_ms: u64, completed_ms: u64) {
        let kill_ms = self.kill_ms.load(Ordering::Acquire);
        if kill_ms == NEVER || invoked_ms < kill_ms {
            return;
        }
        let after_kill = completed_ms.saturating_sub(kill_ms);
        let (first, count) = match operation {
            Operation::Get => (&self.first_read_after_kill_ms, &self.reads_after_kill),
            Operation::Put => (&self.first_write_after_kill_ms, &self.writes_after_kill),
            Operation::Delete => (&self.first_delete_after_kill_ms, &self.deletes_after_kill),
        };
        let _ = first.compare_exchange(NEVER, after_kill, Ordering::AcqRel, Ordering::Relaxed);
        count.fetch_add(1, Ordering::Relaxed);
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
    /// The requests that ended with a capacity error.
    capacity_backpressure_errors: u64,
    /// The retries the client made because of capacity.
    capacity_backpressure_retries: u64,
    may_have_applied: u64,
    rollbacks: u64,
    forbidden_rollbacks: u64,
    rollback_kinds: BTreeMap<String, u64>,
    /// The reads the shortened history cannot explain.
    undecidable: u64,
    undecidable_samples: Vec<String>,
    impossible: u64,
    violations: Vec<String>,
    /// The acknowledged states the bounded history dropped, over the whole
    /// run: a key keeps its model across rounds.
    dropped_acknowledged: u64,
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
        self.capacity_backpressure_errors += other.capacity_backpressure_errors;
        self.capacity_backpressure_retries += other.capacity_backpressure_retries;
        self.may_have_applied += other.may_have_applied;
        self.rollbacks += other.rollbacks;
        self.forbidden_rollbacks += other.forbidden_rollbacks;
        for (kind, count) in other.rollback_kinds {
            *self.rollback_kinds.entry(kind).or_default() += count;
        }
        self.undecidable += other.undecidable;
        if self.undecidable_samples.len() < 10 {
            let room = 10 - self.undecidable_samples.len();
            self.undecidable_samples
                .extend(other.undecidable_samples.into_iter().take(room));
        }
        self.impossible += other.impossible;
        self.dropped_acknowledged += other.dropped_acknowledged;
        if self.violations.len() < 100 {
            let room = 100 - self.violations.len();
            self.violations
                .extend(other.violations.into_iter().take(room));
        }
    }

    fn requests(&self) -> u64 {
        self.ops.values().sum::<u64>() + self.errors.values().sum::<u64>()
    }

    /// Records a read the history cannot explain, with a sample of what it
    /// saw.
    fn record_undecidable(&mut self, sample: String) {
        self.undecidable += 1;
        if self.undecidable_samples.len() < 10 {
            self.undecidable_samples.push(sample);
        }
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

struct WorkerContext {
    worker: usize,
    keys: Vec<u64>,
    models: BTreeMap<u64, KeyModel>,
    endpoint: String,
    operation_timeout: Duration,
    value_bytes: usize,
    mix: OperationMix,
    started_at: Instant,
    failure_epoch: u64,
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
        failure_epoch,
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
            value_hex: value.as_deref().map(hex::encode),
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
                    entry.value_hex = Some(hex::encode(&output.value));
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
                    match model.observe_at(
                        Some(&output.value),
                        Some(&output.version),
                        output.topology_epoch,
                        failure_epoch,
                        progress.kill_ms.load(Ordering::Acquire) != NEVER,
                    ) {
                        Observation::Matches => {}
                        Observation::Rollback {
                            first,
                            kind,
                            eligible,
                        } => {
                            if !eligible {
                                stats.forbidden_rollbacks += 1;
                            }
                            if first {
                                stats.rollbacks += 1;
                                *stats.rollback_kinds.entry(kind.to_owned()).or_default() += 1;
                            }
                        }
                        Observation::Undecidable => {
                            stats.record_undecidable(format!(
                                "GET key {key} returned a value the shortened history cannot \
                                 explain at epoch {}",
                                output.topology_epoch
                            ));
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
                    entry.end_epoch = client.epoch().await;
                    *stats.ops.entry("get_not_found".to_owned()).or_default() += 1;
                    progress.record_success(
                        operation,
                        invoked_ms,
                        started_at.elapsed().as_millis() as u64,
                    );
                    match model.observe_at(
                        None,
                        None,
                        entry.end_epoch,
                        failure_epoch,
                        progress.kill_ms.load(Ordering::Acquire) != NEVER,
                    ) {
                        Observation::Matches => {}
                        Observation::Rollback {
                            first,
                            kind,
                            eligible,
                        } => {
                            if !eligible {
                                stats.forbidden_rollbacks += 1;
                            }
                            if first {
                                stats.rollbacks += 1;
                                *stats.rollback_kinds.entry(kind.to_owned()).or_default() += 1;
                            }
                        }
                        Observation::Undecidable => {
                            stats.record_undecidable(format!(
                                "GET key {key} found no record and the shortened history cannot \
                                 explain it"
                            ));
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
                            stats.capacity_backpressure_errors += 1;
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
                        stats.capacity_backpressure_errors += 1;
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
                stats.capacity_backpressure_retries += *count;
            }
        }
        stats.retry_total += entry.retries;
        progress.record_reduced_rf(&entry, invoked_ms, failure_epoch);
        ledger.record(&entry)?;
        let _ = completed;
        step += 1;
    }
    stats.dropped_acknowledged = models
        .values()
        .map(|model| model.dropped_acknowledged)
        .sum::<u64>();
    stats.models = models;
    Ok(stats)
}

/// The checks one round of a fault run has to pass.
///
/// The checks are collected instead of raised one by one: a round that fails
/// ends the run, and the run then reports everything the round found instead
/// of only the first problem.
struct RoundChecks<'a> {
    round: u32,
    scenario: FaultScenario,
    write_ack_policy: WriteAckPolicy,
    mix: &'a OperationMix,
    reads_after_kill: u64,
    writes_after_kill: u64,
    owner_unchanged: bool,
    reduced_rf: &'a ReducedRfProgress,
}

impl RoundChecks<'_> {
    /// The problems the round found. An empty list means the round passed.
    fn failures(&self, stats: &WorkerStats) -> Vec<String> {
        let mut failures = Vec::new();
        if stats.impossible > 0 {
            failures.push(format!(
                "round {} observed {} values the history does not allow: {:?}",
                self.round, stats.impossible, stats.violations
            ));
        }
        if stats.requests() == 0 {
            failures.push(format!(
                "round {} completed no requests under load",
                self.round
            ));
        }
        // The load has to keep working after the failure, not only before it:
        // a run whose writes stopped at the kill would still have completed
        // requests before it.
        for (operation, completed) in [
            (Operation::Get, self.reads_after_kill),
            (Operation::Put, self.writes_after_kill),
        ] {
            if self.mix.offers(operation) && completed == 0 {
                failures.push(format!(
                    "round {}: the load completed no {} after the kill",
                    self.round,
                    operation.name()
                ));
            }
        }
        if self.scenario == FaultScenario::Follower
            && self.write_ack_policy == WriteAckPolicy::OwnerOnly
        {
            for (operation, count) in [
                (Operation::Put, self.reduced_rf.puts),
                (Operation::Delete, self.reduced_rf.deletes),
            ] {
                if self.mix.offers(operation) && count == 0 {
                    failures.push(format!(
                        "round {}: no {} succeeded at reduced RF before failover publication",
                        self.round,
                        operation.name()
                    ));
                }
            }
            if self.reduced_rf.admission_rejections > 0 {
                failures.push(format!(
                    "round {}: {} unexpected ACK/admission rejections at reduced RF",
                    self.round, self.reduced_rf.admission_rejections
                ));
            }
        }
        if !self.owner_unchanged {
            failures.push(format!(
                "round {}: killing a follower changed the owner of a selected key",
                self.round
            ));
        }
        let forbidden = if self.scenario == FaultScenario::Follower
            || self.write_ack_policy != WriteAckPolicy::OwnerOnly
        {
            stats.rollbacks
        } else {
            stats.forbidden_rollbacks
        };
        if forbidden > 0 {
            failures.push(format!(
                "round {} rolled back {} acknowledged changes: {:?}",
                self.round, forbidden, stats.rollback_kinds
            ));
        }
        if stats.undecidable > 0 {
            // A read the history cannot explain is not a verified read. The
            // run does not count it as a pass, in either scenario: under
            // `OwnerOnly` a rollback is allowed, but once the history has been
            // shortened it cannot be told apart from an impossible value, and
            // a follower loss allows no rollback at all. The dropped figure is
            // the round's total across its keys, not the number of states the
            // keys of these reads dropped.
            failures.push(format!(
                "round {}: {} reads cannot be verified, because the acknowledged history was \
                 shortened: {} states have been dropped in total from the history of the round's \
                 keys, so a read cannot be told apart from a rollback to a dropped state: {:?}",
                self.round,
                stats.undecidable,
                stats.dropped_acknowledged,
                stats.undecidable_samples
            ));
        }
        failures
    }
}

/// One injected failure and everything the run measured about it.
///
/// Every duration is counted from the kill of the victim, so the figures of
/// two runs can be compared without knowing how long the load ran before the
/// failure.
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
    reduced_rf_progress: ReducedRfProgress,
    replica_status_after_failure: Value,
    /// The seconds from the kill to the failover's publication.
    kill_to_publish_seconds: f64,
    /// The seconds from the kill to the first successful read.
    read_recovery_seconds: Option<f64>,
    /// The seconds from the kill to the first successful write.
    write_recovery_seconds: Option<f64>,
    /// The seconds from the kill to the first successful delete.
    delete_recovery_seconds: Option<f64>,
    /// The milliseconds from the kill to the first failed request.
    first_failure_after_kill_ms: Option<u64>,
    /// The milliseconds from the kill to the last failed request.
    last_failure_after_kill_ms: Option<u64>,
    failures_after_kill: u64,
    /// The reads, writes and deletes the load completed after the kill. A
    /// read that found no record counts as a successful read: the field says
    /// that the read path still works, not that the data was there.
    reads_after_kill: u64,
    writes_after_kill: u64,
    deletes_after_kill: u64,
    replacement_to_full_rf_seconds: f64,
    failure_to_full_rf_seconds: f64,
    final_epoch: u64,
    collateral_victim_owned_keys: u64,
    collateral_rollbacks: u64,
    load: Value,
    rollbacks: u64,
    /// The reads the history cannot explain, because the bounded history had
    /// already dropped a state the read may have rolled back to. A run with
    /// such a read does not count as a pass.
    undecidable_reads: u64,
    undecidable_samples: Vec<String>,
    impossible_reads: u64,
    /// The acknowledged states this round dropped. The history of a key is
    /// cumulative over the run, so the round reports the difference from the
    /// start of its own load, and the run's total is in
    /// `observations.fault_dropped_acknowledged_states`.
    dropped_acknowledged_states: u64,
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

/// What the cohort's ranges looked like in one sample of the replica status.
///
/// The reasons a range was not writable are counted per range, so the caller
/// adds them up over its samples: the sum counts the (poll, range) pairs that
/// were blocked, not the number of distinct ranges.
fn cohort_replica_stats(
    status: &ReplicaStatusResponse,
    topology: &TopologySnapshot,
    keys: &[u64],
) -> (Option<usize>, usize, usize, BTreeMap<String, usize>) {
    let mut live_rf = usize::MAX;
    let mut under_replicated = 0;
    let mut not_writable = 0;
    let mut block_reasons: BTreeMap<String, usize> = BTreeMap::new();
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
        if !range.writable {
            // The reason a range was not writable is what tells a reader
            // whether the load could have written to it.
            let reason = if range.write_block_reason.is_empty() {
                "unnamed".to_owned()
            } else {
                range.write_block_reason.clone()
            };
            *block_reasons.entry(reason).or_default() += 1;
        }
    }
    (
        (live_rf != usize::MAX).then_some(live_rf),
        under_replicated,
        not_writable,
        block_reasons,
    )
}

fn load_json(stats: &WorkerStats, elapsed: Duration, dropped_acknowledged: u64) -> Value {
    json!({
        "requests": stats.requests(),
        "seconds": elapsed.as_secs_f64(),
        "operations_per_second": stats.requests() as f64 / elapsed.as_secs_f64().max(f64::EPSILON),
        "ops": stats.ops,
        "errors": stats.errors,
        "retries": stats.retries,
        "retry_total": stats.retry_total,
        "capacity_backpressure_errors": stats.capacity_backpressure_errors,
        "capacity_backpressure_retries": stats.capacity_backpressure_retries,
        "may_have_applied": stats.may_have_applied,
        "rollback_kinds": stats.rollback_kinds,
        "forbidden_rollbacks": stats.forbidden_rollbacks,
        "undecidable_reads": stats.undecidable,
        "dropped_acknowledged_states": dropped_acknowledged,
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
    let mut rounds = Vec::new();
    let mut killed = BTreeSet::new();
    let mut failures: Vec<String> = Vec::new();
    let mut total_rollbacks = 0_u64;
    let mut total_impossible = 0_u64;
    let mut total_undecidable = 0_u64;
    let mut total_dropped = 0_u64;
    let mut total_capacity_errors = 0_u64;
    let mut total_capacity_retries = 0_u64;
    let mut total_requests = 0_u64;
    let started = Instant::now();
    let mut active_round = 1;

    let execution: Result<()> = async {
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
                    KeyModel::new(
                        PossibleState {
                            value: Some(deterministic_value(key, 0, config.value_bytes)),
                            version: None,
                        },
                        config.fault_acknowledged_window,
                    ),
                )
            })
            .collect();
        for round in 1..=config.fault_rounds {
            active_round = round;
            // Final verification also observes keys outside the load cohort.
            for model in models.values_mut() {
                model.begin_round();
            }
            events.record(
                "fault_round_started",
                json!({ "round": round, "processes": processes.pids() }),
            )?;
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
            // A key keeps its model across rounds, so its dropped count is
            // cumulative. The round reports only the states it dropped itself, so
            // adding the rounds up does not count a state twice. The snapshot is
            // taken before the pre-failure read: a window too small to hold the
            // state that read finds drops one there too, and that drop belongs to
            // this round instead of falling between the rounds.
            let dropped_before: u64 = victim
                .cohort
                .iter()
                .map(|key| {
                    models
                        .get(key)
                        .expect("every cohort key has a model")
                        .dropped_acknowledged
                })
                .sum();
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
            let workers = config.concurrency.min(victim.cohort.len()).max(1);
            let stop = Arc::new(AtomicBool::new(false));
            let progress = Arc::new(Progress::new());
            let started_at = Instant::now();
            // Dropping a JoinSet on an error aborts its workers. Detached workers
            // must not continue issuing requests after the run has failed.
            let mut handles = tokio::task::JoinSet::new();
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
                handles.spawn(run_worker(WorkerContext {
                    worker,
                    keys,
                    models: worker_models,
                    endpoint: coordinator_endpoint.to_owned(),
                    operation_timeout,
                    value_bytes: config.value_bytes,
                    mix: mix.clone(),
                    started_at,
                    failure_epoch: topology.epoch,
                    stop: Arc::clone(&stop),
                    progress: Arc::clone(&progress),
                    ledger: Arc::clone(&ledger),
                    next_id: Arc::clone(&next_id),
                }));
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

            // Sample after the pre-window, with load workers still running: the
            // cursors must include the mutations whose survival we are testing.
            let status = observer.replica_status().await?;
            let (pre_progress, affected_ranges) = ranges_for_keys(&status, &topology, &victim.cohort);
            events.record(
                "fault_pre_failure_replica_progress",
                json!({ "round": round, "ranges": affected_ranges }),
            )?;

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
            let mut block_reasons: BTreeMap<String, usize> = BTreeMap::new();
            let published = loop {
                processes.ensure_running()?;
                let current = observer.refresh_topology().await?;
                if let Ok(Ok(status)) =
                    tokio::time::timeout(Duration::from_secs(2), observer.replica_status()).await
                {
                    let (live_rf, under_replicated, not_writable, reasons) =
                        cohort_replica_stats(&status, &current, &victim.cohort);
                    if let Some(live_rf) = live_rf {
                        min_live_rf = min_live_rf.min(live_rf);
                    }
                    max_under_replicated = max_under_replicated.max(under_replicated);
                    writable_false = writable_false.max(not_writable);
                    for (reason, count) in reasons {
                        *block_reasons.entry(reason).or_default() += count;
                    }
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
            progress.published_ms.store(
                started_at.elapsed().as_millis() as u64,
                Ordering::Release,
            );
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
            let (
                post_failure_min_live_rf,
                post_failure_under_replicated,
                post_failure_not_writable,
                post_failure_block_reasons,
            ) = match observer.replica_status().await {
                Ok(status) => cohort_replica_stats(&status, &published, &victim.cohort),
                Err(error) => {
                    events.record(
                        "fault_post_failure_status_failed",
                        json!({ "round": round, "error": error.to_string() }),
                    )?;
                    (None, 0, 0, BTreeMap::new())
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
            while let Some(handle) = handles.join_next().await {
                let mut worker = handle.context("a fault load worker panicked")??;
                all_models.append(&mut worker.models);
                stats.merge(worker);
            }
            models = all_models;
            ledger.flush()?;
            let dropped_this_round = stats.dropped_acknowledged.saturating_sub(dropped_before);
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
            let reads_after_kill = progress.reads_after_kill.load(Ordering::Acquire);
            let writes_after_kill = progress.writes_after_kill.load(Ordering::Acquire);
            let deletes_after_kill = progress.deletes_after_kill.load(Ordering::Acquire);
            let reduced_rf = progress.reduced_rf.lock().expect("progress lock").clone();

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
                let observation = model.observe_at(
                    value.as_deref(),
                    version.as_ref(),
                    workload_client.epoch().await,
                    topology.epoch,
                    true,
                );
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
                    Observation::Rollback {
                        first,
                        kind,
                        eligible,
                    } => {
                        if !eligible {
                            stats.forbidden_rollbacks += 1;
                        }
                        if first {
                            stats.rollbacks += 1;
                            *stats.rollback_kinds.entry(kind.to_owned()).or_default() += 1;
                        }
                    }
                    Observation::Undecidable => {
                        stats.record_undecidable(format!(
                            "final GET key {key} returned a value the shortened history cannot explain"
                        ));
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
            total_undecidable += stats.undecidable;
            total_dropped += dropped_this_round;
            total_capacity_errors += stats.capacity_backpressure_errors;
            total_capacity_retries += stats.capacity_backpressure_retries;
            total_requests += stats.requests();
            let load = load_json(&stats, load_elapsed, dropped_this_round);
            let record = FaultRound {
                round,
                scenario: scenario_name,
                victim: victim.node_id.clone(),
                victim_epoch: topology.epoch,
                cohort_keys: victim.cohort.len(),
                affected_ranges,
                owner_unchanged,
                pre_failure_replica_progress: pre_progress,
                reduced_rf_progress: reduced_rf.clone(),
                replica_status_after_failure: json!({
                    "min_live_rf_during_failure": (min_live_rf != usize::MAX).then_some(min_live_rf),
                    "max_under_replicated_ranges_during_failure": max_under_replicated,
                    "max_not_writable_ranges_during_failure": writable_false,
                    "write_block_reason_samples_during_failure": block_reasons,
                    "min_live_rf_after_publish": post_failure_min_live_rf,
                    "under_replicated_ranges_after_publish": post_failure_under_replicated,
                    "not_writable_ranges_after_publish": post_failure_not_writable,
                    "write_block_reason_samples_after_publish": post_failure_block_reasons,
                }),
                kill_to_publish_seconds,
                read_recovery_seconds,
                write_recovery_seconds,
                delete_recovery_seconds,
                first_failure_after_kill_ms,
                last_failure_after_kill_ms,
                failures_after_kill,
                reads_after_kill,
                writes_after_kill,
                deletes_after_kill,
                replacement_to_full_rf_seconds,
                failure_to_full_rf_seconds,
                final_epoch,
                collateral_victim_owned_keys: collateral.len() as u64,
                collateral_rollbacks,
                load,
                rollbacks: stats.rollbacks,
                undecidable_reads: stats.undecidable,
                undecidable_samples: stats.undecidable_samples.clone(),
                impossible_reads: stats.impossible,
                dropped_acknowledged_states: dropped_this_round,
                violations: stats.violations.clone(),
                new_owner_counts,
            };
            measurements.insert(
                format!("fault_round_{round}_load"),
                measurement(stats.requests(), load_elapsed),
            );
            rounds.push(record);
            killed.insert(victim.node_id.clone());

            // The checks of a round. A round that fails ends the run, but its
            // record and the rounds before it stay in the summary: the evidence of
            // a failed run is the reason it failed.
            let round_failures = RoundChecks {
                round,
                scenario: config.fault_scenario,
                write_ack_policy: config.write_ack_policy,
                mix: &mix,
                reads_after_kill,
                writes_after_kill,
                owner_unchanged,
                reduced_rf: &reduced_rf,
            }
            .failures(&stats);
            failures.extend(round_failures);
            save_run(
                observations,
                scenario_name,
                config.fault_rounds,
                config.fault_acknowledged_window,
                &rounds,
                &RunTotals {
                    rollbacks: total_rollbacks,
                    impossible: total_impossible,
                    undecidable: total_undecidable,
                    dropped: total_dropped,
                    capacity_errors: total_capacity_errors,
                    capacity_retries: total_capacity_retries,
                    requests: total_requests,
                    seconds: started.elapsed().as_secs_f64(),
                },
                &failures,
            )?;
            events.record("fault_round_complete", serde_json::to_value(rounds.last())?)?;
            if !failures.is_empty() {
                break;
            }
        }

        Ok(())
    }
    .await;
    if let Err(error) = &execution {
        failures.push(format!(
            "round {}: infrastructure error: {error:#}",
            active_round
        ));
    }

    let totals = RunTotals {
        rollbacks: total_rollbacks,
        impossible: total_impossible,
        undecidable: total_undecidable,
        dropped: total_dropped,
        capacity_errors: total_capacity_errors,
        capacity_retries: total_capacity_retries,
        requests: total_requests,
        seconds: started.elapsed().as_secs_f64(),
    };
    let recorded = record_run(
        observations,
        scenario_name,
        config.fault_rounds,
        config.fault_acknowledged_window,
        &rounds,
        &totals,
        &failures,
    );
    let final_event = events.record(
        "fault_run_complete",
        json!({
            "rounds_requested": config.fault_rounds,
            "rounds_completed": rounds.len(),
            "rollbacks": total_rollbacks,
            "impossible_reads": total_impossible,
            "undecidable_reads": total_undecidable,
            "dropped_acknowledged_states": total_dropped,
            "requests": total_requests,
            "seconds": started.elapsed().as_secs_f64(),
            "failures": failures,
        }),
    );
    if let Err(error) = &final_event {
        failures.push(format!("finalizing fault run: {error:#}"));
        save_run(
            observations,
            scenario_name,
            config.fault_rounds,
            config.fault_acknowledged_window,
            &rounds,
            &totals,
            &failures,
        )?;
    }
    execution?;
    recorded?;
    final_event?;
    Ok(rounds.last().map_or(0, |round| round.final_epoch))
}

/// What a whole run measured, across its rounds.
struct RunTotals {
    rollbacks: u64,
    impossible: u64,
    undecidable: u64,
    /// The states the rounds dropped, added up: each round reports its own.
    dropped: u64,
    capacity_errors: u64,
    capacity_retries: u64,
    requests: u64,
    seconds: f64,
}

/// Records what the run measured, and then fails it if a round failed.
///
/// The record goes in before the check: a failed run still says what it saw,
/// because the summary keeps the rounds it completed and the reasons it failed
/// in `fault_failures` rather than only in the error text.
fn record_run(
    observations: &mut BTreeMap<String, Value>,
    scenario_name: &str,
    rounds_requested: u32,
    acknowledged_window: usize,
    rounds: &[FaultRound],
    totals: &RunTotals,
    failures: &[String],
) -> Result<()> {
    save_run(
        observations,
        scenario_name,
        rounds_requested,
        acknowledged_window,
        rounds,
        totals,
        failures,
    )?;
    ensure!(failures.is_empty(), "{}", failures.join("; "));
    Ok(())
}

fn save_run(
    observations: &mut BTreeMap<String, Value>,
    scenario_name: &str,
    rounds_requested: u32,
    acknowledged_window: usize,
    rounds: &[FaultRound],
    totals: &RunTotals,
    failures: &[String],
) -> Result<()> {
    observations.insert("fault_scenario".into(), json!(scenario_name));
    observations.insert("fault_rounds_requested".into(), json!(rounds_requested));
    observations.insert("fault_rounds_completed".into(), json!(rounds.len()));
    observations.insert("fault_rounds".into(), serde_json::to_value(rounds)?);
    observations.insert(
        "fault_acknowledged_window".into(),
        json!(acknowledged_window),
    );
    observations.insert(
        "fault_acknowledged_rollbacks".into(),
        json!(totals.rollbacks),
    );
    observations.insert("fault_impossible_reads".into(), json!(totals.impossible));
    observations.insert("fault_undecidable_reads".into(), json!(totals.undecidable));
    observations.insert(
        "fault_dropped_acknowledged_states".into(),
        json!(totals.dropped),
    );
    observations.insert(
        "fault_capacity_backpressure_errors".into(),
        json!(totals.capacity_errors),
    );
    observations.insert(
        "fault_capacity_backpressure_retries".into(),
        json!(totals.capacity_retries),
    );
    observations.insert("fault_requests".into(), json!(totals.requests));
    observations.insert("fault_total_seconds".into(), json!(totals.seconds));
    observations.insert("fault_failures".into(), json!(failures));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn version(sequence: u64) -> RecordVersion {
        RecordVersion {
            topology_epoch: 1,
            owner_sequence: sequence,
            owner_node_id: "node-1".to_owned(),
        }
    }

    #[test]
    fn every_worker_offers_every_operation_of_the_mix() {
        // A total of 74 shares a factor with the fixed step the mix used
        // before, which left every worker without one of the three operations.
        let mix = OperationMix::parse("20:27:27").unwrap();
        assert_eq!(mix.total, 74);
        assert_eq!(gcd(mix.stride, mix.total), 1);
        for worker in 0..3 {
            let operations: BTreeSet<Operation> = (0..mix.total)
                .map(|step| mix.choose(step, worker))
                .collect();
            assert_eq!(
                operations.len(),
                3,
                "worker {worker} never offers one of the operations"
            );
        }
    }

    #[test]
    fn the_default_mix_keeps_its_share() {
        let mix = OperationMix::parse("70:20:10").unwrap();
        let mut counts = BTreeMap::new();
        for step in 0..mix.total {
            *counts.entry(mix.choose(step, 0)).or_insert(0_u64) += 1;
        }
        assert_eq!(counts[&Operation::Get], 70);
        assert_eq!(counts[&Operation::Put], 20);
        assert_eq!(counts[&Operation::Delete], 10);
    }

    #[test]
    fn an_empty_mix_is_rejected() {
        assert!(OperationMix::parse("0:0:0").is_err());
        assert!(OperationMix::parse("70:20").is_err());
    }

    /// A window small enough to fill in a test.
    const WINDOW: usize = 4;

    #[test]
    fn the_acknowledged_history_is_bounded_and_reports_what_it_dropped() {
        let mut model = KeyModel::new(PossibleState::absent(None), WINDOW);
        let writes = WINDOW as u64 + 1;
        for sequence in 1..=writes {
            model.on_put(sequence.to_be_bytes().to_vec(), version(sequence));
        }
        assert_eq!(model.acknowledged.len(), WINDOW);
        // The state the key started with and the first write are the ones the
        // bound dropped.
        assert_eq!(model.dropped_acknowledged, 2);

        // The oldest state is gone, so a read of it cannot be told apart from
        // a value the history never held.
        assert_eq!(
            model.observe(Some(&1_u64.to_be_bytes()), Some(&version(1))),
            Observation::Undecidable
        );
        // A state the history still holds is a rollback, not an unknown value.
        assert!(matches!(
            model.observe(Some(&2_u64.to_be_bytes()), Some(&version(2))),
            Observation::Rollback { .. }
        ));
    }

    #[test]
    fn a_state_that_is_acknowledged_again_stays_in_the_window() {
        let mut model = KeyModel::new(PossibleState::absent(None), WINDOW);
        for sequence in 1..=WINDOW as u64 + 1 {
            model.on_put(sequence.to_be_bytes().to_vec(), version(sequence));
        }
        let oldest = PossibleState::present(1_u64.to_be_bytes().to_vec(), version(1));
        // The window is full, so the oldest state left it.
        assert!(
            !model
                .acknowledged
                .iter()
                .any(|entry| *entry == AcknowledgedState::of(&oldest))
        );

        // Acknowledging it again makes it one of the most recent states, so it
        // is kept instead of being the first one dropped.
        model.on_put(
            oldest.value.clone().expect("the state has a value"),
            oldest.version.clone().expect("the state has a version"),
        );
        assert_eq!(
            model.acknowledged.back(),
            Some(&AcknowledgedState::of(&oldest))
        );
    }

    #[test]
    fn a_read_the_history_never_held_is_impossible() {
        let mut model = KeyModel::new(PossibleState::absent(None), WINDOW);
        model.on_put(b"value".to_vec(), version(1));
        assert_eq!(
            model.observe(Some(b"other"), Some(&version(9))),
            Observation::Impossible
        );
    }

    #[test]
    fn a_read_resolves_ambiguous_put_and_preserves_its_exact_history() {
        let mut model = KeyModel::new(PossibleState::present(b"A".to_vec(), version(1)), 16);
        model.on_ambiguous_put(b"B".to_vec());
        assert_eq!(
            model.observe(Some(b"B"), Some(&version(2))),
            Observation::Matches
        );
        assert!(matches!(
            model.observe(Some(b"A"), Some(&version(1))),
            Observation::Rollback { .. }
        ));
        model.on_put(b"C".to_vec(), version(3));
        assert!(matches!(
            model.observe(Some(b"B"), Some(&version(2))),
            Observation::Rollback { .. }
        ));
        assert_eq!(
            model.observe(Some(b"B"), Some(&version(99))),
            Observation::Impossible
        );
    }

    #[test]
    fn uncertain_put_and_delete_survive_a_later_acknowledgement() {
        for observe_first in [false, true] {
            let mut put = KeyModel::new(PossibleState::present(b"A".to_vec(), version(1)), 16);
            put.on_ambiguous_put(b"B".to_vec());
            if observe_first {
                assert_eq!(
                    put.observe(Some(b"B"), Some(&version(2))),
                    Observation::Matches
                );
            }
            put.on_put(b"C".to_vec(), version(3));
            assert!(matches!(
                put.observe(Some(b"B"), Some(&version(2))),
                Observation::Rollback { .. }
            ));

            let mut delete = KeyModel::new(PossibleState::present(b"A".to_vec(), version(1)), 16);
            delete.on_ambiguous_delete();
            if observe_first {
                assert_eq!(delete.observe(None, None), Observation::Matches);
                assert!(matches!(
                    delete.observe(Some(b"A"), Some(&version(1))),
                    Observation::Rollback { .. }
                ));
            }
            delete.on_put(b"C".to_vec(), version(3));
            assert!(matches!(
                delete.observe(None, None),
                Observation::Rollback { .. }
            ));
        }
    }

    #[test]
    fn each_round_counts_a_keys_rollback_again() {
        let mut model = KeyModel::new(PossibleState::present(b"A".to_vec(), version(1)), 16);
        for sequence in [2, 3] {
            model.begin_round();
            model.resync(PossibleState::present(b"A".to_vec(), version(1)));
            model.on_put(b"B".to_vec(), version(sequence));
            assert!(matches!(
                model.observe(Some(b"A"), Some(&version(1))),
                Observation::Rollback { first: true, .. }
            ));
            assert_eq!(
                model.observe(Some(b"A"), Some(&version(1))),
                Observation::Matches
            );
        }
    }

    #[test]
    fn only_the_injected_failure_can_exempt_a_rollback() {
        let mut model = KeyModel::new(PossibleState::present(b"A".to_vec(), version(1)), 16);
        model.on_put(b"B".to_vec(), version(2));
        assert!(matches!(
            model.observe_at(Some(b"A"), Some(&version(1)), 1, 1, false),
            Observation::Rollback {
                eligible: false,
                ..
            }
        ));
        model.on_put(b"B".to_vec(), version(2));
        assert!(matches!(
            model.observe_at(Some(b"A"), Some(&version(1)), 2, 1, true),
            Observation::Rollback { eligible: true, .. }
        ));
        // An old-owner response that was already in flight at the kill
        // cannot attribute a regression to promotion of a follower.
        let mut in_flight = KeyModel::new(PossibleState::present(b"A".to_vec(), version(1)), 16);
        in_flight.on_put(b"B".to_vec(), version(2));
        assert!(matches!(
            in_flight.observe_at(Some(b"A"), Some(&version(1)), 1, 1, true),
            Observation::Rollback {
                eligible: false,
                ..
            }
        ));
        // Even an old-version record observed on the promoted owner fixes
        // the state: that same failure cannot justify a second regression.
        model.on_put(
            b"C".to_vec(),
            RecordVersion {
                topology_epoch: 2,
                ..version(3)
            },
        );
        assert!(matches!(
            model.observe_at(Some(b"B"), Some(&version(2)), 2, 1, true),
            Observation::Rollback {
                eligible: false,
                first: false,
                ..
            }
        ));
    }

    #[test]
    fn a_recovered_read_or_delete_closes_the_rollback_exemption() {
        for delete in [false, true] {
            let mut model = KeyModel::new(PossibleState::present(b"A".to_vec(), version(1)), 16);
            if delete {
                model.on_delete(RecordVersion {
                    topology_epoch: 2,
                    ..version(2)
                });
            } else {
                assert_eq!(
                    model.observe_at(Some(b"A"), Some(&version(1)), 2, 1, true),
                    Observation::Matches
                );
                model.on_ambiguous_put(b"B".to_vec());
                assert_eq!(
                    model.observe_at(Some(b"B"), Some(&version(3)), 2, 1, true),
                    Observation::Matches
                );
            }
            assert!(matches!(
                model.observe_at(Some(b"A"), Some(&version(1)), 2, 1, true),
                Observation::Rollback {
                    eligible: false,
                    ..
                }
            ));
        }
    }

    fn checks<'a>(mix: &'a OperationMix, scenario: FaultScenario) -> RoundChecks<'a> {
        RoundChecks {
            round: 1,
            scenario,
            write_ack_policy: WriteAckPolicy::OwnerOnly,
            mix,
            reads_after_kill: 10,
            writes_after_kill: 10,
            owner_unchanged: true,
            reduced_rf: &ReducedRfProgress {
                puts: 10,
                deletes: 10,
                admission_rejections: 0,
                capacity_errors: 0,
                capacity_retries: 0,
            },
        }
    }

    #[test]
    fn a_round_that_survived_the_failure_passes_its_checks() {
        let mix = OperationMix::parse("70:20:10").unwrap();
        let mut stats = WorkerStats::default();
        stats.ops.insert("get_ok".to_owned(), 10);
        stats.ops.insert("put_ok".to_owned(), 10);
        assert!(
            checks(&mix, FaultScenario::Follower)
                .failures(&stats)
                .is_empty(),
            "{:?}",
            checks(&mix, FaultScenario::Follower).failures(&stats)
        );
    }

    #[test]
    fn a_load_that_stopped_at_the_kill_fails_the_round() {
        let mix = OperationMix::parse("70:20:10").unwrap();
        let mut stats = WorkerStats::default();
        stats.ops.insert("get_ok".to_owned(), 10);
        stats.ops.insert("put_ok".to_owned(), 10);
        let mut round = checks(&mix, FaultScenario::Follower);
        round.writes_after_kill = 0;
        let failures = round.failures(&stats);
        assert_eq!(failures.len(), 1, "{failures:?}");
        assert!(
            failures[0].contains("no put after the kill"),
            "{failures:?}"
        );
    }

    #[test]
    fn a_read_the_run_cannot_verify_fails_the_round_in_both_scenarios() {
        // The owner scenario allows a rollback, but a read the shortened
        // history cannot explain is not a verified read, so the run does not
        // count it as a pass.
        let mix = OperationMix::parse("70:20:10").unwrap();
        let mut stats = WorkerStats::default();
        stats.ops.insert("get_ok".to_owned(), 10);
        stats.ops.insert("put_ok".to_owned(), 10);
        stats.undecidable = 3;
        stats.dropped_acknowledged = 44;
        stats.undecidable_samples = vec!["GET key 7 returned a value".to_owned()];
        for scenario in [FaultScenario::Owner, FaultScenario::Follower] {
            let failures = checks(&mix, scenario).failures(&stats);
            assert_eq!(failures.len(), 1, "{scenario:?}: {failures:?}");
            assert!(failures[0].contains("cannot be verified"), "{failures:?}");
            // The message says that the history was shortened and that the
            // figure is a total, so a reader does not mistake it for a
            // rollback the follower loss caused or for this key's count.
            assert!(
                failures[0].contains("44 states have been dropped in total"),
                "{failures:?}"
            );
        }
    }

    #[test]
    fn a_rollback_the_policy_forbids_fails_the_round() {
        let mix = OperationMix::parse("70:20:10").unwrap();
        let mut stats = WorkerStats::default();
        stats.ops.insert("get_ok".to_owned(), 10);
        stats.ops.insert("put_ok".to_owned(), 10);
        stats.rollbacks = 2;
        stats
            .rollback_kinds
            .insert("older_value_reappeared".to_owned(), 2);

        // `OwnerOnly` allows a rollback after an owner failure.
        assert!(
            checks(&mix, FaultScenario::Owner)
                .failures(&stats)
                .is_empty(),
            "{:?}",
            checks(&mix, FaultScenario::Owner).failures(&stats)
        );
        // A follower loss allows none.
        let failures = checks(&mix, FaultScenario::Follower).failures(&stats);
        assert_eq!(failures.len(), 1, "{failures:?}");
        assert!(failures[0].contains("rolled back 2"), "{failures:?}");
    }

    #[test]
    fn an_owner_round_rejects_regressions_outside_the_failure_boundary() {
        let mix = OperationMix::parse("70:20:10").unwrap();
        let mut stats = WorkerStats::default();
        stats.ops.insert("get_ok".into(), 10);
        stats.rollbacks = 1;
        // A forbidden second regression must fail even when the per-key
        // rollback count was already deduplicated by an earlier valid one.
        stats.forbidden_rollbacks = 1;
        let failures = checks(&mix, FaultScenario::Owner).failures(&stats);
        assert_eq!(failures.len(), 1, "{failures:?}");
        assert!(failures[0].contains("rolled back 1"));
    }

    #[test]
    fn post_repair_success_cannot_prove_reduced_rf_writability() {
        let mix = OperationMix::parse("70:20:10").unwrap();
        let mut stats = WorkerStats::default();
        stats.ops.insert("put_ok".into(), 10);
        let missing = ReducedRfProgress::default();
        let mut round = checks(&mix, FaultScenario::Follower);
        round.reduced_rf = &missing;
        let failures = round.failures(&stats);
        assert_eq!(failures.len(), 2, "{failures:?}");
        assert!(
            failures
                .iter()
                .all(|failure| failure.contains("at reduced RF"))
        );
        let backpressure = ReducedRfProgress {
            puts: 1,
            deletes: 1,
            capacity_errors: 5,
            capacity_retries: 9,
            ..Default::default()
        };
        round.reduced_rf = &backpressure;
        assert!(round.failures(&stats).is_empty());
        let rejected = ReducedRfProgress {
            admission_rejections: 1,
            ..backpressure
        };
        round.reduced_rf = &rejected;
        assert_eq!(round.failures(&stats).len(), 1);
    }

    #[test]
    fn reduced_rf_progress_excludes_pre_kill_and_reconfigured_success() {
        let progress = Progress::new();
        progress.kill_ms.store(10, Ordering::Release);
        progress.published_ms.store(30, Ordering::Release);
        let mut entry = RequestEntry {
            id: 1,
            worker: 0,
            key: 7,
            operation: "put",
            value_hex: None,
            invoked_unix_ms: 0,
            completed_unix_ms: 0,
            elapsed_us: 0,
            start_epoch: 1,
            end_epoch: 1,
            result: "ok",
            error: None,
            error_code: None,
            outcome: None,
            version: Some(VersionView::from(&version(1))),
            topology_epoch: Some(1),
            retries: 0,
            retry_reasons: BTreeMap::new(),
        };
        progress.record_reduced_rf(&entry, 9, 1);
        progress.record_reduced_rf(&entry, 10, 1);
        progress.record_reduced_rf(&entry, 31, 1);
        assert_eq!(progress.reduced_rf.lock().unwrap().puts, 0);
        progress.record_reduced_rf(&entry, 11, 1);
        assert_eq!(progress.reduced_rf.lock().unwrap().puts, 1);
        entry.version.as_mut().unwrap().topology_epoch = 2;
        progress.record_reduced_rf(&entry, 12, 1);
        assert_eq!(progress.reduced_rf.lock().unwrap().puts, 1);
        entry.result = "error";
        entry.error_code = Some("ResourceExhausted".into());
        entry.retry_reasons.insert("resource_exhausted".into(), 2);
        progress.record_reduced_rf(&entry, 13, 1);
        {
            let reduced = progress.reduced_rf.lock().unwrap();
            assert_eq!(reduced.capacity_errors, 1);
            assert_eq!(reduced.capacity_retries, 2);
            assert_eq!(reduced.admission_rejections, 0);
        }
        entry.error_code = Some("TemporarilyUnavailable".into());
        entry.retry_reasons.clear();
        entry.retry_reasons.insert("replica_not_ready".into(), 2);
        progress.record_reduced_rf(&entry, 14, 1);
        assert_eq!(progress.reduced_rf.lock().unwrap().admission_rejections, 3);
    }

    fn a_recorded_round() -> FaultRound {
        FaultRound {
            round: 1,
            scenario: "owner",
            victim: "node-1".to_owned(),
            victim_epoch: 1,
            cohort_keys: 8,
            affected_ranges: 4,
            owner_unchanged: true,
            pre_failure_replica_progress: json!({}),
            reduced_rf_progress: ReducedRfProgress::default(),
            replica_status_after_failure: json!({}),
            kill_to_publish_seconds: 0.5,
            read_recovery_seconds: Some(0.5),
            write_recovery_seconds: Some(0.5),
            delete_recovery_seconds: Some(0.5),
            first_failure_after_kill_ms: None,
            last_failure_after_kill_ms: None,
            failures_after_kill: 0,
            reads_after_kill: 10,
            writes_after_kill: 10,
            deletes_after_kill: 10,
            replacement_to_full_rf_seconds: 1.0,
            failure_to_full_rf_seconds: 1.0,
            final_epoch: 2,
            collateral_victim_owned_keys: 0,
            collateral_rollbacks: 0,
            load: json!({}),
            rollbacks: 0,
            undecidable_reads: 0,
            undecidable_samples: Vec::new(),
            impossible_reads: 0,
            dropped_acknowledged_states: 0,
            violations: Vec::new(),
            new_owner_counts: BTreeMap::new(),
        }
    }

    fn totals() -> RunTotals {
        RunTotals {
            rollbacks: 0,
            impossible: 0,
            undecidable: 0,
            dropped: 0,
            capacity_errors: 0,
            capacity_retries: 0,
            requests: 10,
            seconds: 1.0,
        }
    }

    #[test]
    fn a_failed_run_keeps_its_rounds_and_names_its_failures() {
        // The run has to fail, but the summary it leaves behind still says
        // what the rounds measured and why the run failed: a machine reading
        // `summary.json` does not have to parse the error text.
        let mut observations = BTreeMap::new();
        let rounds = [a_recorded_round()];
        let failures = vec!["round 1: 3 reads cannot be verified".to_owned()];
        let result = record_run(
            &mut observations,
            "owner",
            2,
            WINDOW,
            &rounds,
            &totals(),
            &failures,
        );
        assert_eq!(
            result.unwrap_err().to_string(),
            "round 1: 3 reads cannot be verified"
        );
        assert_eq!(
            observations["fault_rounds"].as_array().map(Vec::len),
            Some(1)
        );
        assert_eq!(
            observations["fault_failures"],
            json!(["round 1: 3 reads cannot be verified"])
        );
        assert_eq!(observations["fault_acknowledged_window"], json!(WINDOW));
        // A failed run reports how many rounds it ran and how many it asked
        // for: `fault_rounds` is shorter than the request when a round fails.
        assert_eq!(observations["fault_rounds_requested"], json!(2));
        assert_eq!(observations["fault_rounds_completed"], json!(1));
        // The round the run completed is in the record even though its check
        // failed.
        assert_eq!(observations["fault_rounds"][0]["final_epoch"], json!(2));
    }

    #[test]
    fn a_passing_run_records_no_failures() {
        let mut observations = BTreeMap::new();
        let result = record_run(
            &mut observations,
            "follower",
            1,
            WINDOW,
            &[a_recorded_round()],
            &totals(),
            &[],
        );
        assert!(result.is_ok(), "{:?}", result.err());
        assert_eq!(observations["fault_failures"], json!([]));
        assert_eq!(observations["fault_rounds_requested"], json!(1));
        assert_eq!(observations["fault_rounds_completed"], json!(1));
        assert_eq!(observations["fault_dropped_acknowledged_states"], json!(0));
    }

    #[test]
    fn the_window_keeps_its_index_in_step_with_its_order() {
        // The index is what makes acknowledging a new state constant time, so
        // it must count exactly the states the window holds.
        let mut model = KeyModel::new(PossibleState::absent(None), WINDOW);
        for sequence in 1..=(WINDOW as u64 * 4) {
            model.on_put(sequence.to_be_bytes().to_vec(), version(sequence));
            // Acknowledge a state the window already dropped, which inserts it
            // again, and one it holds, which moves it to the back.
            if sequence % 3 == 0 {
                model.on_put((sequence / 2).to_be_bytes().to_vec(), version(sequence / 2));
            }
            assert_eq!(
                model.known.values().sum::<usize>(),
                model.acknowledged.len(),
                "the index and the order disagree after {sequence} writes"
            );
            for entry in &model.acknowledged {
                assert!(
                    model
                        .known
                        .get(&entry.fingerprint())
                        .is_some_and(|count| *count > 0),
                    "the order holds a state the index does not after {sequence} writes"
                );
            }
        }
    }

    #[test]
    fn a_fingerprint_collision_keeps_the_state_in_the_window() {
        // Two states can share a fingerprint. A window whose index hits one and
        // holds the other has to add the new state anyway: dropping it would
        // take an acknowledged state out of the history, and a later read of it
        // would look impossible instead of being a rollback.
        let mut model = KeyModel::new(PossibleState::absent(None), WINDOW);
        let state = PossibleState::present(b"collided".to_vec(), version(7));
        let entry = AcknowledgedState::of(&state);
        // Stand in for another state in the window with the same fingerprint.
        model.known.insert(entry.fingerprint(), 1);

        model.remember(&state);
        assert_eq!(model.acknowledged.back(), Some(&entry));
        assert_eq!(model.known.get(&entry.fingerprint()), Some(&2));
        // The state is in the history, so the read is a rollback, not a value
        // the history never held.
        assert!(
            matches!(
                model.observe(state.value.as_deref(), state.version.as_ref()),
                Observation::Rollback { .. }
            ),
            "the collided state did not enter the window"
        );

        // When it leaves the window the index falls back to the other state's
        // count instead of losing a fingerprint the window still holds.
        for sequence in 1..=WINDOW as u64 {
            model.on_put(sequence.to_be_bytes().to_vec(), version(100 + sequence));
        }
        assert!(!model.acknowledged.contains(&entry));
        assert_eq!(model.known.get(&entry.fingerprint()), Some(&1));
        // The injected count is the only one that is not a state in the window.
        assert_eq!(
            model.known.values().sum::<usize>(),
            model.acknowledged.len() + 1
        );
    }
}
