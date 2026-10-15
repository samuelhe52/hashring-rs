// tonic service signatures intentionally return its concrete Status type.
#![allow(clippy::result_large_err)]

use std::{
    cmp::{Ordering, Reverse},
    collections::{BinaryHeap, HashMap, HashSet, VecDeque},
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering},
    },
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use tokio::sync::{Mutex, OwnedSemaphorePermit, RwLock, Semaphore, mpsc, watch};
use tonic::{
    Request, Response, Status,
    transport::{Channel, Endpoint},
};

pub use expiry::{EXPIRY_SWEEP_INTERVAL, EXPIRY_SWEEP_LIMIT};
pub use hashring_core::limits::{
    DEFAULT_MAX_KEY_BYTES, DEFAULT_MAX_VALUE_BYTES, MAX_CONTROL_MESSAGE_BYTES,
    MAX_DATA_MESSAGE_BYTES, MAX_MIGRATION_PAGE_BYTES, MAX_MUTATION_ID_BYTES,
};
pub use hashring_core::transport::{configure_coordinator_client, fetch_topology};

use hashring_core::{
    proto::{
        self, ApplyDedupBatchRequest, ApplyMigrationBatchRequest, ChangelogPageRequest,
        ChangelogPageResponse, DedupSnapshotPageResponse, DeduplicationRecord, DeleteRequest,
        DeleteResponse, ErrorCode, GetRequest, GetResponse, InstallTopologyRequest, JournalRecord,
        MigrationRecord, NodeInfoResponse, OperationError, PauseRangeResponse,
        PolicyWriteFenceRequest, PrepareDestinationRangeResponse, PrepareRangeRequest,
        PrepareSourceRangeResponse, PutRequest, PutResponse, RangeControlRequest,
        RangeDigestResponse, RecordVersion, RegisterNodeRequest, ReplicateMutationResponse,
        ReplicateMutationsResponse, ReplicationBatch, ReplicationCheckpointRequest,
        ReplicationEntry, ReplicationProgressRequest, ReplicationProgressResponse,
        SnapshotPageRequest, SnapshotPageResponse, StopRequest,
        coordinator_client::CoordinatorClient, data_node_client::DataNodeClient,
        data_node_server::DataNode,
    },
    topology::{TopologySnapshot, WriteAckPolicy},
    transport::configure_data_node_client,
};

pub const DEFAULT_MAX_MIGRATION_JOURNAL_BYTES: usize = 16 * 1024 * 1024;
pub const DEFAULT_MAX_PENDING_REPLICATION_BYTES: usize = 64 * 1024 * 1024;
const REPLICATION_BATCH_MAX_ENTRIES: usize = 64;
// Stay under the data-node client's encode limit so one batch fits a message.
const REPLICATION_BATCH_MAX_BYTES: usize = MAX_DATA_MESSAGE_BYTES - 64 * 1024;
pub const DEFAULT_REPLICATION_STREAM_QUEUE_CAPACITY: usize = 32;
const MAX_REPLICATION_FINGERPRINTS: u64 = 4_096;
pub const DEFAULT_REPLICATION_RPC_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);
/// Bounds the replica-readiness calls a node makes to the coordinator. Lease
/// renewal derives its own timeout from the cluster's lease duration.
const COORDINATOR_RPC_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);
// A node retains deduplication receipts for the full retention period on owner and
// follower paths. Account for live entries separately from staged migration
// records so the default limit bounds retained data without double charging.
pub const DEFAULT_MAX_DEDUP_BYTES: usize = 128 * 1024 * 1024;
pub const DEFAULT_REQUIRED_ACK_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);
const PEER_PROBE_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(500);

type RequiredAck = (u64, String, u64);

/// Keys or mutation IDs ordered by ring token.
///
/// A bucket per token rather than a plain set: two distinct keys can collide
/// on a 64-bit token, and dropping one would silently lose a record. Records
/// almost never collide, so a bucket is usually `One` and costs no allocation
/// beyond the tree node. Receipts do share tokens routinely, because every
/// retained receipt for a hot key lands in that key's bucket; `Many` keeps
/// their removal logarithmic in the bucket size.
#[derive(Debug)]
struct TokenIndex<T> {
    by_token: std::collections::BTreeMap<u64, TokenBucket<T>>,
}

#[derive(Debug)]
enum TokenBucket<T> {
    One(T),
    Many(std::collections::BTreeSet<T>),
}

impl<T> Default for TokenIndex<T> {
    fn default() -> Self {
        Self {
            by_token: std::collections::BTreeMap::new(),
        }
    }
}

impl<T: Ord + Clone> TokenIndex<T> {
    fn insert(&mut self, token: u64, value: T) {
        use std::collections::btree_map::Entry;
        match self.by_token.entry(token) {
            Entry::Vacant(entry) => {
                entry.insert(TokenBucket::One(value));
            }
            Entry::Occupied(mut entry) => {
                let bucket = entry.get_mut();
                match bucket {
                    TokenBucket::One(existing) if *existing == value => {}
                    TokenBucket::One(existing) => {
                        *bucket = TokenBucket::Many([existing.clone(), value].into());
                    }
                    TokenBucket::Many(values) => {
                        values.insert(value);
                    }
                }
            }
        }
    }

    /// Removes `value` from `token`'s bucket. Takes any borrowed form, so a
    /// caller holding `&[u8]` or `&str` need not build an `Arc` to compare.
    fn remove<Q>(&mut self, token: u64, value: &Q) -> bool
    where
        T: std::borrow::Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let Some(bucket) = self.by_token.get_mut(&token) else {
            return false;
        };
        let emptied = match bucket {
            TokenBucket::One(existing) if existing.borrow() == value => true,
            TokenBucket::One(_) => return false,
            TokenBucket::Many(values) => {
                if !values.remove(value) {
                    return false;
                }
                values.is_empty()
            }
        };
        if emptied {
            self.by_token.remove(&token);
        }
        true
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.by_token
            .values()
            .map(|bucket| match bucket {
                TokenBucket::One(_) => 1,
                TokenBucket::Many(values) => values.len(),
            })
            .sum()
    }

    #[cfg(test)]
    fn contains<Q>(&self, token: u64, value: &Q) -> bool
    where
        T: std::borrow::Borrow<Q>,
        Q: Ord + ?Sized,
    {
        match self.by_token.get(&token) {
            Some(TokenBucket::One(existing)) => existing.borrow() == value,
            Some(TokenBucket::Many(values)) => values.contains(value),
            None => false,
        }
    }

    /// Every value whose token falls in `range`, with that token, in token
    /// order. Equivalent to testing `range.contains` on every value, without
    /// the hashing.
    fn in_range(&self, range: &RangeSpec) -> Vec<(u64, T)> {
        let mut found = Vec::new();
        for bounds in range.token_bounds() {
            for (&token, bucket) in self.by_token.range(bounds) {
                match bucket {
                    TokenBucket::One(value) => found.push((token, value.clone())),
                    TokenBucket::Many(values) => {
                        found.extend(values.iter().map(|value| (token, value.clone())))
                    }
                }
            }
        }
        found
    }
}

struct DedupEntry {
    key: Arc<[u8]>,
    fingerprint: [u8; 32],
    version: RecordVersion,
    deleted: bool,
    retained_bytes: usize,
    expires_at: Instant,
    required_acks: Vec<RequiredAck>,
}

#[derive(Clone)]
struct Record {
    value: Arc<[u8]>,
    version: RecordVersion,
    deleted: bool,
    /// Absolute Unix milliseconds at which the record stops being readable, or
    /// `None` when it never expires. Absolute rather than a duration because
    /// every replica enforces it with its own clock and a follower promoted
    /// months later must agree with the record it already holds.
    expires_at_unix_millis: Option<u64>,
}

impl Record {
    /// A record past its deadline is absent, whichever replica is asked, so
    /// the expiry is enforced on every read rather than only on the owner.
    fn is_expired(&self, now_unix_millis: u64) -> bool {
        self.expires_at_unix_millis
            .is_some_and(|expires_at| now_unix_millis >= expires_at)
    }
}

/// The deadline a stored record carries. Zero is how a write says "no deadline"
/// on the wire, and a record never holds it, because a deadline of zero would
/// read as already expired.
fn stored_deadline(expires_at_unix_millis: Option<u64>) -> Option<u64> {
    expires_at_unix_millis.filter(|expires_at| *expires_at != 0)
}

#[derive(Clone, Eq, PartialEq)]
struct RangeSpec {
    change_id: String,
    range_id: String,
    start_exclusive: u64,
    end_inclusive: u64,
    source_node_id: String,
    destination_node_id: String,
}

impl RangeSpec {
    fn contains(&self, token: u64) -> bool {
        match self.start_exclusive.cmp(&self.end_inclusive) {
            Ordering::Less => token > self.start_exclusive && token <= self.end_inclusive,
            Ordering::Greater => token > self.start_exclusive || token <= self.end_inclusive,
            Ordering::Equal => true,
        }
    }

    /// The `BTreeMap` bounds covering exactly the tokens `contains` accepts:
    /// `(start_exclusive, end_inclusive]`. The start token belongs to the
    /// previous range, so excluding it keeps a boundary key in exactly one
    /// range.
    fn token_bounds(&self) -> Vec<(std::ops::Bound<&u64>, std::ops::Bound<&u64>)> {
        use std::ops::Bound;
        let start = Bound::Excluded(&self.start_exclusive);
        let end = Bound::Included(&self.end_inclusive);
        match self.start_exclusive.cmp(&self.end_inclusive) {
            // A wrapping range crosses the end of the token space, so it is
            // `[0, end]` plus `(start, MAX]`.
            Ordering::Greater => vec![(Bound::Unbounded, end), (start, Bound::Unbounded)],
            // An empty range would be `(Excluded(x), Included(x))`, which matches
            // nothing; `contains` treats start == end as covering everything.
            Ordering::Equal => vec![(Bound::Unbounded, Bound::Unbounded)],
            Ordering::Less => vec![(start, end)],
        }
    }

    fn key(&self) -> (String, String) {
        (self.change_id.clone(), self.range_id.clone())
    }
}

impl TryFrom<proto::RangeSpec> for RangeSpec {
    type Error = Status;

    fn try_from(range: proto::RangeSpec) -> Result<Self, Self::Error> {
        if range.change_id.is_empty()
            || range.range_id.is_empty()
            || range.source_node_id.is_empty()
            || range.destination_node_id.is_empty()
        {
            return Err(Status::invalid_argument(
                "range identifiers and node identifiers must not be empty",
            ));
        }
        Ok(Self {
            change_id: range.change_id,
            range_id: range.range_id,
            start_exclusive: range.start_exclusive,
            end_inclusive: range.end_inclusive,
            source_node_id: range.source_node_id,
            destination_node_id: range.destination_node_id,
        })
    }
}

struct SourceMigration {
    range: RangeSpec,
    /// The `Arc` keys the token index returned, in token order. Shared with
    /// `records`, so capturing a range's key list copies no key bytes.
    snapshot_keys: Vec<Arc<[u8]>>,
    snapshot_dedup_ids: Vec<Arc<str>>,
    journal: Vec<JournalRecord>,
    journal_bytes: usize,
    watermark: u64,
    writes_paused: bool,
}

struct DestinationMigration {
    range: RangeSpec,
    records: HashMap<Vec<u8>, Record>,
    dedup: HashMap<String, StagedDedup>,
    dedup_bytes: usize,
    watermark: u64,
    committed: bool,
    writes_activated: bool,
}

#[derive(Clone)]
struct StagedDedup {
    record: DeduplicationRecord,
    expires_at: Instant,
    retained_bytes: usize,
}

#[derive(Default)]
struct FollowerStreamState {
    applied_sequence: u64,
    last_owner_sequence: u64,
    checkpoint_sequence: u64,
    fingerprints: HashMap<u64, String>,
}

struct PreparedReplication {
    follower_node_id: String,
    follower_endpoint: String,
    stream_sequence: u64,
    mutation: Arc<ReplicationMutation>,
}

struct ReplicationMutation {
    topology_epoch: u64,
    owner_node_id: String,
    key: Arc<[u8]>,
    value: Arc<[u8]>,
    deleted: bool,
    version: RecordVersion,
    mutation_id: String,
    expires_at_unix_millis: Option<u64>,
    preserve_value: bool,
}

impl PreparedReplication {
    fn to_proto(&self) -> ReplicationEntry {
        ReplicationEntry {
            topology_epoch: self.mutation.topology_epoch,
            owner_node_id: self.mutation.owner_node_id.clone(),
            stream_sequence: self.stream_sequence,
            key: self.mutation.key.to_vec(),
            value: self.mutation.value.to_vec(),
            deleted: self.mutation.deleted,
            version: Some(self.mutation.version.clone()),
            mutation_id: self.mutation.mutation_id.clone(),
            expires_at_unix_millis: self.mutation.expires_at_unix_millis,
            preserve_value: self.mutation.preserve_value,
        }
    }
}

struct PendingReplication {
    prepared: PreparedReplication,
    _budget: Arc<ReplicationBudget>,
}

struct ReplicationReservation {
    queue: mpsc::OwnedPermit<PendingReplication>,
    budget: Arc<ReplicationBudget>,
}

struct ReplicationBudget(#[allow(dead_code)] OwnedSemaphorePermit);

type ReplicationStreamKey = (u64, String, String);

#[derive(Clone)]
struct ReplicationDispatcher {
    state: Arc<RwLock<NodeState>>,
    streams: Arc<StdMutex<HashMap<ReplicationStreamKey, mpsc::Sender<PendingReplication>>>>,
    failed_streams: Arc<StdMutex<HashSet<ReplicationStreamKey>>>,
    retained_budget: Arc<Semaphore>,
    active_rpc_budget: Arc<Semaphore>,
    max_pending_bytes: usize,
    stream_queue_capacity: usize,
    rpc_timeout: std::time::Duration,
    pressure: Arc<NodePressureStats>,
}

#[derive(Default)]
struct NodePressureStats {
    owner_write_timing: diagnostics::StateWriteTiming,
    follower_write_timing: diagnostics::StateWriteTiming,
    ack_write_timing: diagnostics::StateWriteTiming,
    owner_dedup_rejections: AtomicU64,
    follower_dedup_rejections: AtomicU64,
    replication_reservation_rejections: AtomicU64,
    replication_rpc_retries: AtomicU64,
    replication_stream_peak_pending: AtomicU64,
}

struct NodeState {
    topology: TopologySnapshot,
    records: HashMap<Arc<[u8]>, Record>,
    /// The same `Arc` keys as `records`, ordered by
    /// `TopologySnapshot::key_token`. A key's token depends only on its bytes
    /// and the snapshot's hash seed, so this index stays correct across epochs,
    /// members and virtual-node changes; only a reseed rebuilds it (see
    /// `set_topology`).
    records_by_token: TokenIndex<Arc<[u8]>>,
    /// The same `Arc` keys as `records`, ordered by their expiry deadline.
    /// Reads hide an expired record, so this heap exists only to find those
    /// records again without scanning every key on the node. Entries left
    /// behind by a later write to the same key are dropped when popped, since
    /// the map holds the only current deadline.
    record_expirations: BinaryHeap<Reverse<(Instant, Arc<[u8]>)>>,
    next_sequence: u64,
    owner_stream_sequences: HashMap<(u64, String), u64>,
    owner_stream_unacked: HashMap<(u64, String), VecDeque<(u64, u64)>>,
    ack_progress: HashMap<(u64, String), watch::Sender<u64>>,
    ack_process_instances: HashMap<(u64, String), String>,
    admitted_followers: HashMap<String, String>,
    follower_streams: HashMap<(u64, String), FollowerStreamState>,
    sources: HashMap<(String, String), SourceMigration>,
    destinations: HashMap<(String, String), DestinationMigration>,
    journal_bytes_total: usize,
    lease: Option<(u64, Instant)>,
    policy_write_fence: Option<(String, u64)>,
    dedup: HashMap<Arc<str>, DedupEntry>,
    /// The same `Arc<str>` mutation IDs as `dedup`, ordered by the token of the
    /// receipt's key. Holds the same invariant as `records_by_token`.
    dedup_by_token: TokenIndex<Arc<str>>,
    dedup_expirations: BinaryHeap<Reverse<(Instant, Arc<str>)>>,
    dedup_bytes: usize,
    dedup_peak_bytes: usize,
    ack_progress_needs_prune: bool,
    next_ack_prune_at: Instant,
    cleanup_timing: proto::ReceiptCleanupTiming,
}

/// Every mutation of `records` and `dedup` goes through these, so the token
/// indices cannot drift from the maps they mirror. Reaching into either map
/// directly is the one way to break that, which is what the tests'
/// `debug_assert_index` checks.
impl NodeState {
    fn token_of(&self, key: &[u8]) -> u64 {
        self.topology.key_token(key)
    }

    /// Swaps in a new topology snapshot. The token indices are keyed by
    /// `TopologySnapshot::key_token`, a function of the key bytes and the hash
    /// seed. The coordinator reuses the committed seed for every target
    /// topology (see `hashring_core::migration`), but a recreated coordinator
    /// picks a fresh one, so a reseed rebuilds both indices rather than leaving
    /// range queries answering from stale positions.
    fn set_topology(&mut self, topology: TopologySnapshot) {
        let reseeded = topology.hash_seed != self.topology.hash_seed;
        self.topology = topology;
        if !reseeded {
            return;
        }
        let mut records_by_token = TokenIndex::default();
        for key in self.records.keys() {
            records_by_token.insert(self.topology.key_token(key), key.clone());
        }
        let mut dedup_by_token = TokenIndex::default();
        for (id, entry) in &self.dedup {
            dedup_by_token.insert(self.topology.key_token(&entry.key), id.clone());
        }
        self.records_by_token = records_by_token;
        self.dedup_by_token = dedup_by_token;
    }

    /// Hashes the key itself rather than trusting a caller's token: a write
    /// computes its routing token before the write lock, and a reseed in
    /// between would index the key where no range query looks for it.
    fn insert_record(&mut self, key: Arc<[u8]>, record: Record) {
        if let Some(expires_at_unix_millis) = record.expires_at_unix_millis {
            self.record_expirations.push(Reverse((
                deadline_instant(expires_at_unix_millis),
                key.clone(),
            )));
        }
        // Replacing an existing key keeps the map's original `Arc`, which the
        // index already holds, so only a new key is indexed. Indexing the new
        // `Arc` instead would leave the map and index with separate copies.
        if self.records.insert(key.clone(), record).is_none() {
            let token = self.token_of(&key);
            self.records_by_token.insert(token, key);
        }
    }

    /// Applies a record only when it is newer than the stored one, matching
    /// [`apply_internal_record`]'s version rule.
    fn apply_record(&mut self, key: Arc<[u8]>, record: Record) -> bool {
        if self.records.get(&key).is_some_and(|current| {
            crate::migration::compare_versions(&record.version, &current.version).is_le()
        }) {
            return false;
        }
        self.insert_record(key, record);
        true
    }

    #[cfg(test)]
    fn remove_record(&mut self, key: &[u8]) -> Option<Record> {
        let token = self.token_of(key);
        self.remove_record_at(token, key)
    }

    /// [`remove_record`] for a caller that already has the key's token from
    /// [`record_keys_in_range`] under the same lock, so it matches the index.
    fn remove_record_at(&mut self, token: u64, key: &[u8]) -> Option<Record> {
        let removed = self.records.remove(key);
        if removed.is_some() {
            let removed = self.records_by_token.remove(token, key);
            debug_assert!(removed, "removed record was missing from the index");
        }
        removed
    }

    /// Every record key in `range` with its token, in token order.
    fn record_keys_in_range(&self, range: &RangeSpec) -> Vec<(u64, Arc<[u8]>)> {
        self.records_by_token.in_range(range)
    }

    /// Every live receipt id whose key falls in `range`, in token order.
    fn dedup_ids_in_range(&self, range: &RangeSpec) -> Vec<Arc<str>> {
        self.dedup_by_token
            .in_range(range)
            .into_iter()
            .map(|(_, id)| id)
            .collect()
    }

    /// Test-only reset. The indices cannot be cleared through `records.clear`,
    /// so the whole reset lives here where the invariant is kept in one place.
    #[cfg(test)]
    fn clear_records(&mut self) {
        self.records.clear();
        self.records_by_token = TokenIndex::default();
    }

    #[cfg(test)]
    fn clear_dedup(&mut self) {
        self.dedup.clear();
        self.dedup_by_token = TokenIndex::default();
    }

    fn insert_dedup_index(&mut self, id: &Arc<str>, key: &[u8]) {
        let token = self.token_of(key);
        self.dedup_by_token.insert(token, id.clone());
    }

    fn remove_dedup_index(&mut self, id: &Arc<str>, key: &[u8]) {
        let token = self.token_of(key);
        let removed = self.dedup_by_token.remove(token, &**id);
        debug_assert!(removed, "removed receipt was missing from the index");
    }

    /// The indices are load-bearing for migration correctness, so tests check
    /// them against the maps. Test-only: it rehashes every key on the node, so
    /// running it under the write lock would bring back the full scan the
    /// indices exist to avoid.
    #[cfg(test)]
    fn debug_assert_index(&self) {
        debug_assert_eq!(
            self.records_by_token.len(),
            self.records.len(),
            "records and records_by_token disagree",
        );
        debug_assert_eq!(
            self.dedup_by_token.len(),
            self.dedup.len(),
            "dedup and dedup_by_token disagree",
        );
        for key in self.records.keys() {
            debug_assert!(
                self.records_by_token.contains(self.token_of(key), key),
                "record is missing from records_by_token",
            );
        }
        for (id, entry) in &self.dedup {
            debug_assert!(
                self.dedup_by_token.contains(self.token_of(&entry.key), id),
                "receipt is missing from dedup_by_token",
            );
        }
    }
}

/// Per-node resource budgets and timeouts. Nodes in one cluster may use
/// different values; none of them affects correctness, only when a node
/// rejects or gives up on work.
#[derive(Clone, Debug)]
pub struct NodeSettings {
    /// Retained deduplication receipts, and separately each migration
    /// destination's staged receipts.
    pub max_dedup_bytes: usize,
    /// Mutations queued for followers, and separately the payload copies held
    /// by in-flight replication RPCs.
    pub max_pending_replication_bytes: usize,
    /// Writes journaled across this node's active migrations.
    pub max_migration_journal_bytes: usize,
    /// Mutations queued per follower stream, regardless of their size.
    pub replication_stream_queue_capacity: usize,
    /// How long a write waits for its required follower ACKs.
    pub required_ack_timeout: std::time::Duration,
    /// Bounds each replication connect and batch RPC.
    pub replication_rpc_timeout: std::time::Duration,
}

impl Default for NodeSettings {
    fn default() -> Self {
        Self {
            max_dedup_bytes: DEFAULT_MAX_DEDUP_BYTES,
            max_pending_replication_bytes: DEFAULT_MAX_PENDING_REPLICATION_BYTES,
            max_migration_journal_bytes: DEFAULT_MAX_MIGRATION_JOURNAL_BYTES,
            replication_stream_queue_capacity: DEFAULT_REPLICATION_STREAM_QUEUE_CAPACITY,
            required_ack_timeout: DEFAULT_REQUIRED_ACK_TIMEOUT,
            replication_rpc_timeout: DEFAULT_REPLICATION_RPC_TIMEOUT,
        }
    }
}

impl NodeSettings {
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(self.max_dedup_bytes > 0, "max_dedup_bytes must be positive");
        anyhow::ensure!(
            self.max_pending_replication_bytes > 0
                && self.max_pending_replication_bytes <= Semaphore::MAX_PERMITS,
            "max_pending_replication_bytes must be positive and at most {}",
            Semaphore::MAX_PERMITS
        );
        anyhow::ensure!(
            self.max_migration_journal_bytes > 0,
            "max_migration_journal_bytes must be positive"
        );
        anyhow::ensure!(
            self.replication_stream_queue_capacity > 0,
            "replication_stream_queue_capacity must be positive"
        );
        anyhow::ensure!(
            !self.required_ack_timeout.is_zero() && !self.replication_rpc_timeout.is_zero(),
            "timeouts must be positive"
        );
        Ok(())
    }
}

#[derive(Clone)]
pub struct DataNodeService {
    node_id: String,
    process_instance_id: String,
    coordinator_endpoint: String,
    coordinator_channel: Channel,
    state: Arc<RwLock<NodeState>>,
    replication_dispatch: ReplicationDispatcher,
    pressure: Arc<NodePressureStats>,
    refresh_lock: Arc<Mutex<()>>,
    max_key_bytes: usize,
    max_value_bytes: usize,
    max_journal_bytes: usize,
    max_dedup_bytes: usize,
    required_ack_timeout: std::time::Duration,
    stop_response_delay: std::time::Duration,
    stop_prepared: Arc<AtomicBool>,
    shutdown: watch::Sender<bool>,
}

mod dedup;
mod diagnostics;
mod expiry;
mod migration;
mod replication;
mod rpc;
mod service;

use dedup::*;
use expiry::*;
use migration::*;
use replication::*;

fn temporarily_unavailable(epoch: u64, message: impl Into<String>) -> OperationError {
    OperationError {
        current_epoch: epoch,
        ..operation_error(ErrorCode::TemporarilyUnavailable, message, true)
    }
}

fn peer_probe_targets(
    topology: &TopologySnapshot,
    node_id: &str,
) -> Vec<hashring_core::topology::Member> {
    let Some(index) = topology
        .members
        .iter()
        .position(|member| member.node_id == node_id)
    else {
        return Vec::new();
    };
    let count = topology.members.len();
    let mut targets = Vec::new();
    // Four stable reporters per physical node give each reachable reporter a
    // one-second cadence independent of cluster size. With three nodes, both
    // peers report; one broken link alone can never confirm a failure.
    for offset in [1, 2, count.saturating_sub(1), count.saturating_sub(2)] {
        let member = &topology.members[(index + offset) % count];
        if member.node_id != node_id
            && !targets
                .iter()
                .any(|existing: &hashring_core::topology::Member| {
                    existing.node_id == member.node_id
                })
        {
            targets.push(member.clone());
        }
    }
    targets
}

fn clear_replica_repair_staging(state: &mut NodeState) {
    let keys: Vec<_> = state
        .sources
        .keys()
        .filter(|(change_id, _)| change_id.starts_with("repair-"))
        .cloned()
        .collect();
    for key in keys {
        if let Some(source) = state.sources.remove(&key) {
            state.journal_bytes_total = state
                .journal_bytes_total
                .saturating_sub(source.journal_bytes);
        }
    }
    state
        .destinations
        .retain(|(change_id, _), _| !change_id.starts_with("repair-"));
}

fn outcome_unknown(epoch: u64) -> OperationError {
    OperationError {
        current_epoch: epoch,
        unknown_write_outcome: true,
        ..operation_error(
            ErrorCode::OutcomeUnknown,
            "required replica acknowledgement is uncertain",
            true,
        )
    }
}

fn range_contains(start: u64, end: u64, token: u64) -> bool {
    if start < end {
        token > start && token <= end
    } else if start > end {
        token > start || token <= end
    } else {
        true
    }
}

impl DataNodeService {
    fn validate_stop_request(&self, request: &StopRequest) -> Result<(), Status> {
        if request.node_id != self.node_id
            || request.process_instance_id != self.process_instance_id
        {
            return Err(Status::failed_precondition(
                "stop request does not match this node process instance",
            ));
        }
        Ok(())
    }
}

fn operation_error(code: ErrorCode, message: impl Into<String>, retryable: bool) -> OperationError {
    OperationError {
        code: code.into(),
        message: message.into(),
        retryable,
        ..Default::default()
    }
}

async fn fetch_pending_topology(endpoint: &str) -> anyhow::Result<Option<TopologySnapshot>> {
    let mut client =
        configure_coordinator_client(CoordinatorClient::connect(endpoint.to_owned()).await?);
    let response = client
        .get_topology_change(proto::Empty {})
        .await?
        .into_inner();
    response
        .change
        .filter(|change| {
            !matches!(
                proto::MigrationPhase::try_from(change.phase),
                Ok(proto::MigrationPhase::Complete | proto::MigrationPhase::Aborted)
            )
        })
        .and_then(|change| change.target_topology)
        .map(TryInto::try_into)
        .transpose()
        .map_err(Into::into)
}

async fn register_node(
    endpoint: &str,
    node_id: String,
    process_instance_id: String,
) -> anyhow::Result<()> {
    let mut client =
        configure_coordinator_client(CoordinatorClient::connect(endpoint.to_owned()).await?);
    client
        .register_node(RegisterNodeRequest {
            node_id,
            process_instance_id,
        })
        .await?;
    Ok(())
}

async fn wait_for_durable_stop_confirmation(
    coordinator_endpoint: String,
    node_id: String,
    process_instance_id: String,
    shutdown: watch::Sender<bool>,
) {
    loop {
        let confirmed = match CoordinatorClient::connect(coordinator_endpoint.clone()).await {
            Ok(client) => {
                let mut client = configure_coordinator_client(client);
                client
                    .is_stop_confirmed(StopRequest {
                        node_id: node_id.clone(),
                        process_instance_id: process_instance_id.clone(),
                    })
                    .await
                    .ok()
                    .is_some_and(|response| response.into_inner().confirmed)
            }
            Err(_) => false,
        };
        if confirmed {
            // Give the coordinator time to issue the final Stop RPC. If its
            // acknowledgement is lost, this durable-confirmation fallback
            // still terminates the exact prepared process instance.
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            let _ = shutdown.send(true);
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

#[cfg(test)]
mod tests;
