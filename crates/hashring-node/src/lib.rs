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
        ReplicationCheckpointRequest, ReplicationEntry, ReplicationProgressRequest,
        ReplicationProgressResponse, SnapshotPageRequest, SnapshotPageResponse, StopRequest,
        coordinator_client::CoordinatorClient, data_node_client::DataNodeClient,
        data_node_server::DataNode,
    },
    topology::{TopologySnapshot, WriteAckPolicy},
    transport::configure_data_node_client,
};

pub const DEFAULT_MAX_MIGRATION_JOURNAL_BYTES: usize = 16 * 1024 * 1024;
const MAX_PENDING_REPLICATION_BYTES: usize = 64 * 1024 * 1024;
const REPLICATION_STREAM_QUEUE_CAPACITY: usize = 32;
const MAX_REPLICATION_FINGERPRINTS: u64 = 4_096;
const REPLICATION_RPC_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);
const IDEMPOTENCY_WINDOW: std::time::Duration = std::time::Duration::from_secs(60);
// A node retains mutation IDs for the full retry window on both owner and
// follower paths. Account for live entries separately from staged migration
// records so the default limit bounds retained data without double charging.
const MAX_DEDUP_BYTES: usize = 128 * 1024 * 1024;
const REQUIRED_ACK_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);
const PEER_PROBE_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(500);

type RequiredAck = (u64, String, u64);

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
    snapshot_keys: Option<Vec<Vec<u8>>>,
    snapshot_dedup_ids: Vec<Arc<str>>,
    snapshot_ready: watch::Sender<bool>,
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
    records: HashMap<Vec<u8>, Record>,
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
    dedup_expirations: BinaryHeap<Reverse<(Instant, Arc<str>)>>,
    dedup_bytes: usize,
    dedup_peak_bytes: usize,
    ack_progress_needs_prune: bool,
    next_ack_prune_at: Instant,
    cleanup_timing: proto::ReceiptCleanupTiming,
}

struct SnapshotPreparationGuard {
    state: Arc<RwLock<NodeState>>,
    key: Option<(String, String)>,
}

impl SnapshotPreparationGuard {
    fn disarm(&mut self) {
        self.key = None;
    }
}

impl Drop for SnapshotPreparationGuard {
    fn drop(&mut self) {
        let Some(key) = self.key.take() else {
            return;
        };
        let state = self.state.clone();
        tokio::spawn(async move {
            let mut state = state.write().await;
            if state
                .sources
                .get(&key)
                .is_some_and(|source| source.snapshot_keys.is_none())
                && let Some(source) = state.sources.remove(&key)
            {
                state.journal_bytes_total = state
                    .journal_bytes_total
                    .saturating_sub(source.journal_bytes);
            }
        });
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
    stop_response_delay: std::time::Duration,
    stop_prepared: Arc<AtomicBool>,
    shutdown: watch::Sender<bool>,
}

mod dedup;
mod diagnostics;
mod migration;
mod replication;
mod rpc;
mod service;

use dedup::*;
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
