// tonic service signatures intentionally return its concrete Status type.
#![allow(clippy::result_large_err)]

use std::{
    cmp::{Ordering, Reverse},
    collections::{BinaryHeap, HashMap, HashSet, VecDeque},
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicBool, Ordering as AtomicOrdering},
    },
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use tokio::sync::{Mutex, OwnedSemaphorePermit, RwLock, Semaphore, mpsc, watch};
use tonic::{Request, Response, Status};

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
const REPLICATION_STREAM_QUEUE_CAPACITY: usize = 8;
const MAX_REPLICATION_FINGERPRINTS: u64 = 4_096;
const REPLICATION_RPC_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);
const IDEMPOTENCY_WINDOW: std::time::Duration = std::time::Duration::from_secs(60);
const MAX_DEDUP_BYTES: usize = 16 * 1024 * 1024;
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
    snapshot_dedup_ids: Vec<String>,
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
}

struct NodeState {
    topology: TopologySnapshot,
    records: HashMap<Vec<u8>, Record>,
    next_sequence: u64,
    owner_stream_sequences: HashMap<(u64, String), u64>,
    owner_stream_unacked: HashMap<(u64, String), VecDeque<(u64, u64)>>,
    ack_progress: HashMap<(u64, String), watch::Sender<u64>>,
    follower_streams: HashMap<(u64, String), FollowerStreamState>,
    sources: HashMap<(String, String), SourceMigration>,
    destinations: HashMap<(String, String), DestinationMigration>,
    journal_bytes_total: usize,
    lease: Option<(u64, Instant)>,
    policy_write_fence: Option<(String, u64)>,
    dedup: HashMap<String, DedupEntry>,
    dedup_expirations: BinaryHeap<Reverse<(Instant, String)>>,
    dedup_bytes: usize,
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
    state: Arc<RwLock<NodeState>>,
    replication_dispatch: ReplicationDispatcher,
    refresh_lock: Arc<Mutex<()>>,
    max_key_bytes: usize,
    max_value_bytes: usize,
    max_journal_bytes: usize,
    max_dedup_bytes: usize,
    stop_response_delay: std::time::Duration,
    stop_prepared: Arc<AtomicBool>,
    shutdown: watch::Sender<bool>,
}

impl DataNodeService {
    async fn ready_followers(&self, key: &[u8]) -> Result<(u64, Vec<String>), OperationError> {
        let topology = self.state.read().await.topology.clone();
        let token = topology.key_token(key);
        let guard = &topology.write_availability_guard;
        let needs_status = guard.minimum_admitted_copies > 1
            || guard.minimum_healthy_followers > 0
            || topology.write_ack_policy != WriteAckPolicy::OwnerOnly;
        let status = if needs_status {
            let result = tokio::time::timeout(REPLICATION_RPC_TIMEOUT, async {
                let mut client = configure_coordinator_client(
                    CoordinatorClient::connect(self.coordinator_endpoint.clone())
                        .await
                        .map_err(|error| error.to_string())?,
                );
                client
                    .get_replica_status(proto::Empty {})
                    .await
                    .map(Response::into_inner)
                    .map_err(|error| error.to_string())
            })
            .await;
            // Transport and RPC failures are deliberately indistinguishable at the
            // write gate: neither proves that a required copy is ready.
            match result {
                Ok(Ok(status)) => Some(status),
                _ => {
                    return Err(temporarily_unavailable(
                        topology.epoch,
                        "replica readiness could not be verified",
                    ));
                }
            }
        } else {
            None
        };
        let followers = required_followers(&topology, token, status.as_ref(), &self.node_id)?;
        Ok((topology.epoch, followers))
    }

    async fn wait_required_acks(
        &self,
        required: &[RequiredAck],
        epoch: u64,
    ) -> Result<(), OperationError> {
        if !required.is_empty() {
            let mut receivers = {
                let state = self.state.read().await;
                required
                    .iter()
                    .map(|(stream_epoch, node_id, sequence)| {
                        state
                            .ack_progress
                            .get(&(*stream_epoch, node_id.clone()))
                            .map(|sender| (sender.subscribe(), *sequence))
                    })
                    .collect::<Option<Vec<_>>>()
            }
            .ok_or_else(|| outcome_unknown(epoch))?;
            let wait = async {
                for (receiver, sequence) in &mut receivers {
                    while *receiver.borrow_and_update() < *sequence {
                        receiver
                            .changed()
                            .await
                            .map_err(|_| outcome_unknown(epoch))?;
                    }
                }
                Ok(())
            };
            tokio::time::timeout(REQUIRED_ACK_TIMEOUT, wait)
                .await
                .unwrap_or_else(|_| Err(outcome_unknown(epoch)))?;
        }
        let state = self.state.read().await;
        if state.lease.is_none_or(|(lease_epoch, expires_at)| {
            lease_epoch != epoch || state.topology.epoch != epoch || Instant::now() >= expires_at
        }) {
            return Err(outcome_unknown(epoch));
        }
        Ok(())
    }

    pub async fn connect(node_id: String, coordinator_endpoint: String) -> anyhow::Result<Self> {
        let process_instance_id = uuid::Uuid::new_v4().to_string();
        let topology = fetch_topology(&coordinator_endpoint).await?;
        let is_committed_member = topology
            .members
            .iter()
            .any(|member| member.node_id == node_id);
        let is_pending_member = if is_committed_member {
            false
        } else {
            fetch_pending_topology(&coordinator_endpoint)
                .await?
                .is_some_and(|target| {
                    target
                        .members
                        .iter()
                        .any(|member| member.node_id == node_id)
                })
        };
        if !is_committed_member && !is_pending_member {
            anyhow::bail!("node {node_id:?} is not present in coordinator topology");
        }
        register_node(
            &coordinator_endpoint,
            node_id.clone(),
            process_instance_id.clone(),
        )
        .await?;
        let (shutdown, _) = watch::channel(false);
        let state = Arc::new(RwLock::new(NodeState {
            topology,
            records: HashMap::new(),
            next_sequence: 0,
            owner_stream_sequences: HashMap::new(),
            owner_stream_unacked: HashMap::new(),
            ack_progress: HashMap::new(),
            follower_streams: HashMap::new(),
            sources: HashMap::new(),
            destinations: HashMap::new(),
            journal_bytes_total: 0,
            lease: None,
            policy_write_fence: None,
            dedup: HashMap::new(),
            dedup_expirations: BinaryHeap::new(),
            dedup_bytes: 0,
        }));
        let replication_dispatch = start_replication_dispatch(state.clone());
        let service = Self {
            node_id,
            process_instance_id,
            coordinator_endpoint,
            state,
            replication_dispatch,
            refresh_lock: Arc::new(Mutex::new(())),
            max_key_bytes: DEFAULT_MAX_KEY_BYTES,
            max_value_bytes: DEFAULT_MAX_VALUE_BYTES,
            max_journal_bytes: DEFAULT_MAX_MIGRATION_JOURNAL_BYTES,
            max_dedup_bytes: MAX_DEDUP_BYTES,
            stop_response_delay: std::time::Duration::ZERO,
            stop_prepared: Arc::new(AtomicBool::new(false)),
            shutdown,
        };
        if is_committed_member {
            service.renew_lease_once().await?;
        }
        service.start_lease_renewal();
        service.start_peer_probes();
        Ok(service)
    }

    async fn renew_lease_once(&self) -> anyhow::Result<()> {
        let epoch = self.state.read().await.topology.epoch;
        // Discount the entire RPC round trip from the granted lifetime. This
        // makes local authority expire no later than the coordinator's grant.
        let sent_at = Instant::now();
        let response = tokio::time::timeout(REPLICATION_RPC_TIMEOUT, async {
            let mut client = configure_coordinator_client(
                CoordinatorClient::connect(self.coordinator_endpoint.clone()).await?,
            );
            Ok::<_, anyhow::Error>(
                client
                    .renew_node_lease(proto::RenewNodeLeaseRequest {
                        node_id: self.node_id.clone(),
                        process_instance_id: self.process_instance_id.clone(),
                        topology_epoch: epoch,
                    })
                    .await?
                    .into_inner(),
            )
        })
        .await??;
        if response.topology_epoch == epoch && response.lease_duration_millis > 0 {
            let mut state = self.state.write().await;
            if state.topology.epoch == epoch {
                state.lease = Some((
                    epoch,
                    sent_at + std::time::Duration::from_millis(response.lease_duration_millis),
                ));
            }
        }
        Ok(())
    }

    fn start_lease_renewal(&self) {
        let service = self.clone();
        let mut shutdown = self.shutdown.subscribe();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        if service.renew_lease_once().await.is_err() {
                            // The coordinator may have published a new epoch.
                            let _ = service.refresh_topology().await;
                        }
                    }
                    _ = shutdown.changed() => {
                        if *shutdown.borrow() { break; }
                    }
                }
            }
        });
    }

    fn start_peer_probes(&self) {
        let service = self.clone();
        let mut shutdown = self.shutdown.subscribe();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = interval.tick() => service.probe_peers().await,
                    _ = shutdown.changed() => {
                        if *shutdown.borrow() { break; }
                    }
                }
            }
        });
    }

    async fn probe_peers(&self) {
        let (topology, leased) = {
            let state = self.state.read().await;
            let leased = state.lease.is_some_and(|(epoch, expires_at)| {
                epoch == state.topology.epoch && Instant::now() < expires_at
            });
            (state.topology.clone(), leased)
        };
        if !leased {
            return;
        }
        let selected = peer_probe_targets(&topology, &self.node_id);
        if selected.is_empty() {
            return;
        }
        let coordinator = match tokio::time::timeout(
            PEER_PROBE_TIMEOUT,
            CoordinatorClient::connect(self.coordinator_endpoint.clone()),
        )
        .await
        {
            Ok(Ok(client)) => configure_coordinator_client(client),
            _ => return,
        };
        let mut probes = tokio::task::JoinSet::new();
        for member in selected {
            let mut coordinator = coordinator.clone();
            let reporter_node_id = self.node_id.clone();
            let reporter_process_instance_id = self.process_instance_id.clone();
            let topology_epoch = topology.epoch;
            probes.spawn(async move {
                let reachable = tokio::time::timeout(PEER_PROBE_TIMEOUT, async {
                    let mut client = configure_data_node_client(
                        DataNodeClient::connect(member.endpoint.clone())
                            .await
                            .ok()?,
                    );
                    let info = client
                        .get_process_info(proto::Empty {})
                        .await
                        .ok()?
                        .into_inner();
                    Some(info.node_id == member.node_id)
                })
                .await
                .ok()
                .flatten()
                .unwrap_or(false);
                let report = proto::ReportPeerHealthRequest {
                    reporter_node_id,
                    reporter_process_instance_id,
                    peer_node_id: member.node_id.clone(),
                    topology_epoch,
                    reachable,
                };
                let _ = tokio::time::timeout(
                    PEER_PROBE_TIMEOUT,
                    coordinator.report_peer_health(report),
                )
                .await;
            });
        }
        while probes.join_next().await.is_some() {}
    }

    pub fn with_stop_response_delay(mut self, delay: std::time::Duration) -> Self {
        self.stop_response_delay = delay;
        self
    }

    pub fn shutdown_receiver(&self) -> watch::Receiver<bool> {
        self.shutdown.subscribe()
    }

    pub fn process_instance_id(&self) -> &str {
        &self.process_instance_id
    }

    async fn refresh_if_newer(&self, request_epoch: u64) -> Result<(), Status> {
        if self.state.read().await.topology.epoch >= request_epoch {
            return Ok(());
        }
        let _guard = self.refresh_lock.lock().await;
        if self.state.read().await.topology.epoch >= request_epoch {
            return Ok(());
        }
        self.refresh_topology().await
    }

    async fn refresh_topology(&self) -> Result<(), Status> {
        let topology = fetch_topology(&self.coordinator_endpoint)
            .await
            .map_err(|error| Status::unavailable(error.to_string()))?;
        let mut state = self.state.write().await;
        if topology.epoch >= state.topology.epoch {
            let epoch = topology.epoch;
            if epoch != state.topology.epoch {
                state.lease = None;
                clear_replica_repair_staging(&mut state);
            }
            state.topology = topology;
            state
                .owner_stream_sequences
                .retain(|(stream_epoch, _), _| *stream_epoch == epoch);
            state
                .owner_stream_unacked
                .retain(|(stream_epoch, _), _| *stream_epoch == epoch);
            state
                .follower_streams
                .retain(|(stream_epoch, _), _| *stream_epoch == epoch);
            prune_ack_progress(&mut state);
            self.replication_dispatch.prune_epoch(epoch);
        }
        Ok(())
    }

    fn validate_key(&self, key: &[u8]) -> Option<OperationError> {
        if key.is_empty() {
            return Some(operation_error(
                ErrorCode::InvalidArgument,
                "key must not be empty",
                false,
            ));
        }
        if key.len() > self.max_key_bytes {
            return Some(operation_error(
                ErrorCode::TooLarge,
                format!("key exceeds {} bytes", self.max_key_bytes),
                false,
            ));
        }
        None
    }

    fn owner_error(&self, state: &NodeState, key: &[u8]) -> Result<Option<OperationError>, Status> {
        let owner = state
            .topology
            .owner(key)
            .map_err(|error| Status::internal(error.to_string()))?;
        if owner.node_id == self.node_id {
            if state.lease.is_none_or(|(epoch, expires_at)| {
                epoch != state.topology.epoch || Instant::now() >= expires_at
            }) {
                return Ok(Some(OperationError {
                    current_epoch: state.topology.epoch,
                    ..operation_error(ErrorCode::LeaseExpired, "owner lease has expired", true)
                }));
            }
            return Ok(None);
        }
        Ok(Some(OperationError {
            code: ErrorCode::Moved.into(),
            message: format!("key is owned by {}", owner.node_id),
            current_epoch: state.topology.epoch,
            owner_endpoint: owner.endpoint.clone(),
            retryable: true,
            unknown_write_outcome: false,
        }))
    }
}

#[tonic::async_trait]
impl DataNode for DataNodeService {
    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let request = request.into_inner();
        if let Some(error) = self.validate_key(&request.key) {
            return Ok(Response::new(GetResponse {
                error: Some(error),
                ..Default::default()
            }));
        }
        self.refresh_if_newer(request.topology_epoch).await?;
        let state = self.state.read().await;
        if let Some(error) = self.owner_error(&state, &request.key)? {
            return Ok(Response::new(GetResponse {
                current_epoch: error.current_epoch,
                error: Some(error),
                ..Default::default()
            }));
        }
        let Some(record) = state
            .records
            .get(&request.key)
            .filter(|record| !record.deleted)
        else {
            return Ok(Response::new(GetResponse {
                current_epoch: state.topology.epoch,
                error: Some(OperationError {
                    current_epoch: state.topology.epoch,
                    ..operation_error(ErrorCode::NotFound, "key not found", false)
                }),
                ..Default::default()
            }));
        };
        Ok(Response::new(GetResponse {
            value: record.value.to_vec(),
            version: Some(record.version.clone()),
            current_epoch: state.topology.epoch,
            error: None,
        }))
    }

    async fn put(&self, request: Request<PutRequest>) -> Result<Response<PutResponse>, Status> {
        let request = request.into_inner();
        if let Some(error) = self.validate_key(&request.key) {
            return Ok(Response::new(PutResponse {
                error: Some(error),
                ..Default::default()
            }));
        }
        if request.request_id.is_empty() || request.request_id.len() > MAX_MUTATION_ID_BYTES {
            return Ok(Response::new(PutResponse {
                error: Some(operation_error(
                    ErrorCode::InvalidArgument,
                    format!("mutation id must contain 1 to {MAX_MUTATION_ID_BYTES} bytes"),
                    false,
                )),
                ..Default::default()
            }));
        }
        if request.value.len() > self.max_value_bytes {
            return Ok(Response::new(PutResponse {
                error: Some(operation_error(
                    ErrorCode::TooLarge,
                    format!("value exceeds {} bytes", self.max_value_bytes),
                    false,
                )),
                ..Default::default()
            }));
        }
        self.refresh_if_newer(request.topology_epoch).await?;
        {
            let state = self.state.read().await;
            if let Some(error) = self.owner_error(&state, &request.key)? {
                return Ok(Response::new(PutResponse {
                    current_epoch: error.current_epoch,
                    error: Some(error),
                    ..Default::default()
                }));
            }
            if let Some(existing) = state
                .dedup
                .get(&request.request_id)
                .filter(|entry| entry.expires_at > Instant::now())
            {
                if existing.fingerprint != mutation_fingerprint(&request.key, &request.value, false)
                    || existing.deleted
                {
                    return Ok(Response::new(PutResponse {
                        current_epoch: state.topology.epoch,
                        error: Some(operation_error(
                            ErrorCode::MutationIdConflict,
                            "mutation ID was reused with a different operation or payload",
                            false,
                        )),
                        ..Default::default()
                    }));
                }
                let version = existing.version.clone();
                let required = existing.required_acks.clone();
                let epoch = state.topology.epoch;
                drop(state);
                let error = self.wait_required_acks(&required, epoch).await.err();
                return Ok(Response::new(PutResponse {
                    version: error.is_none().then_some(version),
                    current_epoch: epoch,
                    error,
                }));
            }
        }
        let (readiness_epoch, ready_followers) = match self.ready_followers(&request.key).await {
            Ok(ready) => ready,
            Err(error) => {
                return Ok(Response::new(PutResponse {
                    current_epoch: error.current_epoch,
                    error: Some(error),
                    ..Default::default()
                }));
            }
        };
        let mut state = self.state.write().await;
        if let Some(error) = self.owner_error(&state, &request.key)? {
            return Ok(Response::new(PutResponse {
                current_epoch: error.current_epoch,
                error: Some(error),
                ..Default::default()
            }));
        }
        if state.topology.epoch != readiness_epoch {
            let error = temporarily_unavailable(
                state.topology.epoch,
                "topology changed while checking replica readiness",
            );
            return Ok(Response::new(PutResponse {
                current_epoch: error.current_epoch,
                error: Some(error),
                ..Default::default()
            }));
        }
        let dedup_now = Instant::now();
        purge_expired_dedup(&mut state, dedup_now);
        let fingerprint = mutation_fingerprint(&request.key, &request.value, false);
        if let Some(existing) = state.dedup.get(&request.request_id) {
            if existing.fingerprint == fingerprint && !existing.deleted {
                let version = existing.version.clone();
                let required = existing.required_acks.clone();
                let epoch = state.topology.epoch;
                drop(state);
                let error = self.wait_required_acks(&required, epoch).await.err();
                return Ok(Response::new(PutResponse {
                    version: error.is_none().then_some(version),
                    current_epoch: epoch,
                    error,
                }));
            }
            return Ok(Response::new(PutResponse {
                current_epoch: state.topology.epoch,
                error: Some(operation_error(
                    ErrorCode::MutationIdConflict,
                    "mutation ID was reused with a different operation or payload",
                    false,
                )),
                ..Default::default()
            }));
        }
        let ack_cost = required_ack_retained_bytes(&ready_followers);
        let dedup_cost =
            dedup_retained_bytes(&request.request_id, request.key.len(), self.node_id.len())
                .saturating_add(ack_cost);
        if state.dedup_bytes.saturating_add(dedup_cost) > self.max_dedup_bytes {
            return Ok(Response::new(PutResponse {
                current_epoch: state.topology.epoch,
                error: Some(operation_error(
                    ErrorCode::ResourceExhausted,
                    "mutation retry window is full; retry with backoff",
                    true,
                )),
                ..Default::default()
            }));
        }

        if state.policy_write_fence.is_some() {
            return Ok(Response::new(PutResponse {
                current_epoch: state.topology.epoch,
                error: Some(OperationError {
                    current_epoch: state.topology.epoch,
                    ..operation_error(
                        ErrorCode::RangeBusy,
                        "writes are fenced for ACK-policy transition",
                        true,
                    )
                }),
                ..Default::default()
            }));
        }
        let token = state.topology.key_token(&request.key);
        let journal_record_bytes =
            request.key.len() + request.value.len() + request.request_id.len() + 128;
        let matching_sources = state
            .sources
            .values()
            .filter(|source| source.range.contains(token))
            .count();
        for source in state
            .sources
            .values()
            .filter(|source| source.range.contains(token))
        {
            if source.writes_paused {
                return Ok(Response::new(PutResponse {
                    current_epoch: state.topology.epoch,
                    error: Some(OperationError {
                        current_epoch: state.topology.epoch,
                        ..operation_error(
                            ErrorCode::RangeBusy,
                            "range writes are fenced for topology cutover",
                            true,
                        )
                    }),
                    ..Default::default()
                }));
            }
        }
        if state
            .destinations
            .values()
            .any(|destination| !destination.writes_activated && destination.range.contains(token))
        {
            return Ok(Response::new(PutResponse {
                current_epoch: state.topology.epoch,
                error: Some(OperationError {
                    current_epoch: state.topology.epoch,
                    ..operation_error(
                        ErrorCode::RangeBusy,
                        "range writes are fenced for topology cutover",
                        true,
                    )
                }),
                ..Default::default()
            }));
        }
        let journal_growth = journal_record_bytes.saturating_mul(matching_sources);
        if state.journal_bytes_total.saturating_add(journal_growth) > self.max_journal_bytes {
            return Ok(Response::new(PutResponse {
                current_epoch: state.topology.epoch,
                error: Some(OperationError {
                    current_epoch: state.topology.epoch,
                    ..operation_error(
                        ErrorCode::ResourceExhausted,
                        "node migration journal budget is full; retry with backoff",
                        true,
                    )
                }),
                ..Default::default()
            }));
        }

        let next_sequence = state.next_sequence + 1;
        let version = RecordVersion {
            topology_epoch: state.topology.epoch,
            owner_sequence: next_sequence,
            owner_node_id: self.node_id.clone(),
        };
        let mutation_id = request.request_id.clone();
        let replications = match prepare_replication_entries(
            &mut state,
            &request.key,
            &request.value,
            false,
            &version,
            request.request_id,
        ) {
            Ok(replications) => replications,
            Err(()) => {
                return Ok(Response::new(PutResponse {
                    current_epoch: state.topology.epoch,
                    error: Some(replication_backpressure_error(state.topology.epoch)),
                    ..Default::default()
                }));
            }
        };
        let reservations = match self.replication_dispatch.reserve(&replications) {
            Ok(reservations) => reservations,
            Err(()) => {
                rollback_replication_sequences(&mut state, &replications);
                return Ok(Response::new(PutResponse {
                    current_epoch: state.topology.epoch,
                    error: Some(replication_backpressure_error(state.topology.epoch)),
                    ..Default::default()
                }));
            }
        };
        let required_acks: Vec<RequiredAck> = replications
            .iter()
            .filter(|entry| ready_followers.contains(&entry.follower_node_id))
            .map(|entry| {
                (
                    entry.mutation.topology_epoch,
                    entry.follower_node_id.clone(),
                    entry.stream_sequence,
                )
            })
            .collect();
        state.next_sequence = next_sequence;
        let now = now_unix_millis();
        for entry in &replications {
            state
                .owner_stream_unacked
                .entry((
                    entry.mutation.topology_epoch,
                    entry.follower_node_id.clone(),
                ))
                .or_default()
                .push_back((entry.stream_sequence, now));
        }
        let record = Record {
            value: request.value.into(),
            version: version.clone(),
            deleted: false,
        };
        state.records.insert(request.key.clone(), record.clone());
        for source in state
            .sources
            .values_mut()
            .filter(|source| source.range.contains(token))
        {
            source.watermark += 1;
            source.journal_bytes += journal_record_bytes;
            source.journal.push(JournalRecord {
                watermark: source.watermark,
                record: Some(MigrationRecord {
                    key: request.key.clone(),
                    value: record.value.to_vec(),
                    version: Some(record.version.clone()),
                    deleted: false,
                    mutation_id: mutation_id.clone(),
                    remaining_window_millis: 0,
                }),
            });
        }
        state.journal_bytes_total += journal_growth;
        insert_dedup(
            &mut state,
            mutation_id.clone(),
            Arc::from(request.key.as_slice()),
            fingerprint,
            version.clone(),
            false,
            Instant::now(),
        );
        let dedup = state
            .dedup
            .get_mut(&mutation_id)
            .expect("new retry record exists");
        dedup.required_acks = required_acks.clone();
        dedup.retained_bytes += ack_cost;
        state.dedup_bytes += ack_cost;
        let current_epoch = state.topology.epoch;
        self.replication_dispatch
            .dispatch(replications, reservations);
        drop(state);
        let error = self
            .wait_required_acks(&required_acks, current_epoch)
            .await
            .err();
        Ok(Response::new(PutResponse {
            version: error.is_none().then_some(version),
            current_epoch,
            error,
        }))
    }

    async fn delete(
        &self,
        request: Request<DeleteRequest>,
    ) -> Result<Response<DeleteResponse>, Status> {
        let request = request.into_inner();
        if let Some(error) = self.validate_key(&request.key) {
            return Ok(Response::new(DeleteResponse {
                error: Some(error),
                ..Default::default()
            }));
        }
        if request.request_id.is_empty() || request.request_id.len() > MAX_MUTATION_ID_BYTES {
            return Ok(Response::new(DeleteResponse {
                error: Some(operation_error(
                    ErrorCode::InvalidArgument,
                    format!("mutation id must contain 1 to {MAX_MUTATION_ID_BYTES} bytes"),
                    false,
                )),
                ..Default::default()
            }));
        }
        self.refresh_if_newer(request.topology_epoch).await?;
        {
            let state = self.state.read().await;
            if let Some(error) = self.owner_error(&state, &request.key)? {
                return Ok(Response::new(DeleteResponse {
                    current_epoch: error.current_epoch,
                    error: Some(error),
                }));
            }
            if let Some(existing) = state
                .dedup
                .get(&request.request_id)
                .filter(|entry| entry.expires_at > Instant::now())
            {
                if existing.fingerprint != mutation_fingerprint(&request.key, &[], true)
                    || !existing.deleted
                {
                    return Ok(Response::new(DeleteResponse {
                        current_epoch: state.topology.epoch,
                        error: Some(operation_error(
                            ErrorCode::MutationIdConflict,
                            "mutation ID was reused with a different operation or payload",
                            false,
                        )),
                    }));
                }
                let required = existing.required_acks.clone();
                let epoch = state.topology.epoch;
                drop(state);
                let error = self.wait_required_acks(&required, epoch).await.err();
                return Ok(Response::new(DeleteResponse {
                    current_epoch: epoch,
                    error,
                }));
            }
        }
        let (readiness_epoch, ready_followers) = match self.ready_followers(&request.key).await {
            Ok(ready) => ready,
            Err(error) => {
                return Ok(Response::new(DeleteResponse {
                    current_epoch: error.current_epoch,
                    error: Some(error),
                }));
            }
        };
        let mut state = self.state.write().await;
        if let Some(error) = self.owner_error(&state, &request.key)? {
            return Ok(Response::new(DeleteResponse {
                current_epoch: error.current_epoch,
                error: Some(error),
            }));
        }
        if state.topology.epoch != readiness_epoch {
            let error = temporarily_unavailable(
                state.topology.epoch,
                "topology changed while checking replica readiness",
            );
            return Ok(Response::new(DeleteResponse {
                current_epoch: error.current_epoch,
                error: Some(error),
            }));
        }
        let dedup_now = Instant::now();
        purge_expired_dedup(&mut state, dedup_now);
        let fingerprint = mutation_fingerprint(&request.key, &[], true);
        if let Some(existing) = state.dedup.get(&request.request_id) {
            if existing.fingerprint == fingerprint && existing.deleted {
                let required = existing.required_acks.clone();
                let epoch = state.topology.epoch;
                drop(state);
                let error = self.wait_required_acks(&required, epoch).await.err();
                return Ok(Response::new(DeleteResponse {
                    current_epoch: epoch,
                    error,
                }));
            }
            return Ok(Response::new(DeleteResponse {
                current_epoch: state.topology.epoch,
                error: Some(operation_error(
                    ErrorCode::MutationIdConflict,
                    "mutation ID was reused with a different operation or payload",
                    false,
                )),
            }));
        }
        let ack_cost = required_ack_retained_bytes(&ready_followers);
        let dedup_cost =
            dedup_retained_bytes(&request.request_id, request.key.len(), self.node_id.len())
                .saturating_add(ack_cost);
        if state.dedup_bytes.saturating_add(dedup_cost) > self.max_dedup_bytes {
            return Ok(Response::new(DeleteResponse {
                current_epoch: state.topology.epoch,
                error: Some(operation_error(
                    ErrorCode::ResourceExhausted,
                    "mutation retry window is full; retry with backoff",
                    true,
                )),
            }));
        }

        if state.policy_write_fence.is_some() {
            return Ok(Response::new(DeleteResponse {
                current_epoch: state.topology.epoch,
                error: Some(OperationError {
                    current_epoch: state.topology.epoch,
                    ..operation_error(
                        ErrorCode::RangeBusy,
                        "writes are fenced for ACK-policy transition",
                        true,
                    )
                }),
            }));
        }
        let token = state.topology.key_token(&request.key);
        for source in state
            .sources
            .values()
            .filter(|source| source.range.contains(token))
        {
            if source.writes_paused {
                return Ok(Response::new(DeleteResponse {
                    current_epoch: state.topology.epoch,
                    error: Some(OperationError {
                        current_epoch: state.topology.epoch,
                        ..operation_error(
                            ErrorCode::RangeBusy,
                            "range writes are fenced for topology cutover",
                            true,
                        )
                    }),
                }));
            }
        }
        if state
            .destinations
            .values()
            .any(|destination| !destination.writes_activated && destination.range.contains(token))
        {
            return Ok(Response::new(DeleteResponse {
                current_epoch: state.topology.epoch,
                error: Some(OperationError {
                    current_epoch: state.topology.epoch,
                    ..operation_error(
                        ErrorCode::RangeBusy,
                        "range writes are fenced for topology cutover",
                        true,
                    )
                }),
            }));
        }

        let journal_record_bytes = request.key.len() + request.request_id.len() + 128;
        let matching_sources = state
            .sources
            .values()
            .filter(|source| source.range.contains(token))
            .count();
        let journal_growth = journal_record_bytes.saturating_mul(matching_sources);
        if state.journal_bytes_total.saturating_add(journal_growth) > self.max_journal_bytes {
            return Ok(Response::new(DeleteResponse {
                current_epoch: state.topology.epoch,
                error: Some(OperationError {
                    current_epoch: state.topology.epoch,
                    ..operation_error(
                        ErrorCode::ResourceExhausted,
                        "node migration journal budget is full; retry with backoff",
                        true,
                    )
                }),
            }));
        }

        let next_sequence = state.next_sequence + 1;
        let version = RecordVersion {
            topology_epoch: state.topology.epoch,
            owner_sequence: next_sequence,
            owner_node_id: self.node_id.clone(),
        };
        let mutation_id = request.request_id.clone();
        let replications = match prepare_replication_entries(
            &mut state,
            &request.key,
            &[],
            true,
            &version,
            request.request_id,
        ) {
            Ok(replications) => replications,
            Err(()) => {
                return Ok(Response::new(DeleteResponse {
                    current_epoch: state.topology.epoch,
                    error: Some(replication_backpressure_error(state.topology.epoch)),
                }));
            }
        };
        let required_acks: Vec<RequiredAck> = replications
            .iter()
            .filter(|entry| ready_followers.contains(&entry.follower_node_id))
            .map(|entry| {
                (
                    entry.mutation.topology_epoch,
                    entry.follower_node_id.clone(),
                    entry.stream_sequence,
                )
            })
            .collect();
        let reservations = match self.replication_dispatch.reserve(&replications) {
            Ok(reservations) => reservations,
            Err(()) => {
                rollback_replication_sequences(&mut state, &replications);
                return Ok(Response::new(DeleteResponse {
                    current_epoch: state.topology.epoch,
                    error: Some(replication_backpressure_error(state.topology.epoch)),
                }));
            }
        };
        state.next_sequence = next_sequence;
        let now = now_unix_millis();
        for entry in &replications {
            state
                .owner_stream_unacked
                .entry((
                    entry.mutation.topology_epoch,
                    entry.follower_node_id.clone(),
                ))
                .or_default()
                .push_back((entry.stream_sequence, now));
        }
        apply_internal_record(
            &mut state.records,
            request.key.clone(),
            Record {
                value: Arc::from([]),
                version: version.clone(),
                deleted: true,
            },
        );
        for source in state
            .sources
            .values_mut()
            .filter(|source| source.range.contains(token))
        {
            source.watermark += 1;
            source.journal_bytes += journal_record_bytes;
            source.journal.push(JournalRecord {
                watermark: source.watermark,
                record: Some(MigrationRecord {
                    key: request.key.clone(),
                    value: Vec::new(),
                    version: Some(version.clone()),
                    deleted: true,
                    mutation_id: mutation_id.clone(),
                    remaining_window_millis: 0,
                }),
            });
        }
        state.journal_bytes_total += journal_growth;
        insert_dedup(
            &mut state,
            mutation_id.clone(),
            Arc::from(request.key.as_slice()),
            fingerprint,
            version.clone(),
            true,
            Instant::now(),
        );
        let dedup = state
            .dedup
            .get_mut(&mutation_id)
            .expect("new retry record exists");
        dedup.required_acks = required_acks.clone();
        dedup.retained_bytes += ack_cost;
        state.dedup_bytes += ack_cost;
        let current_epoch = state.topology.epoch;
        self.replication_dispatch
            .dispatch(replications, reservations);
        drop(state);
        let error = self
            .wait_required_acks(&required_acks, current_epoch)
            .await
            .err();
        Ok(Response::new(DeleteResponse {
            current_epoch,
            error,
        }))
    }

    async fn get_replication_progress(
        &self,
        request: Request<ReplicationProgressRequest>,
    ) -> Result<Response<ReplicationProgressResponse>, Status> {
        let request = request.into_inner();
        let state = self.state.read().await;
        if request.topology_epoch != state.topology.epoch {
            return Err(Status::failed_precondition(
                "replication progress epoch is stale",
            ));
        }
        if request.owner_node_id.is_empty() || request.follower_node_id.is_empty() {
            return Err(Status::invalid_argument(
                "replication stream identity is empty",
            ));
        }
        let (stream_sequence, oldest_unacked_unix_millis) = if self.node_id == request.owner_node_id
        {
            if self
                .replication_dispatch
                .failed_streams
                .lock()
                .map_err(|_| Status::internal("replication stream state is poisoned"))?
                .contains(&(
                    request.topology_epoch,
                    request.owner_node_id.clone(),
                    request.follower_node_id.clone(),
                ))
            {
                return Err(Status::failed_precondition(
                    "owner replication stream is terminal",
                ));
            }
            let key = (request.topology_epoch, request.follower_node_id);
            (
                state
                    .owner_stream_sequences
                    .get(&key)
                    .copied()
                    .unwrap_or_default(),
                state
                    .owner_stream_unacked
                    .get(&key)
                    .and_then(|queue| queue.front().map(|(_, at)| *at))
                    .unwrap_or_default(),
            )
        } else if self.node_id == request.follower_node_id {
            let key = (request.topology_epoch, request.owner_node_id);
            state
                .follower_streams
                .get(&key)
                .map_or((0, 0), |stream| (stream.applied_sequence, 0))
        } else {
            return Err(Status::failed_precondition(
                "node is not part of this replication stream",
            ));
        };
        Ok(Response::new(ReplicationProgressResponse {
            process_instance_id: self.process_instance_id.clone(),
            stream_sequence,
            oldest_unacked_unix_millis,
        }))
    }

    async fn install_replication_checkpoint(
        &self,
        request: Request<ReplicationCheckpointRequest>,
    ) -> Result<Response<ReplicationProgressResponse>, Status> {
        let request = request.into_inner();
        let mut state = self.state.write().await;
        if request.topology_epoch != state.topology.epoch
            || request.follower_node_id != self.node_id
        {
            return Err(Status::failed_precondition(
                "checkpoint does not match follower epoch",
            ));
        }
        let expected: HashSet<_> = state
            .topology
            .derived_ranges()
            .map_err(|error| Status::internal(error.to_string()))?
            .into_iter()
            .filter(|range| {
                range.owner_node_id == request.owner_node_id
                    && range.follower_node_ids.contains(&self.node_id)
            })
            .map(|range| (range.start_exclusive, range.end_inclusive))
            .collect();
        if expected.is_empty() || request.verified_ranges.len() != expected.len() {
            return Err(Status::failed_precondition(
                "checkpoint does not cover the full follower stream",
            ));
        }
        let mut actual = HashSet::new();
        for control in &request.verified_ranges {
            let destination = state
                .destinations
                .get(&(control.change_id.clone(), control.range_id.clone()))
                .ok_or_else(|| Status::failed_precondition("checkpoint range was not prepared"))?;
            if !destination.committed
                || destination.range.source_node_id != request.owner_node_id
                || destination.range.destination_node_id != self.node_id
                || !actual.insert((
                    destination.range.start_exclusive,
                    destination.range.end_inclusive,
                ))
            {
                return Err(Status::failed_precondition(
                    "checkpoint contains an unverified or duplicate range",
                ));
            }
        }
        if actual != expected {
            return Err(Status::failed_precondition(
                "checkpoint has incomplete stream coverage",
            ));
        }
        let stream = state
            .follower_streams
            .entry((request.topology_epoch, request.owner_node_id))
            .or_default();
        if request.stream_sequence < stream.applied_sequence {
            return Err(Status::failed_precondition(
                "checkpoint would rewind the follower stream",
            ));
        }
        stream.applied_sequence = request.stream_sequence;
        stream.checkpoint_sequence = request.stream_sequence;
        Ok(Response::new(ReplicationProgressResponse {
            process_instance_id: self.process_instance_id.clone(),
            stream_sequence: stream.applied_sequence,
            oldest_unacked_unix_millis: 0,
        }))
    }

    async fn replicate_mutation(
        &self,
        request: Request<ReplicationEntry>,
    ) -> Result<Response<ReplicateMutationResponse>, Status> {
        let entry = request.into_inner();
        if entry.stream_sequence == 0 {
            return Err(Status::invalid_argument(
                "replication stream sequence must be greater than zero",
            ));
        }
        if entry.mutation_id.is_empty() || entry.mutation_id.len() > MAX_MUTATION_ID_BYTES {
            return Err(Status::invalid_argument(format!(
                "mutation id must contain 1 to {MAX_MUTATION_ID_BYTES} bytes"
            )));
        }
        if entry.key.is_empty() || entry.key.len() > self.max_key_bytes {
            return Err(Status::invalid_argument("replication key size is invalid"));
        }
        if entry.value.len() > self.max_value_bytes {
            return Err(Status::resource_exhausted(
                "replication value exceeds the node limit",
            ));
        }
        if entry.deleted && !entry.value.is_empty() {
            return Err(Status::invalid_argument(
                "deleted replication entry must omit its value",
            ));
        }
        let version = entry
            .version
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("replication entry omitted version"))?;
        if version.topology_epoch != entry.topology_epoch
            || version.owner_node_id != entry.owner_node_id
        {
            return Err(Status::failed_precondition(
                "replication entry version does not match its stream authority",
            ));
        }

        let mut state = self.state.write().await;
        if entry.topology_epoch != state.topology.epoch {
            return Err(Status::failed_precondition(format!(
                "replication epoch {} does not match installed epoch {}",
                entry.topology_epoch, state.topology.epoch
            )));
        }
        let owner = state
            .topology
            .owner(&entry.key)
            .map_err(|error| Status::internal(error.to_string()))?;
        if owner.node_id != entry.owner_node_id {
            return Err(Status::failed_precondition(
                "replication entry was not issued by the current owner",
            ));
        }
        let replicas = state
            .topology
            .replica_node_ids_for_token(state.topology.key_token(&entry.key))
            .map_err(|error| Status::internal(error.to_string()))?;
        if !replicas[1..].contains(&self.node_id.as_str()) {
            return Err(Status::failed_precondition(
                "this node is not a desired follower for the mutation",
            ));
        }

        let fingerprint = replication_fingerprint(&entry);
        let stream_key = (entry.topology_epoch, entry.owner_node_id.clone());
        let stream = state.follower_streams.entry(stream_key).or_default();
        if entry.stream_sequence <= stream.applied_sequence {
            if stream
                .fingerprints
                .get(&entry.stream_sequence)
                .is_some_and(|existing| existing == &fingerprint)
            {
                return Ok(Response::new(ReplicateMutationResponse {
                    applied_stream_sequence: stream.applied_sequence,
                }));
            }
            // The verified full-stream snapshot supersedes queued entries from
            // before its checkpoint. They cannot mutate data after the snapshot.
            if entry.stream_sequence <= stream.checkpoint_sequence
                && !stream.fingerprints.contains_key(&entry.stream_sequence)
            {
                return Ok(Response::new(ReplicateMutationResponse {
                    applied_stream_sequence: stream.applied_sequence,
                }));
            }
            return Err(Status::already_exists(
                "replication stream sequence conflicts with an applied entry",
            ));
        }
        let expected = stream.applied_sequence.saturating_add(1);
        if entry.stream_sequence != expected {
            return Err(Status::aborted(format!(
                "replication gap: expected sequence {expected}, received {}",
                entry.stream_sequence
            )));
        }
        if version.owner_sequence <= stream.last_owner_sequence {
            return Err(Status::failed_precondition(
                "replication record version did not advance the owner sequence",
            ));
        }

        let dedup_now = Instant::now();
        purge_expired_dedup(&mut state, dedup_now);
        let mutation_fingerprint = mutation_fingerprint(&entry.key, &entry.value, entry.deleted);
        if state.dedup.contains_key(&entry.mutation_id) {
            return Err(Status::already_exists(
                "replication stream reused a live mutation ID",
            ));
        }
        if state.dedup_bytes.saturating_add(dedup_retained_bytes(
            &entry.mutation_id,
            entry.key.len(),
            version.owner_node_id.len(),
        )) > self.max_dedup_bytes
        {
            return Err(Status::resource_exhausted(
                "follower mutation retry window is full",
            ));
        }
        let mutation_id = entry.mutation_id.clone();
        let dedup_key: Arc<[u8]> = Arc::from(entry.key.as_slice());

        apply_internal_record(
            &mut state.records,
            entry.key,
            Record {
                value: entry.value.into(),
                version: version.clone(),
                deleted: entry.deleted,
            },
        );
        let stream = state
            .follower_streams
            .get_mut(&(entry.topology_epoch, entry.owner_node_id))
            .expect("replication stream was initialized above");
        stream.applied_sequence = entry.stream_sequence;
        stream.last_owner_sequence = version.owner_sequence;
        stream
            .fingerprints
            .insert(entry.stream_sequence, fingerprint);
        if entry.stream_sequence > MAX_REPLICATION_FINGERPRINTS {
            stream
                .fingerprints
                .remove(&(entry.stream_sequence - MAX_REPLICATION_FINGERPRINTS));
        }
        let applied_stream_sequence = stream.applied_sequence;
        insert_dedup(
            &mut state,
            mutation_id,
            dedup_key,
            mutation_fingerprint,
            version.clone(),
            entry.deleted,
            Instant::now(),
        );
        Ok(Response::new(ReplicateMutationResponse {
            applied_stream_sequence,
        }))
    }

    async fn prepare_source_range(
        &self,
        request: Request<PrepareRangeRequest>,
    ) -> Result<Response<PrepareSourceRangeResponse>, Status> {
        let range: RangeSpec = request
            .into_inner()
            .range
            .ok_or_else(|| Status::invalid_argument("missing range"))?
            .try_into()?;
        if range.source_node_id != self.node_id {
            return Err(Status::failed_precondition(
                "this node is not the range source",
            ));
        }
        let key = range.key();
        let (all_keys, dedup_keys, topology, ready) = {
            let mut state = self.state.write().await;
            if let Some(existing) = state.sources.get(&key) {
                if existing.range != range {
                    return Err(Status::failed_precondition(
                        "source range retry conflicts with the prepared specification",
                    ));
                }
                let mut ready = existing.snapshot_ready.subscribe();
                drop(state);
                if !*ready.borrow() {
                    ready.changed().await.map_err(|_| {
                        Status::aborted("source snapshot preparation was cancelled")
                    })?;
                }
                return Ok(Response::new(PrepareSourceRangeResponse {
                    process_instance_id: self.process_instance_id.clone(),
                }));
            }
            let all_keys = state.records.keys().cloned().collect::<Vec<_>>();
            let dedup_keys = state
                .dedup
                .iter()
                .map(|(id, entry)| (id.clone(), entry.key.clone()))
                .collect::<Vec<_>>();
            let topology = state.topology.clone();
            let (ready, _) = watch::channel(false);
            state.sources.insert(
                key.clone(),
                SourceMigration {
                    range: range.clone(),
                    snapshot_keys: None,
                    snapshot_dedup_ids: Vec::new(),
                    snapshot_ready: ready.clone(),
                    journal: Vec::new(),
                    journal_bytes: 0,
                    watermark: 0,
                    writes_paused: false,
                },
            );
            (all_keys, dedup_keys, topology, ready)
        };
        let mut preparation = SnapshotPreparationGuard {
            state: self.state.clone(),
            key: Some(key.clone()),
        };

        // Only keys are captured while traffic is locked. Filtering and sorting
        // happen outside the lock, and values are materialized one bounded page
        // at a time by `read_snapshot_page`.
        let mut snapshot_keys: Vec<_> = all_keys
            .into_iter()
            .filter(|record_key| range.contains(topology.key_token(record_key)))
            .collect();
        snapshot_keys.sort();
        let mut snapshot_dedup_ids: Vec<_> = dedup_keys
            .into_iter()
            .filter(|(_, record_key)| range.contains(topology.key_token(record_key)))
            .map(|(id, _)| id)
            .collect();
        snapshot_dedup_ids.sort();
        let mut state = self.state.write().await;
        let source = state
            .sources
            .get_mut(&key)
            .ok_or_else(|| Status::aborted("source migration was reset during preparation"))?;
        if source.range != range {
            return Err(Status::failed_precondition(
                "source range changed during preparation",
            ));
        }
        source.snapshot_keys = Some(snapshot_keys);
        source.snapshot_dedup_ids = snapshot_dedup_ids;
        ready.send_replace(true);
        preparation.disarm();
        Ok(Response::new(PrepareSourceRangeResponse {
            process_instance_id: self.process_instance_id.clone(),
        }))
    }

    async fn prepare_destination_range(
        &self,
        request: Request<PrepareRangeRequest>,
    ) -> Result<Response<PrepareDestinationRangeResponse>, Status> {
        let range: RangeSpec = request
            .into_inner()
            .range
            .ok_or_else(|| Status::invalid_argument("missing range"))?
            .try_into()?;
        if range.destination_node_id != self.node_id {
            return Err(Status::failed_precondition(
                "this node is not the range destination",
            ));
        }
        let mut state = self.state.write().await;
        if let Some(existing) = state.destinations.get(&range.key()) {
            if existing.range != range {
                return Err(Status::failed_precondition(
                    "destination range retry conflicts with the prepared specification",
                ));
            }
        } else {
            state.destinations.insert(
                range.key(),
                DestinationMigration {
                    range,
                    records: HashMap::new(),
                    dedup: HashMap::new(),
                    dedup_bytes: 0,
                    watermark: 0,
                    committed: false,
                    writes_activated: false,
                },
            );
        }
        Ok(Response::new(PrepareDestinationRangeResponse {
            process_instance_id: self.process_instance_id.clone(),
        }))
    }

    async fn get_process_info(
        &self,
        _request: Request<proto::Empty>,
    ) -> Result<Response<NodeInfoResponse>, Status> {
        Ok(Response::new(NodeInfoResponse {
            node_id: self.node_id.clone(),
            process_instance_id: self.process_instance_id.clone(),
        }))
    }

    async fn read_snapshot_page(
        &self,
        request: Request<SnapshotPageRequest>,
    ) -> Result<Response<SnapshotPageResponse>, Status> {
        let request = request.into_inner();
        let state = self.state.read().await;
        let source = state
            .sources
            .get(&(request.change_id, request.range_id))
            .ok_or_else(|| Status::not_found("source migration not prepared"))?;
        let snapshot_keys = source
            .snapshot_keys
            .as_ref()
            .ok_or_else(|| Status::unavailable("source snapshot is still being prepared"))?;
        let start = usize::try_from(request.cursor)
            .map_err(|_| Status::invalid_argument("snapshot cursor is too large"))?;
        if start > snapshot_keys.len() {
            return Err(Status::out_of_range("snapshot cursor exceeds record count"));
        }
        let max_bytes = page_limit(request.max_bytes);
        let mut bytes = 0;
        let mut records = Vec::new();
        let mut next_cursor = start;
        for key in &snapshot_keys[start..] {
            let Some(record) = state.records.get(key) else {
                next_cursor += 1;
                continue;
            };
            let record = MigrationRecord {
                key: key.clone(),
                value: record.value.to_vec(),
                version: Some(record.version.clone()),
                deleted: record.deleted,
                mutation_id: String::new(),
                remaining_window_millis: 0,
            };
            let size = migration_record_size(&record);
            if !records.is_empty() && bytes + size > max_bytes {
                break;
            }
            bytes += size;
            records.push(record);
            next_cursor += 1;
        }
        Ok(Response::new(SnapshotPageResponse {
            records,
            next_cursor: next_cursor as u64,
            done: next_cursor == snapshot_keys.len(),
        }))
    }

    async fn read_dedup_snapshot_page(
        &self,
        request: Request<SnapshotPageRequest>,
    ) -> Result<Response<DedupSnapshotPageResponse>, Status> {
        let request = request.into_inner();
        let state = self.state.read().await;
        let source = state
            .sources
            .get(&(request.change_id, request.range_id))
            .ok_or_else(|| Status::not_found("source migration not prepared"))?;
        if source.snapshot_keys.is_none() {
            return Err(Status::unavailable(
                "source snapshot is still being prepared",
            ));
        }
        let ids = &source.snapshot_dedup_ids;
        let start = usize::try_from(request.cursor)
            .map_err(|_| Status::invalid_argument("dedup cursor is too large"))?;
        if start > ids.len() {
            return Err(Status::out_of_range("dedup cursor exceeds record count"));
        }
        let mut next_cursor = start;
        let mut bytes = 0;
        let mut records = Vec::new();
        let now = Instant::now();
        for id in &ids[start..] {
            next_cursor += 1;
            let Some(entry) = state.dedup.get(id).filter(|entry| entry.expires_at > now) else {
                continue;
            };
            let record = DeduplicationRecord {
                mutation_id: id.clone(),
                key: entry.key.to_vec(),
                fingerprint: entry.fingerprint.to_vec(),
                version: Some(entry.version.clone()),
                deleted: entry.deleted,
                remaining_window_millis: remaining_window_millis(entry.expires_at, now),
            };
            let size = dedup_record_size(&record);
            if !records.is_empty() && bytes + size > page_limit(request.max_bytes) {
                next_cursor -= 1;
                break;
            }
            bytes += size;
            records.push(record);
        }
        Ok(Response::new(DedupSnapshotPageResponse {
            records,
            next_cursor: next_cursor as u64,
            done: next_cursor == ids.len(),
        }))
    }

    async fn read_changelog_page(
        &self,
        request: Request<ChangelogPageRequest>,
    ) -> Result<Response<ChangelogPageResponse>, Status> {
        let request = request.into_inner();
        let state = self.state.read().await;
        let source = state
            .sources
            .get(&(request.change_id, request.range_id))
            .ok_or_else(|| Status::not_found("source migration not prepared"))?;
        let max_bytes = page_limit(request.max_bytes);
        let mut bytes = 0;
        let mut records = Vec::new();
        for entry in source
            .journal
            .iter()
            .filter(|entry| entry.watermark > request.after_watermark)
        {
            let size = entry
                .record
                .as_ref()
                .map(migration_record_size)
                .unwrap_or_default()
                + 16;
            if !records.is_empty() && bytes + size > max_bytes {
                break;
            }
            bytes += size;
            let mut copied = entry.clone();
            if let Some(record) = copied.record.as_mut() {
                record.remaining_window_millis = state
                    .dedup
                    .get(&record.mutation_id)
                    .filter(|dedup| {
                        dedup.fingerprint
                            == mutation_fingerprint(&record.key, &record.value, record.deleted)
                            && Some(&dedup.version) == record.version.as_ref()
                    })
                    .map_or(0, |dedup| {
                        remaining_window_millis(dedup.expires_at, Instant::now())
                    });
            }
            records.push(copied);
        }
        Ok(Response::new(ChangelogPageResponse {
            records,
            current_watermark: source.watermark,
        }))
    }

    async fn apply_migration_batch(
        &self,
        request: Request<ApplyMigrationBatchRequest>,
    ) -> Result<Response<proto::Empty>, Status> {
        let request = request.into_inner();
        let mut state = self.state.write().await;
        let topology = state.topology.clone();
        let destination = state
            .destinations
            .get_mut(&(request.change_id, request.range_id))
            .ok_or_else(|| Status::not_found("destination migration not prepared"))?;
        if destination.committed {
            return Err(Status::failed_precondition(
                "destination range is already committed",
            ));
        }
        purge_staged_dedup(destination, Instant::now());
        for record in request.snapshot_records {
            if !destination.range.contains(topology.key_token(&record.key)) {
                return Err(Status::invalid_argument(
                    "snapshot record is outside the prepared range",
                ));
            }
            apply_record(&mut destination.records, record)?;
        }
        for entry in request.journal_records {
            if entry.watermark <= destination.watermark {
                continue;
            }
            if entry.watermark != destination.watermark + 1 {
                return Err(Status::failed_precondition(format!(
                    "non-contiguous changelog: expected {}, received {}",
                    destination.watermark + 1,
                    entry.watermark
                )));
            }
            let record = entry
                .record
                .ok_or_else(|| Status::invalid_argument("journal entry omitted record"))?;
            if !destination.range.contains(topology.key_token(&record.key)) {
                return Err(Status::invalid_argument(
                    "journal record is outside the prepared range",
                ));
            }
            if !record.mutation_id.is_empty() {
                let dedup = DeduplicationRecord {
                    mutation_id: record.mutation_id.clone(),
                    key: record.key.clone(),
                    fingerprint: mutation_fingerprint(&record.key, &record.value, record.deleted)
                        .to_vec(),
                    version: record.version.clone(),
                    deleted: record.deleted,
                    remaining_window_millis: record.remaining_window_millis,
                };
                stage_dedup(destination, dedup, self.max_dedup_bytes)?;
            }
            apply_record(&mut destination.records, record)?;
            destination.watermark = entry.watermark;
        }
        Ok(Response::new(proto::Empty {}))
    }

    async fn apply_dedup_batch(
        &self,
        request: Request<ApplyDedupBatchRequest>,
    ) -> Result<Response<proto::Empty>, Status> {
        let request = request.into_inner();
        let mut state = self.state.write().await;
        let topology = state.topology.clone();
        let destination = state
            .destinations
            .get_mut(&(request.change_id, request.range_id))
            .ok_or_else(|| Status::not_found("destination migration not prepared"))?;
        if destination.committed {
            return Err(Status::failed_precondition(
                "destination range is already committed",
            ));
        }
        purge_staged_dedup(destination, Instant::now());
        for record in request.records {
            if !destination.range.contains(topology.key_token(&record.key)) {
                return Err(Status::invalid_argument(
                    "dedup record is outside the prepared range",
                ));
            }
            stage_dedup(destination, record, self.max_dedup_bytes)?;
        }
        Ok(Response::new(proto::Empty {}))
    }

    async fn pause_range_writes(
        &self,
        request: Request<RangeControlRequest>,
    ) -> Result<Response<PauseRangeResponse>, Status> {
        let request = request.into_inner();
        let mut state = self.state.write().await;
        let source = state
            .sources
            .get_mut(&(request.change_id, request.range_id))
            .ok_or_else(|| Status::not_found("source migration not prepared"))?;
        source.writes_paused = true;
        Ok(Response::new(PauseRangeResponse {
            final_watermark: source.watermark,
        }))
    }

    async fn source_range_digest(
        &self,
        request: Request<RangeControlRequest>,
    ) -> Result<Response<RangeDigestResponse>, Status> {
        let request = request.into_inner();
        let state = self.state.read().await;
        let source = state
            .sources
            .get(&(request.change_id, request.range_id))
            .ok_or_else(|| Status::not_found("source migration not prepared"))?;
        let watermark = source.watermark;
        let records: HashMap<_, _> = state
            .records
            .iter()
            .filter(|(key, _)| source.range.contains(state.topology.key_token(key)))
            .map(|(key, record)| (key.clone(), record.clone()))
            .collect();
        drop(state);
        Ok(Response::new(range_digest(&records, watermark)))
    }

    async fn destination_range_digest(
        &self,
        request: Request<RangeControlRequest>,
    ) -> Result<Response<RangeDigestResponse>, Status> {
        let request = request.into_inner();
        let state = self.state.read().await;
        let destination = state
            .destinations
            .get(&(request.change_id, request.range_id))
            .ok_or_else(|| Status::not_found("destination migration not prepared"))?;
        let records = destination.records.clone();
        let watermark = destination.watermark;
        drop(state);
        Ok(Response::new(range_digest(&records, watermark)))
    }

    async fn commit_destination_range(
        &self,
        request: Request<RangeControlRequest>,
    ) -> Result<Response<proto::Empty>, Status> {
        let request = request.into_inner();
        let mut state = self.state.write().await;
        let key = (request.change_id, request.range_id);
        let (range, records, dedup) = {
            let destination = state
                .destinations
                .get(&key)
                .ok_or_else(|| Status::not_found("destination migration not prepared"))?;
            if destination.committed {
                return Ok(Response::new(proto::Empty {}));
            }
            (
                destination.range.clone(),
                destination.records.clone(),
                destination
                    .dedup
                    .iter()
                    .filter(|(_, entry)| entry.expires_at > Instant::now())
                    .map(|(id, entry)| (id.clone(), entry.clone()))
                    .collect::<HashMap<_, _>>(),
            )
        };
        purge_expired_dedup(&mut state, Instant::now());
        let mut added_bytes = 0usize;
        for staged in dedup.values() {
            let record = &staged.record;
            if let Some(existing) = state.dedup.get(&record.mutation_id) {
                if existing.fingerprint.as_slice() != record.fingerprint
                    || Some(&existing.version) != record.version.as_ref()
                    || existing.deleted != record.deleted
                {
                    return Err(Status::failed_precondition(
                        "destination mutation ID conflicts with existing retry record",
                    ));
                }
            } else {
                let version = record
                    .version
                    .as_ref()
                    .expect("staged dedup version was validated");
                added_bytes = added_bytes.saturating_add(dedup_retained_bytes(
                    &record.mutation_id,
                    record.key.len(),
                    version.owner_node_id.len(),
                ));
            }
        }
        if state.dedup_bytes.saturating_add(added_bytes) > self.max_dedup_bytes {
            return Err(Status::resource_exhausted(
                "destination mutation retry window is full",
            ));
        }
        let topology = state.topology.clone();
        state
            .records
            .retain(|record_key, _| !range.contains(topology.key_token(record_key)));
        for (key, record) in records {
            apply_internal_record(&mut state.records, key, record);
        }
        for (id, staged) in dedup {
            if state.dedup.contains_key(&id) {
                continue;
            }
            let record = staged.record;
            insert_dedup_until(
                &mut state,
                id,
                Arc::from(record.key),
                record
                    .fingerprint
                    .try_into()
                    .expect("staged fingerprint was validated"),
                record.version.expect("staged version was validated"),
                record.deleted,
                staged.expires_at,
            );
        }
        state
            .destinations
            .get_mut(&key)
            .expect("destination was checked above")
            .committed = true;
        Ok(Response::new(proto::Empty {}))
    }

    async fn activate_destination_range(
        &self,
        request: Request<RangeControlRequest>,
    ) -> Result<Response<proto::Empty>, Status> {
        let request = request.into_inner();
        let mut state = self.state.write().await;
        let destination = state
            .destinations
            .get_mut(&(request.change_id, request.range_id))
            .ok_or_else(|| Status::not_found("destination migration not prepared"))?;
        if !destination.committed {
            return Err(Status::failed_precondition(
                "destination range is not committed",
            ));
        }
        destination.writes_activated = true;
        Ok(Response::new(proto::Empty {}))
    }

    async fn cleanup_source_range(
        &self,
        request: Request<RangeControlRequest>,
    ) -> Result<Response<proto::Empty>, Status> {
        let request = request.into_inner();
        let mut state = self.state.write().await;
        let key = (request.change_id, request.range_id);
        let Some(range) = state.sources.get(&key).map(|source| source.range.clone()) else {
            return Ok(Response::new(proto::Empty {}));
        };
        let topology = state.topology.clone();
        let keys: Vec<_> = state
            .records
            .keys()
            .filter(|record_key| {
                let token = topology.key_token(record_key);
                range.contains(token)
                    && topology
                        .replica_node_ids_for_token(token)
                        .is_ok_and(|replicas| !replicas.contains(&self.node_id.as_str()))
            })
            .cloned()
            .collect();
        for record_key in keys {
            state.records.remove(&record_key);
        }
        if let Some(source) = state.sources.remove(&key) {
            state.journal_bytes_total = state
                .journal_bytes_total
                .saturating_sub(source.journal_bytes);
        }
        Ok(Response::new(proto::Empty {}))
    }

    async fn abort_range_migration(
        &self,
        request: Request<RangeControlRequest>,
    ) -> Result<Response<proto::Empty>, Status> {
        let request = request.into_inner();
        let key = (request.change_id, request.range_id);
        let mut state = self.state.write().await;
        if let Some(source) = state.sources.remove(&key) {
            state.journal_bytes_total = state
                .journal_bytes_total
                .saturating_sub(source.journal_bytes);
        }
        state.destinations.remove(&key);
        Ok(Response::new(proto::Empty {}))
    }

    async fn abort_replica_repairs(
        &self,
        request: Request<proto::AbortReplicaRepairsRequest>,
    ) -> Result<Response<proto::Empty>, Status> {
        let epoch = request.into_inner().topology_epoch;
        let mut state = self.state.write().await;
        if state.topology.epoch != epoch {
            return Err(Status::failed_precondition(
                "repair cleanup epoch does not match installed topology",
            ));
        }
        clear_replica_repair_staging(&mut state);
        Ok(Response::new(proto::Empty {}))
    }

    async fn install_topology(
        &self,
        request: Request<InstallTopologyRequest>,
    ) -> Result<Response<proto::Empty>, Status> {
        let request = request.into_inner();
        let topology: TopologySnapshot = request
            .topology
            .ok_or_else(|| Status::invalid_argument("missing topology"))?
            .try_into()
            .map_err(|error: hashring_core::topology::TopologyError| {
                Status::invalid_argument(error.to_string())
            })?;
        let mut state = self.state.write().await;
        if topology.epoch < state.topology.epoch {
            return Err(Status::failed_precondition(
                "cannot install an older topology",
            ));
        }
        let epoch = topology.epoch;
        if epoch != state.topology.epoch {
            state.lease = None;
            clear_replica_repair_staging(&mut state);
        }
        state.topology = topology;
        state
            .owner_stream_sequences
            .retain(|(stream_epoch, _), _| *stream_epoch == epoch);
        state
            .owner_stream_unacked
            .retain(|(stream_epoch, _), _| *stream_epoch == epoch);
        state
            .follower_streams
            .retain(|(stream_epoch, _), _| *stream_epoch == epoch);
        prune_ack_progress(&mut state);
        self.replication_dispatch.prune_epoch(epoch);
        drop(state);
        if request.require_lease
            && self
                .state
                .read()
                .await
                .topology
                .members
                .iter()
                .any(|member| member.node_id == self.node_id)
        {
            self.renew_lease_once().await.map_err(|error| {
                Status::unavailable(format!("new-epoch lease is unavailable: {error}"))
            })?;
        }
        Ok(Response::new(proto::Empty {}))
    }

    async fn pause_policy_writes(
        &self,
        request: Request<PolicyWriteFenceRequest>,
    ) -> Result<Response<proto::Empty>, Status> {
        let request = request.into_inner();
        if request.change_id.is_empty() {
            return Err(Status::invalid_argument("policy change ID is empty"));
        }
        let mut state = self.state.write().await;
        if state.topology.epoch != request.base_epoch {
            return Err(Status::failed_precondition(
                "policy fence base epoch does not match installed topology",
            ));
        }
        match &state.policy_write_fence {
            Some((change_id, epoch))
                if change_id != &request.change_id || *epoch != request.base_epoch =>
            {
                return Err(Status::failed_precondition(
                    "another policy change has fenced writes",
                ));
            }
            _ => state.policy_write_fence = Some((request.change_id, request.base_epoch)),
        }
        Ok(Response::new(proto::Empty {}))
    }

    async fn resume_policy_writes(
        &self,
        request: Request<PolicyWriteFenceRequest>,
    ) -> Result<Response<proto::Empty>, Status> {
        let request = request.into_inner();
        let mut state = self.state.write().await;
        if state.policy_write_fence.as_ref() == Some(&(request.change_id, request.base_epoch)) {
            state.policy_write_fence = None;
        }
        Ok(Response::new(proto::Empty {}))
    }

    async fn stop(&self, request: Request<StopRequest>) -> Result<Response<proto::Empty>, Status> {
        let request = request.into_inner();
        self.validate_stop_request(&request)?;
        if !self.stop_prepared.load(AtomicOrdering::Acquire) {
            return Err(Status::failed_precondition(
                "stop must be prepared before finalization",
            ));
        }
        let shutdown = self.shutdown.clone();
        tokio::spawn(async move {
            // Let the acknowledged response reach the coordinator so it can
            // durably record shutdown progress before the server exits.
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            let _ = shutdown.send(true);
        });
        if !self.stop_response_delay.is_zero() {
            tokio::time::sleep(self.stop_response_delay).await;
        }
        Ok(Response::new(proto::Empty {}))
    }

    async fn prepare_stop(
        &self,
        request: Request<StopRequest>,
    ) -> Result<Response<proto::Empty>, Status> {
        let request = request.into_inner();
        self.validate_stop_request(&request)?;
        if !self.stop_prepared.swap(true, AtomicOrdering::AcqRel) {
            let coordinator_endpoint = self.coordinator_endpoint.clone();
            let node_id = self.node_id.clone();
            let process_instance_id = self.process_instance_id.clone();
            let shutdown = self.shutdown.clone();
            tokio::spawn(async move {
                wait_for_durable_stop_confirmation(
                    coordinator_endpoint,
                    node_id,
                    process_instance_id,
                    shutdown,
                )
                .await;
            });
        }
        Ok(Response::new(proto::Empty {}))
    }
}

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

fn required_followers(
    topology: &TopologySnapshot,
    token: u64,
    status: Option<&proto::ReplicaStatusResponse>,
    owner_node_id: &str,
) -> Result<Vec<String>, OperationError> {
    let epoch = topology.epoch;
    let replicas = topology
        .replica_node_ids_for_token(token)
        .map_err(|error| temporarily_unavailable(epoch, error.to_string()))?;
    let guard = &topology.write_availability_guard;
    if topology.write_ack_policy == WriteAckPolicy::FirstSuccessor && replicas.len() < 2 {
        return Err(temporarily_unavailable(
            epoch,
            "first successor is not in the desired placement",
        ));
    }
    if topology.write_ack_policy == WriteAckPolicy::AllReplicas
        && replicas.len() < topology.desired_replication_factor as usize
    {
        return Err(temporarily_unavailable(
            epoch,
            "complete desired replication factor is unavailable",
        ));
    }
    if guard.minimum_admitted_copies <= 1
        && guard.minimum_healthy_followers == 0
        && topology.write_ack_policy == WriteAckPolicy::OwnerOnly
    {
        return Ok(Vec::new());
    }
    let range = status
        .filter(|status| status.topology_epoch == epoch)
        .and_then(|status| {
            status.ranges.iter().find(|range| {
                range.owner_node_id == owner_node_id
                    && range_contains(range.start_exclusive, range.end_inclusive, token)
            })
        })
        .ok_or_else(|| temporarily_unavailable(epoch, "replica admission status is unavailable"))?;
    let admitted = range
        .followers
        .iter()
        .filter(|follower| follower.admitted)
        .count() as u32;
    let healthy = range
        .followers
        .iter()
        .filter(|follower| {
            follower.admitted
                && follower.lag_known
                && follower.lag_millis <= guard.max_replica_lag_millis
        })
        .count() as u32;
    if admitted.saturating_add(1) < guard.minimum_admitted_copies
        || healthy < guard.minimum_healthy_followers
    {
        return Err(temporarily_unavailable(
            epoch,
            "minimum admitted-copy or healthy-follower guard is not satisfied",
        ));
    }
    let desired: Vec<_> = match topology.write_ack_policy {
        WriteAckPolicy::OwnerOnly => Vec::new(),
        WriteAckPolicy::FirstSuccessor => replicas[1..2].to_vec(),
        WriteAckPolicy::AllReplicas => replicas[1..].to_vec(),
    };
    for node_id in &desired {
        if !range.followers.iter().any(|follower| {
            follower.node_id == *node_id
                && follower.admitted
                && follower.lag_known
                && follower.lag_millis <= guard.max_replica_lag_millis
        }) {
            return Err(OperationError {
                current_epoch: epoch,
                ..operation_error(
                    ErrorCode::ReplicaNotReady,
                    format!("required follower {node_id} is not healthy and admitted"),
                    true,
                )
            });
        }
    }
    Ok(desired.into_iter().map(str::to_owned).collect())
}

fn prepare_replication_entries(
    state: &mut NodeState,
    key: &[u8],
    value: &[u8],
    deleted: bool,
    version: &RecordVersion,
    mutation_id: String,
) -> Result<Vec<PreparedReplication>, ()> {
    let token = state.topology.key_token(key);
    let follower_ids: Vec<_> = state
        .topology
        .replica_node_ids_for_token(token)
        .expect("installed topology was validated")
        .into_iter()
        .skip(1)
        .map(str::to_owned)
        .collect();
    let followers: Vec<_> = follower_ids
        .into_iter()
        .map(|node_id| {
            let endpoint = state
                .topology
                .members
                .iter()
                .find(|member| member.node_id == node_id)
                .expect("replica placement references a topology member")
                .endpoint
                .clone();
            (node_id, endpoint)
        })
        .collect();

    let per_entry_bytes = key
        .len()
        .checked_add(value.len())
        .and_then(|bytes| bytes.checked_add(version.owner_node_id.len()))
        .and_then(|bytes| bytes.checked_add(mutation_id.len()))
        .and_then(|bytes| bytes.checked_add(256))
        .ok_or(())?;
    if per_entry_bytes > MAX_PENDING_REPLICATION_BYTES {
        return Err(());
    }
    let mutation = Arc::new(ReplicationMutation {
        topology_epoch: state.topology.epoch,
        owner_node_id: version.owner_node_id.clone(),
        key: Arc::from(key),
        value: Arc::from(value),
        deleted,
        version: version.clone(),
        mutation_id,
    });

    Ok(followers
        .into_iter()
        .map(|(follower_node_id, follower_endpoint)| {
            state
                .ack_progress
                .entry((state.topology.epoch, follower_node_id.clone()))
                .or_insert_with(|| watch::channel(0).0);
            let sequence = state
                .owner_stream_sequences
                .entry((state.topology.epoch, follower_node_id.clone()))
                .or_default();
            *sequence += 1;
            PreparedReplication {
                follower_node_id,
                follower_endpoint,
                stream_sequence: *sequence,
                mutation: mutation.clone(),
            }
        })
        .collect())
}

fn rollback_replication_sequences(state: &mut NodeState, entries: &[PreparedReplication]) {
    for entry in entries {
        let key = (
            entry.mutation.topology_epoch,
            entry.follower_node_id.clone(),
        );
        let sequence = state
            .owner_stream_sequences
            .get_mut(&key)
            .expect("prepared replication initialized its owner stream");
        debug_assert_eq!(*sequence, entry.stream_sequence);
        *sequence -= 1;
        if *sequence == 0 {
            state.owner_stream_sequences.remove(&key);
        }
    }
}

fn replication_backpressure_error(epoch: u64) -> OperationError {
    OperationError {
        current_epoch: epoch,
        ..operation_error(
            ErrorCode::ResourceExhausted,
            "replication queue is full or requires catch-up; retry with backoff",
            true,
        )
    }
}

fn replication_fingerprint(entry: &ReplicationEntry) -> String {
    let mut digest = blake3::Hasher::new();
    digest.update(b"hashring-rs:replication-entry:v1\0");
    digest.update(&entry.topology_epoch.to_be_bytes());
    digest.update(&(entry.owner_node_id.len() as u64).to_be_bytes());
    digest.update(entry.owner_node_id.as_bytes());
    digest.update(&entry.stream_sequence.to_be_bytes());
    digest.update(&(entry.key.len() as u64).to_be_bytes());
    digest.update(&entry.key);
    digest.update(&(entry.value.len() as u64).to_be_bytes());
    digest.update(&entry.value);
    digest.update(&[u8::from(entry.deleted)]);
    if let Some(version) = &entry.version {
        digest.update(&version.topology_epoch.to_be_bytes());
        digest.update(&version.owner_sequence.to_be_bytes());
        digest.update(&(version.owner_node_id.len() as u64).to_be_bytes());
        digest.update(version.owner_node_id.as_bytes());
    }
    digest.update(&(entry.mutation_id.len() as u64).to_be_bytes());
    digest.update(entry.mutation_id.as_bytes());
    digest.finalize().to_hex().to_string()
}

fn mutation_fingerprint(key: &[u8], value: &[u8], deleted: bool) -> [u8; 32] {
    let mut digest = blake3::Hasher::new();
    digest.update(b"hashring-rs:mutation:v1\0");
    digest.update(&(key.len() as u64).to_be_bytes());
    digest.update(key);
    digest.update(&[u8::from(deleted)]);
    digest.update(&(value.len() as u64).to_be_bytes());
    digest.update(value);
    *digest.finalize().as_bytes()
}

fn dedup_retained_bytes(mutation_id: &str, key_len: usize, owner_len: usize) -> usize {
    mutation_id.len().saturating_mul(2)
        + key_len
        + owner_len
        + std::mem::size_of::<DedupEntry>()
        + std::mem::size_of::<(Instant, String)>()
        + std::mem::size_of::<DeduplicationRecord>()
        + std::mem::size_of::<StagedDedup>()
        + 128 // hash-table buckets and allocator metadata
}

fn required_ack_retained_bytes(followers: &[String]) -> usize {
    followers
        .iter()
        .map(|node_id| 2 * std::mem::size_of::<RequiredAck>() + node_id.len() + 64)
        .sum()
}

fn dedup_record_size(record: &DeduplicationRecord) -> usize {
    record.mutation_id.len()
        + record.key.len()
        + record.fingerprint.len()
        + record
            .version
            .as_ref()
            .map_or(0, |version| version.owner_node_id.len())
        + 128
}

fn stage_dedup(
    destination: &mut DestinationMigration,
    record: DeduplicationRecord,
    max_dedup_bytes: usize,
) -> Result<(), Status> {
    if record.remaining_window_millis == 0 {
        return Ok(());
    }
    if record.mutation_id.is_empty()
        || record.mutation_id.len() > MAX_MUTATION_ID_BYTES
        || record.fingerprint.len() != 32
        || record.version.is_none()
    {
        return Err(Status::invalid_argument("invalid deduplication record"));
    }
    let expires_at = Instant::now()
        + std::time::Duration::from_millis(record.remaining_window_millis.min(60_000));
    if let Some(existing) = destination.dedup.get_mut(&record.mutation_id) {
        let same = existing.record.key == record.key
            && existing.record.fingerprint == record.fingerprint
            && existing.record.version == record.version
            && existing.record.deleted == record.deleted;
        if !same {
            return Err(Status::failed_precondition(
                "conflicting mutation ID in destination migration",
            ));
        }
        existing.expires_at = existing.expires_at.max(expires_at);
        return Ok(());
    }
    let version = record.version.as_ref().expect("version was checked above");
    let cost = dedup_retained_bytes(
        &record.mutation_id,
        record.key.len(),
        version.owner_node_id.len(),
    );
    if destination.dedup_bytes.saturating_add(cost) > max_dedup_bytes {
        return Err(Status::resource_exhausted(
            "staged mutation retry window is full",
        ));
    }
    destination.dedup_bytes += cost;
    destination.dedup.insert(
        record.mutation_id.clone(),
        StagedDedup {
            record,
            expires_at,
            retained_bytes: cost,
        },
    );
    Ok(())
}

fn purge_staged_dedup(destination: &mut DestinationMigration, now: Instant) {
    destination.dedup.retain(|_, entry| entry.expires_at > now);
    destination.dedup_bytes = destination
        .dedup
        .values()
        .map(|entry| entry.retained_bytes)
        .sum();
}

fn remaining_window_millis(expires_at: Instant, now: Instant) -> u64 {
    let remaining = expires_at.saturating_duration_since(now);
    if remaining.is_zero() {
        return 0;
    }
    u64::try_from(remaining.as_nanos().div_ceil(1_000_000))
        .unwrap_or(u64::MAX)
        .min(60_000)
}

fn purge_expired_dedup(state: &mut NodeState, now: Instant) {
    while state
        .dedup_expirations
        .peek()
        .is_some_and(|Reverse((expires_at, _))| *expires_at <= now)
    {
        let Reverse((_, mutation_id)) = state
            .dedup_expirations
            .pop()
            .expect("expiration was checked above");
        if state
            .dedup
            .get(&mutation_id)
            .is_some_and(|entry| entry.expires_at <= now)
            && let Some(entry) = state.dedup.remove(&mutation_id)
        {
            state.dedup_bytes -= entry.retained_bytes;
        }
    }
    prune_ack_progress(state);
}

fn prune_ack_progress(state: &mut NodeState) {
    let live: HashSet<_> = state
        .dedup
        .values()
        .flat_map(|entry| {
            entry
                .required_acks
                .iter()
                .map(|(epoch, node_id, _)| (*epoch, node_id.clone()))
        })
        .collect();
    let current_epoch = state.topology.epoch;
    state
        .ack_progress
        .retain(|key, _| key.0 == current_epoch || live.contains(key));
}

fn insert_dedup(
    state: &mut NodeState,
    mutation_id: String,
    key: Arc<[u8]>,
    fingerprint: [u8; 32],
    version: RecordVersion,
    deleted: bool,
    now: Instant,
) {
    let expires_at = now + IDEMPOTENCY_WINDOW;
    insert_dedup_until(
        state,
        mutation_id,
        key,
        fingerprint,
        version,
        deleted,
        expires_at,
    );
}

fn insert_dedup_until(
    state: &mut NodeState,
    mutation_id: String,
    key: Arc<[u8]>,
    fingerprint: [u8; 32],
    version: RecordVersion,
    deleted: bool,
    expires_at: Instant,
) {
    let retained_bytes = dedup_retained_bytes(&mutation_id, key.len(), version.owner_node_id.len());
    state
        .dedup_expirations
        .push(Reverse((expires_at, mutation_id.clone())));
    state.dedup.insert(
        mutation_id,
        DedupEntry {
            key,
            fingerprint,
            version,
            deleted,
            retained_bytes,
            expires_at,
            required_acks: Vec::new(),
        },
    );
    state.dedup_bytes += retained_bytes;
}

fn now_unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn start_replication_dispatch(state: Arc<RwLock<NodeState>>) -> ReplicationDispatcher {
    ReplicationDispatcher {
        state,
        streams: Arc::new(StdMutex::new(HashMap::new())),
        failed_streams: Arc::new(StdMutex::new(HashSet::new())),
        retained_budget: Arc::new(Semaphore::new(MAX_PENDING_REPLICATION_BYTES)),
        active_rpc_budget: Arc::new(Semaphore::new(MAX_PENDING_REPLICATION_BYTES)),
    }
}

impl ReplicationDispatcher {
    fn prune_epoch(&self, epoch: u64) {
        if let Ok(mut streams) = self.streams.lock() {
            streams.retain(|(stream_epoch, _, _), sender| {
                *stream_epoch == epoch && !sender.is_closed()
            });
        }
        if let Ok(mut failed) = self.failed_streams.lock() {
            failed.retain(|(stream_epoch, _, _)| *stream_epoch == epoch);
        }
    }

    fn reserve(&self, entries: &[PreparedReplication]) -> Result<Vec<ReplicationReservation>, ()> {
        let failed = self.failed_streams.lock().map_err(|_| ())?;
        let mut streams = self.streams.lock().map_err(|_| ())?;
        let mut reservations = Vec::with_capacity(entries.len());
        let budget = entries
            .first()
            .map(|entry| {
                let bytes =
                    u32::try_from(replication_mutation_bytes(&entry.mutation)).map_err(|_| ())?;
                self.retained_budget
                    .clone()
                    .try_acquire_many_owned(bytes)
                    .map(ReplicationBudget)
                    .map(Arc::new)
                    .map_err(|_| ())
            })
            .transpose()?;
        for entry in entries {
            let stream_key = replication_stream_key(entry);
            if failed.contains(&stream_key) {
                return Err(());
            }
            let sender = streams.entry(stream_key.clone()).or_insert_with(|| {
                let (sender, receiver) = mpsc::channel(REPLICATION_STREAM_QUEUE_CAPACITY);
                tokio::spawn(deliver_replication_stream(
                    self.state.clone(),
                    self.failed_streams.clone(),
                    self.active_rpc_budget.clone(),
                    stream_key,
                    receiver,
                ));
                sender
            });
            let queue = sender.clone().try_reserve_owned().map_err(|_| ())?;
            reservations.push(ReplicationReservation {
                queue,
                budget: budget.clone().expect("non-empty replication has a budget"),
            });
        }
        Ok(reservations)
    }

    fn dispatch(
        &self,
        entries: Vec<PreparedReplication>,
        reservations: Vec<ReplicationReservation>,
    ) {
        debug_assert_eq!(entries.len(), reservations.len());
        for (prepared, reservation) in entries.into_iter().zip(reservations) {
            reservation.queue.send(PendingReplication {
                prepared,
                _budget: reservation.budget,
            });
        }
    }
}

fn replication_stream_key(entry: &PreparedReplication) -> ReplicationStreamKey {
    (
        entry.mutation.topology_epoch,
        entry.mutation.owner_node_id.clone(),
        entry.follower_node_id.clone(),
    )
}

fn replication_mutation_bytes(mutation: &ReplicationMutation) -> usize {
    mutation.key.len()
        + mutation.value.len()
        + mutation.owner_node_id.len()
        + mutation.mutation_id.len()
        + 256
}

async fn deliver_replication_stream(
    state: Arc<RwLock<NodeState>>,
    failed_streams: Arc<StdMutex<HashSet<ReplicationStreamKey>>>,
    active_rpc_budget: Arc<Semaphore>,
    stream_key: ReplicationStreamKey,
    mut entries: mpsc::Receiver<PendingReplication>,
) {
    let mut client = None;
    while let Some(pending) = entries.recv().await {
        let prepared = &pending.prepared;
        let mut retry_delay = std::time::Duration::from_millis(25);
        loop {
            if state.read().await.topology.epoch != prepared.mutation.topology_epoch {
                return;
            }
            if client.is_none() {
                match tokio::time::timeout(
                    REPLICATION_RPC_TIMEOUT,
                    DataNodeClient::connect(prepared.follower_endpoint.clone()),
                )
                .await
                {
                    Ok(Ok(connected)) => client = Some(configure_data_node_client(connected)),
                    Ok(Err(_)) | Err(_) => {
                        tokio::time::sleep(retry_delay).await;
                        retry_delay = (retry_delay * 2).min(std::time::Duration::from_secs(1));
                        continue;
                    }
                }
            }
            let active_bytes = u32::try_from(replication_mutation_bytes(&prepared.mutation))
                .expect("validated replication size fits u32");
            let Ok(active_budget) = active_rpc_budget
                .clone()
                .acquire_many_owned(active_bytes)
                .await
            else {
                return;
            };
            let result = tokio::time::timeout(
                REPLICATION_RPC_TIMEOUT,
                client
                    .as_mut()
                    .expect("replication client was connected above")
                    .replicate_mutation(prepared.to_proto()),
            )
            .await
            .map(|result| result.map(Response::into_inner));
            drop(active_budget);
            match result {
                Ok(Ok(response))
                    if response.applied_stream_sequence >= prepared.stream_sequence =>
                {
                    let mut state = state.write().await;
                    if let Some(progress) = state.ack_progress.get(&(
                        prepared.mutation.topology_epoch,
                        prepared.follower_node_id.clone(),
                    )) {
                        progress.send_if_modified(|sequence| {
                            if *sequence < response.applied_stream_sequence {
                                *sequence = response.applied_stream_sequence;
                                true
                            } else {
                                false
                            }
                        });
                    }
                    if let Some(queue) = state.owner_stream_unacked.get_mut(&(
                        prepared.mutation.topology_epoch,
                        prepared.follower_node_id.clone(),
                    )) {
                        while queue.front().is_some_and(|(sequence, _)| {
                            *sequence <= response.applied_stream_sequence
                        }) {
                            queue.pop_front();
                        }
                    }
                    break;
                }
                Ok(Err(status)) if retryable_replication_status(status.code()) => {
                    client = None;
                    tokio::time::sleep(retry_delay).await;
                    retry_delay = (retry_delay * 2).min(std::time::Duration::from_secs(1));
                }
                Err(_) => {
                    client = None;
                    tokio::time::sleep(retry_delay).await;
                    retry_delay = (retry_delay * 2).min(std::time::Duration::from_secs(1));
                }
                Ok(Ok(_)) | Ok(Err(_)) => {
                    if let Ok(mut failed) = failed_streams.lock() {
                        failed.insert(stream_key);
                    }
                    return;
                }
            }
        }
    }
}

fn retryable_replication_status(code: tonic::Code) -> bool {
    matches!(
        code,
        tonic::Code::Aborted
            | tonic::Code::Cancelled
            | tonic::Code::Unknown
            | tonic::Code::DeadlineExceeded
            | tonic::Code::ResourceExhausted
            | tonic::Code::Unavailable
    )
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

fn page_limit(requested: u64) -> usize {
    usize::try_from(requested)
        .unwrap_or(MAX_MIGRATION_PAGE_BYTES)
        .clamp(1, MAX_MIGRATION_PAGE_BYTES)
}

fn migration_record_size(record: &MigrationRecord) -> usize {
    record.key.len() + record.value.len() + record.mutation_id.len() + 128
}

fn apply_record(
    records: &mut HashMap<Vec<u8>, Record>,
    record: MigrationRecord,
) -> Result<(), Status> {
    let version = record
        .version
        .ok_or_else(|| Status::invalid_argument("migration record omitted version"))?;
    if record.deleted && !record.value.is_empty() {
        return Err(Status::invalid_argument(
            "deleted migration record must omit its value",
        ));
    }
    apply_internal_record(
        records,
        record.key,
        Record {
            value: record.value.into(),
            version,
            deleted: record.deleted,
        },
    );
    Ok(())
}

fn apply_internal_record(records: &mut HashMap<Vec<u8>, Record>, key: Vec<u8>, record: Record) {
    if records
        .get(&key)
        .is_none_or(|current| compare_versions(&record.version, &current.version).is_gt())
    {
        records.insert(key, record);
    }
}

fn compare_versions(left: &RecordVersion, right: &RecordVersion) -> Ordering {
    (
        left.topology_epoch,
        left.owner_sequence,
        &left.owner_node_id,
    )
        .cmp(&(
            right.topology_epoch,
            right.owner_sequence,
            &right.owner_node_id,
        ))
}

fn range_digest(records: &HashMap<Vec<u8>, Record>, watermark: u64) -> RangeDigestResponse {
    let mut ordered: Vec<_> = records.iter().collect();
    ordered.sort_by_key(|(key, _)| *key);
    let mut digest = blake3::Hasher::new();
    digest.update(b"hashring-rs:records:v2\0");
    for (key, record) in &ordered {
        digest.update(&(key.len() as u64).to_be_bytes());
        digest.update(key);
        digest.update(&record.version.topology_epoch.to_be_bytes());
        digest.update(&record.version.owner_sequence.to_be_bytes());
        digest.update(&(record.version.owner_node_id.len() as u64).to_be_bytes());
        digest.update(record.version.owner_node_id.as_bytes());
        digest.update(&(record.value.len() as u64).to_be_bytes());
        digest.update(record.value.as_ref());
        digest.update(&[u8::from(record.deleted)]);
    }
    RangeDigestResponse {
        record_count: ordered.len() as u64,
        digest: digest.finalize().to_hex().to_string(),
        changelog_watermark: watermark,
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
mod tests {
    use super::*;
    use hashring_core::topology::{Member, TopologyConfig, WriteAvailabilityGuard};

    fn service() -> DataNodeService {
        let topology = TopologySnapshot::new_with_config(
            1,
            42,
            8,
            vec![Member {
                node_id: "node-1".into(),
                endpoint: "http://127.0.0.1:5001".into(),
            }],
            TopologyConfig {
                desired_replication_factor: 1,
                write_availability_guard: WriteAvailabilityGuard {
                    minimum_admitted_copies: 1,
                    minimum_healthy_followers: 0,
                    ..WriteAvailabilityGuard::default()
                },
                ..TopologyConfig::default()
            },
        )
        .unwrap();
        service_for("node-1", topology)
    }

    fn service_for(node_id: &str, topology: TopologySnapshot) -> DataNodeService {
        let (shutdown, _) = watch::channel(false);
        let test_epoch = topology.epoch;
        let state = Arc::new(RwLock::new(NodeState {
            topology,
            records: HashMap::new(),
            next_sequence: 0,
            owner_stream_sequences: HashMap::new(),
            owner_stream_unacked: HashMap::new(),
            ack_progress: HashMap::new(),
            follower_streams: HashMap::new(),
            sources: HashMap::new(),
            destinations: HashMap::new(),
            journal_bytes_total: 0,
            lease: Some((
                test_epoch,
                Instant::now() + std::time::Duration::from_secs(3600),
            )),
            policy_write_fence: None,
            dedup: HashMap::new(),
            dedup_expirations: BinaryHeap::new(),
            dedup_bytes: 0,
        }));
        let replication_dispatch = start_replication_dispatch(state.clone());
        DataNodeService {
            node_id: node_id.into(),
            process_instance_id: "instance-1".into(),
            coordinator_endpoint: "http://127.0.0.1:5000".into(),
            state,
            replication_dispatch,
            refresh_lock: Arc::new(Mutex::new(())),
            max_key_bytes: DEFAULT_MAX_KEY_BYTES,
            max_value_bytes: DEFAULT_MAX_VALUE_BYTES,
            max_journal_bytes: DEFAULT_MAX_MIGRATION_JOURNAL_BYTES,
            max_dedup_bytes: MAX_DEDUP_BYTES,
            stop_response_delay: std::time::Duration::ZERO,
            stop_prepared: Arc::new(AtomicBool::new(false)),
            shutdown,
        }
    }

    fn replication_topology(desired_replication_factor: u32) -> TopologySnapshot {
        TopologySnapshot::new_with_config(
            1,
            42,
            8,
            vec![
                Member {
                    node_id: "node-1".into(),
                    endpoint: "http://127.0.0.1:5001".into(),
                },
                Member {
                    node_id: "node-2".into(),
                    endpoint: "http://127.0.0.1:5002".into(),
                },
                Member {
                    node_id: "node-3".into(),
                    endpoint: "http://127.0.0.1:5003".into(),
                },
            ],
            TopologyConfig {
                desired_replication_factor,
                write_availability_guard: WriteAvailabilityGuard {
                    minimum_admitted_copies: 1,
                    minimum_healthy_followers: 0,
                    ..WriteAvailabilityGuard::default()
                },
                ..TopologyConfig::default()
            },
        )
        .unwrap()
    }

    #[test]
    fn peer_probes_keep_two_independent_reporters_at_large_membership() {
        let members = (0..128)
            .map(|index| Member {
                node_id: format!("node-{index:03}"),
                endpoint: format!("http://127.0.0.1:{}", 6000 + index),
            })
            .collect();
        let topology = TopologySnapshot::new(1, 42, 1, members).unwrap();
        let mut reporters: HashMap<String, HashSet<String>> = HashMap::new();
        for member in &topology.members {
            let targets = peer_probe_targets(&topology, &member.node_id);
            assert_eq!(targets.len(), 4);
            for target in targets {
                reporters
                    .entry(target.node_id)
                    .or_default()
                    .insert(member.node_id.clone());
            }
        }
        assert!(reporters.values().all(|reporters| reporters.len() == 4));
    }

    fn key_with_placement(
        topology: &TopologySnapshot,
        predicate: impl Fn(&[&str]) -> bool,
    ) -> Vec<u8> {
        (0_u64..100_000)
            .map(|value| value.to_be_bytes().to_vec())
            .find(|key| {
                topology
                    .replica_node_ids_for_token(topology.key_token(key))
                    .is_ok_and(|replicas| predicate(&replicas))
            })
            .expect("test topology should contain a matching placement")
    }

    fn replication_entry(
        topology: &TopologySnapshot,
        key: Vec<u8>,
        stream_sequence: u64,
        owner_sequence: u64,
        value: &[u8],
        deleted: bool,
    ) -> ReplicationEntry {
        let owner = topology.owner(&key).unwrap();
        ReplicationEntry {
            topology_epoch: topology.epoch,
            owner_node_id: owner.node_id.clone(),
            stream_sequence,
            key,
            value: value.to_vec(),
            deleted,
            version: Some(RecordVersion {
                topology_epoch: topology.epoch,
                owner_sequence,
                owner_node_id: owner.node_id.clone(),
            }),
            mutation_id: format!("mutation-{stream_sequence}"),
        }
    }

    fn range(end_inclusive: u64) -> proto::RangeSpec {
        proto::RangeSpec {
            change_id: "change-1".into(),
            range_id: "range-1".into(),
            start_exclusive: 0,
            end_inclusive,
            source_node_id: "node-1".into(),
            destination_node_id: "node-2".into(),
        }
    }

    #[tokio::test]
    async fn follower_applies_only_contiguous_authorized_replication() {
        let topology = replication_topology(3);
        let key = key_with_placement(&topology, |replicas| replicas[1..].contains(&"node-2"));
        let service = service_for("node-2", topology.clone());
        let put = replication_entry(&topology, key.clone(), 1, 10, b"value", false);

        let applied = service
            .replicate_mutation(Request::new(put.clone()))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(applied.applied_stream_sequence, 1);
        assert_eq!(
            service.state.read().await.records[&key].value.as_ref(),
            b"value"
        );
        let client_read = service
            .get(Request::new(GetRequest {
                key: key.clone(),
                topology_epoch: topology.epoch,
                request_id: "get".into(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(client_read.error.unwrap().code, ErrorCode::Moved as i32);

        let duplicate = service
            .replicate_mutation(Request::new(put.clone()))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(duplicate.applied_stream_sequence, 1);
        let mut conflict = put;
        conflict.value = b"different".to_vec();
        assert_eq!(
            service
                .replicate_mutation(Request::new(conflict))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::AlreadyExists
        );

        let gap = replication_entry(&topology, key.clone(), 3, 12, b"later", false);
        assert_eq!(
            service
                .replicate_mutation(Request::new(gap))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::Aborted
        );
        let stale_version = replication_entry(&topology, key.clone(), 2, 10, b"later", false);
        assert_eq!(
            service
                .replicate_mutation(Request::new(stale_version))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::FailedPrecondition
        );

        let delete = replication_entry(&topology, key.clone(), 2, 11, b"", true);
        let applied = service
            .replicate_mutation(Request::new(delete))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(applied.applied_stream_sequence, 2);
        assert!(service.state.read().await.records[&key].deleted);
    }

    #[tokio::test]
    async fn full_stream_checkpoint_repairs_a_gap_without_replaying_old_entries() {
        let topology = replication_topology(3);
        let follower = service_for("node-2", topology.clone());
        let ranges: Vec<_> = topology
            .derived_ranges()
            .unwrap()
            .into_iter()
            .filter(|range| {
                range.owner_node_id == "node-1"
                    && range.follower_node_ids.iter().any(|node| node == "node-2")
            })
            .collect();
        assert!(ranges.len() > 1);
        let controls: Vec<_> = ranges
            .iter()
            .enumerate()
            .map(|(index, range)| {
                let control = RangeControlRequest {
                    change_id: "repair-test".into(),
                    range_id: format!("range-{index}"),
                };
                let spec = proto::RangeSpec {
                    change_id: control.change_id.clone(),
                    range_id: control.range_id.clone(),
                    start_exclusive: range.start_exclusive,
                    end_inclusive: range.end_inclusive,
                    source_node_id: "node-1".into(),
                    destination_node_id: "node-2".into(),
                };
                (control, spec)
            })
            .collect();
        for (control, spec) in &controls {
            follower
                .prepare_destination_range(Request::new(PrepareRangeRequest {
                    range: Some(spec.clone()),
                }))
                .await
                .unwrap();
            follower
                .commit_destination_range(Request::new(control.clone()))
                .await
                .unwrap();
        }
        let checkpoint = ReplicationCheckpointRequest {
            topology_epoch: 1,
            owner_node_id: "node-1".into(),
            follower_node_id: "node-2".into(),
            stream_sequence: 5,
            verified_ranges: controls
                .iter()
                .map(|(control, _)| control.clone())
                .collect(),
        };
        let mut incomplete = checkpoint.clone();
        incomplete.verified_ranges.pop();
        assert_eq!(
            follower
                .install_replication_checkpoint(Request::new(incomplete))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::FailedPrecondition
        );
        let installed = follower
            .install_replication_checkpoint(Request::new(checkpoint))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(installed.stream_sequence, 5);
        let key = key_with_placement(&topology, |replicas| {
            replicas[0] == "node-1" && replicas.contains(&"node-2")
        });
        let old = replication_entry(&topology, key.clone(), 1, 1, b"stale", false);
        assert_eq!(
            follower
                .replicate_mutation(Request::new(old))
                .await
                .unwrap()
                .into_inner()
                .applied_stream_sequence,
            5
        );
        assert!(!follower.state.read().await.records.contains_key(&key));
        let gap = replication_entry(&topology, key.clone(), 7, 7, b"gap", false);
        assert_eq!(
            follower
                .replicate_mutation(Request::new(gap))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::Aborted
        );
        let next = replication_entry(&topology, key.clone(), 6, 6, b"current", false);
        follower
            .replicate_mutation(Request::new(next))
            .await
            .unwrap();
        assert_eq!(
            follower.state.read().await.records[&key].value.as_ref(),
            b"current"
        );
    }

    #[tokio::test]
    async fn follower_rejects_stale_wrong_owner_and_out_of_coverage_entries() {
        let topology = replication_topology(2);
        let follower_key = key_with_placement(&topology, |replicas| {
            replicas[0] != "node-2" && replicas[1..].contains(&"node-2")
        });
        let outside_key = key_with_placement(&topology, |replicas| !replicas.contains(&"node-2"));
        let service = service_for("node-2", topology.clone());

        let mut stale = replication_entry(&topology, follower_key.clone(), 1, 1, b"value", false);
        stale.topology_epoch = 0;
        stale.version.as_mut().unwrap().topology_epoch = 0;
        assert_eq!(
            service
                .replicate_mutation(Request::new(stale))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::FailedPrecondition
        );

        let mut oversized_id =
            replication_entry(&topology, follower_key.clone(), 1, 1, b"value", false);
        oversized_id.mutation_id = "x".repeat(MAX_MUTATION_ID_BYTES + 1);
        assert_eq!(
            service
                .replicate_mutation(Request::new(oversized_id))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );

        let mut wrong_owner = replication_entry(&topology, follower_key, 1, 1, b"value", false);
        let incorrect_node_id = topology
            .members
            .iter()
            .find(|member| member.node_id != wrong_owner.owner_node_id)
            .unwrap()
            .node_id
            .clone();
        wrong_owner.owner_node_id = incorrect_node_id.clone();
        wrong_owner.version.as_mut().unwrap().owner_node_id = incorrect_node_id;
        assert_eq!(
            service
                .replicate_mutation(Request::new(wrong_owner))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::FailedPrecondition
        );

        let outside = replication_entry(&topology, outside_key, 1, 1, b"value", false);
        assert_eq!(
            service
                .replicate_mutation(Request::new(outside))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::FailedPrecondition
        );
    }

    #[test]
    fn owner_assigns_independent_contiguous_sequences_per_follower() {
        let topology = replication_topology(3);
        let key = key_with_placement(&topology, |replicas| replicas[0] == "node-1");
        let mut state = NodeState {
            topology,
            records: HashMap::new(),
            next_sequence: 0,
            owner_stream_sequences: HashMap::new(),
            owner_stream_unacked: HashMap::new(),
            ack_progress: HashMap::new(),
            follower_streams: HashMap::new(),
            sources: HashMap::new(),
            destinations: HashMap::new(),
            journal_bytes_total: 0,
            lease: Some((1, Instant::now() + std::time::Duration::from_secs(3600))),
            policy_write_fence: None,
            dedup: HashMap::new(),
            dedup_expirations: BinaryHeap::new(),
            dedup_bytes: 0,
        };
        let version = RecordVersion {
            topology_epoch: 1,
            owner_sequence: 1,
            owner_node_id: "node-1".into(),
        };
        let first = prepare_replication_entries(
            &mut state,
            &key,
            b"one",
            false,
            &version,
            "mutation-1".into(),
        )
        .unwrap();
        let second = prepare_replication_entries(
            &mut state,
            &key,
            b"two",
            false,
            &RecordVersion {
                owner_sequence: 2,
                ..version
            },
            "mutation-2".into(),
        )
        .unwrap();

        assert_eq!(first.len(), 2);
        assert_eq!(second.len(), 2);
        for follower in first {
            let next = second
                .iter()
                .find(|candidate| candidate.follower_node_id == follower.follower_node_id)
                .unwrap();
            assert_eq!(follower.stream_sequence, 1);
            assert_eq!(next.stream_sequence, 2);
        }
    }

    #[tokio::test]
    async fn owner_only_delivers_put_and_delete_to_follower_in_order() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let follower_endpoint = format!("http://{}", listener.local_addr().unwrap());
        let topology = TopologySnapshot::new_with_config(
            1,
            42,
            8,
            vec![
                Member {
                    node_id: "node-1".into(),
                    endpoint: "http://127.0.0.1:1".into(),
                },
                Member {
                    node_id: "node-2".into(),
                    endpoint: follower_endpoint,
                },
            ],
            TopologyConfig {
                desired_replication_factor: 2,
                write_availability_guard: WriteAvailabilityGuard {
                    minimum_admitted_copies: 1,
                    minimum_healthy_followers: 0,
                    ..WriteAvailabilityGuard::default()
                },
                ..TopologyConfig::default()
            },
        )
        .unwrap();
        let follower = service_for("node-2", topology.clone());
        let follower_state = follower.state.clone();
        let server = tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(proto::data_node_server::DataNodeServer::new(follower))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
        );
        let owner = service_for("node-1", topology.clone());
        let key = key_with_placement(&topology, |replicas| replicas[0] == "node-1");

        let put = owner
            .put(Request::new(PutRequest {
                key: key.clone(),
                value: b"replicated".to_vec(),
                topology_epoch: 1,
                request_id: "put-1".into(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(put.error.is_none());
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                if follower_state
                    .read()
                    .await
                    .records
                    .get(&key)
                    .is_some_and(|record| record.value.as_ref() == b"replicated")
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            follower_state.read().await.dedup["put-1"]
                .version
                .owner_sequence,
            1
        );

        let deleted = owner
            .delete(Request::new(DeleteRequest {
                key: key.clone(),
                topology_epoch: 1,
                request_id: "delete-1".into(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(deleted.error.is_none());
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                let state = follower_state.read().await;
                if state.records.get(&key).is_some_and(|record| record.deleted)
                    && state
                        .follower_streams
                        .values()
                        .any(|stream| stream.applied_sequence == 2)
                {
                    break;
                }
                drop(state);
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        let mut concurrent = Vec::new();
        for index in 0..6_u8 {
            let owner = owner.clone();
            let key = key.clone();
            concurrent.push(tokio::spawn(async move {
                owner
                    .put(Request::new(PutRequest {
                        key,
                        value: vec![index],
                        topology_epoch: 1,
                        request_id: format!("concurrent-{index}"),
                    }))
                    .await
                    .unwrap()
                    .into_inner()
            }));
        }
        for write in concurrent {
            assert!(write.await.unwrap().error.is_none());
        }
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                let state = follower_state.read().await;
                if state
                    .follower_streams
                    .values()
                    .any(|stream| stream.applied_sequence == 8)
                    && state.records[&key].version.owner_sequence == 8
                {
                    break;
                }
                drop(state);
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        server.abort();
    }

    #[tokio::test]
    async fn dispatcher_resumes_after_a_verified_gap_checkpoint() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let topology = TopologySnapshot::new_with_config(
            1,
            42,
            4,
            vec![
                Member {
                    node_id: "node-1".into(),
                    endpoint: "http://127.0.0.1:1".into(),
                },
                Member {
                    node_id: "node-2".into(),
                    endpoint: format!("http://{}", listener.local_addr().unwrap()),
                },
            ],
            TopologyConfig {
                desired_replication_factor: 2,
                write_availability_guard: WriteAvailabilityGuard {
                    minimum_admitted_copies: 1,
                    minimum_healthy_followers: 0,
                    ..WriteAvailabilityGuard::default()
                },
                ..TopologyConfig::default()
            },
        )
        .unwrap();
        let follower = service_for("node-2", topology.clone());
        let follower_rpc = follower.clone();
        let follower_state = follower.state.clone();
        let server = tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(proto::data_node_server::DataNodeServer::new(follower))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
        );
        let owner = service_for("node-1", topology.clone());
        let key = key_with_placement(&topology, |replicas| replicas[0] == "node-1");
        for (id, value) in [("first", b"one".as_slice())] {
            let result = owner
                .put(Request::new(PutRequest {
                    key: key.clone(),
                    value: value.to_vec(),
                    topology_epoch: 1,
                    request_id: id.into(),
                }))
                .await
                .unwrap()
                .into_inner();
            assert!(result.error.is_none());
        }
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                if follower_state
                    .read()
                    .await
                    .follower_streams
                    .values()
                    .any(|stream| stream.applied_sequence == 1)
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        {
            let mut state = follower_state.write().await;
            state.follower_streams.clear();
            state.records.clear();
        }
        let result = owner
            .put(Request::new(PutRequest {
                key: key.clone(),
                value: b"two".to_vec(),
                topology_epoch: 1,
                request_id: "second".into(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(result.error.is_none());
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert!(
            follower_state
                .read()
                .await
                .follower_streams
                .values()
                .all(|stream| stream.applied_sequence == 0)
        );
        let source_records = owner.state.read().await.records.clone();
        let mut controls = Vec::new();
        for (index, range) in topology
            .derived_ranges()
            .unwrap()
            .into_iter()
            .filter(|range| {
                range.owner_node_id == "node-1"
                    && range.follower_node_ids.contains(&"node-2".into())
            })
            .enumerate()
        {
            let control = RangeControlRequest {
                change_id: "repair-dispatch".into(),
                range_id: format!("range-{index}"),
            };
            let spec = RangeSpec {
                change_id: control.change_id.clone(),
                range_id: control.range_id.clone(),
                start_exclusive: range.start_exclusive,
                end_inclusive: range.end_inclusive,
                source_node_id: "node-1".into(),
                destination_node_id: "node-2".into(),
            };
            let records = source_records
                .iter()
                .filter(|(key, _)| spec.contains(topology.key_token(key)))
                .map(|(key, record)| (key.clone(), record.clone()))
                .collect();
            follower_state.write().await.destinations.insert(
                spec.key(),
                DestinationMigration {
                    range: spec,
                    records,
                    dedup: HashMap::new(),
                    dedup_bytes: 0,
                    watermark: 0,
                    committed: false,
                    writes_activated: false,
                },
            );
            follower_rpc
                .commit_destination_range(Request::new(control.clone()))
                .await
                .unwrap();
            controls.push(control);
        }
        let checkpoint = follower_rpc
            .install_replication_checkpoint(Request::new(ReplicationCheckpointRequest {
                topology_epoch: 1,
                owner_node_id: "node-1".into(),
                follower_node_id: "node-2".into(),
                stream_sequence: 2,
                verified_ranges: controls,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(checkpoint.stream_sequence, 2);
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                let state = owner.state.read().await;
                if state.owner_stream_unacked.values().all(VecDeque::is_empty) {
                    break;
                }
                drop(state);
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let result = owner
            .put(Request::new(PutRequest {
                key: key.clone(),
                value: b"three".to_vec(),
                topology_epoch: 1,
                request_id: "third".into(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(result.error.is_none());
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                let state = follower_state.read().await;
                if state
                    .follower_streams
                    .values()
                    .any(|stream| stream.applied_sequence == 3)
                    && state.records[&key].value.as_ref() == b"three"
                {
                    break;
                }
                drop(state);
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        server.abort();
    }

    #[tokio::test]
    async fn unavailable_follower_backpressures_before_unbounded_queue_growth() {
        let topology = TopologySnapshot::new_with_config(
            1,
            42,
            8,
            vec![
                Member {
                    node_id: "node-1".into(),
                    endpoint: "http://127.0.0.1:1".into(),
                },
                Member {
                    node_id: "node-2".into(),
                    endpoint: "http://127.0.0.1:2".into(),
                },
            ],
            TopologyConfig {
                desired_replication_factor: 2,
                write_availability_guard: WriteAvailabilityGuard {
                    minimum_admitted_copies: 1,
                    minimum_healthy_followers: 0,
                    ..WriteAvailabilityGuard::default()
                },
                ..TopologyConfig::default()
            },
        )
        .unwrap();
        let owner = service_for("node-1", topology.clone());
        let key = key_with_placement(&topology, |replicas| replicas[0] == "node-1");
        let mut successful = 0;
        let rejected = loop {
            let response = owner
                .put(Request::new(PutRequest {
                    key: key.clone(),
                    value: vec![successful as u8],
                    topology_epoch: 1,
                    request_id: format!("put-{successful}"),
                }))
                .await
                .unwrap()
                .into_inner();
            if let Some(error) = response.error {
                break error;
            }
            successful += 1;
            assert!(successful <= REPLICATION_STREAM_QUEUE_CAPACITY + 1);
        };

        assert_eq!(rejected.code, ErrorCode::ResourceExhausted as i32);
        let state = owner.state.read().await;
        assert_eq!(state.next_sequence, successful as u64);
        assert_eq!(
            state.records[&key].version.owner_sequence,
            successful as u64
        );
        assert_eq!(
            state.owner_stream_sequences.values().copied().next(),
            Some(successful as u64)
        );
    }

    #[tokio::test]
    async fn older_replicated_delete_cannot_erase_newer_seeded_value() {
        let topology = replication_topology(3);
        let key = key_with_placement(&topology, |replicas| replicas[1..].contains(&"node-2"));
        let service = service_for("node-2", topology.clone());
        let owner_node_id = topology.owner(&key).unwrap().node_id.clone();
        service.state.write().await.records.insert(
            key.clone(),
            Record {
                value: Arc::from(b"newer".as_slice()),
                version: RecordVersion {
                    topology_epoch: topology.epoch,
                    owner_sequence: 11,
                    owner_node_id,
                },
                deleted: false,
            },
        );

        let delete = replication_entry(&topology, key.clone(), 1, 10, b"", true);
        service
            .replicate_mutation(Request::new(delete))
            .await
            .unwrap();
        let state = service.state.read().await;
        assert_eq!(state.records[&key].value.as_ref(), b"newer");
        assert!(!state.records[&key].deleted);
    }

    #[tokio::test]
    async fn source_cleanup_retains_records_needed_as_follower_copies() {
        let topology = TopologySnapshot::new_with_config(
            2,
            42,
            8,
            vec![
                Member {
                    node_id: "node-1".into(),
                    endpoint: "http://127.0.0.1:5001".into(),
                },
                Member {
                    node_id: "node-2".into(),
                    endpoint: "http://127.0.0.1:5002".into(),
                },
            ],
            TopologyConfig {
                desired_replication_factor: 2,
                ..TopologyConfig::default()
            },
        )
        .unwrap();
        let key = key_with_placement(&topology, |replicas| {
            replicas[0] == "node-2" && replicas[1] == "node-1"
        });
        let service = service_for("node-1", topology);
        let (snapshot_ready, _) = watch::channel(true);
        {
            let mut state = service.state.write().await;
            state.records.insert(
                key.clone(),
                Record {
                    value: Arc::from(b"replica".as_slice()),
                    version: RecordVersion {
                        topology_epoch: 1,
                        owner_sequence: 1,
                        owner_node_id: "node-1".into(),
                    },
                    deleted: false,
                },
            );
            state.sources.insert(
                ("change-1".into(), "range-1".into()),
                SourceMigration {
                    range: RangeSpec {
                        change_id: "change-1".into(),
                        range_id: "range-1".into(),
                        start_exclusive: 0,
                        end_inclusive: 0,
                        source_node_id: "node-1".into(),
                        destination_node_id: "node-2".into(),
                    },
                    snapshot_keys: Some(Vec::new()),
                    snapshot_dedup_ids: Vec::new(),
                    snapshot_ready,
                    journal: Vec::new(),
                    journal_bytes: 0,
                    watermark: 0,
                    writes_paused: true,
                },
            );
        }

        service
            .cleanup_source_range(Request::new(RangeControlRequest {
                change_id: "change-1".into(),
                range_id: "range-1".into(),
            }))
            .await
            .unwrap();
        assert!(service.state.read().await.records.contains_key(&key));
    }

    #[tokio::test]
    async fn strong_ack_policies_fail_writes_closed_until_replication_is_active() {
        for policy in [WriteAckPolicy::FirstSuccessor, WriteAckPolicy::AllReplicas] {
            let service = service();
            let topology = TopologySnapshot::new_with_config(
                2,
                42,
                8,
                vec![Member {
                    node_id: "node-1".into(),
                    endpoint: "http://127.0.0.1:5001".into(),
                }],
                TopologyConfig {
                    write_ack_policy: policy,
                    ..TopologyConfig::default()
                },
            )
            .unwrap();
            service
                .install_topology(Request::new(InstallTopologyRequest {
                    topology: Some((&topology).into()),
                    require_lease: false,
                }))
                .await
                .unwrap();
            service.state.write().await.lease =
                Some((2, Instant::now() + std::time::Duration::from_secs(3600)));

            let put = service
                .put(Request::new(PutRequest {
                    key: b"key".to_vec(),
                    value: b"value".to_vec(),
                    topology_epoch: 2,
                    request_id: "put".into(),
                }))
                .await
                .unwrap()
                .into_inner();
            assert_eq!(
                put.error.unwrap().code,
                ErrorCode::TemporarilyUnavailable as i32
            );
            let delete = service
                .delete(Request::new(DeleteRequest {
                    key: b"key".to_vec(),
                    topology_epoch: 2,
                    request_id: "delete".into(),
                }))
                .await
                .unwrap()
                .into_inner();
            assert_eq!(
                delete.error.unwrap().code,
                ErrorCode::TemporarilyUnavailable as i32
            );
            assert!(service.state.read().await.records.is_empty());
        }
    }

    #[tokio::test]
    async fn first_successor_requires_the_exact_healthy_follower_and_its_ack() {
        let base = replication_topology(3);
        let key = key_with_placement(&base, |replicas| replicas[0] == "node-1");
        let token = base.key_token(&key);
        let range = base
            .derived_ranges()
            .unwrap()
            .into_iter()
            .find(|range| {
                range.owner_node_id == "node-1"
                    && range_contains(range.start_exclusive, range.end_inclusive, token)
            })
            .unwrap();
        let first = range.follower_node_ids[0].clone();
        let second = range.follower_node_ids[1].clone();
        let mut topology = TopologySnapshot::new_with_config(
            1,
            42,
            8,
            base.members.clone(),
            TopologyConfig {
                desired_replication_factor: 3,
                write_ack_policy: WriteAckPolicy::FirstSuccessor,
                write_availability_guard: WriteAvailabilityGuard {
                    minimum_admitted_copies: 2,
                    minimum_healthy_followers: 1,
                    ..WriteAvailabilityGuard::default()
                },
            },
        )
        .unwrap();
        let mut status = proto::ReplicaStatusResponse {
            topology_epoch: 1,
            ranges: vec![proto::RangeReplicaStatus {
                start_exclusive: range.start_exclusive,
                end_inclusive: range.end_inclusive,
                owner_node_id: "node-1".into(),
                desired_rf: 3,
                current_rf: 3,
                followers: vec![
                    proto::FollowerReplicaStatus {
                        node_id: first.clone(),
                        admitted: true,
                        lag_known: false,
                        ..Default::default()
                    },
                    proto::FollowerReplicaStatus {
                        node_id: second.clone(),
                        admitted: true,
                        lag_known: true,
                        ..Default::default()
                    },
                ],
            }],
        };
        assert!(required_followers(&topology, token, Some(&status), "node-1").is_err());
        status.ranges[0].followers[0].lag_known = true;
        assert_eq!(
            required_followers(&topology, token, Some(&status), "node-1").unwrap(),
            vec![first.clone()]
        );
        topology.write_ack_policy = WriteAckPolicy::AllReplicas;
        assert_eq!(
            required_followers(&topology, token, Some(&status), "node-1").unwrap(),
            vec![first.clone(), second.clone()]
        );
        status.ranges[0].followers[1].admitted = false;
        assert!(required_followers(&topology, token, Some(&status), "node-1").is_err());
        topology.write_ack_policy = WriteAckPolicy::OwnerOnly;
        assert!(
            required_followers(&topology, token, Some(&status), "node-1")
                .unwrap()
                .is_empty()
        );

        let service = service();
        let (first_sender, _) = watch::channel(0);
        let (second_sender, _) = watch::channel(0);
        {
            let mut state = service.state.write().await;
            state
                .ack_progress
                .insert((1, first.clone()), first_sender.clone());
            state
                .ack_progress
                .insert((1, second), second_sender.clone());
        }
        let waiter =
            tokio::spawn(async move { service.wait_required_acks(&[(1, first, 1)], 1).await });
        second_sender.send_replace(1);
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());
        first_sender.send_replace(1);
        assert!(waiter.await.unwrap().is_ok());
    }

    #[tokio::test]
    async fn prepare_rejects_conflicting_retry_specification() {
        let service = service();
        service
            .prepare_source_range(Request::new(PrepareRangeRequest {
                range: Some(range(10)),
            }))
            .await
            .unwrap();
        let error = service
            .prepare_source_range(Request::new(PrepareRangeRequest {
                range: Some(range(11)),
            }))
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    }

    #[tokio::test]
    async fn paused_source_range_keeps_reads_available_and_blocks_writes() {
        let service = service();
        service
            .put(Request::new(PutRequest {
                key: b"key".to_vec(),
                value: b"value".to_vec(),
                topology_epoch: 1,
                request_id: "put-1".into(),
            }))
            .await
            .unwrap();
        service
            .prepare_source_range(Request::new(PrepareRangeRequest {
                range: Some(range(0)),
            }))
            .await
            .unwrap();
        let control = RangeControlRequest {
            change_id: "change-1".into(),
            range_id: "range-1".into(),
        };
        service
            .pause_range_writes(Request::new(control.clone()))
            .await
            .unwrap();
        let readable = service
            .get(Request::new(GetRequest {
                key: b"key".to_vec(),
                topology_epoch: 1,
                request_id: "get-1".into(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(readable.error.is_none());
        assert_eq!(readable.value, b"value");
        let paused_put = service
            .put(Request::new(PutRequest {
                key: b"key".to_vec(),
                value: b"replacement".to_vec(),
                topology_epoch: 1,
                request_id: "put-2".into(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(paused_put.error.unwrap().code, ErrorCode::RangeBusy as i32);
        let paused_delete = service
            .delete(Request::new(DeleteRequest {
                key: b"key".to_vec(),
                topology_epoch: 1,
                request_id: "delete-1".into(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            paused_delete.error.unwrap().code,
            ErrorCode::RangeBusy as i32
        );
        service
            .abort_range_migration(Request::new(control))
            .await
            .unwrap();
        let resumed = service
            .put(Request::new(PutRequest {
                key: b"key".to_vec(),
                value: b"replacement".to_vec(),
                topology_epoch: 1,
                request_id: "put-3".into(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(resumed.error.is_none());
    }

    #[tokio::test]
    async fn interrupted_repair_cleanup_unpauses_only_repair_ranges() {
        let service = service();
        let mut repair = range(0);
        repair.change_id = "repair-interrupted".into();
        repair.range_id = "repair-range".into();
        service
            .prepare_source_range(Request::new(PrepareRangeRequest {
                range: Some(repair),
            }))
            .await
            .unwrap();
        service
            .pause_range_writes(Request::new(RangeControlRequest {
                change_id: "repair-interrupted".into(),
                range_id: "repair-range".into(),
            }))
            .await
            .unwrap();
        let busy = service
            .put(Request::new(PutRequest {
                key: b"key".to_vec(),
                value: b"value".to_vec(),
                topology_epoch: 1,
                request_id: "before-cleanup".into(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(busy.error.unwrap().code, ErrorCode::RangeBusy as i32);
        service
            .abort_replica_repairs(Request::new(proto::AbortReplicaRepairsRequest {
                topology_epoch: 1,
            }))
            .await
            .unwrap();
        assert!(service.state.read().await.sources.is_empty());
        let write = service
            .put(Request::new(PutRequest {
                key: b"key".to_vec(),
                value: b"value".to_vec(),
                topology_epoch: 1,
                request_id: "after-cleanup".into(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(write.error.is_none());
    }

    #[tokio::test]
    async fn committed_destination_fences_writes_until_idempotent_activation() {
        let mut destination = service();
        destination.node_id = "node-2".into();
        destination
            .prepare_destination_range(Request::new(PrepareRangeRequest {
                range: Some(range(0)),
            }))
            .await
            .unwrap();
        destination
            .apply_migration_batch(Request::new(ApplyMigrationBatchRequest {
                change_id: "change-1".into(),
                range_id: "range-1".into(),
                snapshot_records: vec![MigrationRecord {
                    key: b"key".to_vec(),
                    value: b"migrated".to_vec(),
                    version: Some(RecordVersion {
                        topology_epoch: 1,
                        owner_sequence: 1,
                        owner_node_id: "node-1".into(),
                    }),
                    deleted: false,
                    mutation_id: String::new(),
                    remaining_window_millis: 0,
                }],
                journal_records: Vec::new(),
            }))
            .await
            .unwrap();
        let control = RangeControlRequest {
            change_id: "change-1".into(),
            range_id: "range-1".into(),
        };
        let error = destination
            .activate_destination_range(Request::new(control.clone()))
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        let error = destination
            .activate_destination_range(Request::new(RangeControlRequest {
                change_id: "change-1".into(),
                range_id: "unknown".into(),
            }))
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::NotFound);

        destination
            .commit_destination_range(Request::new(control.clone()))
            .await
            .unwrap();
        destination
            .commit_destination_range(Request::new(control.clone()))
            .await
            .unwrap();
        let topology = TopologySnapshot::new_with_config(
            2,
            42,
            8,
            vec![Member {
                node_id: "node-2".into(),
                endpoint: "http://127.0.0.1:5002".into(),
            }],
            TopologyConfig {
                desired_replication_factor: 1,
                write_availability_guard: WriteAvailabilityGuard {
                    minimum_admitted_copies: 1,
                    minimum_healthy_followers: 0,
                    ..WriteAvailabilityGuard::default()
                },
                ..TopologyConfig::default()
            },
        )
        .unwrap();
        destination
            .install_topology(Request::new(InstallTopologyRequest {
                topology: Some((&topology).into()),
                require_lease: false,
            }))
            .await
            .unwrap();
        destination.state.write().await.lease =
            Some((2, Instant::now() + std::time::Duration::from_secs(3600)));

        let readable = destination
            .get(Request::new(GetRequest {
                key: b"key".to_vec(),
                topology_epoch: 2,
                request_id: "get-1".into(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(readable.error.is_none());
        assert_eq!(readable.value, b"migrated");
        let fenced_put = destination
            .put(Request::new(PutRequest {
                key: b"key".to_vec(),
                value: b"new".to_vec(),
                topology_epoch: 2,
                request_id: "put-1".into(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(fenced_put.error.unwrap().code, ErrorCode::RangeBusy as i32);
        let fenced_delete = destination
            .delete(Request::new(DeleteRequest {
                key: b"key".to_vec(),
                topology_epoch: 2,
                request_id: "delete-1".into(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            fenced_delete.error.unwrap().code,
            ErrorCode::RangeBusy as i32
        );

        destination
            .activate_destination_range(Request::new(control.clone()))
            .await
            .unwrap();
        destination
            .activate_destination_range(Request::new(control.clone()))
            .await
            .unwrap();
        destination
            .put(Request::new(PutRequest {
                key: b"key".to_vec(),
                value: b"new".to_vec(),
                topology_epoch: 2,
                request_id: "put-2".into(),
            }))
            .await
            .unwrap();
        destination
            .commit_destination_range(Request::new(control))
            .await
            .unwrap();
        let after_retry = destination
            .get(Request::new(GetRequest {
                key: b"key".to_vec(),
                topology_epoch: 2,
                request_id: "get-2".into(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(after_retry.value, b"new");
        let deleted = destination
            .delete(Request::new(DeleteRequest {
                key: b"key".to_vec(),
                topology_epoch: 2,
                request_id: "delete-2".into(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(deleted.error.is_none());
    }

    #[tokio::test]
    async fn owner_retries_reuse_original_result_and_reject_mutation_id_conflicts() {
        let service = service();
        let first = PutRequest {
            key: b"key".to_vec(),
            value: b"original".to_vec(),
            topology_epoch: 1,
            request_id: "same-id".into(),
        };
        let original = service
            .put(Request::new(first.clone()))
            .await
            .unwrap()
            .into_inner();
        assert!(original.error.is_none());
        let retry = service
            .put(Request::new(first.clone()))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(retry.version, original.version);
        assert_eq!(service.state.read().await.next_sequence, 1);

        let mut conflicting = first;
        conflicting.value = b"different".to_vec();
        let conflict = service
            .put(Request::new(conflicting))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            conflict.error.unwrap().code,
            ErrorCode::MutationIdConflict as i32
        );
        let delete_conflict = service
            .delete(Request::new(DeleteRequest {
                key: b"key".to_vec(),
                topology_epoch: 1,
                request_id: "same-id".into(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            delete_conflict.error.unwrap().code,
            ErrorCode::MutationIdConflict as i32
        );
        assert_eq!(service.state.read().await.next_sequence, 1);
        assert_eq!(
            service.state.read().await.records[b"key".as_slice()]
                .value
                .as_ref(),
            b"original"
        );

        let delete = DeleteRequest {
            key: b"key".to_vec(),
            topology_epoch: 1,
            request_id: "delete-id".into(),
        };
        assert!(
            service
                .delete(Request::new(delete.clone()))
                .await
                .unwrap()
                .into_inner()
                .error
                .is_none()
        );
        assert!(
            service
                .delete(Request::new(delete))
                .await
                .unwrap()
                .into_inner()
                .error
                .is_none()
        );
        assert_eq!(service.state.read().await.next_sequence, 2);
    }

    #[tokio::test]
    async fn dedup_budget_rejects_before_apply_and_expired_records_free_capacity() {
        let mut service = service();
        service.max_dedup_bytes = dedup_retained_bytes("first", b"key-1".len(), "node-1".len());
        let first = PutRequest {
            key: b"key-1".to_vec(),
            value: b"one".to_vec(),
            topology_epoch: 1,
            request_id: "first".into(),
        };
        assert!(
            service
                .put(Request::new(first))
                .await
                .unwrap()
                .into_inner()
                .error
                .is_none()
        );
        let second = PutRequest {
            key: b"key-2".to_vec(),
            value: b"two".to_vec(),
            topology_epoch: 1,
            request_id: "other".into(),
        };
        let rejected = service
            .put(Request::new(second.clone()))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            rejected.error.unwrap().code,
            ErrorCode::ResourceExhausted as i32
        );
        assert_eq!(service.state.read().await.next_sequence, 1);
        assert!(
            !service
                .state
                .read()
                .await
                .records
                .contains_key(b"key-2".as_slice())
        );
        let mut state = service.state.write().await;
        state.dedup.get_mut("first").unwrap().expires_at =
            Instant::now() - std::time::Duration::from_millis(1);
        state.dedup_expirations.push(Reverse((
            Instant::now() - std::time::Duration::from_millis(1),
            "first".into(),
        )));
        drop(state);
        assert!(
            service
                .put(Request::new(second))
                .await
                .unwrap()
                .into_inner()
                .error
                .is_none()
        );
        assert_eq!(service.state.read().await.next_sequence, 2);
    }

    #[tokio::test]
    async fn migration_carries_snapshot_and_journal_mutation_ids() {
        let source = service();
        let before = PutRequest {
            key: b"before".to_vec(),
            value: b"one".to_vec(),
            topology_epoch: 1,
            request_id: "before-id".into(),
        };
        let first_version = source
            .put(Request::new(before.clone()))
            .await
            .unwrap()
            .into_inner()
            .version
            .unwrap();
        source
            .prepare_source_range(Request::new(PrepareRangeRequest {
                range: Some(range(0)),
            }))
            .await
            .unwrap();
        let after = PutRequest {
            key: b"after".to_vec(),
            value: b"two".to_vec(),
            topology_epoch: 1,
            request_id: "after-id".into(),
        };
        let second_version = source
            .put(Request::new(after.clone()))
            .await
            .unwrap()
            .into_inner()
            .version
            .unwrap();
        let snapshot = source
            .read_snapshot_page(Request::new(SnapshotPageRequest {
                change_id: "change-1".into(),
                range_id: "range-1".into(),
                cursor: 0,
                max_bytes: MAX_MIGRATION_PAGE_BYTES as u64,
            }))
            .await
            .unwrap()
            .into_inner();
        let dedup = source
            .read_dedup_snapshot_page(Request::new(SnapshotPageRequest {
                change_id: "change-1".into(),
                range_id: "range-1".into(),
                cursor: 0,
                max_bytes: MAX_MIGRATION_PAGE_BYTES as u64,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(dedup.records.len(), 1);
        assert_eq!(dedup.records[0].mutation_id, "before-id");
        let journal = source
            .read_changelog_page(Request::new(ChangelogPageRequest {
                change_id: "change-1".into(),
                range_id: "range-1".into(),
                after_watermark: 0,
                max_bytes: MAX_MIGRATION_PAGE_BYTES as u64,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(journal.records.len(), 1);
        assert_eq!(
            journal.records[0].record.as_ref().unwrap().mutation_id,
            "after-id"
        );

        let mut destination = service();
        destination.node_id = "node-2".into();
        destination
            .prepare_destination_range(Request::new(PrepareRangeRequest {
                range: Some(range(0)),
            }))
            .await
            .unwrap();
        destination
            .apply_migration_batch(Request::new(ApplyMigrationBatchRequest {
                change_id: "change-1".into(),
                range_id: "range-1".into(),
                snapshot_records: snapshot.records,
                journal_records: Vec::new(),
            }))
            .await
            .unwrap();
        destination
            .apply_dedup_batch(Request::new(ApplyDedupBatchRequest {
                change_id: "change-1".into(),
                range_id: "range-1".into(),
                records: dedup.records,
            }))
            .await
            .unwrap();
        destination
            .apply_migration_batch(Request::new(ApplyMigrationBatchRequest {
                change_id: "change-1".into(),
                range_id: "range-1".into(),
                snapshot_records: Vec::new(),
                journal_records: journal.records,
            }))
            .await
            .unwrap();
        destination
            .commit_destination_range(Request::new(RangeControlRequest {
                change_id: "change-1".into(),
                range_id: "range-1".into(),
            }))
            .await
            .unwrap();
        destination.node_id = "node-1".into(); // The test topology still routes ownership to node-1.
        let retry_before = destination
            .put(Request::new(before))
            .await
            .unwrap()
            .into_inner();
        let retry_after = destination
            .put(Request::new(after))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(retry_before.version, Some(first_version));
        assert_eq!(retry_after.version, Some(second_version));
        assert_eq!(destination.state.read().await.next_sequence, 0);
    }

    #[tokio::test]
    async fn expired_staged_ids_free_migration_budget_before_commit() {
        let mut destination = service();
        destination.node_id = "node-2".into();
        destination.max_dedup_bytes = dedup_retained_bytes("first", b"key-1".len(), "node-1".len());
        destination
            .prepare_destination_range(Request::new(PrepareRangeRequest {
                range: Some(range(0)),
            }))
            .await
            .unwrap();
        let make_record = |id: &str, key: &[u8]| DeduplicationRecord {
            mutation_id: id.into(),
            key: key.to_vec(),
            fingerprint: mutation_fingerprint(key, b"value", false).to_vec(),
            version: Some(RecordVersion {
                topology_epoch: 1,
                owner_sequence: 1,
                owner_node_id: "node-1".into(),
            }),
            deleted: false,
            remaining_window_millis: 60_000,
        };
        let request = |record| ApplyDedupBatchRequest {
            change_id: "change-1".into(),
            range_id: "range-1".into(),
            records: vec![record],
        };
        destination
            .apply_dedup_batch(Request::new(request(make_record("first", b"key-1"))))
            .await
            .unwrap();
        destination
            .state
            .write()
            .await
            .destinations
            .get_mut(&("change-1".into(), "range-1".into()))
            .unwrap()
            .dedup
            .get_mut("first")
            .unwrap()
            .expires_at = Instant::now() - std::time::Duration::from_millis(1);
        destination
            .apply_dedup_batch(Request::new(request(make_record("other", b"key-2"))))
            .await
            .unwrap();
        let staged_expiry = {
            let mut state = destination.state.write().await;
            let entry = state
                .destinations
                .get_mut(&("change-1".into(), "range-1".into()))
                .unwrap()
                .dedup
                .get_mut("other")
                .unwrap();
            entry.expires_at = Instant::now() + std::time::Duration::from_secs(5);
            entry.expires_at
        };
        destination
            .commit_destination_range(Request::new(RangeControlRequest {
                change_id: "change-1".into(),
                range_id: "range-1".into(),
            }))
            .await
            .unwrap();
        let state = destination.state.read().await;
        assert!(!state.dedup.contains_key("first"));
        assert_eq!(state.dedup["other"].expires_at, staged_expiry);
    }

    #[tokio::test]
    async fn delete_is_idempotent_and_put_restores_an_empty_value() {
        let service = service();
        service
            .put(Request::new(PutRequest {
                key: b"key".to_vec(),
                value: Vec::new(),
                topology_epoch: 1,
                request_id: "put-1".into(),
            }))
            .await
            .unwrap();

        let first = service
            .delete(Request::new(DeleteRequest {
                key: b"key".to_vec(),
                topology_epoch: 1,
                request_id: "delete-1".into(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(first.error.is_none());
        assert_eq!(service.state.read().await.next_sequence, 2);
        assert!(service.state.read().await.records[b"key".as_slice()].deleted);

        let second = service
            .delete(Request::new(DeleteRequest {
                key: b"key".to_vec(),
                topology_epoch: 1,
                request_id: "delete-2".into(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(second.error.is_none());
        assert_eq!(service.state.read().await.next_sequence, 3);

        service
            .put(Request::new(PutRequest {
                key: b"key".to_vec(),
                value: b"restored".to_vec(),
                topology_epoch: 1,
                request_id: "put-2".into(),
            }))
            .await
            .unwrap();
        let restored = service
            .get(Request::new(GetRequest {
                key: b"key".to_vec(),
                topology_epoch: 1,
                request_id: "get-1".into(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(restored.value, b"restored");
        assert_eq!(restored.version.unwrap().owner_sequence, 4);
    }

    #[tokio::test]
    async fn delete_validates_empty_and_oversized_keys() {
        let service = service();
        let empty = service
            .delete(Request::new(DeleteRequest {
                key: Vec::new(),
                topology_epoch: 1,
                request_id: "delete-empty".into(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(empty.error.unwrap().code, ErrorCode::InvalidArgument as i32);

        let oversized = service
            .delete(Request::new(DeleteRequest {
                key: vec![0; DEFAULT_MAX_KEY_BYTES + 1],
                topology_epoch: 1,
                request_id: "delete-oversized".into(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(oversized.error.unwrap().code, ErrorCode::TooLarge as i32);

        let oversized_mutation_id = service
            .delete(Request::new(DeleteRequest {
                key: b"key".to_vec(),
                topology_epoch: 1,
                request_id: "x".repeat(MAX_MUTATION_ID_BYTES + 1),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            oversized_mutation_id.error.unwrap().code,
            ErrorCode::InvalidArgument as i32
        );
        assert_eq!(service.state.read().await.next_sequence, 0);
    }

    #[tokio::test]
    async fn snapshot_and_changelog_preserve_a_deleted_key_tombstone() {
        let source = service();
        source
            .put(Request::new(PutRequest {
                key: b"key".to_vec(),
                value: b"value".to_vec(),
                topology_epoch: 1,
                request_id: "put-1".into(),
            }))
            .await
            .unwrap();
        source
            .prepare_source_range(Request::new(PrepareRangeRequest {
                range: Some(range(0)),
            }))
            .await
            .unwrap();
        source
            .delete(Request::new(DeleteRequest {
                key: b"key".to_vec(),
                topology_epoch: 1,
                request_id: "delete-1".into(),
            }))
            .await
            .unwrap();

        let snapshot = source
            .read_snapshot_page(Request::new(SnapshotPageRequest {
                change_id: "change-1".into(),
                range_id: "range-1".into(),
                cursor: 0,
                max_bytes: MAX_MIGRATION_PAGE_BYTES as u64,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(snapshot.records.len(), 1);
        assert!(snapshot.records[0].deleted);
        assert_eq!(snapshot.next_cursor, 1);
        assert!(snapshot.done);

        let changelog = source
            .read_changelog_page(Request::new(ChangelogPageRequest {
                change_id: "change-1".into(),
                range_id: "range-1".into(),
                after_watermark: 0,
                max_bytes: MAX_MIGRATION_PAGE_BYTES as u64,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(changelog.current_watermark, 1);
        assert!(changelog.records[0].record.as_ref().unwrap().deleted);

        let digest = source
            .source_range_digest(Request::new(RangeControlRequest {
                change_id: "change-1".into(),
                range_id: "range-1".into(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(digest.record_count, 1);
        assert_eq!(digest.changelog_watermark, 1);

        let mut destination = service();
        destination.node_id = "node-2".into();
        destination
            .prepare_destination_range(Request::new(PrepareRangeRequest {
                range: Some(range(0)),
            }))
            .await
            .unwrap();
        destination
            .apply_migration_batch(Request::new(ApplyMigrationBatchRequest {
                change_id: "change-1".into(),
                range_id: "range-1".into(),
                snapshot_records: snapshot.records,
                journal_records: Vec::new(),
            }))
            .await
            .unwrap();
        let destination_digest = destination
            .destination_range_digest(Request::new(RangeControlRequest {
                change_id: "change-1".into(),
                range_id: "range-1".into(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(destination_digest.digest, digest.digest);
        destination
            .commit_destination_range(Request::new(RangeControlRequest {
                change_id: "change-1".into(),
                range_id: "range-1".into(),
            }))
            .await
            .unwrap();
        destination.node_id = "node-1".into();
        let read = destination
            .get(Request::new(GetRequest {
                key: b"key".to_vec(),
                topology_epoch: 1,
                request_id: "get-deleted".into(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(read.error.unwrap().code, ErrorCode::NotFound as i32);
    }

    #[tokio::test]
    async fn deletion_replay_and_commit_remove_stale_destination_data() {
        let mut destination = service();
        destination.node_id = "node-2".into();
        destination.state.write().await.records.insert(
            b"key".to_vec(),
            Record {
                value: Arc::from(b"stale".as_slice()),
                version: RecordVersion {
                    topology_epoch: 0,
                    owner_sequence: 1,
                    owner_node_id: "node-2".into(),
                },
                deleted: false,
            },
        );
        destination
            .prepare_destination_range(Request::new(PrepareRangeRequest {
                range: Some(range(0)),
            }))
            .await
            .unwrap();
        let deletion = JournalRecord {
            watermark: 1,
            record: Some(MigrationRecord {
                key: b"key".to_vec(),
                value: Vec::new(),
                version: Some(RecordVersion {
                    topology_epoch: 1,
                    owner_sequence: 2,
                    owner_node_id: "node-1".into(),
                }),
                deleted: true,
                mutation_id: String::new(),
                remaining_window_millis: 0,
            }),
        };
        let batch = ApplyMigrationBatchRequest {
            change_id: "change-1".into(),
            range_id: "range-1".into(),
            snapshot_records: Vec::new(),
            journal_records: vec![deletion],
        };
        destination
            .apply_migration_batch(Request::new(batch.clone()))
            .await
            .unwrap();
        destination
            .apply_migration_batch(Request::new(batch))
            .await
            .unwrap();

        let control = RangeControlRequest {
            change_id: "change-1".into(),
            range_id: "range-1".into(),
        };
        let digest = destination
            .destination_range_digest(Request::new(control.clone()))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(digest.record_count, 1);
        assert_eq!(digest.changelog_watermark, 1);
        destination
            .commit_destination_range(Request::new(control))
            .await
            .unwrap();
        assert!(destination.state.read().await.records[b"key".as_slice()].deleted);
    }

    #[tokio::test]
    async fn migration_replay_preserves_delete_and_put_order() {
        let mut destination = service();
        destination.node_id = "node-2".into();
        let version = |owner_sequence| RecordVersion {
            topology_epoch: 1,
            owner_sequence,
            owner_node_id: "node-1".into(),
        };
        let deletion = |watermark, owner_sequence| JournalRecord {
            watermark,
            record: Some(MigrationRecord {
                key: b"key".to_vec(),
                value: Vec::new(),
                version: Some(version(owner_sequence)),
                deleted: true,
                mutation_id: String::new(),
                remaining_window_millis: 0,
            }),
        };
        let put = |watermark, owner_sequence, value: &[u8]| JournalRecord {
            watermark,
            record: Some(MigrationRecord {
                key: b"key".to_vec(),
                value: value.to_vec(),
                version: Some(version(owner_sequence)),
                deleted: false,
                mutation_id: String::new(),
                remaining_window_millis: 0,
            }),
        };

        destination
            .prepare_destination_range(Request::new(PrepareRangeRequest {
                range: Some(range(0)),
            }))
            .await
            .unwrap();
        destination
            .apply_migration_batch(Request::new(ApplyMigrationBatchRequest {
                change_id: "change-1".into(),
                range_id: "range-1".into(),
                snapshot_records: Vec::new(),
                journal_records: vec![deletion(1, 1), put(2, 2, b"restored")],
            }))
            .await
            .unwrap();
        let key = ("change-1".to_owned(), "range-1".to_owned());
        assert_eq!(
            destination.state.read().await.destinations[&key].records[b"key".as_slice()]
                .value
                .as_ref(),
            b"restored"
        );

        destination
            .abort_range_migration(Request::new(RangeControlRequest {
                change_id: "change-1".into(),
                range_id: "range-1".into(),
            }))
            .await
            .unwrap();
        destination
            .prepare_destination_range(Request::new(PrepareRangeRequest {
                range: Some(range(0)),
            }))
            .await
            .unwrap();
        destination
            .apply_migration_batch(Request::new(ApplyMigrationBatchRequest {
                change_id: "change-1".into(),
                range_id: "range-1".into(),
                snapshot_records: Vec::new(),
                journal_records: vec![put(1, 3, b"temporary"), deletion(2, 4)],
            }))
            .await
            .unwrap();
        assert!(
            destination.state.read().await.destinations[&key].records[b"key".as_slice()].deleted
        );
    }

    #[tokio::test]
    async fn stop_is_fenced_to_the_process_instance() {
        let service = service();
        let receiver = service.shutdown_receiver();
        let error = service
            .stop(Request::new(StopRequest {
                node_id: "node-1".into(),
                process_instance_id: "replacement".into(),
            }))
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert!(!*receiver.borrow());

        service
            .prepare_stop(Request::new(StopRequest {
                node_id: "node-1".into(),
                process_instance_id: "instance-1".into(),
            }))
            .await
            .unwrap();
        service
            .stop(Request::new(StopRequest {
                node_id: "node-1".into(),
                process_instance_id: "instance-1".into(),
            }))
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(120)).await;
        assert!(*receiver.borrow());
    }

    #[tokio::test]
    async fn journal_budget_is_aggregate_across_ranges() {
        let mut service = service();
        service.max_journal_bytes = 200;
        let mut first = range(0);
        first.range_id = "range-1".into();
        let mut second = first.clone();
        second.range_id = "range-2".into();
        service
            .prepare_source_range(Request::new(PrepareRangeRequest { range: Some(first) }))
            .await
            .unwrap();
        service
            .prepare_source_range(Request::new(PrepareRangeRequest {
                range: Some(second),
            }))
            .await
            .unwrap();
        let response = service
            .put(Request::new(PutRequest {
                key: b"key".to_vec(),
                value: b"value".to_vec(),
                topology_epoch: 1,
                request_id: "put-1".into(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            response.error.unwrap().code,
            ErrorCode::ResourceExhausted as i32
        );
        assert_eq!(service.state.read().await.journal_bytes_total, 0);
    }

    #[tokio::test]
    async fn delete_leaves_the_value_when_the_journal_budget_is_full() {
        let mut service = service();
        service
            .put(Request::new(PutRequest {
                key: b"key".to_vec(),
                value: b"value".to_vec(),
                topology_epoch: 1,
                request_id: "put-1".into(),
            }))
            .await
            .unwrap();
        service.max_journal_bytes = 1;
        service
            .prepare_source_range(Request::new(PrepareRangeRequest {
                range: Some(range(0)),
            }))
            .await
            .unwrap();
        let response = service
            .delete(Request::new(DeleteRequest {
                key: b"key".to_vec(),
                topology_epoch: 1,
                request_id: "delete-1".into(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            response.error.unwrap().code,
            ErrorCode::ResourceExhausted as i32
        );
        assert_eq!(
            service
                .state
                .read()
                .await
                .records
                .get(b"key".as_slice())
                .unwrap()
                .value
                .as_ref(),
            b"value"
        );
        assert_eq!(service.state.read().await.next_sequence, 1);
    }
}
