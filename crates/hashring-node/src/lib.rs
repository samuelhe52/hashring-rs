// tonic service signatures intentionally return its concrete Status type.
#![allow(clippy::result_large_err)]

use std::{
    cmp::Ordering,
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering as AtomicOrdering},
    },
};

use tokio::sync::{Mutex, RwLock, watch};
use tonic::{Request, Response, Status};

pub use hashring_core::limits::{
    DEFAULT_MAX_KEY_BYTES, DEFAULT_MAX_VALUE_BYTES, MAX_CONTROL_MESSAGE_BYTES,
    MAX_DATA_MESSAGE_BYTES, MAX_MIGRATION_PAGE_BYTES,
};
pub use hashring_core::transport::{configure_coordinator_client, fetch_topology};

use hashring_core::{
    proto::{
        self, ApplyMigrationBatchRequest, ChangelogPageRequest, ChangelogPageResponse,
        DeleteRequest, DeleteResponse, ErrorCode, GetRequest, GetResponse, InstallTopologyRequest,
        JournalRecord, MigrationRecord, NodeInfoResponse, OperationError, PauseRangeResponse,
        PrepareDestinationRangeResponse, PrepareRangeRequest, PrepareSourceRangeResponse,
        PutRequest, PutResponse, RangeControlRequest, RangeDigestResponse, RecordVersion,
        RegisterNodeRequest, SnapshotPageRequest, SnapshotPageResponse, StopRequest,
        coordinator_client::CoordinatorClient, data_node_server::DataNode,
    },
    topology::{TopologySnapshot, WriteAckPolicy},
};

pub const DEFAULT_MAX_MIGRATION_JOURNAL_BYTES: usize = 16 * 1024 * 1024;

#[derive(Clone)]
struct Record {
    value: Arc<[u8]>,
    version: RecordVersion,
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
    snapshot_ready: watch::Sender<bool>,
    journal: Vec<JournalRecord>,
    journal_bytes: usize,
    watermark: u64,
    writes_paused: bool,
}

struct DestinationMigration {
    range: RangeSpec,
    records: HashMap<Vec<u8>, Record>,
    watermark: u64,
    committed: bool,
    writes_activated: bool,
}

struct NodeState {
    topology: TopologySnapshot,
    records: HashMap<Vec<u8>, Record>,
    next_sequence: u64,
    sources: HashMap<(String, String), SourceMigration>,
    destinations: HashMap<(String, String), DestinationMigration>,
    journal_bytes_total: usize,
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
    refresh_lock: Arc<Mutex<()>>,
    max_key_bytes: usize,
    max_value_bytes: usize,
    max_journal_bytes: usize,
    stop_response_delay: std::time::Duration,
    stop_prepared: Arc<AtomicBool>,
    shutdown: watch::Sender<bool>,
}

impl DataNodeService {
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
        Ok(Self {
            node_id,
            process_instance_id,
            coordinator_endpoint,
            state: Arc::new(RwLock::new(NodeState {
                topology,
                records: HashMap::new(),
                next_sequence: 0,
                sources: HashMap::new(),
                destinations: HashMap::new(),
                journal_bytes_total: 0,
            })),
            refresh_lock: Arc::new(Mutex::new(())),
            max_key_bytes: DEFAULT_MAX_KEY_BYTES,
            max_value_bytes: DEFAULT_MAX_VALUE_BYTES,
            max_journal_bytes: DEFAULT_MAX_MIGRATION_JOURNAL_BYTES,
            stop_response_delay: std::time::Duration::ZERO,
            stop_prepared: Arc::new(AtomicBool::new(false)),
            shutdown,
        })
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
        let topology = fetch_topology(&self.coordinator_endpoint)
            .await
            .map_err(|error| Status::unavailable(error.to_string()))?;
        let mut state = self.state.write().await;
        if topology.epoch >= state.topology.epoch {
            state.topology = topology;
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
        let Some(record) = state.records.get(&request.key) else {
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
        let mut state = self.state.write().await;
        if let Some(error) = self.owner_error(&state, &request.key)? {
            return Ok(Response::new(PutResponse {
                current_epoch: error.current_epoch,
                error: Some(error),
                ..Default::default()
            }));
        }
        if let Some(error) = unsupported_write_policy_error(&state) {
            return Ok(Response::new(PutResponse {
                current_epoch: error.current_epoch,
                error: Some(error),
                ..Default::default()
            }));
        }

        let token = state.topology.key_token(&request.key);
        let journal_record_bytes = request.key.len() + request.value.len() + 128;
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

        state.next_sequence += 1;
        let version = RecordVersion {
            topology_epoch: state.topology.epoch,
            owner_sequence: state.next_sequence,
            owner_node_id: self.node_id.clone(),
        };
        let record = Record {
            value: request.value.into(),
            version: version.clone(),
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
                }),
            });
        }
        state.journal_bytes_total += journal_growth;
        Ok(Response::new(PutResponse {
            version: Some(version),
            current_epoch: state.topology.epoch,
            error: None,
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
        self.refresh_if_newer(request.topology_epoch).await?;
        let mut state = self.state.write().await;
        if let Some(error) = self.owner_error(&state, &request.key)? {
            return Ok(Response::new(DeleteResponse {
                current_epoch: error.current_epoch,
                error: Some(error),
            }));
        }
        if let Some(error) = unsupported_write_policy_error(&state) {
            return Ok(Response::new(DeleteResponse {
                current_epoch: error.current_epoch,
                error: Some(error),
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

        if !state.records.contains_key(&request.key) {
            return Ok(Response::new(DeleteResponse {
                current_epoch: state.topology.epoch,
                error: None,
            }));
        }

        let journal_record_bytes = request.key.len() + 128;
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

        state.next_sequence += 1;
        let version = RecordVersion {
            topology_epoch: state.topology.epoch,
            owner_sequence: state.next_sequence,
            owner_node_id: self.node_id.clone(),
        };
        state.records.remove(&request.key);
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
                }),
            });
        }
        state.journal_bytes_total += journal_growth;
        Ok(Response::new(DeleteResponse {
            current_epoch: state.topology.epoch,
            error: None,
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
        let (all_keys, topology, ready) = {
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
            let topology = state.topology.clone();
            let (ready, _) = watch::channel(false);
            state.sources.insert(
                key.clone(),
                SourceMigration {
                    range: range.clone(),
                    snapshot_keys: None,
                    snapshot_ready: ready.clone(),
                    journal: Vec::new(),
                    journal_bytes: 0,
                    watermark: 0,
                    writes_paused: false,
                },
            );
            (all_keys, topology, ready)
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
                deleted: false,
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
            records.push(entry.clone());
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
        for record in request.snapshot_records {
            if record.deleted {
                return Err(Status::invalid_argument(
                    "snapshot record must contain a live value",
                ));
            }
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
            apply_record(&mut destination.records, record)?;
            destination.watermark = entry.watermark;
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
        let (range, records) = {
            let destination = state
                .destinations
                .get_mut(&key)
                .ok_or_else(|| Status::not_found("destination migration not prepared"))?;
            if destination.committed {
                return Ok(Response::new(proto::Empty {}));
            }
            destination.committed = true;
            (destination.range.clone(), destination.records.clone())
        };
        let topology = state.topology.clone();
        state
            .records
            .retain(|record_key, _| !range.contains(topology.key_token(record_key)));
        for (key, record) in records {
            apply_internal_record(&mut state.records, key, record);
        }
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
                range.contains(topology.key_token(record_key))
                    && topology
                        .owner(record_key)
                        .is_ok_and(|owner| owner.node_id != self.node_id)
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

    async fn install_topology(
        &self,
        request: Request<InstallTopologyRequest>,
    ) -> Result<Response<proto::Empty>, Status> {
        let topology: TopologySnapshot = request
            .into_inner()
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
        state.topology = topology;
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

fn unsupported_write_policy_error(state: &NodeState) -> Option<OperationError> {
    (state.topology.write_ack_policy != WriteAckPolicy::OwnerOnly).then(|| OperationError {
        current_epoch: state.topology.epoch,
        ..operation_error(
            ErrorCode::Unavailable,
            "committed write acknowledgement policy is not active yet",
            true,
        )
    })
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
    record.key.len() + record.value.len() + 128
}

fn apply_record(
    records: &mut HashMap<Vec<u8>, Record>,
    record: MigrationRecord,
) -> Result<(), Status> {
    let version = record
        .version
        .ok_or_else(|| Status::invalid_argument("migration record omitted version"))?;
    if record.deleted {
        if !record.value.is_empty() {
            return Err(Status::invalid_argument(
                "deleted migration record must omit its value",
            ));
        }
        records.remove(&record.key);
        return Ok(());
    }
    apply_internal_record(
        records,
        record.key,
        Record {
            value: record.value.into(),
            version,
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
    digest.update(b"hashring-rs:records:v1\0");
    for (key, record) in &ordered {
        digest.update(&(key.len() as u64).to_be_bytes());
        digest.update(key);
        digest.update(&record.version.topology_epoch.to_be_bytes());
        digest.update(&record.version.owner_sequence.to_be_bytes());
        digest.update(&(record.version.owner_node_id.len() as u64).to_be_bytes());
        digest.update(record.version.owner_node_id.as_bytes());
        digest.update(&(record.value.len() as u64).to_be_bytes());
        digest.update(record.value.as_ref());
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
    use hashring_core::topology::{Member, TopologyConfig};

    fn service() -> DataNodeService {
        let topology = TopologySnapshot::new(
            1,
            42,
            8,
            vec![Member {
                node_id: "node-1".into(),
                endpoint: "http://127.0.0.1:5001".into(),
            }],
        )
        .unwrap();
        let (shutdown, _) = watch::channel(false);
        DataNodeService {
            node_id: "node-1".into(),
            process_instance_id: "instance-1".into(),
            coordinator_endpoint: "http://127.0.0.1:5000".into(),
            state: Arc::new(RwLock::new(NodeState {
                topology,
                records: HashMap::new(),
                next_sequence: 0,
                sources: HashMap::new(),
                destinations: HashMap::new(),
                journal_bytes_total: 0,
            })),
            refresh_lock: Arc::new(Mutex::new(())),
            max_key_bytes: DEFAULT_MAX_KEY_BYTES,
            max_value_bytes: DEFAULT_MAX_VALUE_BYTES,
            max_journal_bytes: DEFAULT_MAX_MIGRATION_JOURNAL_BYTES,
            stop_response_delay: std::time::Duration::ZERO,
            stop_prepared: Arc::new(AtomicBool::new(false)),
            shutdown,
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
                }))
                .await
                .unwrap();

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
            assert_eq!(put.error.unwrap().code, ErrorCode::Unavailable as i32);
            let delete = service
                .delete(Request::new(DeleteRequest {
                    key: b"key".to_vec(),
                    topology_epoch: 2,
                    request_id: "delete".into(),
                }))
                .await
                .unwrap()
                .into_inner();
            assert_eq!(delete.error.unwrap().code, ErrorCode::Unavailable as i32);
            assert!(service.state.read().await.records.is_empty());
        }
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
        let topology = TopologySnapshot::new(
            2,
            42,
            8,
            vec![Member {
                node_id: "node-2".into(),
                endpoint: "http://127.0.0.1:5002".into(),
            }],
        )
        .unwrap();
        destination
            .install_topology(Request::new(InstallTopologyRequest {
                topology: Some((&topology).into()),
            }))
            .await
            .unwrap();

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
        assert!(
            !service
                .state
                .read()
                .await
                .records
                .contains_key(b"key".as_slice())
        );

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
        assert_eq!(service.state.read().await.next_sequence, 2);

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
        assert_eq!(restored.version.unwrap().owner_sequence, 3);
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
        assert_eq!(service.state.read().await.next_sequence, 0);
    }

    #[tokio::test]
    async fn snapshot_skips_a_deleted_key_and_journals_the_deletion() {
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
        service
            .delete(Request::new(DeleteRequest {
                key: b"key".to_vec(),
                topology_epoch: 1,
                request_id: "delete-1".into(),
            }))
            .await
            .unwrap();

        let snapshot = service
            .read_snapshot_page(Request::new(SnapshotPageRequest {
                change_id: "change-1".into(),
                range_id: "range-1".into(),
                cursor: 0,
                max_bytes: MAX_MIGRATION_PAGE_BYTES as u64,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(snapshot.records.is_empty());
        assert_eq!(snapshot.next_cursor, 1);
        assert!(snapshot.done);

        let changelog = service
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

        let digest = service
            .source_range_digest(Request::new(RangeControlRequest {
                change_id: "change-1".into(),
                range_id: "range-1".into(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(digest.record_count, 0);
        assert_eq!(digest.changelog_watermark, 1);
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
        assert_eq!(digest.record_count, 0);
        assert_eq!(digest.changelog_watermark, 1);
        destination
            .commit_destination_range(Request::new(control))
            .await
            .unwrap();
        assert!(destination.state.read().await.records.is_empty());
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
            }),
        };
        let put = |watermark, owner_sequence, value: &[u8]| JournalRecord {
            watermark,
            record: Some(MigrationRecord {
                key: b"key".to_vec(),
                value: value.to_vec(),
                version: Some(version(owner_sequence)),
                deleted: false,
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
            destination.state.read().await.destinations[&key]
                .records
                .is_empty()
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
