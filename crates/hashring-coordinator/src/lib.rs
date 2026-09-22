// Public tonic and redb APIs use concrete error types whose context is more
// useful here than erasing or boxing them solely to reduce enum size.
#![allow(clippy::result_large_err)]

use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    path::Path,
    sync::Arc,
    time::Duration,
};

use futures::{StreamExt, stream::FuturesUnordered};
use redb::{Database, TableDefinition};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::{
    sync::{Mutex, RwLock},
    time::Instant,
};
use tonic::{Request, Response, Status};

use hashring_core::{
    limits::{MAX_CONTROL_MESSAGE_BYTES, MAX_MIGRATION_PAGE_BYTES},
    migration::{
        MAX_MIGRATION_RANGES, MAX_REPLICA_OBLIGATIONS, MigrationError, MigrationPhase,
        RangeMigration, TopologyChange,
    },
    proto::{
        self, ApplyMigrationBatchRequest, ChangelogPageRequest, InstallTopologyRequest,
        PrepareRangeRequest, RangeControlRequest, SnapshotPageRequest, StopRequest,
        coordinator_server::Coordinator, data_node_client::DataNodeClient,
    },
    topology::{Member, TopologySnapshot},
};

const TOPOLOGY_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("topology");
const COMMITTED_KEY: &str = "committed";
const CLUSTER_STATE_KEY: &str = "cluster-state-v1";
pub const DEFAULT_RANGE_MOVE_CONCURRENCY: usize = 16;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterState {
    pub committed: TopologySnapshot,
    pub active_change: Option<TopologyChange>,
    #[serde(default)]
    pub process_instances: BTreeMap<String, String>,
    #[serde(default)]
    pub stop_confirmations: BTreeMap<String, BTreeSet<String>>,
}

impl ClusterState {
    fn validate(&self) -> Result<(), RepositoryError> {
        self.committed.validate()?;
        if let Some(change) = &self.active_change {
            if change.ranges.len() > MAX_MIGRATION_RANGES {
                return Err(RepositoryError::InvalidState(format!(
                    "active change exceeds the {MAX_MIGRATION_RANGES} moving-range limit"
                )));
            }
            if change.replica_obligations.len() > MAX_REPLICA_OBLIGATIONS {
                return Err(RepositoryError::InvalidState(format!(
                    "active change exceeds the {MAX_REPLICA_OBLIGATIONS} replica-obligation limit"
                )));
            }
            change.target_topology.validate()?;
            let before_publication = !matches!(
                change.phase,
                MigrationPhase::Published | MigrationPhase::CleaningUp | MigrationPhase::Complete
            );
            let epoch_relation_is_valid = if before_publication {
                change.base_epoch == self.committed.epoch
                    && change.target_topology.epoch == self.committed.epoch.saturating_add(1)
            } else {
                change.target_topology == self.committed
                    && change.base_epoch.saturating_add(1) == self.committed.epoch
            };
            if !epoch_relation_is_valid
                || change.target_topology.hash_seed != self.committed.hash_seed
                || change.target_topology.hash_algorithm != self.committed.hash_algorithm
                || change.target_topology.encoding_version != self.committed.encoding_version
                || change.target_topology.virtual_nodes != self.committed.virtual_nodes
            {
                return Err(RepositoryError::InvalidState(
                    "active change does not descend from committed topology".into(),
                ));
            }
        }
        if self
            .process_instances
            .iter()
            .any(|(node_id, instance_id)| node_id.is_empty() || instance_id.is_empty())
        {
            return Err(RepositoryError::InvalidState(
                "registered process identities must not be empty".into(),
            ));
        }
        if self.stop_confirmations.iter().any(|(node_id, instances)| {
            node_id.is_empty() || instances.is_empty() || instances.iter().any(String::is_empty)
        }) {
            return Err(RepositoryError::InvalidState(
                "durable stop confirmations must identify a node and process instance".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum RepositoryError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("database error: {0}")]
    Database(#[from] redb::DatabaseError),
    #[error("transaction error: {0}")]
    Transaction(#[from] redb::TransactionError),
    #[error("table error: {0}")]
    Table(#[from] redb::TableError),
    #[error("storage error: {0}")]
    Storage(#[from] redb::StorageError),
    #[error("commit error: {0}")]
    Commit(#[from] redb::CommitError),
    #[error("invalid persisted topology: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("invalid topology: {0}")]
    Topology(#[from] hashring_core::topology::TopologyError),
    #[error("invalid migration: {0}")]
    Migration(#[from] MigrationError),
    #[error("invalid coordinator state: {0}")]
    InvalidState(String),
    #[error("the coordinator store is empty; bootstrap members are required")]
    MissingBootstrap,
    #[error("configured bootstrap topology differs from durable topology")]
    BootstrapMismatch,
}

pub trait CoordinatorRepository: Send + Sync {
    fn load_state(&self) -> Result<Option<ClusterState>, RepositoryError>;
    fn store_state(&self, state: &ClusterState) -> Result<(), RepositoryError>;
}

pub struct RedbTopologyRepository {
    database: Database,
}

impl RedbTopologyRepository {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, RepositoryError> {
        if let Some(parent) = path
            .as_ref()
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            std::fs::create_dir_all(parent)?;
        }
        Ok(Self {
            database: Database::create(path)?,
        })
    }
}

impl CoordinatorRepository for RedbTopologyRepository {
    fn load_state(&self) -> Result<Option<ClusterState>, RepositoryError> {
        let read = self.database.begin_read()?;
        let table = match read.open_table(TOPOLOGY_TABLE) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        if let Some(bytes) = table.get(CLUSTER_STATE_KEY)? {
            let mut state: ClusterState = serde_json::from_slice(bytes.value())?;
            upgrade_persisted_state(&mut state)?;
            state.validate()?;
            return Ok(Some(state));
        }
        // Stores created by the first implementation contain only the committed
        // topology. Promote them in memory; the next state write upgrades them.
        if let Some(bytes) = table.get(COMMITTED_KEY)? {
            let committed: TopologySnapshot = serde_json::from_slice(bytes.value())?;
            let state = ClusterState {
                committed,
                active_change: None,
                process_instances: BTreeMap::new(),
                stop_confirmations: BTreeMap::new(),
            };
            state.validate()?;
            return Ok(Some(state));
        }
        Ok(None)
    }

    fn store_state(&self, state: &ClusterState) -> Result<(), RepositoryError> {
        state.validate()?;
        let encoded = serde_json::to_vec(state)?;
        let write = self.database.begin_write()?;
        {
            let mut table = write.open_table(TOPOLOGY_TABLE)?;
            table.insert(CLUSTER_STATE_KEY, encoded.as_slice())?;
        }
        write.commit()?;
        Ok(())
    }
}

fn upgrade_persisted_state(state: &mut ClusterState) -> Result<(), RepositoryError> {
    let Some(change) = &mut state.active_change else {
        return Ok(());
    };
    for range in &mut change.ranges {
        if range.source_endpoint.is_empty() {
            range.source_endpoint = state
                .committed
                .members
                .iter()
                .find(|member| member.node_id == range.source_node_id)
                .map(|member| member.endpoint.clone())
                .ok_or_else(|| {
                    RepositoryError::InvalidState(format!(
                        "missing source member {} for persisted range",
                        range.source_node_id
                    ))
                })?;
        }
        if range.destination_endpoint.is_empty() {
            range.destination_endpoint = change
                .target_topology
                .members
                .iter()
                .find(|member| member.node_id == range.destination_node_id)
                .map(|member| member.endpoint.clone())
                .ok_or_else(|| {
                    RepositoryError::InvalidState(format!(
                        "missing destination member {} for persisted range",
                        range.destination_node_id
                    ))
                })?;
        }
    }
    for node_id in &change.stop_prepared_node_ids {
        if let Some(instance_id) = change
            .ranges
            .iter()
            .find(|range| range.source_node_id == *node_id)
            .map(|range| range.source_process_instance_id.as_str())
            .filter(|instance_id| !instance_id.is_empty())
        {
            state
                .stop_confirmations
                .entry(node_id.clone())
                .or_default()
                .insert(instance_id.to_owned());
        }
    }
    Ok(())
}

pub fn load_or_initialize(
    repository: &impl CoordinatorRepository,
    bootstrap: Option<TopologySnapshot>,
) -> Result<ClusterState, RepositoryError> {
    if let Some(persisted) = repository.load_state()? {
        if let Some(bootstrap) = bootstrap
            && persisted.committed != bootstrap
        {
            return Err(RepositoryError::BootstrapMismatch);
        }
        return Ok(persisted);
    }

    let bootstrap = bootstrap.ok_or(RepositoryError::MissingBootstrap)?;
    let state = ClusterState {
        committed: bootstrap,
        active_change: None,
        process_instances: BTreeMap::new(),
        stop_confirmations: BTreeMap::new(),
    };
    repository.store_state(&state)?;
    Ok(state)
}

#[derive(Clone)]
pub struct CoordinatorService {
    state: Arc<RwLock<ClusterState>>,
    repository: Arc<dyn CoordinatorRepository>,
    execution_lock: Arc<Mutex<()>>,
    migration_timeout: Duration,
    range_move_concurrency: usize,
    pre_publish_delay: Duration,
}

impl CoordinatorService {
    pub fn new(
        state: ClusterState,
        repository: Arc<dyn CoordinatorRepository>,
        migration_timeout: Duration,
    ) -> Self {
        Self {
            state: Arc::new(RwLock::new(state)),
            repository,
            execution_lock: Arc::new(Mutex::new(())),
            migration_timeout,
            range_move_concurrency: DEFAULT_RANGE_MOVE_CONCURRENCY,
            pre_publish_delay: Duration::ZERO,
        }
    }

    pub fn with_range_move_concurrency(mut self, concurrency: usize) -> Self {
        assert!(concurrency > 0, "range move concurrency must be positive");
        self.range_move_concurrency = concurrency;
        self
    }

    pub fn with_pre_publish_delay(mut self, delay: Duration) -> Self {
        self.pre_publish_delay = delay;
        self
    }

    pub async fn resume_interrupted_change(&self) -> Result<Option<TopologyChange>, Status> {
        let change = self.state.read().await.active_change.clone();
        match change {
            Some(change)
                if matches!(
                    change.phase,
                    MigrationPhase::Planned | MigrationPhase::Complete | MigrationPhase::Aborted
                ) =>
            {
                Ok(None)
            }
            None => Ok(None),
            Some(change) => self
                .execute_change(change_identity(&change))
                .await
                .map(Some),
        }
    }

    async fn execute_change(&self, expected: ChangeIdentity) -> Result<TopologyChange, Status> {
        let _execution = self.execution_lock.lock().await;
        let mut change = self
            .state
            .read()
            .await
            .active_change
            .clone()
            .ok_or_else(|| Status::failed_precondition("no topology change is active"))?;
        if change_identity(&change) != expected {
            return Err(Status::failed_precondition(
                "active topology change does not match the execute request",
            ));
        }
        match change.phase {
            MigrationPhase::Complete => return Ok(change),
            MigrationPhase::Aborted => {
                return Err(Status::failed_precondition(
                    "the topology change was aborted",
                ));
            }
            MigrationPhase::Aborting => {
                self.finish_abort(&change).await?;
                change.phase = MigrationPhase::Aborted;
                return Ok(change);
            }
            MigrationPhase::Published | MigrationPhase::CleaningUp => {
                return self
                    .finish_published_change(change, Instant::now() + self.migration_timeout)
                    .await;
            }
            MigrationPhase::Planned => {}
            MigrationPhase::Resetting
            | MigrationPhase::CopyingSnapshot
            | MigrationPhase::ReplayingChangelog
            | MigrationPhase::PausingWrites
            | MigrationPhase::Verifying
            | MigrationPhase::ReadyToPublish => {
                change = self.reset_prepublication_attempt(&change).await?;
            }
        }

        let deadline = Instant::now() + self.migration_timeout;
        self.set_phase(MigrationPhase::CopyingSnapshot).await?;
        change.phase = MigrationPhase::CopyingSnapshot;
        let change_ref = &change;
        let progresses = match try_map_bounded(
            change.ranges.clone(),
            self.range_move_concurrency,
            |range| async move {
                let progress = self.copy_range(change_ref, &range, deadline).await?;
                self.store_range_progress(&progress).await?;
                Ok(progress)
            },
        )
        .await
        {
            Ok(progresses) => progresses,
            Err(error) => return self.abort_after_error(&change, error).await,
        };
        for progress in progresses {
            replace_range_progress(&mut change, progress);
        }

        self.set_phase(MigrationPhase::ReplayingChangelog).await?;
        change.phase = MigrationPhase::ReplayingChangelog;
        self.set_phase(MigrationPhase::PausingWrites).await?;
        change.phase = MigrationPhase::PausingWrites;
        let change_ref = &change;
        let final_watermarks = match try_map_bounded(
            change.ranges.clone(),
            self.range_move_concurrency,
            |range| async move {
                let watermark = self.pause_range(change_ref, &range, deadline).await?;
                Ok((range.range_id, watermark))
            },
        )
        .await
        {
            Ok(watermarks) => watermarks.into_iter().collect::<BTreeMap<_, _>>(),
            Err(error) => return self.abort_after_error(&change, error).await,
        };

        self.set_phase(MigrationPhase::Verifying).await?;
        change.phase = MigrationPhase::Verifying;
        let change_ref = &change;
        let final_watermarks = &final_watermarks;
        let progresses = match try_map_bounded(
            change.ranges.clone(),
            self.range_move_concurrency,
            |range| async move {
                let final_watermark = final_watermarks[&range.range_id];
                let progress = self
                    .finalize_range(change_ref, &range, final_watermark, deadline)
                    .await?;
                self.store_range_progress(&progress).await?;
                Ok(progress)
            },
        )
        .await
        {
            Ok(progresses) => progresses,
            Err(error) => return self.abort_after_error(&change, error).await,
        };
        for progress in progresses {
            replace_range_progress(&mut change, progress);
        }

        self.set_phase(MigrationPhase::ReadyToPublish).await?;
        change.phase = MigrationPhase::ReadyToPublish;
        if !self.pre_publish_delay.is_zero() {
            tokio::time::sleep(self.pre_publish_delay).await;
        }
        if let Err(error) = self.verify_destination_instances(&change, deadline).await {
            return self.abort_after_error(&change, error).await;
        }
        {
            let destinations = destination_instances(&change)?;
            let mut state = self.state.write().await;
            let mut next = state.clone();
            if let Some(node_id) =
                destinations
                    .iter()
                    .find_map(|(node_id, (_, expected_instance))| {
                        (next.process_instances.get(node_id) != Some(expected_instance))
                            .then_some(node_id)
                    })
            {
                let error = Status::failed_precondition(format!(
                    "destination process changed before publication: {node_id}"
                ));
                drop(state);
                return self.abort_after_error(&change, error).await;
            }
            let active = next
                .active_change
                .as_mut()
                .ok_or_else(|| Status::internal("active change disappeared"))?;
            if change_identity(active) != expected {
                return Err(Status::failed_precondition(
                    "active topology change changed before publication",
                ));
            }
            active.phase = MigrationPhase::Published;
            next.committed = active.target_topology.clone();
            self.repository
                .store_state(&next)
                .map_err(|error| Status::internal(error.to_string()))?;
            *state = next;
        }
        change.phase = MigrationPhase::Published;
        self.finish_published_change(change, deadline).await
    }

    async fn abort_after_error(
        &self,
        change: &TopologyChange,
        original: Status,
    ) -> Result<TopologyChange, Status> {
        match self.abort_prepublication_change(change).await {
            Ok(()) => Err(original),
            Err(cleanup) => Err(Status::internal(format!(
                "migration failed ({original}); cleanup remains pending ({cleanup})"
            ))),
        }
    }

    async fn copy_range(
        &self,
        change: &TopologyChange,
        range: &RangeMigration,
        deadline: Instant,
    ) -> Result<RangeMigration, Status> {
        let mut source = connect_node(&range.source_endpoint, deadline).await?;
        let mut destination = connect_node(&range.destination_endpoint, deadline).await?;
        let spec = proto::RangeSpec {
            change_id: change.change_id.clone(),
            range_id: range.range_id.clone(),
            start_exclusive: range.start_exclusive,
            end_inclusive: range.end_inclusive,
            source_node_id: range.source_node_id.clone(),
            destination_node_id: range.destination_node_id.clone(),
        };
        let source_process_instance_id = rpc_before(
            deadline,
            source.prepare_source_range(PrepareRangeRequest {
                range: Some(spec.clone()),
            }),
        )
        .await?
        .into_inner()
        .process_instance_id;
        if source_process_instance_id.is_empty() {
            return Err(Status::data_loss(
                "source omitted its process instance identifier",
            ));
        }
        let destination_process_instance_id = rpc_before(
            deadline,
            destination.prepare_destination_range(PrepareRangeRequest { range: Some(spec) }),
        )
        .await?
        .into_inner()
        .process_instance_id;
        if destination_process_instance_id.is_empty() {
            return Err(Status::data_loss(
                "destination omitted its process instance identifier",
            ));
        }

        let mut cursor = 0;
        let mut snapshot_records = 0;
        loop {
            let page = rpc_before(
                deadline,
                source.read_snapshot_page(SnapshotPageRequest {
                    change_id: change.change_id.clone(),
                    range_id: range.range_id.clone(),
                    cursor,
                    max_bytes: MAX_MIGRATION_PAGE_BYTES as u64,
                }),
            )
            .await?
            .into_inner();
            snapshot_records += page.records.len() as u64;
            if !page.records.is_empty() {
                rpc_before(
                    deadline,
                    destination.apply_migration_batch(ApplyMigrationBatchRequest {
                        change_id: change.change_id.clone(),
                        range_id: range.range_id.clone(),
                        snapshot_records: page.records,
                        journal_records: Vec::new(),
                    }),
                )
                .await?;
            }
            cursor = page.next_cursor;
            if page.done {
                break;
            }
        }

        let watermark = replay_changelog(
            &mut source,
            &mut destination,
            change,
            range,
            0,
            None,
            deadline,
        )
        .await?;
        let mut progress = range.clone();
        progress.source_process_instance_id = source_process_instance_id;
        progress.destination_process_instance_id = destination_process_instance_id;
        progress.snapshot_records = snapshot_records;
        progress.changelog_watermark = watermark;
        Ok(progress)
    }

    async fn verify_destination_instances(
        &self,
        change: &TopologyChange,
        deadline: Instant,
    ) -> Result<(), Status> {
        let destinations = destination_instances(change)?;
        for (node_id, (endpoint, expected_instance)) in destinations {
            let mut client = connect_node(&endpoint, deadline).await?;
            let actual = rpc_before(deadline, client.get_process_info(proto::Empty {}))
                .await?
                .into_inner();
            if actual.node_id != node_id || actual.process_instance_id != expected_instance {
                return Err(Status::failed_precondition(format!(
                    "destination process changed before publication: {node_id}"
                )));
            }
        }
        Ok(())
    }

    async fn pause_range(
        &self,
        change: &TopologyChange,
        range: &RangeMigration,
        deadline: Instant,
    ) -> Result<u64, Status> {
        let mut source = connect_node(&range.source_endpoint, deadline).await?;
        Ok(rpc_before(
            deadline,
            source.pause_range_writes(RangeControlRequest {
                change_id: change.change_id.clone(),
                range_id: range.range_id.clone(),
            }),
        )
        .await?
        .into_inner()
        .final_watermark)
    }

    async fn finalize_range(
        &self,
        change: &TopologyChange,
        range: &RangeMigration,
        final_watermark: u64,
        deadline: Instant,
    ) -> Result<RangeMigration, Status> {
        let mut source = connect_node(&range.source_endpoint, deadline).await?;
        let mut destination = connect_node(&range.destination_endpoint, deadline).await?;
        let watermark = replay_changelog(
            &mut source,
            &mut destination,
            change,
            range,
            range.changelog_watermark,
            Some(final_watermark),
            deadline,
        )
        .await?;
        let control = RangeControlRequest {
            change_id: change.change_id.clone(),
            range_id: range.range_id.clone(),
        };
        let source_digest = rpc_before(deadline, source.source_range_digest(control.clone()))
            .await?
            .into_inner();
        let destination_digest = rpc_before(
            deadline,
            destination.destination_range_digest(control.clone()),
        )
        .await?
        .into_inner();
        if source_digest.record_count != destination_digest.record_count
            || source_digest.digest != destination_digest.digest
            || source_digest.changelog_watermark != destination_digest.changelog_watermark
            || destination_digest.changelog_watermark != final_watermark
            || watermark != final_watermark
        {
            return Err(Status::data_loss(format!(
                "range verification failed for {}",
                range.range_id
            )));
        }
        rpc_before(deadline, destination.commit_destination_range(control)).await?;

        let mut progress = range.clone();
        progress.changelog_watermark = final_watermark;
        progress.verified = true;
        Ok(progress)
    }

    async fn finish_published_change(
        &self,
        mut change: TopologyChange,
        deadline: Instant,
    ) -> Result<TopologyChange, Status> {
        let destination_ids: BTreeSet<_> = change
            .ranges
            .iter()
            .map(|range| range.destination_node_id.as_str())
            .collect();
        let destinations: Vec<_> = change
            .target_topology
            .members
            .iter()
            .filter(|member| destination_ids.contains(member.node_id.as_str()))
            .cloned()
            .collect();
        let other_targets: Vec<_> = change
            .target_topology
            .members
            .iter()
            .filter(|member| !destination_ids.contains(member.node_id.as_str()))
            .cloned()
            .collect();
        self.install_on_members(&change.target_topology, &destinations, deadline)
            .await?;
        self.install_on_members(&change.target_topology, &other_targets, deadline)
            .await?;
        let target_ids: BTreeSet<String> = change
            .target_topology
            .members
            .iter()
            .map(|member| member.node_id.clone())
            .collect();
        let stopping_or_stopped: BTreeSet<_> = change
            .stopping_node_ids
            .iter()
            .chain(&change.stop_prepared_node_ids)
            .chain(&change.stopped_node_ids)
            .map(String::as_str)
            .collect();
        let removed_members: Vec<_> = change
            .ranges
            .iter()
            .filter(|range| {
                !target_ids.contains(&range.source_node_id)
                    && !stopping_or_stopped.contains(range.source_node_id.as_str())
            })
            .map(|range| (range.source_node_id.clone(), range.source_endpoint.clone()))
            .collect::<BTreeMap<_, _>>()
            .into_iter()
            .map(|(node_id, endpoint)| Member { node_id, endpoint })
            .collect();
        self.install_on_members(&change.target_topology, &removed_members, deadline)
            .await?;
        let change_id = &change.change_id;
        try_map_bounded(
            change.ranges.clone(),
            self.range_move_concurrency,
            |range| async move {
                let mut destination = connect_node(&range.destination_endpoint, deadline).await?;
                rpc_before(
                    deadline,
                    destination.activate_destination_range(RangeControlRequest {
                        change_id: change_id.clone(),
                        range_id: range.range_id,
                    }),
                )
                .await?;
                Ok::<(), Status>(())
            },
        )
        .await?;
        self.set_phase(MigrationPhase::CleaningUp).await?;
        change.phase = MigrationPhase::CleaningUp;
        let ranges_to_clean: Vec<_> = change
            .ranges
            .iter()
            .filter(|range| {
                !range.source_cleaned
                    && !change.stopping_node_ids.contains(&range.source_node_id)
                    && !change
                        .stop_prepared_node_ids
                        .contains(&range.source_node_id)
                    && !change.stopped_node_ids.contains(&range.source_node_id)
            })
            .cloned()
            .collect();
        let change_id = &change.change_id;
        let cleaned = try_map_bounded(
            ranges_to_clean,
            self.range_move_concurrency,
            |mut range| async move {
                let mut source = connect_node(&range.source_endpoint, deadline).await?;
                rpc_before(
                    deadline,
                    source.cleanup_source_range(RangeControlRequest {
                        change_id: change_id.clone(),
                        range_id: range.range_id.clone(),
                    }),
                )
                .await?;
                range.source_cleaned = true;
                self.store_range_progress(&range).await?;
                Ok::<RangeMigration, Status>(range)
            },
        )
        .await?;
        for progress in cleaned {
            replace_range_progress(&mut change, progress);
        }
        let mut removed_sources: BTreeMap<String, (String, String)> = BTreeMap::new();
        for range in change
            .ranges
            .iter()
            .filter(|range| !target_ids.contains(&range.source_node_id))
        {
            if range.source_process_instance_id.is_empty() {
                return Err(Status::data_loss(format!(
                    "missing process instance for removed node {}",
                    range.source_node_id
                )));
            }
            let value = (
                range.source_endpoint.clone(),
                range.source_process_instance_id.clone(),
            );
            if removed_sources
                .insert(range.source_node_id.clone(), value.clone())
                .is_some_and(|previous| previous != value)
            {
                return Err(Status::data_loss(format!(
                    "removed node {} changed process instance during migration",
                    range.source_node_id
                )));
            }
        }
        for (node_id, (endpoint, process_instance_id)) in removed_sources {
            if change.stopped_node_ids.contains(&node_id) {
                continue;
            }
            if !change.stopping_node_ids.contains(&node_id)
                && !change.stop_prepared_node_ids.contains(&node_id)
            {
                self.store_stopping_node(&node_id).await?;
                change.stopping_node_ids.push(node_id.clone());
            }
            if !change.stop_prepared_node_ids.contains(&node_id) {
                let mut client = connect_node(&endpoint, deadline).await?;
                let actual = rpc_before(deadline, client.get_process_info(proto::Empty {}))
                    .await?
                    .into_inner();
                if actual.node_id != node_id || actual.process_instance_id != process_instance_id {
                    self.store_stopped_node(&node_id).await?;
                    change
                        .stopping_node_ids
                        .retain(|stopping| stopping != &node_id);
                    change.stopped_node_ids.push(node_id);
                    continue;
                }
                rpc_before(
                    deadline,
                    client.prepare_stop(StopRequest {
                        node_id: node_id.clone(),
                        process_instance_id: process_instance_id.clone(),
                    }),
                )
                .await?;
                self.store_stop_prepared_node(&node_id, &process_instance_id)
                    .await?;
                change
                    .stopping_node_ids
                    .retain(|stopping| stopping != &node_id);
                change.stop_prepared_node_ids.push(node_id.clone());
            }
            let mut client = match connect_node(&endpoint, deadline).await {
                Ok(client) => client,
                Err(error) if is_absence_status(&error) => {
                    self.store_stopped_node(&node_id).await?;
                    change
                        .stop_prepared_node_ids
                        .retain(|prepared| prepared != &node_id);
                    change.stopped_node_ids.push(node_id);
                    continue;
                }
                Err(error) => return Err(error),
            };
            let actual = match rpc_before(deadline, client.get_process_info(proto::Empty {})).await
            {
                Ok(response) => response.into_inner(),
                Err(error) if is_absence_status(&error) => {
                    self.store_stopped_node(&node_id).await?;
                    change
                        .stop_prepared_node_ids
                        .retain(|prepared| prepared != &node_id);
                    change.stopped_node_ids.push(node_id);
                    continue;
                }
                Err(error) => return Err(error),
            };
            if actual.node_id == node_id && actual.process_instance_id == process_instance_id {
                match rpc_before(
                    deadline,
                    client.stop(StopRequest {
                        node_id: node_id.clone(),
                        process_instance_id,
                    }),
                )
                .await
                {
                    Ok(_) => {}
                    Err(error) if is_absence_status(&error) => {}
                    Err(error) => return Err(error),
                }
            }
            self.store_stopped_node(&node_id).await?;
            change
                .stop_prepared_node_ids
                .retain(|prepared| prepared != &node_id);
            change.stopped_node_ids.push(node_id);
        }
        self.set_phase(MigrationPhase::Complete).await?;
        change.phase = MigrationPhase::Complete;
        Ok(change)
    }

    async fn install_on_members(
        &self,
        topology: &TopologySnapshot,
        members: &[Member],
        deadline: Instant,
    ) -> Result<(), Status> {
        for member in members {
            let mut client = connect_node(&member.endpoint, deadline).await?;
            rpc_before(
                deadline,
                client.install_topology(InstallTopologyRequest {
                    topology: Some(topology.into()),
                }),
            )
            .await?;
        }
        Ok(())
    }

    async fn reset_prepublication_attempt(
        &self,
        change: &TopologyChange,
    ) -> Result<TopologyChange, Status> {
        self.set_phase(MigrationPhase::Resetting).await?;
        let deadline = Instant::now() + self.migration_timeout;
        self.clear_prepublication_nodes(change, deadline).await?;
        let mut state = self.state.write().await;
        let mut next = state.clone();
        let active = next
            .active_change
            .as_mut()
            .ok_or_else(|| Status::internal("active change disappeared"))?;
        for range in &mut active.ranges {
            range.source_process_instance_id.clear();
            range.destination_process_instance_id.clear();
            range.source_cleaned = false;
            range.snapshot_records = 0;
            range.changelog_watermark = 0;
            range.verified = false;
        }
        active.phase = MigrationPhase::CopyingSnapshot;
        self.repository
            .store_state(&next)
            .map_err(|error| Status::internal(error.to_string()))?;
        let reset = next
            .active_change
            .clone()
            .expect("active change was checked above");
        *state = next;
        Ok(reset)
    }

    async fn clear_prepublication_nodes(
        &self,
        change: &TopologyChange,
        deadline: Instant,
    ) -> Result<(), Status> {
        let change_id = &change.change_id;
        try_map_bounded(
            change.ranges.clone(),
            self.range_move_concurrency,
            |range| async move {
                let control = RangeControlRequest {
                    change_id: change_id.clone(),
                    range_id: range.range_id,
                };
                let mut source = connect_node(&range.source_endpoint, deadline).await?;
                rpc_before(deadline, source.abort_range_migration(control.clone())).await?;
                // Destination staging is never client-visible before publication,
                // so inability to discard it is not an ownership safety failure.
                if let Ok(mut destination) =
                    connect_node(&range.destination_endpoint, deadline).await
                {
                    let _ = rpc_before(deadline, destination.abort_range_migration(control)).await;
                }
                Ok::<(), Status>(())
            },
        )
        .await
        .map(|_| ())
    }

    async fn abort_prepublication_change(&self, change: &TopologyChange) -> Result<(), Status> {
        self.set_phase(MigrationPhase::Aborting).await?;
        self.finish_abort(change).await
    }

    async fn finish_abort(&self, change: &TopologyChange) -> Result<(), Status> {
        let deadline = Instant::now() + self.migration_timeout;
        self.clear_prepublication_nodes(change, deadline).await?;
        self.set_phase(MigrationPhase::Aborted).await
    }

    async fn store_stopped_node(&self, node_id: &str) -> Result<(), Status> {
        let mut state = self.state.write().await;
        let mut next = state.clone();
        let change = next
            .active_change
            .as_mut()
            .ok_or_else(|| Status::internal("active change disappeared"))?;
        change
            .stopping_node_ids
            .retain(|stopping| stopping != node_id);
        change
            .stop_prepared_node_ids
            .retain(|prepared| prepared != node_id);
        if !change
            .stopped_node_ids
            .iter()
            .any(|stopped| stopped == node_id)
        {
            change.stopped_node_ids.push(node_id.to_owned());
            change.stopped_node_ids.sort();
        }
        next.process_instances.remove(node_id);
        self.repository
            .store_state(&next)
            .map_err(|error| Status::internal(error.to_string()))?;
        *state = next;
        Ok(())
    }

    async fn store_stopping_node(&self, node_id: &str) -> Result<(), Status> {
        let mut state = self.state.write().await;
        let mut next = state.clone();
        let change = next
            .active_change
            .as_mut()
            .ok_or_else(|| Status::internal("active change disappeared"))?;
        if !change
            .stopping_node_ids
            .iter()
            .any(|stopping| stopping == node_id)
        {
            change.stopping_node_ids.push(node_id.to_owned());
            change.stopping_node_ids.sort();
            self.repository
                .store_state(&next)
                .map_err(|error| Status::internal(error.to_string()))?;
            *state = next;
        }
        Ok(())
    }

    async fn store_stop_prepared_node(
        &self,
        node_id: &str,
        process_instance_id: &str,
    ) -> Result<(), Status> {
        let mut state = self.state.write().await;
        let mut next = state.clone();
        let change = next
            .active_change
            .as_mut()
            .ok_or_else(|| Status::internal("active change disappeared"))?;
        change
            .stopping_node_ids
            .retain(|stopping| stopping != node_id);
        if !change
            .stop_prepared_node_ids
            .iter()
            .any(|prepared| prepared == node_id)
        {
            change.stop_prepared_node_ids.push(node_id.to_owned());
            change.stop_prepared_node_ids.sort();
        }
        next.stop_confirmations
            .entry(node_id.to_owned())
            .or_default()
            .insert(process_instance_id.to_owned());
        self.repository
            .store_state(&next)
            .map_err(|error| Status::internal(error.to_string()))?;
        *state = next;
        Ok(())
    }

    async fn set_phase(&self, phase: MigrationPhase) -> Result<(), Status> {
        let mut state = self.state.write().await;
        let mut next = state.clone();
        next.active_change
            .as_mut()
            .ok_or_else(|| Status::internal("active change disappeared"))?
            .phase = phase;
        self.repository
            .store_state(&next)
            .map_err(|error| Status::internal(error.to_string()))?;
        *state = next;
        Ok(())
    }

    async fn store_range_progress(&self, progress: &RangeMigration) -> Result<(), Status> {
        let mut state = self.state.write().await;
        let mut next = state.clone();
        let range = next
            .active_change
            .as_mut()
            .and_then(|change| {
                change
                    .ranges
                    .iter_mut()
                    .find(|range| range.range_id == progress.range_id)
            })
            .ok_or_else(|| Status::internal("migration range disappeared"))?;
        *range = progress.clone();
        self.repository
            .store_state(&next)
            .map_err(|error| Status::internal(error.to_string()))?;
        *state = next;
        Ok(())
    }
}

#[tonic::async_trait]
impl Coordinator for CoordinatorService {
    async fn get_topology(
        &self,
        _request: Request<proto::Empty>,
    ) -> Result<Response<proto::TopologySnapshot>, Status> {
        let state = self.state.read().await;
        Ok(Response::new((&state.committed).into()))
    }

    async fn get_current_epoch(
        &self,
        _request: Request<proto::Empty>,
    ) -> Result<Response<proto::CurrentEpochResponse>, Status> {
        let epoch = self.state.read().await.committed.epoch;
        Ok(Response::new(proto::CurrentEpochResponse { epoch }))
    }

    async fn begin_topology_change(
        &self,
        request: Request<proto::BeginTopologyChangeRequest>,
    ) -> Result<Response<proto::TopologyChangeSnapshot>, Status> {
        let target_members = request
            .into_inner()
            .target_members
            .into_iter()
            .map(|member| Member {
                node_id: member.node_id,
                endpoint: member.endpoint,
            })
            .collect();
        let mut state = self.state.write().await;
        if state
            .active_change
            .as_ref()
            .is_some_and(|change| !change.phase.is_terminal())
        {
            return Err(Status::failed_precondition(
                "another topology change is already active",
            ));
        }
        let change = TopologyChange::plan(&state.committed, target_members)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let mut next = state.clone();
        next.active_change = Some(change.clone());
        self.repository
            .store_state(&next)
            .map_err(|error| Status::internal(error.to_string()))?;
        *state = next;
        Ok(Response::new((&change).into()))
    }

    async fn get_topology_change(
        &self,
        _request: Request<proto::Empty>,
    ) -> Result<Response<proto::GetTopologyChangeResponse>, Status> {
        let state = self.state.read().await;
        Ok(Response::new(proto::GetTopologyChangeResponse {
            change: state.active_change.as_ref().map(Into::into),
        }))
    }

    async fn execute_topology_change(
        &self,
        request: Request<proto::ExecuteTopologyChangeRequest>,
    ) -> Result<Response<proto::TopologyChangeSnapshot>, Status> {
        let request = request.into_inner();
        if request.change_id.is_empty() {
            return Err(Status::invalid_argument("change_id must not be empty"));
        }
        let expected = ChangeIdentity {
            change_id: request.change_id,
            base_epoch: request.base_epoch,
            target_epoch: request.target_epoch,
        };
        // Keep migration recovery running if the requesting client disconnects or
        // reaches its own deadline after the coordinator accepted the command.
        let service = self.clone();
        let change = tokio::spawn(async move { service.execute_change(expected).await })
            .await
            .map_err(|error| Status::internal(format!("migration task failed: {error}")))??;
        Ok(Response::new((&change).into()))
    }

    async fn register_node(
        &self,
        request: Request<proto::RegisterNodeRequest>,
    ) -> Result<Response<proto::Empty>, Status> {
        let request = request.into_inner();
        if request.node_id.is_empty() || request.process_instance_id.is_empty() {
            return Err(Status::invalid_argument(
                "node_id and process_instance_id must not be empty",
            ));
        }
        let mut state = self.state.write().await;
        let is_committed = state
            .committed
            .members
            .iter()
            .any(|member| member.node_id == request.node_id);
        let is_pending = state.active_change.as_ref().is_some_and(|change| {
            !change.phase.is_terminal()
                && change
                    .target_topology
                    .members
                    .iter()
                    .any(|member| member.node_id == request.node_id)
        });
        if !is_committed && !is_pending {
            return Err(Status::failed_precondition(
                "node is not present in committed or pending membership",
            ));
        }
        if let Some(existing) = state.process_instances.get(&request.node_id) {
            if existing == &request.process_instance_id {
                return Ok(Response::new(proto::Empty {}));
            }
            if is_committed {
                return Err(Status::failed_precondition(format!(
                    "committed node {} is fenced to another process instance",
                    request.node_id
                )));
            }
        }
        let mut next = state.clone();
        next.process_instances
            .insert(request.node_id, request.process_instance_id);
        self.repository
            .store_state(&next)
            .map_err(|error| Status::internal(error.to_string()))?;
        *state = next;
        Ok(Response::new(proto::Empty {}))
    }

    async fn is_stop_confirmed(
        &self,
        request: Request<proto::StopRequest>,
    ) -> Result<Response<proto::StopConfirmationResponse>, Status> {
        let request = request.into_inner();
        if request.node_id.is_empty() || request.process_instance_id.is_empty() {
            return Err(Status::invalid_argument(
                "node_id and process_instance_id must not be empty",
            ));
        }
        let state = self.state.read().await;
        let confirmed = state
            .stop_confirmations
            .get(&request.node_id)
            .is_some_and(|instances| instances.contains(&request.process_instance_id));
        Ok(Response::new(proto::StopConfirmationResponse { confirmed }))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ChangeIdentity {
    change_id: String,
    base_epoch: u64,
    target_epoch: u64,
}

fn change_identity(change: &TopologyChange) -> ChangeIdentity {
    ChangeIdentity {
        change_id: change.change_id.clone(),
        base_epoch: change.base_epoch,
        target_epoch: change.target_topology.epoch,
    }
}

fn destination_instances(
    change: &TopologyChange,
) -> Result<BTreeMap<String, (String, String)>, Status> {
    let mut destinations = BTreeMap::new();
    for range in &change.ranges {
        if range.destination_process_instance_id.is_empty() {
            return Err(Status::data_loss(format!(
                "missing process instance for destination {}",
                range.destination_node_id
            )));
        }
        let identity = (
            range.destination_endpoint.clone(),
            range.destination_process_instance_id.clone(),
        );
        if destinations
            .insert(range.destination_node_id.clone(), identity.clone())
            .is_some_and(|previous| previous != identity)
        {
            return Err(Status::data_loss(format!(
                "destination {} changed process instance during migration",
                range.destination_node_id
            )));
        }
    }
    Ok(destinations)
}

fn replace_range_progress(change: &mut TopologyChange, progress: RangeMigration) {
    if let Some(current) = change
        .ranges
        .iter_mut()
        .find(|candidate| candidate.range_id == progress.range_id)
    {
        *current = progress;
    }
}

fn is_absence_status(status: &Status) -> bool {
    status.code() == tonic::Code::Unavailable
}

async fn try_map_bounded<I, F, Fut, T, E>(
    items: I,
    concurrency: usize,
    mut operation: F,
) -> Result<Vec<T>, E>
where
    I: IntoIterator,
    F: FnMut(I::Item) -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    let mut items = items.into_iter();
    let mut in_flight = FuturesUnordered::new();
    for item in items.by_ref().take(concurrency) {
        in_flight.push(operation(item));
    }

    // State-changing RPCs may continue remotely if their client futures are dropped.
    // After the first error, stop launching work but drain every operation already started
    // before the caller begins abort cleanup.
    let mut values = Vec::new();
    let mut first_error = None;
    while let Some(result) = in_flight.next().await {
        match result {
            Ok(value) => {
                values.push(value);
                if first_error.is_none()
                    && let Some(item) = items.next()
                {
                    in_flight.push(operation(item));
                }
            }
            Err(error) if first_error.is_none() => first_error = Some(error),
            Err(_) => {}
        }
    }

    match first_error {
        Some(error) => Err(error),
        None => Ok(values),
    }
}

async fn replay_changelog(
    source: &mut DataNodeClient<tonic::transport::Channel>,
    destination: &mut DataNodeClient<tonic::transport::Channel>,
    change: &TopologyChange,
    range: &RangeMigration,
    mut watermark: u64,
    final_watermark: Option<u64>,
    deadline: Instant,
) -> Result<u64, Status> {
    loop {
        let page = rpc_before(
            deadline,
            source.read_changelog_page(ChangelogPageRequest {
                change_id: change.change_id.clone(),
                range_id: range.range_id.clone(),
                after_watermark: watermark,
                max_bytes: MAX_MIGRATION_PAGE_BYTES as u64,
            }),
        )
        .await?
        .into_inner();
        let target = final_watermark.unwrap_or(page.current_watermark);
        let page_is_empty = page.records.is_empty();
        if let Some(last) = page.records.last() {
            watermark = last.watermark;
        }
        if !page_is_empty {
            rpc_before(
                deadline,
                destination.apply_migration_batch(ApplyMigrationBatchRequest {
                    change_id: change.change_id.clone(),
                    range_id: range.range_id.clone(),
                    snapshot_records: Vec::new(),
                    journal_records: page.records,
                }),
            )
            .await?;
        }
        if watermark >= target {
            return Ok(watermark);
        }
        if page_is_empty && watermark < target {
            return Err(Status::data_loss(
                "source changelog omitted records before its watermark",
            ));
        }
    }
}

async fn connect_node(
    endpoint: &str,
    deadline: Instant,
) -> Result<DataNodeClient<tonic::transport::Channel>, Status> {
    let remaining = deadline
        .checked_duration_since(Instant::now())
        .ok_or_else(|| Status::deadline_exceeded("migration deadline exceeded"))?;
    let client = tokio::time::timeout(remaining, DataNodeClient::connect(endpoint.to_owned()))
        .await
        .map_err(|_| Status::deadline_exceeded("migration deadline exceeded"))?
        .map_err(|error| Status::unavailable(error.to_string()))?;
    Ok(client
        .max_decoding_message_size(MAX_CONTROL_MESSAGE_BYTES)
        .max_encoding_message_size(MAX_CONTROL_MESSAGE_BYTES))
}

async fn rpc_before<T, F>(deadline: Instant, future: F) -> Result<Response<T>, Status>
where
    F: Future<Output = Result<Response<T>, Status>>,
{
    let remaining = deadline
        .checked_duration_since(Instant::now())
        .ok_or_else(|| Status::deadline_exceeded("migration deadline exceeded"))?;
    tokio::time::timeout(remaining, future)
        .await
        .map_err(|_| Status::deadline_exceeded("migration deadline exceeded"))?
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    use super::*;
    use hashring_core::topology::Member;
    use tokio::sync::Barrier;

    struct ActiveWork(Arc<AtomicUsize>);

    impl Drop for ActiveWork {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }

    #[derive(Default)]
    struct MemoryRepository(Mutex<Option<ClusterState>>);

    impl CoordinatorRepository for MemoryRepository {
        fn load_state(&self) -> Result<Option<ClusterState>, RepositoryError> {
            Ok(self.0.lock().unwrap().clone())
        }

        fn store_state(&self, state: &ClusterState) -> Result<(), RepositoryError> {
            *self.0.lock().unwrap() = Some(state.clone());
            Ok(())
        }
    }

    fn topology() -> TopologySnapshot {
        TopologySnapshot::new(
            1,
            7,
            8,
            vec![Member {
                node_id: "n1".into(),
                endpoint: "http://127.0.0.1:5001".into(),
            }],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn bounded_range_work_overlaps_and_honors_limit() {
        let running = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let first_batch = Arc::new(Barrier::new(4));
        let work = try_map_bounded(0..6, 3, |item| {
            let running = running.clone();
            let peak = peak.clone();
            let first_batch = first_batch.clone();
            async move {
                let active = running.fetch_add(1, Ordering::SeqCst) + 1;
                let _guard = ActiveWork(running);
                peak.fetch_max(active, Ordering::SeqCst);
                if item < 3 {
                    first_batch.wait().await;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
                Ok::<_, ()>(item)
            }
        });

        let (_, result) = tokio::join!(first_batch.wait(), work);
        let mut result = result.unwrap();
        result.sort_unstable();
        assert_eq!(result, (0..6).collect::<Vec<_>>());
        assert_eq!(peak.load(Ordering::SeqCst), 3);
        assert_eq!(running.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn bounded_range_work_drains_started_siblings_after_an_error() {
        let sibling_completed = Arc::new(AtomicUsize::new(0));
        let unscheduled_started = Arc::new(AtomicUsize::new(0));
        let work = try_map_bounded(0..3, 2, |item| {
            let sibling_completed = sibling_completed.clone();
            let unscheduled_started = unscheduled_started.clone();
            async move {
                match item {
                    0 => Err("range failed"),
                    1 => {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                        sibling_completed.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    }
                    _ => {
                        unscheduled_started.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    }
                }
            }
        });

        let result = work.await;
        assert_eq!(result, Err("range failed"));
        assert_eq!(sibling_completed.load(Ordering::SeqCst), 1);
        assert_eq!(unscheduled_started.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn initializes_once_then_loads_durable_value() {
        let repository = MemoryRepository::default();
        let expected = topology();
        assert_eq!(
            load_or_initialize(&repository, Some(expected.clone()))
                .unwrap()
                .committed,
            expected
        );
        assert_eq!(
            load_or_initialize(&repository, None).unwrap().committed,
            expected
        );
    }

    #[test]
    fn upgrades_prior_v1_topology_change_shape() {
        let committed = topology();
        let change = TopologyChange::plan(
            &committed,
            vec![
                committed.members[0].clone(),
                Member {
                    node_id: "n2".into(),
                    endpoint: "http://127.0.0.1:5002".into(),
                },
            ],
        )
        .unwrap();
        let state = ClusterState {
            committed,
            active_change: Some(change),
            process_instances: BTreeMap::new(),
            stop_confirmations: BTreeMap::new(),
        };
        let mut old = serde_json::to_value(state).unwrap();
        old.as_object_mut().unwrap().remove("process_instances");
        old.as_object_mut().unwrap().remove("stop_confirmations");
        let active = old["active_change"].as_object_mut().unwrap();
        active.remove("stopped_node_ids");
        active.remove("stopping_node_ids");
        active.remove("stop_prepared_node_ids");
        for range in active["ranges"].as_array_mut().unwrap() {
            let range = range.as_object_mut().unwrap();
            range.remove("source_endpoint");
            range.remove("destination_endpoint");
            range.remove("source_process_instance_id");
            range.remove("destination_process_instance_id");
            range.remove("source_cleaned");
        }

        let mut restored: ClusterState = serde_json::from_value(old).unwrap();
        upgrade_persisted_state(&mut restored).unwrap();
        let restored = restored.active_change.unwrap();
        assert!(restored.ranges.iter().all(
            |range| !range.source_endpoint.is_empty() && !range.destination_endpoint.is_empty()
        ));
        assert!(restored.stopped_node_ids.is_empty());
        assert!(restored.stopping_node_ids.is_empty());
        assert!(restored.stop_prepared_node_ids.is_empty());
    }

    #[tokio::test]
    async fn durable_stop_confirmation_survives_active_change_replacement() {
        let committed = topology();
        let change = TopologyChange::plan(
            &committed,
            vec![Member {
                node_id: "n2".into(),
                endpoint: "http://127.0.0.1:5002".into(),
            }],
        )
        .unwrap();
        let repository = Arc::new(MemoryRepository::default());
        let service = CoordinatorService::new(
            ClusterState {
                committed,
                active_change: Some(change),
                process_instances: BTreeMap::from([("n1".into(), "process-1".into())]),
                stop_confirmations: BTreeMap::new(),
            },
            repository,
            Duration::from_secs(1),
        );

        service
            .store_stop_prepared_node("n1", "process-1")
            .await
            .unwrap();
        service.store_stopped_node("n1").await.unwrap();
        {
            let mut state = service.state.write().await;
            state.active_change = None;
        }

        let response = service
            .is_stop_confirmed(Request::new(proto::StopRequest {
                node_id: "n1".into(),
                process_instance_id: "process-1".into(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(response.confirmed);
        let replacement = service
            .is_stop_confirmed(Request::new(proto::StopRequest {
                node_id: "n1".into(),
                process_instance_id: "process-2".into(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(!replacement.confirmed);
    }
}
