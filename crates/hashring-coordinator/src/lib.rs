// Public tonic and redb APIs use concrete error types whose context is more
// useful here than erasing or boxing them solely to reduce enum size.
#![allow(clippy::result_large_err)]

use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    path::Path,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use futures::{StreamExt, stream::FuturesUnordered};
use redb::{Database, TableDefinition};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::{
    sync::{Mutex, RwLock, watch},
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
        self, ApplyDedupBatchRequest, ApplyMigrationBatchRequest, ChangelogPageRequest,
        InstallTopologyRequest, PolicyWriteFenceRequest, PrepareRangeRequest, RangeControlRequest,
        SnapshotPageRequest, StopRequest, coordinator_server::Coordinator,
        data_node_client::DataNodeClient,
    },
    topology::{Member, TopologySnapshot, WriteAckPolicy},
};

const TOPOLOGY_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("topology");
const COMMITTED_KEY: &str = "committed";
const CLUSTER_STATE_KEY: &str = "cluster-state-v1";
pub const DEFAULT_RANGE_MOVE_CONCURRENCY: usize = 16;
pub const NODE_LEASE_DURATION: Duration = Duration::from_secs(5);

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterState {
    pub committed: TopologySnapshot,
    pub active_change: Option<TopologyChange>,
    #[serde(default)]
    pub process_instances: BTreeMap<String, String>,
    #[serde(default)]
    pub stop_confirmations: BTreeMap<String, BTreeSet<String>>,
    #[serde(default)]
    pub replica_admissions: Vec<ReplicaAdmission>,
    #[serde(default)]
    pub replica_repairs: Vec<ReplicaRepair>,
    #[serde(default)]
    pub fenced_nodes: BTreeSet<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReplicaAdmission {
    pub epoch: u64,
    pub start_exclusive: u64,
    pub end_inclusive: u64,
    pub owner_node_id: String,
    pub node_id: String,
    pub process_instance_id: String,
    pub verified_watermark: u64,
    pub stream_cursor: u64,
    pub digest: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ReplicaRepairPhase {
    Pending,
    Copying,
    Verifying,
    Complete,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReplicaRepair {
    pub epoch: u64,
    pub start_exclusive: u64,
    pub end_inclusive: u64,
    pub owner_node_id: String,
    pub node_id: String,
    pub phase: ReplicaRepairPhase,
}

struct PreparedReplicaSeed {
    task: ReplicaRepair,
    control: RangeControlRequest,
    admission: ReplicaAdmission,
}

struct CopiedReplicaSeed {
    task: ReplicaRepair,
    change: TopologyChange,
    range: RangeMigration,
    control: RangeControlRequest,
    follower_instance: String,
}

fn replica_repair_control(task: &ReplicaRepair) -> RangeControlRequest {
    let mut id = blake3::Hasher::new();
    id.update(b"hashring-rs:replica-repair:v1\0");
    id.update(&task.epoch.to_be_bytes());
    id.update(&task.start_exclusive.to_be_bytes());
    id.update(&task.end_inclusive.to_be_bytes());
    id.update(task.owner_node_id.as_bytes());
    id.update(task.node_id.as_bytes());
    let change_id = format!("repair-{}", id.finalize().to_hex());
    RangeControlRequest {
        range_id: change_id.clone(),
        change_id,
    }
}

impl ClusterState {
    fn validate(&self) -> Result<(), RepositoryError> {
        self.committed.validate()?;
        if self.replica_admissions.len() > MAX_REPLICA_OBLIGATIONS
            || self.replica_repairs.len() > MAX_REPLICA_OBLIGATIONS
        {
            return Err(RepositoryError::InvalidState(format!(
                "replica metadata exceeds the {MAX_REPLICA_OBLIGATIONS} task limit"
            )));
        }
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
        if self.fenced_nodes.iter().any(String::is_empty) {
            return Err(RepositoryError::InvalidState(
                "fenced node identities must not be empty".into(),
            ));
        }
        if self.stop_confirmations.iter().any(|(node_id, instances)| {
            node_id.is_empty() || instances.is_empty() || instances.iter().any(String::is_empty)
        }) {
            return Err(RepositoryError::InvalidState(
                "durable stop confirmations must identify a node and process instance".into(),
            ));
        }
        let desired: BTreeSet<_> = self
            .committed
            .derived_ranges()?
            .into_iter()
            .flat_map(|range| {
                range.follower_node_ids.into_iter().map(move |node_id| {
                    (
                        range.start_exclusive,
                        range.end_inclusive,
                        range.owner_node_id.clone(),
                        node_id,
                    )
                })
            })
            .collect();
        let mut admissions = BTreeSet::new();
        for admission in &self.replica_admissions {
            let key = (
                admission.start_exclusive,
                admission.end_inclusive,
                admission.owner_node_id.clone(),
                admission.node_id.clone(),
            );
            if admission.epoch != self.committed.epoch
                || admission.process_instance_id.is_empty()
                || admission.digest.is_empty()
                || !desired.contains(&key)
                || !admissions.insert(key)
            {
                return Err(RepositoryError::InvalidState(
                    "invalid or duplicate replica admission".into(),
                ));
            }
        }
        let mut repairs = BTreeSet::new();
        for repair in &self.replica_repairs {
            let key = (
                repair.start_exclusive,
                repair.end_inclusive,
                repair.owner_node_id.clone(),
                repair.node_id.clone(),
            );
            if repair.epoch != self.committed.epoch
                || !desired.contains(&key)
                || !repairs.insert(key.clone())
                || (repair.phase == ReplicaRepairPhase::Complete && !admissions.contains(&key))
            {
                return Err(RepositoryError::InvalidState(
                    "invalid or duplicate replica repair".into(),
                ));
            }
        }
        Ok(())
    }
}

fn reconcile_replica_repairs(state: &mut ClusterState) -> Result<(), RepositoryError> {
    let epoch = state.committed.epoch;
    let ranges = state.committed.derived_ranges()?;
    if ranges
        .iter()
        .map(|range| range.follower_node_ids.len())
        .sum::<usize>()
        > MAX_REPLICA_OBLIGATIONS
    {
        return Err(RepositoryError::InvalidState(format!(
            "topology exceeds the {MAX_REPLICA_OBLIGATIONS} replica-task limit"
        )));
    }
    let desired: BTreeSet<_> = ranges
        .iter()
        .flat_map(|range| {
            range.follower_node_ids.iter().map(|node_id| {
                (
                    range.start_exclusive,
                    range.end_inclusive,
                    range.owner_node_id.clone(),
                    node_id.clone(),
                )
            })
        })
        .collect();
    state.replica_admissions.retain(|admission| {
        admission.epoch == epoch
            && state.process_instances.get(&admission.node_id)
                == Some(&admission.process_instance_id)
            && desired.contains(&(
                admission.start_exclusive,
                admission.end_inclusive,
                admission.owner_node_id.clone(),
                admission.node_id.clone(),
            ))
    });
    state.replica_repairs.retain(|repair| {
        repair.epoch == epoch
            && desired.contains(&(
                repair.start_exclusive,
                repair.end_inclusive,
                repair.owner_node_id.clone(),
                repair.node_id.clone(),
            ))
    });
    let admitted: BTreeSet<_> = state
        .replica_admissions
        .iter()
        .map(|admission| {
            (
                admission.start_exclusive,
                admission.end_inclusive,
                admission.owner_node_id.clone(),
                admission.node_id.clone(),
            )
        })
        .collect();
    for repair in &mut state.replica_repairs {
        if repair.phase == ReplicaRepairPhase::Complete
            && !admitted.contains(&(
                repair.start_exclusive,
                repair.end_inclusive,
                repair.owner_node_id.clone(),
                repair.node_id.clone(),
            ))
        {
            repair.phase = ReplicaRepairPhase::Pending;
        }
    }
    let mut existing: BTreeSet<_> = state
        .replica_repairs
        .iter()
        .map(|repair| {
            (
                repair.start_exclusive,
                repair.end_inclusive,
                repair.owner_node_id.clone(),
                repair.node_id.clone(),
            )
        })
        .collect();
    for range in ranges {
        for node_id in range.follower_node_ids {
            if existing.insert((
                range.start_exclusive,
                range.end_inclusive,
                range.owner_node_id.clone(),
                node_id.clone(),
            )) {
                state.replica_repairs.push(ReplicaRepair {
                    epoch,
                    start_exclusive: range.start_exclusive,
                    end_inclusive: range.end_inclusive,
                    owner_node_id: range.owner_node_id.clone(),
                    node_id,
                    phase: ReplicaRepairPhase::Pending,
                });
            }
        }
    }
    Ok(())
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
            reconcile_replica_repairs(&mut state)?;
            state.validate()?;
            return Ok(Some(state));
        }
        // Stores created by the first implementation contain only the committed
        // topology. Promote them in memory; the next state write upgrades them.
        if let Some(bytes) = table.get(COMMITTED_KEY)? {
            let committed: TopologySnapshot = serde_json::from_slice(bytes.value())?;
            let mut state = ClusterState {
                committed,
                active_change: None,
                process_instances: BTreeMap::new(),
                stop_confirmations: BTreeMap::new(),
                replica_admissions: Vec::new(),
                replica_repairs: Vec::new(),
                fenced_nodes: BTreeSet::new(),
            };
            reconcile_replica_repairs(&mut state)?;
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
    let mut state = ClusterState {
        committed: bootstrap,
        active_change: None,
        process_instances: BTreeMap::new(),
        stop_confirmations: BTreeMap::new(),
        replica_admissions: Vec::new(),
        replica_repairs: Vec::new(),
        fenced_nodes: BTreeSet::new(),
    };
    reconcile_replica_repairs(&mut state)?;
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
    lease_grants: Arc<Mutex<BTreeMap<String, NodeLeaseGrant>>>,
    peer_failures: Arc<Mutex<BTreeMap<(String, String), PeerFailure>>>,
    startup_at: Instant,
    repair_interrupt: Arc<watch::Sender<u64>>,
}

#[derive(Clone)]
struct NodeLeaseGrant {
    process_instance_id: String,
    epoch: u64,
    expires_at: Instant,
}

#[derive(Clone)]
struct PeerFailure {
    first_seen: Instant,
    last_seen: Instant,
}

impl CoordinatorService {
    pub fn new(
        state: ClusterState,
        repository: Arc<dyn CoordinatorRepository>,
        migration_timeout: Duration,
    ) -> Self {
        let (repair_interrupt, _) = watch::channel(0);
        Self {
            state: Arc::new(RwLock::new(state)),
            repository,
            execution_lock: Arc::new(Mutex::new(())),
            migration_timeout,
            range_move_concurrency: DEFAULT_RANGE_MOVE_CONCURRENCY,
            pre_publish_delay: Duration::ZERO,
            lease_grants: Arc::new(Mutex::new(BTreeMap::new())),
            peer_failures: Arc::new(Mutex::new(BTreeMap::new())),
            startup_at: Instant::now(),
            repair_interrupt: Arc::new(repair_interrupt),
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

    /// Run one failure-confirmation pass. A missed lease is sufficient evidence;
    /// otherwise two independent, continuously failing peer probes are needed.
    pub async fn run_failure_pass(&self) -> Result<(), Status> {
        if let Some(change) = self.state.read().await.active_change.clone()
            && !change.phase.is_terminal()
        {
            if change.failed_node_id.is_some() {
                self.execute_change(change_identity(&change), true).await?;
            }
            return Ok(());
        }
        let state = self.state.read().await.clone();
        if state.committed.members.len() <= 1 {
            return Ok(());
        }
        let now = Instant::now();
        if now < self.startup_at + NODE_LEASE_DURATION {
            return Ok(());
        }
        let grants = self.lease_grants.lock().await;
        let failures = self.peer_failures.lock().await;
        let candidate = state.committed.members.iter().find(|member| {
            if !state.process_instances.contains_key(&member.node_id)
                || state.fenced_nodes.contains(&member.node_id)
            {
                return false;
            }
            failure_confirmed(&state, &grants, &failures, &member.node_id, now)
        });
        let Some(failed_node_id) = candidate.map(|member| member.node_id.clone()) else {
            return Ok(());
        };
        drop(failures);
        drop(grants);
        let target_members = state
            .committed
            .members
            .iter()
            .filter(|member| member.node_id != failed_node_id)
            .cloned()
            .collect();
        let mut change = TopologyChange::plan(&state.committed, target_members)
            .map_err(|error| Status::failed_precondition(error.to_string()))?;
        change.failed_node_id = Some(failed_node_id.clone());
        let mut current = self.state.write().await;
        if current.committed != state.committed
            || current
                .active_change
                .as_ref()
                .is_some_and(|active| !active.phase.is_terminal())
        {
            return Ok(());
        }
        let mut next = current.clone();
        next.fenced_nodes.insert(failed_node_id);
        next.active_change = Some(change.clone());
        self.repository
            .store_state(&next)
            .map_err(|error| Status::internal(error.to_string()))?;
        *current = next;
        drop(current);
        self.repair_interrupt
            .send_modify(|generation| *generation += 1);
        self.execute_change(change_identity(&change), true).await?;
        Ok(())
    }

    /// Retry durable follower repairs when no topology transition is active.
    /// A failed task remains pending; the next call can safely start it again.
    pub async fn resume_replica_repairs(&self) -> Result<usize, Status> {
        let _execution = self.execution_lock.lock().await;
        let mut interrupt = self.repair_interrupt.subscribe();
        let tasks = {
            let mut state = self.state.write().await;
            if state
                .active_change
                .as_ref()
                .is_some_and(|change| !change.phase.is_terminal())
            {
                return Ok(0);
            }
            let mut next = state.clone();
            reconcile_replica_repairs(&mut next)
                .map_err(|error| Status::internal(error.to_string()))?;
            for repair in &mut next.replica_repairs {
                if repair.phase != ReplicaRepairPhase::Complete {
                    repair.phase = ReplicaRepairPhase::Pending;
                }
            }
            if next != *state {
                self.repository
                    .store_state(&next)
                    .map_err(|error| Status::internal(error.to_string()))?;
                *state = next;
            }
            let mut groups: BTreeMap<(String, String), Vec<ReplicaRepair>> = BTreeMap::new();
            for repair in &state.replica_repairs {
                if state.process_instances.contains_key(&repair.owner_node_id)
                    && state.process_instances.contains_key(&repair.node_id)
                {
                    groups
                        .entry((repair.owner_node_id.clone(), repair.node_id.clone()))
                        .or_default()
                        .push(repair.clone());
                }
            }
            groups.retain(|_, repairs| {
                repairs
                    .iter()
                    .any(|repair| repair.phase == ReplicaRepairPhase::Pending)
            });
            groups.into_values().collect::<Vec<_>>()
        };
        let work = try_map_bounded(
            tasks,
            self.range_move_concurrency.min(4),
            |tasks| async move {
                // Keep other ranges available if one destination is down. Failed
                // tasks retain durable Pending state for the next retry.
                match self.seed_replica_group(&tasks).await {
                    Ok(()) => Ok::<_, Status>(tasks.len()),
                    Err(error) => {
                        tracing::warn!(
                            owner = %tasks[0].owner_node_id,
                            follower = %tasks[0].node_id,
                            %error,
                            "replica seed remains pending"
                        );
                        Ok(0)
                    }
                }
            },
        );
        let results = tokio::select! {
            results = work => results?,
            changed = interrupt.changed() => {
                if changed.is_ok() {
                    self.cleanup_interrupted_repairs().await?;
                    return Ok(0);
                }
                return Err(Status::internal("repair interruption channel closed"));
            }
        };
        Ok(results.into_iter().sum())
    }

    async fn cleanup_interrupted_repairs(&self) -> Result<(), Status> {
        let state = self.state.read().await.clone();
        let affected: BTreeSet<_> = state
            .replica_repairs
            .iter()
            .filter(|repair| {
                matches!(
                    repair.phase,
                    ReplicaRepairPhase::Copying | ReplicaRepairPhase::Verifying
                )
            })
            .flat_map(|repair| [repair.owner_node_id.clone(), repair.node_id.clone()])
            .filter(|node_id| !state.fenced_nodes.contains(node_id))
            .collect();
        let members: Vec<_> = state
            .committed
            .members
            .iter()
            .filter(|member| affected.contains(&member.node_id))
            .cloned()
            .collect();
        let epoch = state.committed.epoch;
        let deadline = Instant::now() + self.migration_timeout;
        try_map_bounded(members, self.range_move_concurrency, |member| async move {
            let mut node = connect_node(&member.endpoint, deadline).await?;
            rpc_before(
                deadline,
                node.abort_replica_repairs(proto::AbortReplicaRepairsRequest {
                    topology_epoch: epoch,
                }),
            )
            .await?;
            Ok::<(), Status>(())
        })
        .await?;
        Ok(())
    }

    // Called while the topology execution lock is held. Target-epoch owners
    // remain unleased until the policy and minimum-copy barrier is verified.
    async fn seed_repairs_for_activation(&self) -> Result<(), Status> {
        self.cleanup_interrupted_repairs().await?;
        let (groups, required) = {
            let state = self.state.read().await;
            let topology = &state.committed;
            let guard = &topology.write_availability_guard;
            let policy_followers = match topology.write_ack_policy {
                WriteAckPolicy::OwnerOnly => 0,
                WriteAckPolicy::FirstSuccessor => 1,
                WriteAckPolicy::AllReplicas => topology.desired_replication_factor as usize - 1,
            };
            let needed = policy_followers
                .max(guard.minimum_admitted_copies.saturating_sub(1) as usize)
                .max(guard.minimum_healthy_followers as usize);
            let mut required = BTreeSet::new();
            for range in topology
                .derived_ranges()
                .map_err(|error| Status::internal(error.to_string()))?
            {
                if range.follower_node_ids.len() < needed {
                    return Err(Status::failed_precondition(
                        "target range cannot satisfy the activation policy and copy guard",
                    ));
                }
                for node_id in range.follower_node_ids.into_iter().take(needed) {
                    required.insert((
                        range.start_exclusive,
                        range.end_inclusive,
                        range.owner_node_id.clone(),
                        node_id,
                    ));
                }
            }
            let mut groups: BTreeMap<(String, String), Vec<ReplicaRepair>> = BTreeMap::new();
            for repair in &state.replica_repairs {
                groups
                    .entry((repair.owner_node_id.clone(), repair.node_id.clone()))
                    .or_default()
                    .push(repair.clone());
            }
            let pending = groups
                .into_values()
                .filter(|repairs| {
                    repairs.iter().any(|repair| {
                        repair.phase != ReplicaRepairPhase::Complete
                            && required.contains(&(
                                repair.start_exclusive,
                                repair.end_inclusive,
                                repair.owner_node_id.clone(),
                                repair.node_id.clone(),
                            ))
                    })
                })
                .collect::<Vec<_>>();
            (pending, required)
        };
        try_map_bounded(
            groups,
            self.range_move_concurrency.min(4),
            |group| async move { self.seed_replica_group(&group).await },
        )
        .await?;
        let state = self.state.read().await;
        if required.iter().any(|(start, end, owner, follower)| {
            !state.replica_admissions.iter().any(|admission| {
                admission.epoch == state.committed.epoch
                    && admission.start_exclusive == *start
                    && admission.end_inclusive == *end
                    && admission.owner_node_id == *owner
                    && admission.node_id == *follower
                    && state.process_instances.get(follower) == Some(&admission.process_instance_id)
            })
        }) {
            return Err(Status::unavailable(
                "required target followers are not all admitted",
            ));
        }
        Ok(())
    }

    async fn copy_replica(&self, task: &ReplicaRepair) -> Result<CopiedReplicaSeed, Status> {
        let (topology, owner_instance, follower_instance) = {
            let state = self.state.read().await;
            if state.committed.epoch != task.epoch {
                return Err(Status::failed_precondition("replica task epoch is stale"));
            }
            let owner_instance = state
                .process_instances
                .get(&task.owner_node_id)
                .ok_or_else(|| Status::unavailable("owner process is not registered"))?
                .clone();
            let follower_instance = state
                .process_instances
                .get(&task.node_id)
                .ok_or_else(|| Status::unavailable("follower process is not registered"))?
                .clone();
            (state.committed.clone(), owner_instance, follower_instance)
        };
        let owner = topology
            .members
            .iter()
            .find(|member| member.node_id == task.owner_node_id)
            .ok_or_else(|| Status::internal("repair owner is absent from topology"))?;
        let follower = topology
            .members
            .iter()
            .find(|member| member.node_id == task.node_id)
            .ok_or_else(|| Status::internal("repair follower is absent from topology"))?;
        let control = replica_repair_control(task);
        let range = RangeMigration {
            range_id: control.range_id.clone(),
            start_exclusive: task.start_exclusive,
            end_inclusive: task.end_inclusive,
            source_node_id: task.owner_node_id.clone(),
            destination_node_id: task.node_id.clone(),
            source_endpoint: owner.endpoint.clone(),
            destination_endpoint: follower.endpoint.clone(),
            source_process_instance_id: String::new(),
            destination_process_instance_id: String::new(),
            source_cleaned: false,
            snapshot_records: 0,
            changelog_watermark: 0,
            verified: false,
        };
        let change = TopologyChange {
            change_id: control.change_id.clone(),
            base_epoch: task.epoch,
            target_topology: topology,
            phase: MigrationPhase::CopyingSnapshot,
            ranges: Vec::new(),
            replica_obligations: Vec::new(),
            stopped_node_ids: Vec::new(),
            stopping_node_ids: Vec::new(),
            stop_prepared_node_ids: Vec::new(),
            failed_node_id: None,
            activation_ready: false,
        };
        let deadline = Instant::now() + self.migration_timeout;
        let mut source = connect_node(&range.source_endpoint, deadline).await?;
        let mut destination = connect_node(&range.destination_endpoint, deadline).await?;
        // A retry first releases any write fence left by an interrupted attempt.
        rpc_before(deadline, source.abort_range_migration(control.clone())).await?;
        rpc_before(deadline, destination.abort_range_migration(control.clone())).await?;
        self.set_repair_phase(task, ReplicaRepairPhase::Copying)
            .await?;
        let copied = self.copy_range(&change, &range, deadline).await?;
        if copied.source_process_instance_id != owner_instance
            || copied.destination_process_instance_id != follower_instance
        {
            return Err(Status::failed_precondition(
                "replica process changed during snapshot",
            ));
        }
        Ok(CopiedReplicaSeed {
            task: task.clone(),
            change,
            range: copied,
            control,
            follower_instance,
        })
    }

    async fn finish_replica(
        &self,
        copied: CopiedReplicaSeed,
        final_watermark: u64,
        deadline: Instant,
    ) -> Result<PreparedReplicaSeed, Status> {
        self.set_repair_phase(&copied.task, ReplicaRepairPhase::Verifying)
            .await?;
        let verified = self
            .finalize_range(&copied.change, &copied.range, final_watermark, deadline)
            .await?;
        let mut source = connect_node(&copied.range.source_endpoint, deadline).await?;
        let digest = rpc_before(deadline, source.source_range_digest(copied.control.clone()))
            .await?
            .into_inner()
            .digest;
        Ok(PreparedReplicaSeed {
            task: copied.task.clone(),
            control: copied.control,
            admission: ReplicaAdmission {
                epoch: copied.task.epoch,
                start_exclusive: copied.task.start_exclusive,
                end_inclusive: copied.task.end_inclusive,
                owner_node_id: copied.task.owner_node_id,
                node_id: copied.task.node_id,
                process_instance_id: copied.follower_instance,
                verified_watermark: verified.changelog_watermark,
                stream_cursor: 0,
                digest,
            },
        })
    }

    async fn seed_replica_group(&self, tasks: &[ReplicaRepair]) -> Result<(), Status> {
        let first = tasks
            .first()
            .ok_or_else(|| Status::invalid_argument("empty repair group"))?;
        let (topology, owner_instance, follower_instance) = {
            let state = self.state.read().await;
            (
                state.committed.clone(),
                state.process_instances.get(&first.owner_node_id).cloned(),
                state.process_instances.get(&first.node_id).cloned(),
            )
        };
        if topology.epoch != first.epoch {
            return Err(Status::failed_precondition("repair stream epoch is stale"));
        }
        let expected: BTreeSet<_> = topology
            .derived_ranges()
            .map_err(|error| Status::internal(error.to_string()))?
            .into_iter()
            .filter(|range| {
                range.owner_node_id == first.owner_node_id
                    && range.follower_node_ids.contains(&first.node_id)
            })
            .map(|range| (range.start_exclusive, range.end_inclusive))
            .collect();
        let actual: BTreeSet<_> = tasks
            .iter()
            .map(|task| (task.start_exclusive, task.end_inclusive))
            .collect();
        if expected.is_empty()
            || expected != actual
            || tasks.iter().any(|task| {
                task.epoch != first.epoch
                    || task.owner_node_id != first.owner_node_id
                    || task.node_id != first.node_id
            })
        {
            return Err(Status::failed_precondition(
                "repair group does not cover its full stream",
            ));
        }
        let owner_endpoint = topology
            .members
            .iter()
            .find(|member| member.node_id == first.owner_node_id)
            .ok_or_else(|| Status::internal("repair owner is absent"))?
            .endpoint
            .clone();
        let follower_endpoint = topology
            .members
            .iter()
            .find(|member| member.node_id == first.node_id)
            .ok_or_else(|| Status::internal("repair follower is absent"))?
            .endpoint
            .clone();
        let mut prepared = Vec::with_capacity(tasks.len());
        let result = async {
            // Snapshot all ranges while writes continue. Only the short final
            // replay/checkpoint window fences writes for this stream.
            let copied = try_map_bounded(
                tasks.to_vec(),
                self.range_move_concurrency.min(4),
                |task| async move { self.copy_replica(&task).await },
            )
            .await?;
            let deadline = Instant::now() + self.migration_timeout;
            let paused =
                try_map_bounded(copied, self.range_move_concurrency, |copied| async move {
                    let watermark = self
                        .pause_range(&copied.change, &copied.range, deadline)
                        .await?;
                    Ok::<_, Status>((copied, watermark))
                })
                .await?;
            prepared = try_map_bounded(
                paused,
                self.range_move_concurrency.min(4),
                |(copied, watermark)| async move {
                    self.finish_replica(copied, watermark, deadline).await
                },
            )
            .await?;
            let deadline = Instant::now() + self.migration_timeout;
            let mut source = connect_node(&owner_endpoint, deadline).await?;
            let mut destination = connect_node(&follower_endpoint, deadline).await?;
            let request = proto::ReplicationProgressRequest {
                topology_epoch: first.epoch,
                owner_node_id: first.owner_node_id.clone(),
                follower_node_id: first.node_id.clone(),
            };
            let head = rpc_before(deadline, source.get_replication_progress(request))
                .await?
                .into_inner();
            if Some(head.process_instance_id.as_str()) != owner_instance.as_deref() {
                return Err(Status::failed_precondition(
                    "owner process changed before checkpoint",
                ));
            }
            let checkpoint = rpc_before(
                deadline,
                destination.install_replication_checkpoint(proto::ReplicationCheckpointRequest {
                    topology_epoch: first.epoch,
                    owner_node_id: first.owner_node_id.clone(),
                    follower_node_id: first.node_id.clone(),
                    stream_sequence: head.stream_sequence,
                    verified_ranges: prepared.iter().map(|seed| seed.control.clone()).collect(),
                }),
            )
            .await?
            .into_inner();
            if Some(checkpoint.process_instance_id.as_str()) != follower_instance.as_deref()
                || checkpoint.stream_sequence < head.stream_sequence
            {
                return Err(Status::failed_precondition(
                    "follower process changed before checkpoint",
                ));
            }
            for seed in &mut prepared {
                seed.admission.stream_cursor = checkpoint.stream_sequence;
                rpc_before(deadline, source.cleanup_source_range(seed.control.clone())).await?;
                rpc_before(
                    deadline,
                    destination.abort_range_migration(seed.control.clone()),
                )
                .await?;
            }
            for seed in &prepared {
                self.store_repair_admission(&seed.task, seed.admission.clone())
                    .await?;
            }
            Ok(())
        }
        .await;
        if result.is_err() {
            let cleanup_deadline = Instant::now() + self.migration_timeout;
            let controls: Vec<_> = tasks.iter().map(replica_repair_control).collect();
            if let Ok(mut source) = connect_node(&owner_endpoint, cleanup_deadline).await {
                for control in &controls {
                    let _ = rpc_before(
                        cleanup_deadline,
                        source.abort_range_migration(control.clone()),
                    )
                    .await;
                }
            }
            if let Ok(mut destination) = connect_node(&follower_endpoint, cleanup_deadline).await {
                for control in &controls {
                    let _ = rpc_before(
                        cleanup_deadline,
                        destination.abort_range_migration(control.clone()),
                    )
                    .await;
                }
            }
            for task in tasks {
                self.set_repair_phase(task, ReplicaRepairPhase::Pending)
                    .await?;
            }
        }
        result
    }

    async fn set_repair_phase(
        &self,
        task: &ReplicaRepair,
        phase: ReplicaRepairPhase,
    ) -> Result<(), Status> {
        let mut state = self.state.write().await;
        let mut next = state.clone();
        let repair = next
            .replica_repairs
            .iter_mut()
            .find(|repair| {
                repair == &task
                    || (repair.epoch == task.epoch
                        && repair.start_exclusive == task.start_exclusive
                        && repair.end_inclusive == task.end_inclusive
                        && repair.owner_node_id == task.owner_node_id
                        && repair.node_id == task.node_id)
            })
            .ok_or_else(|| Status::failed_precondition("replica repair was invalidated"))?;
        repair.phase = phase;
        self.repository
            .store_state(&next)
            .map_err(|error| Status::internal(error.to_string()))?;
        *state = next;
        Ok(())
    }

    async fn store_repair_admission(
        &self,
        task: &ReplicaRepair,
        admission: ReplicaAdmission,
    ) -> Result<(), Status> {
        let mut state = self.state.write().await;
        let mut next = state.clone();
        if next.committed.epoch != task.epoch
            || next.process_instances.get(&task.node_id) != Some(&admission.process_instance_id)
        {
            return Err(Status::failed_precondition(
                "replica changed before durable admission",
            ));
        }
        let repair = next
            .replica_repairs
            .iter_mut()
            .find(|repair| {
                repair.epoch == task.epoch
                    && repair.start_exclusive == task.start_exclusive
                    && repair.end_inclusive == task.end_inclusive
                    && repair.owner_node_id == task.owner_node_id
                    && repair.node_id == task.node_id
            })
            .ok_or_else(|| Status::failed_precondition("replica repair was invalidated"))?;
        repair.phase = ReplicaRepairPhase::Complete;
        next.replica_admissions.retain(|existing| {
            !(existing.epoch == task.epoch
                && existing.start_exclusive == task.start_exclusive
                && existing.end_inclusive == task.end_inclusive
                && existing.owner_node_id == task.owner_node_id
                && existing.node_id == task.node_id)
        });
        next.replica_admissions.push(admission);
        self.repository
            .store_state(&next)
            .map_err(|error| Status::internal(error.to_string()))?;
        *state = next;
        Ok(())
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
                .execute_change(change_identity(&change), true)
                .await
                .map(Some),
        }
    }

    async fn execute_change(
        &self,
        expected: ChangeIdentity,
        retry_transient: bool,
    ) -> Result<TopologyChange, Status> {
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
        if change.failed_node_id.is_some() {
            return self.execute_failed_change(change).await;
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

        if change.ranges.is_empty()
            && change.replica_obligations.is_empty()
            && change.target_topology.members == self.state.read().await.committed.members
            && change.target_topology.write_ack_policy
                != self.state.read().await.committed.write_ack_policy
        {
            return self.execute_policy_change(change, false).await;
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
            Err(error) => {
                return self
                    .abort_after_error(&change, error, retry_transient)
                    .await;
            }
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
            Err(error) => {
                return self
                    .abort_after_error(&change, error, retry_transient)
                    .await;
            }
        };

        if let Err(error) = self.set_phase(MigrationPhase::Verifying).await {
            return self
                .abort_after_error(&change, error, retry_transient)
                .await;
        }
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
            Err(error) => {
                return self
                    .abort_after_error(&change, error, retry_transient)
                    .await;
            }
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
            return self
                .abort_after_error(&change, error, retry_transient)
                .await;
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
                return self
                    .abort_after_error(&change, error, retry_transient)
                    .await;
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
            reconcile_replica_repairs(&mut next)
                .map_err(|error| Status::internal(error.to_string()))?;
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
        retry_transient: bool,
    ) -> Result<TopologyChange, Status> {
        if retry_transient
            && matches!(
                original.code(),
                tonic::Code::Unavailable | tonic::Code::DeadlineExceeded
            )
        {
            // Keep a network-interrupted change recoverable. Resetting is durable,
            // and the next pass repeats cleanup before it copies anything.
            self.set_phase(MigrationPhase::Resetting).await?;
            if let Err(cleanup) = self
                .clear_prepublication_nodes(change, Instant::now() + self.migration_timeout)
                .await
            {
                return Err(Status::unavailable(format!(
                    "migration interrupted ({original}); cleanup remains pending ({cleanup})"
                )));
            }
            return Err(original);
        }
        match self.abort_prepublication_change(change).await {
            Ok(()) => Err(original),
            Err(cleanup) => Err(Status::internal(format!(
                "migration failed ({original}); cleanup remains pending ({cleanup})"
            ))),
        }
    }

    async fn execute_failed_change(
        &self,
        mut change: TopologyChange,
    ) -> Result<TopologyChange, Status> {
        let failed_node_id = change
            .failed_node_id
            .as_ref()
            .ok_or_else(|| Status::internal("failure transition omitted node identity"))?;
        if change.phase == MigrationPhase::Complete {
            return Ok(change);
        }
        if matches!(
            change.phase,
            MigrationPhase::Published | MigrationPhase::CleaningUp
        ) {
            return self.finish_failed_change(change).await;
        }
        if change.phase == MigrationPhase::Aborted {
            return Err(Status::failed_precondition(
                "failure transition was aborted",
            ));
        }
        self.cleanup_interrupted_repairs().await?;
        // A coordinator restart loses the old in-memory grant deadline. A full
        // startup quarantine covers a grant issued immediately before restart.
        let last_grant = self
            .lease_grants
            .lock()
            .await
            .get(failed_node_id)
            .map(|grant| grant.expires_at)
            .unwrap_or(self.startup_at);
        let safe_at = last_grant.max(self.startup_at + NODE_LEASE_DURATION);
        tokio::time::sleep_until(safe_at).await;

        let publication = {
            let mut state = self.state.write().await;
            let grants = self.lease_grants.lock().await;
            prove_failure_coverage(&state, &change.target_topology, &grants, Instant::now())?;
            if !state.fenced_nodes.contains(failed_node_id)
                || state
                    .active_change
                    .as_ref()
                    .is_none_or(|active| change_identity(active) != change_identity(&change))
            {
                return Err(Status::failed_precondition(
                    "failure transition lost its durable fence",
                ));
            }
            let mut next = state.clone();
            next.committed = change.target_topology.clone();
            next.process_instances.remove(failed_node_id);
            // Old admissions are tied to old exact bounds and stream epoch.
            // Deterministic repairs will verify and re-admit new followers.
            next.replica_admissions.clear();
            next.replica_repairs.clear();
            next.active_change
                .as_mut()
                .expect("active failure transition was checked")
                .phase = MigrationPhase::Published;
            next.active_change
                .as_mut()
                .expect("active failure transition was checked")
                .activation_ready = true;
            reconcile_replica_repairs(&mut next)
                .map_err(|error| Status::internal(error.to_string()))?;
            self.repository
                .store_state(&next)
                .map_err(|error| Status::internal(error.to_string()))?;
            *state = next;
            Ok::<(), Status>(())
        };
        publication?;
        change.phase = MigrationPhase::Published;
        self.finish_failed_change(change).await
    }

    async fn finish_failed_change(
        &self,
        mut change: TopologyChange,
    ) -> Result<TopologyChange, Status> {
        let deadline = Instant::now() + self.migration_timeout;
        self.install_on_members(
            &change.target_topology,
            &change.target_topology.members,
            deadline,
        )
        .await?;
        self.set_phase(MigrationPhase::Complete).await?;
        change.phase = MigrationPhase::Complete;
        if let Some(failed_node_id) = &change.failed_node_id {
            self.lease_grants.lock().await.remove(failed_node_id);
            self.peer_failures
                .lock()
                .await
                .retain(|(target, reporter), _| {
                    target != failed_node_id && reporter != failed_node_id
                });
        }
        Ok(change)
    }

    async fn execute_policy_change(
        &self,
        mut change: TopologyChange,
        retry_transient: bool,
    ) -> Result<TopologyChange, Status> {
        let deadline = Instant::now() + self.migration_timeout;
        self.set_phase(MigrationPhase::PausingWrites).await?;
        change.phase = MigrationPhase::PausingWrites;
        if let Err(error) = self.policy_fence_members(&change, true, deadline).await {
            return self
                .abort_after_error(&change, error, retry_transient)
                .await;
        }
        if let Err(error) = self.set_phase(MigrationPhase::Verifying).await {
            return self
                .abort_after_error(&change, error, retry_transient)
                .await;
        }
        change.phase = MigrationPhase::Verifying;
        {
            loop {
                let status = tokio::time::timeout_at(
                    deadline,
                    self.get_replica_status(Request::new(proto::Empty {})),
                )
                .await
                .map_err(|_| Status::deadline_exceeded("policy readiness barrier timed out"))
                .and_then(|response| response.map(Response::into_inner));
                let status = match status {
                    Ok(status) => status,
                    Err(error) => {
                        return self
                            .abort_after_error(&change, error, retry_transient)
                            .await;
                    }
                };
                if policy_readiness_verified(&change.target_topology, &status) {
                    break;
                }
                if Instant::now() >= deadline {
                    return self
                        .abort_after_error(
                            &change,
                            Status::deadline_exceeded(
                                "required followers did not catch up before policy publication",
                            ),
                            false,
                        )
                        .await;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        }
        if let Err(error) = self.set_phase(MigrationPhase::ReadyToPublish).await {
            return self
                .abort_after_error(&change, error, retry_transient)
                .await;
        }
        change.phase = MigrationPhase::ReadyToPublish;
        if !self.pre_publish_delay.is_zero() {
            tokio::time::sleep(self.pre_publish_delay).await;
        }
        // Recheck after the optional pre-publication delay while all old owners are fenced.
        {
            let status = tokio::time::timeout_at(
                deadline,
                self.get_replica_status(Request::new(proto::Empty {})),
            )
            .await
            .map_err(|_| Status::deadline_exceeded("policy readiness recheck timed out"))
            .and_then(|response| response.map(Response::into_inner));
            let status = match status {
                Ok(status) => status,
                Err(error) => {
                    return self
                        .abort_after_error(&change, error, retry_transient)
                        .await;
                }
            };
            if !policy_readiness_verified(&change.target_topology, &status) {
                return self
                    .abort_after_error(
                        &change,
                        Status::failed_precondition("policy readiness was lost before publication"),
                        retry_transient,
                    )
                    .await;
            }
        }
        let publication = {
            let mut state = self.state.write().await;
            let mut next = state.clone();
            (|| -> Result<(), Status> {
                if next
                    .active_change
                    .as_ref()
                    .is_none_or(|active| change_identity(active) != change_identity(&change))
                {
                    return Err(Status::failed_precondition(
                        "policy transition changed before publication",
                    ));
                }
                next.active_change
                    .as_mut()
                    .expect("active transition was checked")
                    .phase = MigrationPhase::Published;
                next.committed = change.target_topology.clone();
                // Identical placements plus the catch-up barrier preserve admission proof.
                for admission in &mut next.replica_admissions {
                    admission.epoch = next.committed.epoch;
                }
                for repair in &mut next.replica_repairs {
                    repair.epoch = next.committed.epoch;
                }
                reconcile_replica_repairs(&mut next)
                    .map_err(|error| Status::internal(error.to_string()))?;
                self.repository
                    .store_state(&next)
                    .map_err(|error| Status::internal(error.to_string()))?;
                *state = next;
                Ok(())
            })()
        };
        if let Err(error) = publication {
            return self
                .abort_after_error(&change, error, retry_transient)
                .await;
        }
        change.phase = MigrationPhase::Published;
        self.finish_published_change(change, deadline).await
    }

    async fn policy_fence_members(
        &self,
        change: &TopologyChange,
        pause: bool,
        deadline: Instant,
    ) -> Result<(), Status> {
        let request = PolicyWriteFenceRequest {
            change_id: change.change_id.clone(),
            base_epoch: change.base_epoch,
        };
        let mut first_error = None;
        for member in &change.target_topology.members {
            let result = async {
                let mut client = connect_node(&member.endpoint, deadline).await?;
                if pause {
                    rpc_before(deadline, client.pause_policy_writes(request.clone())).await?;
                } else {
                    rpc_before(deadline, client.resume_policy_writes(request.clone())).await?;
                }
                Ok::<(), Status>(())
            }
            .await;
            if let Err(error) = result {
                if pause {
                    return Err(error);
                }
                first_error.get_or_insert(error);
            }
        }
        first_error.map_or(Ok(()), Err)
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

        let mut dedup_cursor = 0;
        loop {
            let page = rpc_before(
                deadline,
                source.read_dedup_snapshot_page(SnapshotPageRequest {
                    change_id: change.change_id.clone(),
                    range_id: range.range_id.clone(),
                    cursor: dedup_cursor,
                    max_bytes: MAX_MIGRATION_PAGE_BYTES as u64,
                }),
            )
            .await?
            .into_inner();
            if !page.records.is_empty() {
                rpc_before(
                    deadline,
                    destination.apply_dedup_batch(ApplyDedupBatchRequest {
                        change_id: change.change_id.clone(),
                        range_id: range.range_id.clone(),
                        records: page.records,
                    }),
                )
                .await?;
            }
            dedup_cursor = page.next_cursor;
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
        _deadline: Instant,
    ) -> Result<TopologyChange, Status> {
        let deadline = Instant::now() + self.migration_timeout;
        let needs_activation = !change.activation_ready;
        if needs_activation {
            let old_sources: Vec<_> = change
                .ranges
                .iter()
                .map(|range| (range.source_node_id.clone(), range.source_endpoint.clone()))
                .collect::<BTreeMap<_, _>>()
                .into_iter()
                .map(|(node_id, endpoint)| Member { node_id, endpoint })
                .collect();
            self.install_on_members_with_lease(
                &change.target_topology,
                &old_sources,
                deadline,
                false,
            )
            .await?;
        }
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
        self.install_on_members_with_lease(
            &change.target_topology,
            &destinations,
            deadline,
            !needs_activation,
        )
        .await?;
        self.install_on_members_with_lease(
            &change.target_topology,
            &other_targets,
            deadline,
            !needs_activation,
        )
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
        if needs_activation {
            let guard = &change.target_topology.write_availability_guard;
            let needs_replica_barrier = change.target_topology.write_ack_policy
                != WriteAckPolicy::OwnerOnly
                || guard.minimum_admitted_copies > 1
                || guard.minimum_healthy_followers > 0;
            if needs_replica_barrier
                && (!change.ranges.is_empty() || !change.replica_obligations.is_empty())
            {
                self.seed_repairs_for_activation().await?;
            }
            self.set_activation_ready().await?;
            change.activation_ready = true;
            self.install_on_members(
                &change.target_topology,
                &change.target_topology.members,
                Instant::now() + self.migration_timeout,
            )
            .await?;
        }
        if change.ranges.is_empty() && change.replica_obligations.is_empty() {
            self.policy_fence_members(&change, false, deadline).await?;
        }
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
        self.install_on_members_with_lease(topology, members, deadline, true)
            .await
    }

    async fn install_on_members_with_lease(
        &self,
        topology: &TopologySnapshot,
        members: &[Member],
        deadline: Instant,
        require_lease: bool,
    ) -> Result<(), Status> {
        for member in members {
            let mut client = connect_node(&member.endpoint, deadline).await?;
            rpc_before(
                deadline,
                client.install_topology(InstallTopologyRequest {
                    topology: Some(topology.into()),
                    require_lease,
                }),
            )
            .await?;
        }
        Ok(())
    }

    async fn set_activation_ready(&self) -> Result<(), Status> {
        let mut state = self.state.write().await;
        let mut next = state.clone();
        let change = next
            .active_change
            .as_mut()
            .ok_or_else(|| Status::failed_precondition("no topology change is active"))?;
        if change.phase != MigrationPhase::Published {
            return Err(Status::failed_precondition(
                "topology is not waiting for owner activation",
            ));
        }
        change.activation_ready = true;
        self.repository
            .store_state(&next)
            .map_err(|error| Status::internal(error.to_string()))?;
        *state = next;
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
        if change.ranges.is_empty() && change.replica_obligations.is_empty() {
            self.policy_fence_members(change, false, deadline).await?;
        }
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

    async fn get_replica_status(
        &self,
        _request: Request<proto::Empty>,
    ) -> Result<Response<proto::ReplicaStatusResponse>, Status> {
        let state = self.state.read().await.clone();
        let epoch = state.committed.epoch;
        let now = Instant::now();
        let live_nodes: BTreeSet<_> = self
            .lease_grants
            .lock()
            .await
            .iter()
            .filter(|(node_id, grant)| {
                grant.epoch == epoch
                    && grant.expires_at > now
                    && state.process_instances.get(*node_id) == Some(&grant.process_instance_id)
                    && !state.fenced_nodes.contains(*node_id)
            })
            .map(|(node_id, _)| node_id.clone())
            .collect();
        let members: BTreeMap<_, _> = state
            .committed
            .members
            .iter()
            .map(|member| (member.node_id.clone(), member.endpoint.clone()))
            .collect();
        let pairs: BTreeSet<_> = state
            .replica_repairs
            .iter()
            .map(|repair| (repair.owner_node_id.clone(), repair.node_id.clone()))
            .collect();
        let progress = try_map_bounded(pairs, self.range_move_concurrency, |(owner, follower)| {
            let source_endpoint = members.get(&owner).cloned();
            let destination_endpoint = members.get(&follower).cloned();
            let owner_instance = live_nodes
                .contains(&owner)
                .then(|| state.process_instances.get(&owner).cloned())
                .flatten();
            let follower_instance = live_nodes
                .contains(&follower)
                .then(|| state.process_instances.get(&follower).cloned())
                .flatten();
            async move {
                let current = match (
                    source_endpoint,
                    destination_endpoint,
                    owner_instance,
                    follower_instance,
                ) {
                    (
                        Some(source_endpoint),
                        Some(destination_endpoint),
                        Some(owner_instance),
                        Some(follower_instance),
                    ) => {
                        read_replication_pair(
                            epoch,
                            &owner,
                            &follower,
                            &source_endpoint,
                            &destination_endpoint,
                            &owner_instance,
                            &follower_instance,
                        )
                        .await
                    }
                    _ => None,
                };
                Ok::<_, Status>(((owner, follower), current))
            }
        })
        .await?
        .into_iter()
        .collect::<BTreeMap<_, _>>();
        let admissions: BTreeMap<_, _> = state
            .replica_admissions
            .iter()
            .map(|admission| {
                (
                    (
                        admission.start_exclusive,
                        admission.end_inclusive,
                        admission.owner_node_id.as_str(),
                        admission.node_id.as_str(),
                    ),
                    admission,
                )
            })
            .collect();
        let repairs: BTreeMap<_, _> = state
            .replica_repairs
            .iter()
            .map(|repair| {
                (
                    (
                        repair.start_exclusive,
                        repair.end_inclusive,
                        repair.owner_node_id.as_str(),
                        repair.node_id.as_str(),
                    ),
                    repair,
                )
            })
            .collect();
        let ranges = state
            .committed
            .derived_ranges()
            .map_err(|error| Status::internal(error.to_string()))?
            .into_iter()
            .map(|range| {
                let followers: Vec<_> = range
                    .follower_node_ids
                    .iter()
                    .map(|node_id| {
                        let key = (
                            range.start_exclusive,
                            range.end_inclusive,
                            range.owner_node_id.as_str(),
                            node_id.as_str(),
                        );
                        let admission = admissions.get(&key).copied().filter(|admission| {
                            state.process_instances.get(node_id)
                                == Some(&admission.process_instance_id)
                                && !state.fenced_nodes.contains(node_id)
                        });
                        let repair = repairs.get(&key).copied();
                        let live = progress
                            .get(&(range.owner_node_id.clone(), node_id.clone()))
                            .and_then(Option::as_ref);
                        proto::FollowerReplicaStatus {
                            node_id: node_id.clone(),
                            admitted: admission.is_some(),
                            process_instance_id: admission
                                .map(|admission| admission.process_instance_id.clone())
                                .unwrap_or_default(),
                            verified_watermark: admission
                                .map(|admission| admission.verified_watermark)
                                .unwrap_or_default(),
                            stream_cursor: live
                                .map(|(_, cursor, _)| *cursor)
                                .or_else(|| admission.map(|admission| admission.stream_cursor))
                                .unwrap_or_default(),
                            lag_millis: live.map(|(_, _, lag)| *lag).unwrap_or_default(),
                            lag_known: live.is_some(),
                            stream_head: live.map(|(head, _, _)| *head).unwrap_or_default(),
                            repair_state: repair
                                .map(|repair| format!("{:?}", repair.phase))
                                .unwrap_or_default(),
                        }
                    })
                    .collect();
                proto::RangeReplicaStatus {
                    start_exclusive: range.start_exclusive,
                    end_inclusive: range.end_inclusive,
                    owner_node_id: range.owner_node_id.clone(),
                    desired_rf: state.committed.desired_replication_factor,
                    current_rf: u32::from(
                        state.process_instances.contains_key(&range.owner_node_id)
                            && !state.fenced_nodes.contains(&range.owner_node_id),
                    ) + followers
                        .iter()
                        .filter(|follower| follower.admitted)
                        .count() as u32,
                    followers,
                }
            })
            .collect();
        Ok(Response::new(proto::ReplicaStatusResponse {
            topology_epoch: epoch,
            ranges,
        }))
    }

    async fn begin_topology_change(
        &self,
        request: Request<proto::BeginTopologyChangeRequest>,
    ) -> Result<Response<proto::TopologyChangeSnapshot>, Status> {
        let request = request.into_inner();
        let target_policy = request
            .target_write_ack_policy
            .map(WriteAckPolicy::try_from)
            .transpose()
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let target_members = request
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
        if target_policy.is_some_and(|policy| policy != state.committed.write_ack_policy)
            && target_members != state.committed.members
        {
            return Err(Status::invalid_argument(
                "change membership and ACK policy in separate topology transitions",
            ));
        }
        let change = TopologyChange::plan_with_policy(
            &state.committed,
            target_members,
            target_policy.unwrap_or(state.committed.write_ack_policy),
        )
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let target = &change.target_topology;
        let member_count = target.members.len();
        let guard = &target.write_availability_guard;
        if target.members != state.committed.members
            && (member_count < guard.minimum_admitted_copies as usize
                || member_count.saturating_sub(1) < guard.minimum_healthy_followers as usize
                || (target.write_ack_policy == WriteAckPolicy::FirstSuccessor && member_count < 2)
                || (target.write_ack_policy == WriteAckPolicy::AllReplicas
                    && member_count < target.desired_replication_factor as usize))
        {
            return Err(Status::failed_precondition(
                "target membership cannot satisfy its write policy and availability guard",
            ));
        }
        let target_tasks: usize = change
            .target_topology
            .derived_ranges()
            .map_err(|error| Status::invalid_argument(error.to_string()))?
            .iter()
            .map(|range| range.follower_node_ids.len())
            .sum();
        if target_tasks > MAX_REPLICA_OBLIGATIONS {
            return Err(Status::resource_exhausted(format!(
                "target topology exceeds the {MAX_REPLICA_OBLIGATIONS} replica-task limit"
            )));
        }
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
        let change = tokio::spawn(async move { service.execute_change(expected, false).await })
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
        if is_committed && state.fenced_nodes.contains(&request.node_id) {
            return Err(Status::failed_precondition("node has been fenced"));
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
        if !is_committed {
            next.fenced_nodes.remove(&request.node_id);
        }
        next.process_instances
            .insert(request.node_id, request.process_instance_id);
        self.repository
            .store_state(&next)
            .map_err(|error| Status::internal(error.to_string()))?;
        *state = next;
        Ok(Response::new(proto::Empty {}))
    }

    async fn renew_node_lease(
        &self,
        request: Request<proto::RenewNodeLeaseRequest>,
    ) -> Result<Response<proto::RenewNodeLeaseResponse>, Status> {
        let request = request.into_inner();
        let state = self.state.read().await;
        if request.node_id.is_empty()
            || request.process_instance_id.is_empty()
            || state.committed.epoch != request.topology_epoch
            || !state
                .committed
                .members
                .iter()
                .any(|member| member.node_id == request.node_id)
            || state.process_instances.get(&request.node_id) != Some(&request.process_instance_id)
            || state.fenced_nodes.contains(&request.node_id)
            || state.active_change.as_ref().is_some_and(|change| {
                change.phase == MigrationPhase::Published && !change.activation_ready
            })
        {
            return Err(Status::failed_precondition(
                "lease identity, epoch, or membership is no longer valid",
            ));
        }
        let epoch = state.committed.epoch;
        let mut grants = self.lease_grants.lock().await;
        grants.insert(
            request.node_id,
            NodeLeaseGrant {
                process_instance_id: request.process_instance_id,
                epoch,
                expires_at: Instant::now() + NODE_LEASE_DURATION,
            },
        );
        Ok(Response::new(proto::RenewNodeLeaseResponse {
            topology_epoch: epoch,
            lease_duration_millis: NODE_LEASE_DURATION.as_millis() as u64,
        }))
    }

    async fn report_peer_health(
        &self,
        request: Request<proto::ReportPeerHealthRequest>,
    ) -> Result<Response<proto::Empty>, Status> {
        let request = request.into_inner();
        let state = self.state.read().await;
        if request.reporter_node_id == request.peer_node_id
            || request.peer_node_id.is_empty()
            || request.topology_epoch != state.committed.epoch
            || state.process_instances.get(&request.reporter_node_id)
                != Some(&request.reporter_process_instance_id)
            || state.fenced_nodes.contains(&request.reporter_node_id)
            || !state
                .committed
                .members
                .iter()
                .any(|member| member.node_id == request.peer_node_id)
        {
            return Err(Status::failed_precondition(
                "peer report does not match active membership",
            ));
        }
        let now = Instant::now();
        let grants = self.lease_grants.lock().await;
        if grants.get(&request.reporter_node_id).is_none_or(|grant| {
            grant.epoch != request.topology_epoch
                || grant.process_instance_id != request.reporter_process_instance_id
                || grant.expires_at <= now
        }) {
            return Err(Status::failed_precondition("reporter has no valid lease"));
        }
        let mut failures = self.peer_failures.lock().await;
        let key = (request.peer_node_id, request.reporter_node_id);
        if request.reachable {
            failures.remove(&key);
        } else {
            failures
                .entry(key)
                .and_modify(|failure| {
                    if failure.last_seen + Duration::from_secs(2) < now {
                        failure.first_seen = now;
                    }
                    failure.last_seen = now;
                })
                .or_insert(PeerFailure {
                    first_seen: now,
                    last_seen: now,
                });
        }
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

fn policy_readiness_verified(
    topology: &TopologySnapshot,
    status: &proto::ReplicaStatusResponse,
) -> bool {
    if status.topology_epoch != topology.epoch.saturating_sub(1) {
        return false;
    }
    let Ok(ranges) = topology.derived_ranges() else {
        return false;
    };
    let guard = &topology.write_availability_guard;
    ranges.iter().all(|range| {
        let Some(actual) = status.ranges.iter().find(|actual| {
            actual.start_exclusive == range.start_exclusive
                && actual.end_inclusive == range.end_inclusive
                && actual.owner_node_id == range.owner_node_id
        }) else {
            return false;
        };
        let admitted = actual
            .followers
            .iter()
            .filter(|follower| follower.admitted)
            .count() as u32;
        let healthy = actual
            .followers
            .iter()
            .filter(|follower| {
                follower.admitted
                    && follower.lag_known
                    && follower.lag_millis <= guard.max_replica_lag_millis
                    && follower.stream_cursor == follower.stream_head
            })
            .count() as u32;
        if actual.followers.iter().any(|follower| {
            follower.admitted
                && (!follower.lag_known || follower.stream_cursor != follower.stream_head)
        }) {
            return false;
        }
        if admitted.saturating_add(1) < guard.minimum_admitted_copies
            || healthy < guard.minimum_healthy_followers
        {
            return false;
        }
        let needed: Vec<_> = match topology.write_ack_policy {
            WriteAckPolicy::OwnerOnly => Vec::new(),
            WriteAckPolicy::FirstSuccessor => range.follower_node_ids.first().into_iter().collect(),
            WriteAckPolicy::AllReplicas => {
                if range.follower_node_ids.len() + 1 < topology.desired_replication_factor as usize
                {
                    return false;
                }
                range.follower_node_ids.iter().collect()
            }
        };
        needed.into_iter().all(|node_id| {
            actual.followers.iter().any(|follower| {
                &follower.node_id == node_id
                    && follower.admitted
                    && follower.lag_known
                    && follower.lag_millis <= guard.max_replica_lag_millis
                    && follower.stream_cursor == follower.stream_head
            })
        })
    })
}

fn token_in_range(start: u64, end: u64, token: u64) -> bool {
    if start < end {
        token > start && token <= end
    } else if start > end {
        token > start || token <= end
    } else {
        true
    }
}

fn failure_confirmed(
    state: &ClusterState,
    grants: &BTreeMap<String, NodeLeaseGrant>,
    reports: &BTreeMap<(String, String), PeerFailure>,
    target: &str,
    now: Instant,
) -> bool {
    let has_lease = |node_id: &str| {
        grants.get(node_id).is_some_and(|grant| {
            grant.epoch == state.committed.epoch
                && state.process_instances.get(node_id) == Some(&grant.process_instance_id)
                && grant.expires_at > now
                && !state.fenced_nodes.contains(node_id)
        })
    };
    if !has_lease(target) {
        return true;
    }
    reports
        .iter()
        .filter(|((reported_target, reporter), report)| {
            reported_target == target
                && reporter != reported_target
                && has_lease(reporter)
                && report.first_seen + NODE_LEASE_DURATION <= now
                && report.last_seen + Duration::from_secs(2) >= now
        })
        .count()
        >= 2
}

fn prove_failure_coverage(
    state: &ClusterState,
    target: &TopologySnapshot,
    grants: &BTreeMap<String, NodeLeaseGrant>,
    now: Instant,
) -> Result<(), Status> {
    let old_ranges = state
        .committed
        .derived_ranges()
        .map_err(|error| Status::internal(error.to_string()))?;
    let target_ranges = target
        .derived_ranges()
        .map_err(|error| Status::internal(error.to_string()))?;
    for target_range in target_ranges {
        let owner = &target_range.owner_node_id;
        let process_instance_id = state.process_instances.get(owner).ok_or_else(|| {
            Status::failed_precondition(format!("promoted owner {owner} is not registered"))
        })?;
        if state.fenced_nodes.contains(owner)
            || grants.get(owner).is_none_or(|grant| {
                grant.epoch != state.committed.epoch
                    || grant.process_instance_id != *process_instance_id
                    || grant.expires_at <= now
            })
        {
            return Err(Status::failed_precondition(format!(
                "promoted owner {owner} has no current lease"
            )));
        }
        let constituents: Vec<_> = old_ranges
            .iter()
            .filter(|range| {
                token_in_range(
                    target_range.start_exclusive,
                    target_range.end_inclusive,
                    range.end_inclusive,
                )
            })
            .collect();
        if constituents.is_empty() {
            return Err(Status::data_loss("merged range has no old constituents"));
        }
        for old_range in constituents {
            if old_range.owner_node_id == *owner {
                continue;
            }
            if old_range.follower_node_ids.first() != Some(owner)
                || !state.replica_admissions.iter().any(|admission| {
                    admission.epoch == state.committed.epoch
                        && admission.start_exclusive == old_range.start_exclusive
                        && admission.end_inclusive == old_range.end_inclusive
                        && admission.owner_node_id == old_range.owner_node_id
                        && admission.node_id == *owner
                        && admission.process_instance_id == *process_instance_id
                })
            {
                return Err(Status::failed_precondition(format!(
                    "promoted owner {owner} lacks first-successor coverage for ({}, {}]",
                    old_range.start_exclusive, old_range.end_inclusive
                )));
            }
        }
    }
    Ok(())
}

async fn read_replication_pair(
    epoch: u64,
    owner: &str,
    follower: &str,
    owner_endpoint: &str,
    follower_endpoint: &str,
    owner_instance: &str,
    follower_instance: &str,
) -> Option<(u64, u64, u64)> {
    let deadline = Instant::now() + Duration::from_millis(500);
    let mut source = connect_node(owner_endpoint, deadline).await.ok()?;
    let mut destination = connect_node(follower_endpoint, deadline).await.ok()?;
    let request = proto::ReplicationProgressRequest {
        topology_epoch: epoch,
        owner_node_id: owner.to_owned(),
        follower_node_id: follower.to_owned(),
    };
    let head = rpc_before(deadline, source.get_replication_progress(request.clone()))
        .await
        .ok()?
        .into_inner();
    let applied = rpc_before(deadline, destination.get_replication_progress(request))
        .await
        .ok()?
        .into_inner();
    if head.process_instance_id != owner_instance
        || applied.process_instance_id != follower_instance
    {
        return None;
    }
    let lag = if applied.stream_sequence >= head.stream_sequence {
        0
    } else {
        let last = head.oldest_unacked_unix_millis;
        if last == 0 {
            return None;
        }
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()?
            .as_millis()
            .saturating_sub(u128::from(last))
            .try_into()
            .unwrap_or(u64::MAX)
    };
    Some((head.stream_sequence, applied.stream_sequence, lag))
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

    #[test]
    fn failover_requires_admitted_coverage_for_every_old_interval() {
        let members: Vec<_> = (1..=3)
            .map(|index| Member {
                node_id: format!("n{index}"),
                endpoint: format!("http://127.0.0.1:500{index}"),
            })
            .collect();
        let old = TopologySnapshot::new(1, 7, 4, members.clone()).unwrap();
        let target = TopologyChange::plan(
            &old,
            members
                .into_iter()
                .filter(|member| member.node_id != "n1")
                .collect(),
        )
        .unwrap()
        .target_topology;
        let repo = MemoryRepository::default();
        let mut state = load_or_initialize(&repo, Some(old.clone())).unwrap();
        for node_id in ["n1", "n2", "n3"] {
            state
                .process_instances
                .insert(node_id.into(), format!("{node_id}-process"));
        }
        for range in old.derived_ranges().unwrap() {
            let Some(follower) = range.follower_node_ids.first() else {
                continue;
            };
            state.replica_admissions.push(ReplicaAdmission {
                epoch: old.epoch,
                start_exclusive: range.start_exclusive,
                end_inclusive: range.end_inclusive,
                owner_node_id: range.owner_node_id,
                node_id: follower.clone(),
                process_instance_id: format!("{follower}-process"),
                verified_watermark: 0,
                stream_cursor: 0,
                digest: "verified".into(),
            });
        }
        let now = Instant::now();
        let grants = ["n2", "n3"]
            .into_iter()
            .map(|node_id| {
                (
                    node_id.into(),
                    NodeLeaseGrant {
                        process_instance_id: format!("{node_id}-process"),
                        epoch: old.epoch,
                        expires_at: now + NODE_LEASE_DURATION,
                    },
                )
            })
            .collect();
        assert!(prove_failure_coverage(&state, &target, &grants, now).is_ok());
        let missing = state
            .replica_admissions
            .iter()
            .position(|admission| admission.owner_node_id == "n1")
            .expect("failed owner has at least one range");
        state.replica_admissions.remove(missing);
        assert!(prove_failure_coverage(&state, &target, &grants, now).is_err());
    }

    #[test]
    fn a_single_failed_peer_link_never_confirms_node_failure() {
        let members: Vec<_> = (1..=3)
            .map(|index| Member {
                node_id: format!("n{index}"),
                endpoint: format!("http://127.0.0.1:500{index}"),
            })
            .collect();
        let topology = TopologySnapshot::new(1, 7, 1, members).unwrap();
        let repo = MemoryRepository::default();
        let mut state = load_or_initialize(&repo, Some(topology)).unwrap();
        let now = Instant::now();
        let mut grants = BTreeMap::new();
        for node_id in ["n1", "n2", "n3"] {
            let process = format!("{node_id}-process");
            state
                .process_instances
                .insert(node_id.into(), process.clone());
            grants.insert(
                node_id.into(),
                NodeLeaseGrant {
                    process_instance_id: process,
                    epoch: 1,
                    expires_at: now + NODE_LEASE_DURATION,
                },
            );
        }
        let failure = PeerFailure {
            first_seen: now - Duration::from_secs(6),
            last_seen: now,
        };
        let mut reports = BTreeMap::from([(("n1".into(), "n2".into()), failure.clone())]);
        assert!(!failure_confirmed(&state, &grants, &reports, "n1", now));
        reports.insert(("n1".into(), "n3".into()), failure);
        assert!(failure_confirmed(&state, &grants, &reports, "n1", now));
        reports.clear();
        grants.remove("n1");
        assert!(failure_confirmed(&state, &grants, &reports, "n1", now));
    }

    #[tokio::test]
    async fn confirmed_failure_fences_before_waiting_for_repair_serialization() {
        let members: Vec<_> = (1..=3)
            .map(|index| Member {
                node_id: format!("n{index}"),
                endpoint: format!("http://127.0.0.1:500{index}"),
            })
            .collect();
        let topology = TopologySnapshot::new(1, 7, 1, members).unwrap();
        let repository = Arc::new(MemoryRepository::default());
        let mut state = load_or_initialize(repository.as_ref(), Some(topology)).unwrap();
        for node_id in ["n1", "n2", "n3"] {
            state
                .process_instances
                .insert(node_id.into(), format!("{node_id}-process"));
        }
        let mut service = CoordinatorService::new(state, repository, Duration::from_secs(1));
        service.startup_at = Instant::now() - NODE_LEASE_DURATION;
        let held_repair_lock = service.execution_lock.lock().await;
        let mut interrupt = service.repair_interrupt.subscribe();
        let runner = service.clone();
        let task = tokio::spawn(async move { runner.run_failure_pass().await });
        tokio::time::timeout(Duration::from_secs(1), interrupt.changed())
            .await
            .expect("fencing should interrupt a repair without waiting for its lock")
            .unwrap();
        let state = service.state.read().await;
        assert!(state.fenced_nodes.contains("n1"));
        assert_eq!(
            state
                .active_change
                .as_ref()
                .unwrap()
                .failed_node_id
                .as_deref(),
            Some("n1")
        );
        drop(state);
        task.abort();
        drop(held_repair_lock);
    }

    #[test]
    fn failed_owner_vnodes_promote_to_multiple_clockwise_successors() {
        let members: Vec<_> = (1..=4)
            .map(|index| Member {
                node_id: format!("n{index}"),
                endpoint: format!("http://127.0.0.1:500{index}"),
            })
            .collect();
        let old = TopologySnapshot::new(1, 7, 32, members.clone()).unwrap();
        let target = TopologyChange::plan(
            &old,
            members
                .into_iter()
                .filter(|member| member.node_id != "n1")
                .collect(),
        )
        .unwrap()
        .target_topology;
        let successors: BTreeSet<_> = old
            .derived_ranges()
            .unwrap()
            .into_iter()
            .filter(|range| range.owner_node_id == "n1")
            .map(|range| {
                target
                    .owner_for_token(range.end_inclusive)
                    .unwrap()
                    .node_id
                    .clone()
            })
            .collect();
        assert!(
            successors.len() > 1,
            "vnode failover should distribute ownership"
        );
    }

    #[test]
    fn policy_epoch_barrier_requires_every_admitted_stream_caught_up() {
        let base = TopologySnapshot::new_with_config(
            1,
            7,
            1,
            vec![
                Member {
                    node_id: "n1".into(),
                    endpoint: "http://127.0.0.1:5001".into(),
                },
                Member {
                    node_id: "n2".into(),
                    endpoint: "http://127.0.0.1:5002".into(),
                },
                Member {
                    node_id: "n3".into(),
                    endpoint: "http://127.0.0.1:5003".into(),
                },
            ],
            hashring_core::topology::TopologyConfig::default(),
        )
        .unwrap();
        let target = TopologyChange::plan_with_policy(
            &base,
            base.members.clone(),
            WriteAckPolicy::FirstSuccessor,
        )
        .unwrap()
        .target_topology;
        let mut status = proto::ReplicaStatusResponse {
            topology_epoch: base.epoch,
            ranges: target
                .derived_ranges()
                .unwrap()
                .into_iter()
                .map(|range| proto::RangeReplicaStatus {
                    start_exclusive: range.start_exclusive,
                    end_inclusive: range.end_inclusive,
                    owner_node_id: range.owner_node_id,
                    desired_rf: 3,
                    current_rf: 3,
                    followers: range
                        .follower_node_ids
                        .into_iter()
                        .map(|node_id| proto::FollowerReplicaStatus {
                            node_id,
                            admitted: true,
                            lag_known: true,
                            stream_head: 1,
                            stream_cursor: 1,
                            ..Default::default()
                        })
                        .collect(),
                })
                .collect(),
        };
        assert!(policy_readiness_verified(&target, &status));
        status.ranges[0].followers[1].stream_cursor = 0;
        assert!(!policy_readiness_verified(&target, &status));
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
            replica_admissions: Vec::new(),
            replica_repairs: Vec::new(),
            fenced_nodes: BTreeSet::new(),
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
                replica_admissions: Vec::new(),
                replica_repairs: Vec::new(),
                fenced_nodes: BTreeSet::new(),
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

    #[tokio::test]
    async fn replica_status_only_counts_verified_current_processes() {
        let repository = Arc::new(MemoryRepository::default());
        let topology = TopologySnapshot::new(
            1,
            7,
            1,
            vec![
                Member {
                    node_id: "n1".into(),
                    endpoint: "http://127.0.0.1:5001".into(),
                },
                Member {
                    node_id: "n2".into(),
                    endpoint: "http://127.0.0.1:5002".into(),
                },
                Member {
                    node_id: "n3".into(),
                    endpoint: "http://127.0.0.1:5003".into(),
                },
            ],
        )
        .unwrap();
        let mut state = load_or_initialize(repository.as_ref(), Some(topology)).unwrap();
        assert_eq!(
            state.replica_repairs.len(),
            state.committed.derived_ranges().unwrap().len() * 2
        );
        let repair = state.replica_repairs[0].clone();
        state
            .process_instances
            .insert(repair.owner_node_id.clone(), "owner-1".into());
        state
            .process_instances
            .insert(repair.node_id.clone(), "follower-1".into());
        state.replica_admissions.push(ReplicaAdmission {
            epoch: repair.epoch,
            start_exclusive: repair.start_exclusive,
            end_inclusive: repair.end_inclusive,
            owner_node_id: repair.owner_node_id.clone(),
            node_id: repair.node_id.clone(),
            process_instance_id: "follower-1".into(),
            verified_watermark: 4,
            stream_cursor: 7,
            digest: "verified".into(),
        });
        state.replica_repairs[0].phase = ReplicaRepairPhase::Complete;
        repository.store_state(&state).unwrap();
        let restored = load_or_initialize(repository.as_ref(), None).unwrap();
        assert_eq!(restored.replica_admissions, state.replica_admissions);
        let service = CoordinatorService::new(restored, repository, Duration::from_secs(1));
        let status = service
            .get_replica_status(Request::new(proto::Empty {}))
            .await
            .unwrap()
            .into_inner();
        let range = status
            .ranges
            .iter()
            .find(|range| {
                range.start_exclusive == repair.start_exclusive
                    && range.end_inclusive == repair.end_inclusive
            })
            .unwrap();
        assert_eq!(range.current_rf, 2);
        assert!(
            range
                .followers
                .iter()
                .any(|follower| follower.node_id == repair.node_id && follower.admitted)
        );
        service
            .state
            .write()
            .await
            .process_instances
            .insert(repair.node_id, "follower-2".into());
        let status = service
            .get_replica_status(Request::new(proto::Empty {}))
            .await
            .unwrap()
            .into_inner();
        let range = status
            .ranges
            .iter()
            .find(|range| {
                range.start_exclusive == repair.start_exclusive
                    && range.end_inclusive == repair.end_inclusive
            })
            .unwrap();
        assert_eq!(range.current_rf, 1);
        let mut state = service.state.read().await.clone();
        reconcile_replica_repairs(&mut state).unwrap();
        assert!(state.replica_admissions.is_empty());
        assert_eq!(state.replica_repairs[0].phase, ReplicaRepairPhase::Pending);
    }
}
