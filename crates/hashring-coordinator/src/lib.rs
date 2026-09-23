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
use tonic::{
    Request, Response, Status,
    transport::{Channel, Endpoint},
};

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
    transport::configure_data_node_client,
};

const TOPOLOGY_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("topology");
const COMMITTED_KEY: &str = "committed";
const CLUSTER_STATE_KEY: &str = "cluster-state-v1";
fn unix_millis_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}
pub const DEFAULT_RANGE_MOVE_CONCURRENCY: usize = 16;
const MAX_REPAIR_GROUPS_PER_PASS: usize = 32;
const MAX_PENDING_PROBES_PER_PASS: usize = 32;
const PREPUBLICATION_FAILURE_GRACE: Duration = Duration::from_secs(30);
pub const NODE_LEASE_DURATION: Duration = Duration::from_secs(5);

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterState {
    pub committed: TopologySnapshot,
    pub active_change: Option<TopologyChange>,
    #[serde(default)]
    pub superseded_change: Option<TopologyChange>,
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
    #[serde(default)]
    pub recovery_block_reason: String,
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
    #[serde(default)]
    pub retry_count: u32,
    #[serde(default)]
    pub next_attempt_unix_millis: u64,
    #[serde(default)]
    pub last_error: String,
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
            if let Some(superseded_id) = &change.supersedes_change_id
                && self.superseded_change.as_ref().is_none_or(|old| {
                    old.change_id != *superseded_id
                        || old.target_topology.epoch != change.base_epoch
                        || old.phase != MigrationPhase::Published
                })
            {
                return Err(RepositoryError::InvalidState(
                    "recovery change has no matching published predecessor".into(),
                ));
            }
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
            if let Some(base) = &change.base_topology {
                base.validate()?;
                if base.epoch != change.base_epoch
                    || (before_publication && base != &self.committed)
                {
                    return Err(RepositoryError::InvalidState(
                        "active change has an invalid original topology".into(),
                    ));
                }
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
                    retry_count: 0,
                    next_attempt_unix_millis: 0,
                    last_error: String::new(),
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
                superseded_change: None,
                process_instances: BTreeMap::new(),
                stop_confirmations: BTreeMap::new(),
                replica_admissions: Vec::new(),
                replica_repairs: Vec::new(),
                fenced_nodes: BTreeSet::new(),
                recovery_block_reason: String::new(),
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
        superseded_change: None,
        process_instances: BTreeMap::new(),
        stop_confirmations: BTreeMap::new(),
        replica_admissions: Vec::new(),
        replica_repairs: Vec::new(),
        fenced_nodes: BTreeSet::new(),
        recovery_block_reason: String::new(),
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
    post_publish_delay: Duration,
    lease_grants: Arc<Mutex<BTreeMap<String, NodeLeaseGrant>>>,
    peer_failures: Arc<Mutex<BTreeMap<(String, String), PeerFailure>>>,
    pending_change_outages: Arc<Mutex<BTreeMap<String, Instant>>>,
    pending_change_probe_cursor: Arc<Mutex<usize>>,
    status_channels: Arc<Mutex<BTreeMap<String, Channel>>>,
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

mod change;
mod failure;
mod merge;
mod migration_helpers;
mod policy;
mod removal;
mod repair;
mod rpc;

use failure::*;
use merge::*;
use migration_helpers::*;
use removal::*;

#[cfg(test)]
mod tests;
