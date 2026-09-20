use std::{path::Path, sync::Arc};

use redb::{Database, TableDefinition};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::RwLock;
use tonic::{Request, Response, Status};

use crate::{
    migration::{MigrationError, TopologyChange},
    proto::{self, coordinator_server::Coordinator},
    topology::{Member, TopologySnapshot},
};

const TOPOLOGY_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("topology");
const COMMITTED_KEY: &str = "committed";
const CLUSTER_STATE_KEY: &str = "cluster-state-v1";

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterState {
    pub committed: TopologySnapshot,
    pub active_change: Option<TopologyChange>,
}

impl ClusterState {
    fn validate(&self) -> Result<(), RepositoryError> {
        self.committed.validate()?;
        if let Some(change) = &self.active_change {
            change.target_topology.validate()?;
            if change.base_epoch != self.committed.epoch
                || change.target_topology.epoch != self.committed.epoch.saturating_add(1)
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
    Topology(#[from] crate::topology::TopologyError),
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
            let state: ClusterState = serde_json::from_slice(bytes.value())?;
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
    };
    repository.store_state(&state)?;
    Ok(state)
}

#[derive(Clone)]
pub struct CoordinatorService {
    state: Arc<RwLock<ClusterState>>,
    repository: Arc<dyn CoordinatorRepository>,
}

impl CoordinatorService {
    pub fn new(state: ClusterState, repository: Arc<dyn CoordinatorRepository>) -> Self {
        Self {
            state: Arc::new(RwLock::new(state)),
            repository,
        }
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
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;
    use crate::topology::Member;

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
}
