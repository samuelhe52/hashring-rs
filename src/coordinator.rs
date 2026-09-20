use std::{path::Path, sync::Arc};

use redb::{Database, TableDefinition};
use thiserror::Error;
use tonic::{Request, Response, Status};

use crate::{
    proto::{self, coordinator_server::Coordinator},
    topology::TopologySnapshot,
};

const TOPOLOGY_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("topology");
const COMMITTED_KEY: &str = "committed";

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
    #[error("the coordinator store is empty; bootstrap members are required")]
    MissingBootstrap,
    #[error("configured bootstrap topology differs from durable topology")]
    BootstrapMismatch,
}

pub trait TopologyRepository {
    fn load_committed(&self) -> Result<Option<TopologySnapshot>, RepositoryError>;
    fn store_committed(&self, snapshot: &TopologySnapshot) -> Result<(), RepositoryError>;
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

impl TopologyRepository for RedbTopologyRepository {
    fn load_committed(&self) -> Result<Option<TopologySnapshot>, RepositoryError> {
        let read = self.database.begin_read()?;
        let table = match read.open_table(TOPOLOGY_TABLE) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let Some(bytes) = table.get(COMMITTED_KEY)? else {
            return Ok(None);
        };
        let snapshot: TopologySnapshot = serde_json::from_slice(bytes.value())?;
        snapshot.validate()?;
        Ok(Some(snapshot))
    }

    fn store_committed(&self, snapshot: &TopologySnapshot) -> Result<(), RepositoryError> {
        snapshot.validate()?;
        let encoded = serde_json::to_vec(snapshot)?;
        let write = self.database.begin_write()?;
        {
            let mut table = write.open_table(TOPOLOGY_TABLE)?;
            table.insert(COMMITTED_KEY, encoded.as_slice())?;
        }
        write.commit()?;
        Ok(())
    }
}

pub fn load_or_initialize(
    repository: &impl TopologyRepository,
    bootstrap: Option<TopologySnapshot>,
) -> Result<TopologySnapshot, RepositoryError> {
    if let Some(persisted) = repository.load_committed()? {
        if let Some(bootstrap) = bootstrap
            && persisted != bootstrap
        {
            return Err(RepositoryError::BootstrapMismatch);
        }
        return Ok(persisted);
    }

    let bootstrap = bootstrap.ok_or(RepositoryError::MissingBootstrap)?;
    repository.store_committed(&bootstrap)?;
    Ok(bootstrap)
}

#[derive(Clone)]
pub struct CoordinatorService {
    topology: Arc<TopologySnapshot>,
}

impl CoordinatorService {
    pub fn new(topology: TopologySnapshot) -> Self {
        Self {
            topology: Arc::new(topology),
        }
    }
}

#[tonic::async_trait]
impl Coordinator for CoordinatorService {
    async fn get_topology(
        &self,
        _request: Request<proto::Empty>,
    ) -> Result<Response<proto::TopologySnapshot>, Status> {
        Ok(Response::new(self.topology.as_ref().into()))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;
    use crate::topology::Member;

    #[derive(Default)]
    struct MemoryRepository(Mutex<Option<TopologySnapshot>>);

    impl TopologyRepository for MemoryRepository {
        fn load_committed(&self) -> Result<Option<TopologySnapshot>, RepositoryError> {
            Ok(self.0.lock().unwrap().clone())
        }

        fn store_committed(&self, snapshot: &TopologySnapshot) -> Result<(), RepositoryError> {
            *self.0.lock().unwrap() = Some(snapshot.clone());
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
            load_or_initialize(&repository, Some(expected.clone())).unwrap(),
            expected
        );
        assert_eq!(load_or_initialize(&repository, None).unwrap(), expected);
    }
}
