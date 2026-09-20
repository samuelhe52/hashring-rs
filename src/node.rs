use std::{collections::HashMap, sync::Arc};

use tokio::sync::{Mutex, RwLock};
use tonic::{Request, Response, Status};

use crate::{
    proto::{
        self, ErrorCode, GetRequest, GetResponse, OperationError, PutRequest, PutResponse,
        RecordVersion, coordinator_client::CoordinatorClient, data_node_server::DataNode,
    },
    topology::TopologySnapshot,
};

pub const DEFAULT_MAX_KEY_BYTES: usize = 64 * 1024;
pub const DEFAULT_MAX_VALUE_BYTES: usize = 8 * 1024 * 1024;
/// Accommodates the maximum key and value plus protobuf framing and metadata.
pub const MAX_DATA_MESSAGE_BYTES: usize = 9 * 1024 * 1024;

#[derive(Clone)]
struct Record {
    value: Vec<u8>,
    version: RecordVersion,
}

#[derive(Clone)]
pub struct DataNodeService {
    node_id: String,
    coordinator_endpoint: String,
    topology: Arc<RwLock<TopologySnapshot>>,
    refresh_lock: Arc<Mutex<()>>,
    records: Arc<RwLock<HashMap<Vec<u8>, Record>>>,
    next_sequence: Arc<Mutex<u64>>,
    max_key_bytes: usize,
    max_value_bytes: usize,
}

impl DataNodeService {
    pub async fn connect(node_id: String, coordinator_endpoint: String) -> anyhow::Result<Self> {
        let topology = fetch_topology(&coordinator_endpoint).await?;
        if !topology
            .members
            .iter()
            .any(|member| member.node_id == node_id)
        {
            anyhow::bail!("node {node_id:?} is not present in coordinator topology");
        }
        Ok(Self {
            node_id,
            coordinator_endpoint,
            topology: Arc::new(RwLock::new(topology)),
            refresh_lock: Arc::new(Mutex::new(())),
            records: Arc::new(RwLock::new(HashMap::new())),
            next_sequence: Arc::new(Mutex::new(0)),
            max_key_bytes: DEFAULT_MAX_KEY_BYTES,
            max_value_bytes: DEFAULT_MAX_VALUE_BYTES,
        })
    }

    async fn refresh_if_epoch_differs(&self, request_epoch: u64) -> Result<(), Status> {
        if self.topology.read().await.epoch == request_epoch {
            return Ok(());
        }
        let _guard = self.refresh_lock.lock().await;
        if self.topology.read().await.epoch == request_epoch {
            return Ok(());
        }
        let topology = fetch_topology(&self.coordinator_endpoint)
            .await
            .map_err(|error| Status::unavailable(error.to_string()))?;
        *self.topology.write().await = topology;
        Ok(())
    }

    async fn owner_error(&self, key: &[u8]) -> Result<Option<OperationError>, Status> {
        let topology = self.topology.read().await;
        let owner = topology
            .owner(key)
            .map_err(|error| Status::internal(error.to_string()))?;
        if owner.node_id == self.node_id {
            return Ok(None);
        }
        Ok(Some(OperationError {
            code: ErrorCode::Moved.into(),
            message: format!("key is owned by {}", owner.node_id),
            current_epoch: topology.epoch,
            owner_endpoint: owner.endpoint.clone(),
            retryable: true,
            unknown_write_outcome: false,
        }))
    }

    async fn current_epoch(&self) -> u64 {
        self.topology.read().await.epoch
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
        self.refresh_if_epoch_differs(request.topology_epoch)
            .await?;
        if let Some(error) = self.owner_error(&request.key).await? {
            return Ok(Response::new(GetResponse {
                current_epoch: error.current_epoch,
                error: Some(error),
                ..Default::default()
            }));
        }

        let epoch = self.current_epoch().await;
        let records = self.records.read().await;
        let Some(record) = records.get(&request.key) else {
            return Ok(Response::new(GetResponse {
                current_epoch: epoch,
                error: Some(OperationError {
                    current_epoch: epoch,
                    ..operation_error(ErrorCode::NotFound, "key not found", false)
                }),
                ..Default::default()
            }));
        };
        Ok(Response::new(GetResponse {
            value: record.value.clone(),
            version: Some(record.version.clone()),
            current_epoch: epoch,
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
        self.refresh_if_epoch_differs(request.topology_epoch)
            .await?;
        if let Some(error) = self.owner_error(&request.key).await? {
            return Ok(Response::new(PutResponse {
                current_epoch: error.current_epoch,
                error: Some(error),
                ..Default::default()
            }));
        }

        let epoch = self.current_epoch().await;
        let mut sequence = self.next_sequence.lock().await;
        *sequence += 1;
        let version = RecordVersion {
            topology_epoch: epoch,
            owner_sequence: *sequence,
            owner_node_id: self.node_id.clone(),
        };
        self.records.write().await.insert(
            request.key,
            Record {
                value: request.value,
                version: version.clone(),
            },
        );
        Ok(Response::new(PutResponse {
            version: Some(version),
            current_epoch: epoch,
            error: None,
        }))
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

pub async fn fetch_topology(endpoint: &str) -> anyhow::Result<TopologySnapshot> {
    let mut client = CoordinatorClient::connect(endpoint.to_owned()).await?;
    let response = client.get_topology(proto::Empty {}).await?.into_inner();
    Ok(response.try_into()?)
}
