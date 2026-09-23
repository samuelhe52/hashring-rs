use std::{
    collections::HashMap,
    sync::{
        Arc, Weak,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use thiserror::Error;
use tokio::{
    sync::{Mutex, RwLock},
    time::Instant,
};
use tonic::Code;
use tonic::transport::{Channel, Endpoint};

use hashring_core::{
    limits::{DEFAULT_MAX_KEY_BYTES, DEFAULT_MAX_VALUE_BYTES, MAX_DATA_MESSAGE_BYTES},
    migration::TopologyChange,
    proto::{
        self, BeginTopologyChangeRequest, DeleteRequest, ErrorCode, ExecuteTopologyChangeRequest,
        GetRequest, OperationError, PutRequest, RecordVersion,
        coordinator_client::CoordinatorClient, data_node_client::DataNodeClient,
    },
    topology::{Member, TopologyError, TopologySnapshot, WriteAckPolicy},
    transport::{configure_coordinator_client, fetch_topology},
};

#[derive(Clone, Debug)]
pub struct GetOutput {
    pub value: Vec<u8>,
    pub version: RecordVersion,
    pub topology_epoch: u64,
}

#[derive(Clone, Debug)]
pub struct PutOutput {
    pub version: RecordVersion,
    pub topology_epoch: u64,
}

#[derive(Clone, Debug)]
pub struct DeleteOutput {
    pub topology_epoch: u64,
}

#[derive(Clone, Debug, Error)]
#[error("{code:?}: {message}")]
pub struct OperationFailure {
    pub code: ErrorCode,
    pub message: String,
    pub retryable: bool,
    pub unknown_write_outcome: bool,
}

impl From<OperationError> for OperationFailure {
    fn from(error: OperationError) -> Self {
        Self {
            code: ErrorCode::try_from(error.code).unwrap_or(ErrorCode::Unspecified),
            message: error.message,
            retryable: error.retryable,
            unknown_write_outcome: error.unknown_write_outcome,
        }
    }
}

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("failed to fetch or validate topology: {0}")]
    Topology(#[from] anyhow::Error),
    #[error("topology refresh failed: {source} (unknown_write_outcome={unknown_write_outcome})")]
    TopologyRefresh {
        source: anyhow::Error,
        unknown_write_outcome: bool,
    },
    #[error(transparent)]
    Operation(#[from] OperationFailure),
    #[error("logical operation deadline exceeded (unknown_write_outcome={unknown_write_outcome})")]
    DeadlineExceeded { unknown_write_outcome: bool },
    #[error("protocol response omitted record version")]
    MissingVersion,
    #[error(
        "node RPC failed with {code:?}: {message} (unknown_write_outcome={unknown_write_outcome})"
    )]
    Rpc {
        code: Code,
        message: String,
        unknown_write_outcome: bool,
    },
}

#[derive(Debug, Error)]
enum TopologyRefreshError {
    #[error("coordinator transport failed: {0}")]
    Transport(#[from] tonic::transport::Error),
    #[error("coordinator topology RPC failed: {0}")]
    Rpc(Box<tonic::Status>),
    #[error("coordinator returned an invalid topology: {0}")]
    InvalidTopology(#[from] TopologyError),
    #[error(
        "coordinator returned conflicting topology digests for epoch {epoch}: cached={cached_digest}, fetched={fetched_digest}"
    )]
    ConflictingEpoch {
        epoch: u64,
        cached_digest: String,
        fetched_digest: String,
    },
}

impl TopologyRefreshError {
    fn is_retryable(&self) -> bool {
        matches!(self, Self::Transport(_))
            || matches!(self, Self::Rpc(status) if retryable_status(status))
    }
}

const DEFAULT_TOPOLOGY_POLL_INTERVAL: Duration = Duration::from_secs(5);
const POLL_JITTER_PERCENT: u32 = 20;

#[derive(Clone, Debug)]
pub struct ClientConfig {
    pub operation_timeout: Duration,
    pub topology_refresh_timeout: Duration,
    pub topology_poll_interval: Option<Duration>,
}

impl ClientConfig {
    pub fn new(operation_timeout: Duration) -> Self {
        Self {
            operation_timeout,
            topology_refresh_timeout: operation_timeout,
            topology_poll_interval: Some(DEFAULT_TOPOLOGY_POLL_INTERVAL),
        }
    }
}

#[derive(Clone)]
pub struct HashringClient {
    inner: Arc<ClientInner>,
}

struct ClientInner {
    coordinator_endpoint: String,
    topology: RwLock<TopologySnapshot>,
    channels: RwLock<HashMap<String, Channel>>,
    operation_timeout: Duration,
    topology_refresh_timeout: Duration,
    refresh_lock: Mutex<()>,
    refresh_generation: AtomicU64,
    background_refresh_running: AtomicBool,
    background_minimum_epoch: AtomicU64,
    background_trigger_generation: AtomicU64,
}

impl HashringClient {
    pub async fn connect(
        coordinator_endpoint: impl Into<String>,
        operation_timeout: Duration,
    ) -> Result<Self, ClientError> {
        Self::connect_with_config(coordinator_endpoint, ClientConfig::new(operation_timeout)).await
    }

    pub async fn connect_with_config(
        coordinator_endpoint: impl Into<String>,
        config: ClientConfig,
    ) -> Result<Self, ClientError> {
        let coordinator_endpoint = coordinator_endpoint.into();
        let topology = tokio::time::timeout(
            config.operation_timeout,
            fetch_topology(&coordinator_endpoint),
        )
        .await
        .map_err(|_| ClientError::DeadlineExceeded {
            unknown_write_outcome: false,
        })??;
        let inner = Arc::new(ClientInner {
            coordinator_endpoint,
            topology: RwLock::new(topology),
            channels: RwLock::new(HashMap::new()),
            operation_timeout: config.operation_timeout,
            topology_refresh_timeout: config.topology_refresh_timeout,
            refresh_lock: Mutex::new(()),
            refresh_generation: AtomicU64::new(0),
            background_refresh_running: AtomicBool::new(false),
            background_minimum_epoch: AtomicU64::new(0),
            background_trigger_generation: AtomicU64::new(0),
        });
        if let Some(interval) = config
            .topology_poll_interval
            .filter(|value| !value.is_zero())
        {
            tokio::spawn(poll_for_topology_changes(Arc::downgrade(&inner), interval));
        }
        Ok(Self { inner })
    }

    pub async fn topology(&self) -> TopologySnapshot {
        self.inner.topology.read().await.clone()
    }

    pub async fn refresh_topology(&self) -> Result<TopologySnapshot, ClientError> {
        self.refresh_topology_for(self.inner.operation_timeout, false, None)
            .await
    }

    pub async fn begin_topology_change(
        &self,
        target_members: Vec<Member>,
    ) -> Result<TopologyChange, ClientError> {
        self.begin_topology_change_with_policy(target_members, None)
            .await
    }

    pub async fn begin_write_policy_change(
        &self,
        target_policy: WriteAckPolicy,
    ) -> Result<TopologyChange, ClientError> {
        let members = self.topology().await.members;
        self.begin_topology_change_with_policy(members, Some(target_policy))
            .await
    }

    async fn begin_topology_change_with_policy(
        &self,
        target_members: Vec<Member>,
        target_policy: Option<WriteAckPolicy>,
    ) -> Result<TopologyChange, ClientError> {
        let deadline = Instant::now() + self.inner.operation_timeout;
        let mut client = match tokio::time::timeout(
            remaining(deadline, false)?,
            CoordinatorClient::connect(self.inner.coordinator_endpoint.clone()),
        )
        .await
        {
            Ok(Ok(client)) => configure_coordinator_client(client),
            Ok(Err(error)) => return Err(ClientError::Topology(error.into())),
            Err(_) => {
                return Err(ClientError::DeadlineExceeded {
                    unknown_write_outcome: false,
                });
            }
        };
        let request = BeginTopologyChangeRequest {
            target_write_ack_policy: target_policy
                .map(|policy| proto::WriteAckPolicy::from(policy).into()),
            target_members: target_members.iter().map(proto::Member::from).collect(),
        };
        let response = match tokio::time::timeout(
            remaining(deadline, false)?,
            client.begin_topology_change(request),
        )
        .await
        {
            Ok(Ok(response)) => response.into_inner(),
            Ok(Err(status)) => {
                let unknown_write_outcome = status_may_have_applied(&status);
                return Err(rpc_error(status, unknown_write_outcome));
            }
            Err(_) => {
                return Err(ClientError::DeadlineExceeded {
                    unknown_write_outcome: true,
                });
            }
        };
        Ok(response.try_into().map_err(anyhow::Error::from)?)
    }

    pub async fn topology_change(&self) -> Result<Option<TopologyChange>, ClientError> {
        let endpoint = self.inner.coordinator_endpoint.clone();
        let response = tokio::time::timeout(self.inner.operation_timeout, async move {
            let mut client =
                configure_coordinator_client(CoordinatorClient::connect(endpoint).await?);
            let response = client
                .get_topology_change(proto::Empty {})
                .await?
                .into_inner();
            Ok::<_, anyhow::Error>(response)
        })
        .await
        .map_err(|_| ClientError::DeadlineExceeded {
            unknown_write_outcome: false,
        })??;
        response
            .change
            .map(TryInto::try_into)
            .transpose()
            .map_err(anyhow::Error::from)
            .map_err(ClientError::from)
    }

    pub async fn replica_status(&self) -> Result<proto::ReplicaStatusResponse, ClientError> {
        let endpoint = self.inner.coordinator_endpoint.clone();
        tokio::time::timeout(self.inner.operation_timeout, async move {
            let mut client =
                configure_coordinator_client(CoordinatorClient::connect(endpoint).await?);
            Ok::<_, anyhow::Error>(
                client
                    .get_replica_status(proto::Empty {})
                    .await?
                    .into_inner(),
            )
        })
        .await
        .map_err(|_| ClientError::DeadlineExceeded {
            unknown_write_outcome: false,
        })?
        .map_err(ClientError::from)
    }

    pub async fn execute_topology_change(
        &self,
        change_id: impl Into<String>,
        base_epoch: u64,
        target_epoch: u64,
    ) -> Result<TopologyChange, ClientError> {
        let deadline = Instant::now() + self.inner.operation_timeout;
        let mut client = match tokio::time::timeout(
            remaining(deadline, false)?,
            CoordinatorClient::connect(self.inner.coordinator_endpoint.clone()),
        )
        .await
        {
            Ok(Ok(client)) => configure_coordinator_client(client),
            Ok(Err(error)) => return Err(ClientError::Topology(error.into())),
            Err(_) => {
                return Err(ClientError::DeadlineExceeded {
                    unknown_write_outcome: false,
                });
            }
        };
        let response = match tokio::time::timeout(
            remaining(deadline, false)?,
            client.execute_topology_change(ExecuteTopologyChangeRequest {
                change_id: change_id.into(),
                base_epoch,
                target_epoch,
            }),
        )
        .await
        {
            Ok(Ok(response)) => response.into_inner(),
            Ok(Err(status)) => {
                let unknown_write_outcome = status_may_have_applied(&status);
                return Err(rpc_error(status, unknown_write_outcome));
            }
            Err(_) => {
                return Err(ClientError::DeadlineExceeded {
                    unknown_write_outcome: true,
                });
            }
        };
        Ok(response.try_into().map_err(anyhow::Error::from)?)
    }

    pub async fn get(&self, key: Vec<u8>) -> Result<GetOutput, ClientError> {
        validate_key_size(&key)?;
        let deadline = Instant::now() + self.inner.operation_timeout;
        let request_id = uuid::Uuid::new_v4().to_string();
        let mut attempt = 0_u32;

        loop {
            let topology = self.topology().await;
            let owner = topology.owner(&key).map_err(anyhow::Error::from)?.clone();
            let mut client = match self.data_node_client(&owner.endpoint, deadline).await {
                Ok(client) => client,
                Err(ClientError::DeadlineExceeded { .. }) => {
                    return Err(ClientError::DeadlineExceeded {
                        unknown_write_outcome: false,
                    });
                }
                Err(_) => {
                    self.refresh_after_unavailable(topology.epoch, deadline, false)
                        .await?;
                    self.retry_delay(deadline, &mut attempt, false).await?;
                    continue;
                }
            };

            let request = GetRequest {
                key: key.clone(),
                topology_epoch: topology.epoch,
                request_id: request_id.clone(),
            };
            let response = match tokio::time::timeout(
                remaining(deadline, false)?,
                client.get(request),
            )
            .await
            {
                Ok(Ok(response)) => response.into_inner(),
                Ok(Err(status)) => {
                    if retryable_status(&status) {
                        self.refresh_after_unavailable(topology.epoch, deadline, false)
                            .await?;
                        self.retry_delay(deadline, &mut attempt, false).await?;
                        continue;
                    }
                    return Err(rpc_error(status, false));
                }
                Err(_) => {
                    return Err(ClientError::DeadlineExceeded {
                        unknown_write_outcome: false,
                    });
                }
            };

            if let Some(error) = response.error {
                if self
                    .handle_retryable(error.clone(), topology.epoch, deadline, &mut attempt, false)
                    .await?
                {
                    continue;
                }
                return Err(OperationFailure::from(error).into());
            }
            let output = GetOutput {
                value: response.value,
                version: response.version.ok_or(ClientError::MissingVersion)?,
                topology_epoch: response.current_epoch,
            };
            if response.current_epoch > topology.epoch {
                self.trigger_background_refresh(response.current_epoch);
            }
            return Ok(output);
        }
    }

    pub async fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<PutOutput, ClientError> {
        validate_key_size(&key)?;
        if value.len() > DEFAULT_MAX_VALUE_BYTES {
            return Err(size_failure(format!(
                "value exceeds {DEFAULT_MAX_VALUE_BYTES} bytes"
            )));
        }
        let deadline = Instant::now() + self.inner.operation_timeout;
        let request_id = uuid::Uuid::new_v4().to_string();
        let mut attempt = 0_u32;
        let mut unknown_write_outcome = false;

        loop {
            let topology = self.topology().await;
            let owner = topology.owner(&key).map_err(anyhow::Error::from)?.clone();
            let mut client = match self.data_node_client(&owner.endpoint, deadline).await {
                Ok(client) => client,
                Err(ClientError::DeadlineExceeded { .. }) => {
                    return Err(ClientError::DeadlineExceeded {
                        unknown_write_outcome,
                    });
                }
                Err(_) => {
                    self.refresh_after_unavailable(topology.epoch, deadline, unknown_write_outcome)
                        .await?;
                    self.retry_delay(deadline, &mut attempt, unknown_write_outcome)
                        .await?;
                    continue;
                }
            };

            let request = PutRequest {
                key: key.clone(),
                value: value.clone(),
                topology_epoch: topology.epoch,
                request_id: request_id.clone(),
            };
            let response = match tokio::time::timeout(
                remaining(deadline, unknown_write_outcome)?,
                client.put(request),
            )
            .await
            {
                Ok(Ok(response)) => response.into_inner(),
                Ok(Err(status)) => {
                    unknown_write_outcome =
                        accumulated_write_ambiguity(unknown_write_outcome, &status);
                    if retryable_status(&status) {
                        self.refresh_after_unavailable(
                            topology.epoch,
                            deadline,
                            unknown_write_outcome,
                        )
                        .await?;
                        self.retry_delay(deadline, &mut attempt, unknown_write_outcome)
                            .await?;
                        continue;
                    }
                    return Err(rpc_error(status, unknown_write_outcome));
                }
                Err(_) => {
                    return Err(ClientError::DeadlineExceeded {
                        unknown_write_outcome: true,
                    });
                }
            };

            if let Some(error) = response.error {
                let error_unknown_write_outcome =
                    unknown_write_outcome || error.unknown_write_outcome;
                if self
                    .handle_retryable(
                        error.clone(),
                        topology.epoch,
                        deadline,
                        &mut attempt,
                        error_unknown_write_outcome,
                    )
                    .await?
                {
                    continue;
                }
                let mut failure = OperationFailure::from(error);
                failure.unknown_write_outcome = error_unknown_write_outcome;
                return Err(failure.into());
            }
            let output = PutOutput {
                version: response.version.ok_or(ClientError::MissingVersion)?,
                topology_epoch: response.current_epoch,
            };
            if response.current_epoch > topology.epoch {
                self.trigger_background_refresh(response.current_epoch);
            }
            return Ok(output);
        }
    }

    pub async fn delete(&self, key: Vec<u8>) -> Result<DeleteOutput, ClientError> {
        validate_key_size(&key)?;
        let deadline = Instant::now() + self.inner.operation_timeout;
        let request_id = uuid::Uuid::new_v4().to_string();
        let mut attempt = 0_u32;
        let mut unknown_write_outcome = false;

        loop {
            let topology = self.topology().await;
            let owner = topology.owner(&key).map_err(anyhow::Error::from)?.clone();
            let mut client = match self.data_node_client(&owner.endpoint, deadline).await {
                Ok(client) => client,
                Err(ClientError::DeadlineExceeded { .. }) => {
                    return Err(ClientError::DeadlineExceeded {
                        unknown_write_outcome,
                    });
                }
                Err(_) => {
                    self.refresh_after_unavailable(topology.epoch, deadline, unknown_write_outcome)
                        .await?;
                    self.retry_delay(deadline, &mut attempt, unknown_write_outcome)
                        .await?;
                    continue;
                }
            };

            let request = DeleteRequest {
                key: key.clone(),
                topology_epoch: topology.epoch,
                request_id: request_id.clone(),
            };
            let response = match tokio::time::timeout(
                remaining(deadline, unknown_write_outcome)?,
                client.delete(request),
            )
            .await
            {
                Ok(Ok(response)) => response.into_inner(),
                Ok(Err(status)) => {
                    unknown_write_outcome =
                        accumulated_write_ambiguity(unknown_write_outcome, &status);
                    if retryable_status(&status) {
                        self.refresh_after_unavailable(
                            topology.epoch,
                            deadline,
                            unknown_write_outcome,
                        )
                        .await?;
                        self.retry_delay(deadline, &mut attempt, unknown_write_outcome)
                            .await?;
                        continue;
                    }
                    return Err(rpc_error(status, unknown_write_outcome));
                }
                Err(_) => {
                    return Err(ClientError::DeadlineExceeded {
                        unknown_write_outcome: true,
                    });
                }
            };

            if let Some(error) = response.error {
                let error_unknown_write_outcome =
                    unknown_write_outcome || error.unknown_write_outcome;
                if self
                    .handle_retryable(
                        error.clone(),
                        topology.epoch,
                        deadline,
                        &mut attempt,
                        error_unknown_write_outcome,
                    )
                    .await?
                {
                    continue;
                }
                let mut failure = OperationFailure::from(error);
                failure.unknown_write_outcome = error_unknown_write_outcome;
                return Err(failure.into());
            }
            let output = DeleteOutput {
                topology_epoch: response.current_epoch,
            };
            if response.current_epoch > topology.epoch {
                self.trigger_background_refresh(response.current_epoch);
            }
            return Ok(output);
        }
    }

    async fn handle_retryable(
        &self,
        error: OperationError,
        attempted_epoch: u64,
        deadline: Instant,
        attempt: &mut u32,
        unknown_write_outcome: bool,
    ) -> Result<bool, ClientError> {
        let code = ErrorCode::try_from(error.code).unwrap_or(ErrorCode::Unspecified);
        match code {
            ErrorCode::Moved => {
                let required_epoch = error.current_epoch.max(attempted_epoch.saturating_add(1));
                self.refresh_topology_before(deadline, unknown_write_outcome, Some(required_epoch))
                    .await?;
                self.retry_delay(deadline, attempt, unknown_write_outcome)
                    .await?;
                Ok(true)
            }
            ErrorCode::Unavailable if error.retryable => {
                self.refresh_after_unavailable(attempted_epoch, deadline, unknown_write_outcome)
                    .await?;
                self.retry_delay(deadline, attempt, unknown_write_outcome)
                    .await?;
                Ok(true)
            }
            ErrorCode::RangeBusy
            | ErrorCode::ResourceExhausted
            | ErrorCode::TemporarilyUnavailable
            | ErrorCode::ReplicaNotReady
            | ErrorCode::OutcomeUnknown
                if error.retryable =>
            {
                if error.current_epoch > attempted_epoch {
                    self.refresh_topology_before(
                        deadline,
                        unknown_write_outcome,
                        Some(error.current_epoch),
                    )
                    .await?;
                }
                self.retry_delay(deadline, attempt, unknown_write_outcome)
                    .await?;
                Ok(true)
            }
            ErrorCode::LeaseExpired if error.retryable => {
                self.refresh_topology_before(deadline, unknown_write_outcome, None)
                    .await?;
                self.retry_delay(deadline, attempt, unknown_write_outcome)
                    .await?;
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    async fn refresh_topology_before(
        &self,
        deadline: Instant,
        unknown_write_outcome: bool,
        minimum_epoch: Option<u64>,
    ) -> Result<TopologySnapshot, ClientError> {
        let available = remaining(deadline, unknown_write_outcome)?;
        self.refresh_topology_for(available, unknown_write_outcome, minimum_epoch)
            .await
    }

    async fn refresh_topology_for(
        &self,
        timeout: Duration,
        unknown_write_outcome: bool,
        minimum_epoch: Option<u64>,
    ) -> Result<TopologySnapshot, ClientError> {
        refresh_topology_for_inner(&self.inner, timeout, unknown_write_outcome, minimum_epoch).await
    }

    async fn refresh_after_unavailable(
        &self,
        observed_epoch: u64,
        deadline: Instant,
        unknown_write_outcome: bool,
    ) -> Result<(), ClientError> {
        if self.inner.topology.read().await.epoch > observed_epoch {
            return Ok(());
        }
        let generation = self.inner.refresh_generation.load(Ordering::SeqCst);
        let _guard = tokio::time::timeout(
            remaining(deadline, unknown_write_outcome)?,
            self.inner.refresh_lock.lock(),
        )
        .await
        .map_err(|_| ClientError::DeadlineExceeded {
            unknown_write_outcome,
        })?;
        if self.inner.topology.read().await.epoch > observed_epoch
            || self.inner.refresh_generation.load(Ordering::SeqCst) != generation
        {
            return Ok(());
        }
        match tokio::time::timeout(
            remaining(deadline, unknown_write_outcome)?,
            fetch_topology_for_refresh(&self.inner.coordinator_endpoint),
        )
        .await
        {
            Ok(Ok(topology)) => {
                let mut current = self.inner.topology.write().await;
                if topology.epoch > current.epoch {
                    *current = topology;
                } else if topology.epoch == current.epoch && topology != *current {
                    return Err(ClientError::TopologyRefresh {
                        source: TopologyRefreshError::ConflictingEpoch {
                            epoch: topology.epoch,
                            cached_digest: current.digest.clone(),
                            fetched_digest: topology.digest,
                        }
                        .into(),
                        unknown_write_outcome,
                    });
                }
                self.inner.refresh_generation.fetch_add(1, Ordering::SeqCst);
            }
            Ok(Err(_)) => {}
            Err(_) => {
                return Err(ClientError::DeadlineExceeded {
                    unknown_write_outcome,
                });
            }
        }
        Ok(())
    }

    fn trigger_background_refresh(&self, minimum_epoch: u64) {
        self.inner
            .background_minimum_epoch
            .fetch_max(minimum_epoch, Ordering::SeqCst);
        self.inner
            .background_trigger_generation
            .fetch_add(1, Ordering::SeqCst);
        if self
            .inner
            .background_refresh_running
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }
        let inner = Arc::clone(&self.inner);
        tokio::spawn(run_background_refresh(inner));
    }

    async fn retry_delay(
        &self,
        deadline: Instant,
        attempt: &mut u32,
        unknown_write_outcome: bool,
    ) -> Result<(), ClientError> {
        retry_delay(deadline, attempt, unknown_write_outcome).await
    }

    async fn data_node_client(
        &self,
        endpoint: &str,
        deadline: Instant,
    ) -> Result<DataNodeClient<Channel>, ClientError> {
        if let Some(channel) = self.inner.channels.read().await.get(endpoint).cloned() {
            return Ok(configure_data_client(DataNodeClient::new(channel)));
        }
        let transport = Endpoint::from_shared(endpoint.to_owned())
            .map_err(anyhow::Error::from)
            .map_err(ClientError::from)?;
        let channel = tokio::time::timeout(remaining(deadline, false)?, transport.connect())
            .await
            .map_err(|_| ClientError::DeadlineExceeded {
                unknown_write_outcome: false,
            })?
            .map_err(anyhow::Error::from)
            .map_err(ClientError::from)?;
        self.inner
            .channels
            .write()
            .await
            .insert(endpoint.to_owned(), channel.clone());
        Ok(configure_data_client(DataNodeClient::new(channel)))
    }
}

async fn refresh_topology_for_inner(
    inner: &Arc<ClientInner>,
    timeout: Duration,
    unknown_write_outcome: bool,
    minimum_epoch: Option<u64>,
) -> Result<TopologySnapshot, ClientError> {
    let deadline = Instant::now() + timeout;
    if let Some(minimum_epoch) = minimum_epoch
        && inner.topology.read().await.epoch >= minimum_epoch
    {
        return Ok(inner.topology.read().await.clone());
    }
    let _refresh = tokio::time::timeout(
        remaining(deadline, unknown_write_outcome)?,
        inner.refresh_lock.lock(),
    )
    .await
    .map_err(|_| ClientError::DeadlineExceeded {
        unknown_write_outcome,
    })?;
    if let Some(minimum_epoch) = minimum_epoch
        && inner.topology.read().await.epoch >= minimum_epoch
    {
        return Ok(inner.topology.read().await.clone());
    }
    let mut attempt = 0_u32;
    loop {
        let fetched = tokio::time::timeout(
            remaining(deadline, unknown_write_outcome)?,
            fetch_topology_for_refresh(&inner.coordinator_endpoint),
        )
        .await;
        match fetched {
            Ok(Ok(topology)) => {
                let mut current = inner.topology.write().await;
                if topology.epoch > current.epoch {
                    *current = topology;
                } else if topology.epoch == current.epoch && topology != *current {
                    return Err(ClientError::TopologyRefresh {
                        source: TopologyRefreshError::ConflictingEpoch {
                            epoch: topology.epoch,
                            cached_digest: current.digest.clone(),
                            fetched_digest: topology.digest,
                        }
                        .into(),
                        unknown_write_outcome,
                    });
                }
                inner.refresh_generation.fetch_add(1, Ordering::SeqCst);
                if minimum_epoch.is_none_or(|minimum| current.epoch >= minimum) {
                    return Ok(current.clone());
                }
                drop(current);
                retry_delay(deadline, &mut attempt, unknown_write_outcome).await?;
            }
            Ok(Err(error)) if error.is_retryable() => {
                retry_delay(deadline, &mut attempt, unknown_write_outcome).await?;
            }
            Ok(Err(error)) => {
                return Err(ClientError::TopologyRefresh {
                    source: error.into(),
                    unknown_write_outcome,
                });
            }
            Err(_) => {
                return Err(ClientError::DeadlineExceeded {
                    unknown_write_outcome,
                });
            }
        }
    }
}

async fn retry_delay(
    deadline: Instant,
    attempt: &mut u32,
    unknown_write_outcome: bool,
) -> Result<(), ClientError> {
    let base_ms = 5_u64.saturating_mul(1_u64 << (*attempt).min(5));
    let jitter_ms = rand::random::<u64>() % (base_ms + 1);
    *attempt = attempt.saturating_add(1);
    let delay = Duration::from_millis((base_ms + jitter_ms).min(200));
    let available = remaining(deadline, unknown_write_outcome)?;
    if delay >= available {
        tokio::time::sleep(available).await;
        return Err(ClientError::DeadlineExceeded {
            unknown_write_outcome,
        });
    }
    tokio::time::sleep(delay).await;
    Ok(())
}

fn configure_data_client(
    client: DataNodeClient<tonic::transport::Channel>,
) -> DataNodeClient<tonic::transport::Channel> {
    client
        .max_decoding_message_size(MAX_DATA_MESSAGE_BYTES)
        .max_encoding_message_size(MAX_DATA_MESSAGE_BYTES)
}

fn retryable_status(status: &tonic::Status) -> bool {
    status.code() == Code::Unavailable
}

async fn poll_for_topology_changes(inner: Weak<ClientInner>, interval: Duration) {
    loop {
        tokio::time::sleep(jittered_poll_delay(interval)).await;
        let Some(inner) = inner.upgrade() else {
            return;
        };
        let refresh_timeout = inner.topology_refresh_timeout;
        let observed_epoch = match tokio::time::timeout(
            refresh_timeout,
            fetch_current_epoch(&inner.coordinator_endpoint),
        )
        .await
        {
            Ok(Ok(epoch)) => epoch,
            Ok(Err(_)) | Err(_) => continue,
        };
        if inner.topology.read().await.epoch < observed_epoch {
            let _ =
                refresh_topology_for_inner(&inner, refresh_timeout, false, Some(observed_epoch))
                    .await;
        }
    }
}

async fn run_background_refresh(inner: Arc<ClientInner>) {
    loop {
        let handled_generation = inner.background_trigger_generation.load(Ordering::SeqCst);
        let requested_epoch = inner.background_minimum_epoch.load(Ordering::SeqCst);
        let _ = refresh_topology_for_inner(
            &inner,
            inner.topology_refresh_timeout,
            false,
            Some(requested_epoch),
        )
        .await;

        let newest_generation = inner.background_trigger_generation.load(Ordering::SeqCst);
        if newest_generation > handled_generation {
            continue;
        }
        inner
            .background_refresh_running
            .store(false, Ordering::SeqCst);
        let raced_generation = inner.background_trigger_generation.load(Ordering::SeqCst);
        if raced_generation > newest_generation
            && inner
                .background_refresh_running
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        {
            continue;
        }
        return;
    }
}

fn jittered_poll_delay(interval: Duration) -> Duration {
    let jitter_steps = POLL_JITTER_PERCENT.saturating_mul(2).saturating_add(1);
    let percent = 100_u32
        .saturating_sub(POLL_JITTER_PERCENT)
        .saturating_add(rand::random::<u32>() % jitter_steps);
    interval.mul_f64(f64::from(percent) / 100.0)
}

async fn fetch_current_epoch(endpoint: &str) -> Result<u64, TopologyRefreshError> {
    let mut client =
        configure_coordinator_client(CoordinatorClient::connect(endpoint.to_owned()).await?);
    Ok(client
        .get_current_epoch(proto::Empty {})
        .await
        .map_err(|status| TopologyRefreshError::Rpc(Box::new(status)))?
        .into_inner()
        .epoch)
}

async fn fetch_topology_for_refresh(
    endpoint: &str,
) -> Result<TopologySnapshot, TopologyRefreshError> {
    let mut client =
        configure_coordinator_client(CoordinatorClient::connect(endpoint.to_owned()).await?);
    let response = client
        .get_topology(proto::Empty {})
        .await
        .map_err(|status| TopologyRefreshError::Rpc(Box::new(status)))?
        .into_inner();
    Ok(response.try_into()?)
}

fn status_may_have_applied(status: &tonic::Status) -> bool {
    matches!(
        status.code(),
        Code::Cancelled
            | Code::Unknown
            | Code::DeadlineExceeded
            | Code::Internal
            | Code::Unavailable
            | Code::DataLoss
    )
}

fn accumulated_write_ambiguity(already_unknown: bool, status: &tonic::Status) -> bool {
    already_unknown || status_may_have_applied(status)
}

fn rpc_error(status: tonic::Status, unknown_write_outcome: bool) -> ClientError {
    ClientError::Rpc {
        code: status.code(),
        message: status.message().to_owned(),
        unknown_write_outcome,
    }
}

fn validate_key_size(key: &[u8]) -> Result<(), ClientError> {
    if key.len() > DEFAULT_MAX_KEY_BYTES {
        return Err(size_failure(format!(
            "key exceeds {DEFAULT_MAX_KEY_BYTES} bytes"
        )));
    }
    Ok(())
}

fn size_failure(message: String) -> ClientError {
    OperationFailure {
        code: ErrorCode::TooLarge,
        message,
        retryable: false,
        unknown_write_outcome: false,
    }
    .into()
}

fn remaining(deadline: Instant, unknown_write_outcome: bool) -> Result<Duration, ClientError> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|remaining| !remaining.is_zero())
        .ok_or(ClientError::DeadlineExceeded {
            unknown_write_outcome,
        })
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use hashring_core::proto::coordinator_server::{Coordinator, CoordinatorServer};
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::{Request, Response, Status};

    use super::*;

    #[derive(Clone)]
    struct FakeCoordinator {
        topology: Arc<RwLock<TopologySnapshot>>,
        topology_calls: Arc<AtomicUsize>,
        epoch_calls: Arc<AtomicUsize>,
        topology_delay: Duration,
        topology_error: Arc<std::sync::Mutex<Option<Code>>>,
        topology_failures_remaining: Arc<AtomicUsize>,
    }

    #[tonic::async_trait]
    impl Coordinator for FakeCoordinator {
        async fn get_replica_status(
            &self,
            _request: Request<proto::Empty>,
        ) -> Result<Response<proto::ReplicaStatusResponse>, Status> {
            Err(Status::unimplemented("unused by client tests"))
        }

        async fn get_topology(
            &self,
            _request: Request<proto::Empty>,
        ) -> Result<Response<proto::TopologySnapshot>, Status> {
            self.topology_calls.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(self.topology_delay).await;
            if self
                .topology_failures_remaining
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
            {
                let code = self.topology_error.lock().unwrap().unwrap();
                return Err(Status::new(code, "injected topology failure"));
            }
            Ok(Response::new((&*self.topology.read().await).into()))
        }

        async fn get_current_epoch(
            &self,
            _request: Request<proto::Empty>,
        ) -> Result<Response<proto::CurrentEpochResponse>, Status> {
            self.epoch_calls.fetch_add(1, Ordering::SeqCst);
            Ok(Response::new(proto::CurrentEpochResponse {
                epoch: self.topology.read().await.epoch,
            }))
        }

        async fn begin_topology_change(
            &self,
            _request: Request<proto::BeginTopologyChangeRequest>,
        ) -> Result<Response<proto::TopologyChangeSnapshot>, Status> {
            Err(Status::unimplemented("unused by client tests"))
        }

        async fn get_topology_change(
            &self,
            _request: Request<proto::Empty>,
        ) -> Result<Response<proto::GetTopologyChangeResponse>, Status> {
            Err(Status::unimplemented("unused by client tests"))
        }

        async fn execute_topology_change(
            &self,
            _request: Request<proto::ExecuteTopologyChangeRequest>,
        ) -> Result<Response<proto::TopologyChangeSnapshot>, Status> {
            Err(Status::unimplemented("unused by client tests"))
        }

        async fn register_node(
            &self,
            _request: Request<proto::RegisterNodeRequest>,
        ) -> Result<Response<proto::Empty>, Status> {
            Err(Status::unimplemented("unused by client tests"))
        }

        async fn renew_node_lease(
            &self,
            _request: Request<proto::RenewNodeLeaseRequest>,
        ) -> Result<Response<proto::RenewNodeLeaseResponse>, Status> {
            Err(Status::unimplemented("unused by client tests"))
        }

        async fn report_peer_health(
            &self,
            _request: Request<proto::ReportPeerHealthRequest>,
        ) -> Result<Response<proto::Empty>, Status> {
            Err(Status::unimplemented("unused by client tests"))
        }

        async fn is_stop_confirmed(
            &self,
            _request: Request<proto::StopRequest>,
        ) -> Result<Response<proto::StopConfirmationResponse>, Status> {
            Err(Status::unimplemented("unused by client tests"))
        }
    }

    fn test_topology(epoch: u64) -> TopologySnapshot {
        TopologySnapshot::new(
            epoch,
            1,
            1,
            vec![Member {
                node_id: "node-1".into(),
                endpoint: "http://127.0.0.1:1".into(),
            }],
        )
        .unwrap()
    }

    async fn start_fake_coordinator(
        topology_delay: Duration,
    ) -> (String, FakeCoordinator, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let service = FakeCoordinator {
            topology: Arc::new(RwLock::new(test_topology(1))),
            topology_calls: Arc::new(AtomicUsize::new(0)),
            epoch_calls: Arc::new(AtomicUsize::new(0)),
            topology_delay,
            topology_error: Arc::new(std::sync::Mutex::new(None)),
            topology_failures_remaining: Arc::new(AtomicUsize::new(0)),
        };
        let server_service = service.clone();
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(CoordinatorServer::new(server_service))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        let endpoint = format!("http://{address}");
        (endpoint, service, server)
    }

    #[test]
    fn write_ambiguity_is_sticky_across_later_permanent_errors() {
        let unavailable = tonic::Status::unavailable("response was lost");
        let unimplemented = tonic::Status::unimplemented("wrong endpoint");

        let unknown = accumulated_write_ambiguity(false, &unavailable);
        assert!(unknown);
        assert!(accumulated_write_ambiguity(unknown, &unimplemented));
        assert!(!accumulated_write_ambiguity(false, &unimplemented));
    }

    #[test]
    fn topology_refresh_retries_only_transient_rpc_failures() {
        assert!(
            TopologyRefreshError::Rpc(Box::new(tonic::Status::unavailable(
                "coordinator is restarting",
            )))
            .is_retryable()
        );
        assert!(
            !TopologyRefreshError::Rpc(Box::new(tonic::Status::invalid_argument(
                "malformed request",
            )))
            .is_retryable()
        );
        assert!(!TopologyRefreshError::InvalidTopology(TopologyError::NoMembers).is_retryable());
    }

    #[tokio::test]
    async fn idle_polling_fetches_topology_only_after_the_epoch_advances() {
        let (endpoint, service, server) = start_fake_coordinator(Duration::ZERO).await;
        let mut config = ClientConfig::new(Duration::from_secs(1));
        config.topology_poll_interval = Some(Duration::from_millis(20));
        let client = HashringClient::connect_with_config(endpoint, config)
            .await
            .unwrap();

        for _ in 0..50 {
            if service.epoch_calls.load(Ordering::SeqCst) >= 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(service.epoch_calls.load(Ordering::SeqCst) >= 2);
        assert_eq!(service.topology_calls.load(Ordering::SeqCst), 1);
        *service.topology.write().await = test_topology(2);
        for _ in 0..50 {
            if client.topology().await.epoch == 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(client.topology().await.epoch, 2);
        assert_eq!(service.topology_calls.load(Ordering::SeqCst), 2);

        *service.topology.write().await = test_topology(1);
        assert_eq!(client.refresh_topology().await.unwrap().epoch, 2);
        assert_eq!(client.topology().await.epoch, 2);

        server.abort();
    }

    #[tokio::test]
    async fn background_refresh_triggers_are_task_and_network_single_flight() {
        let (endpoint, service, server) = start_fake_coordinator(Duration::from_millis(50)).await;
        let mut config = ClientConfig::new(Duration::from_secs(1));
        config.topology_poll_interval = None;
        let client = HashringClient::connect_with_config(endpoint, config)
            .await
            .unwrap();
        *service.topology.write().await = test_topology(2);
        *service.topology_error.lock().unwrap() = Some(Code::InvalidArgument);
        service
            .topology_failures_remaining
            .store(1, Ordering::SeqCst);

        client.trigger_background_refresh(2);
        for _ in 0..100 {
            if service.topology_calls.load(Ordering::SeqCst) >= 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        assert_eq!(service.topology_calls.load(Ordering::SeqCst), 2);
        *service.topology.write().await = test_topology(3);
        for _ in 0..1_000 {
            client.trigger_background_refresh(3);
        }
        for _ in 0..100 {
            if client.topology().await.epoch == 3 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(client.topology().await.epoch, 3);
        assert_eq!(service.topology_calls.load(Ordering::SeqCst), 3);
        server.abort();
    }

    #[tokio::test]
    async fn unavailable_refresh_rejects_a_conflicting_equal_epoch() {
        let (endpoint, service, server) = start_fake_coordinator(Duration::ZERO).await;
        let mut config = ClientConfig::new(Duration::from_secs(1));
        config.topology_poll_interval = None;
        let client = HashringClient::connect_with_config(endpoint, config)
            .await
            .unwrap();
        *service.topology.write().await = TopologySnapshot::new(
            1,
            1,
            1,
            vec![Member {
                node_id: "node-2".into(),
                endpoint: "http://127.0.0.1:2".into(),
            }],
        )
        .unwrap();

        assert!(matches!(
            client
                .refresh_after_unavailable(1, Instant::now() + Duration::from_secs(1), true)
                .await,
            Err(ClientError::TopologyRefresh {
                unknown_write_outcome: true,
                ..
            })
        ));
        assert_eq!(client.topology().await, test_topology(1));
        server.abort();
    }

    #[tokio::test]
    async fn permanent_moved_refresh_failure_preserves_unknown_write_outcome() {
        let (endpoint, service, server) = start_fake_coordinator(Duration::ZERO).await;
        let mut config = ClientConfig::new(Duration::from_secs(1));
        config.topology_poll_interval = None;
        let client = HashringClient::connect_with_config(endpoint, config)
            .await
            .unwrap();
        *service.topology_error.lock().unwrap() = Some(Code::InvalidArgument);
        service
            .topology_failures_remaining
            .store(1, Ordering::SeqCst);

        let error = client
            .handle_retryable(
                OperationError {
                    code: ErrorCode::Moved.into(),
                    current_epoch: 2,
                    retryable: true,
                    ..Default::default()
                },
                1,
                Instant::now() + Duration::from_secs(1),
                &mut 0,
                true,
            )
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            ClientError::TopologyRefresh {
                unknown_write_outcome: true,
                ..
            }
        ));
        server.abort();
    }

    #[tokio::test]
    async fn moved_refresh_preserves_an_unknown_write_outcome_through_its_deadline() {
        let topology = TopologySnapshot::new(
            1,
            1,
            1,
            vec![Member {
                node_id: "node-1".into(),
                endpoint: "http://127.0.0.1:1".into(),
            }],
        )
        .unwrap();
        let client = HashringClient {
            inner: Arc::new(ClientInner {
                coordinator_endpoint: "http://127.0.0.1:1".into(),
                topology: RwLock::new(topology),
                channels: RwLock::new(HashMap::new()),
                operation_timeout: Duration::from_millis(100),
                topology_refresh_timeout: Duration::from_millis(100),
                refresh_lock: Mutex::new(()),
                refresh_generation: AtomicU64::new(0),
                background_refresh_running: AtomicBool::new(false),
                background_minimum_epoch: AtomicU64::new(0),
                background_trigger_generation: AtomicU64::new(0),
            }),
        };
        let deadline = Instant::now() + client.inner.operation_timeout;
        let error = client
            .handle_retryable(
                OperationError {
                    code: ErrorCode::Moved.into(),
                    retryable: true,
                    ..Default::default()
                },
                1,
                deadline,
                &mut 0,
                true,
            )
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            ClientError::DeadlineExceeded {
                unknown_write_outcome: true
            }
        ));
    }

    #[tokio::test]
    async fn degraded_and_unknown_write_errors_retry_without_forcing_same_epoch_refresh() {
        let (endpoint, coordinator, server) = start_fake_coordinator(Duration::ZERO).await;
        let client = HashringClient::connect(endpoint, Duration::from_secs(1))
            .await
            .unwrap();
        let initial_calls = coordinator.topology_calls.load(Ordering::SeqCst);
        let deadline = Instant::now() + Duration::from_secs(1);
        let mut attempt = 0;
        for (code, unknown) in [
            (ErrorCode::TemporarilyUnavailable, false),
            (ErrorCode::OutcomeUnknown, true),
        ] {
            assert!(
                client
                    .handle_retryable(
                        OperationError {
                            code: code.into(),
                            current_epoch: 1,
                            retryable: true,
                            unknown_write_outcome: unknown,
                            ..Default::default()
                        },
                        1,
                        deadline,
                        &mut attempt,
                        unknown
                    )
                    .await
                    .unwrap()
            );
        }
        assert_eq!(attempt, 2);
        assert_eq!(
            coordinator.topology_calls.load(Ordering::SeqCst),
            initial_calls
        );
        server.abort();
    }
}
