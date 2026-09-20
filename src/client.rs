use std::{sync::Arc, time::Duration};

use thiserror::Error;
use tokio::{sync::RwLock, time::Instant};
use tonic::Code;

use crate::{
    node::{
        DEFAULT_MAX_KEY_BYTES, DEFAULT_MAX_VALUE_BYTES, MAX_DATA_MESSAGE_BYTES, fetch_topology,
    },
    proto::{
        ErrorCode, GetRequest, OperationError, PutRequest, RecordVersion,
        data_node_client::DataNodeClient,
    },
    topology::TopologySnapshot,
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

#[derive(Clone)]
pub struct HashringClient {
    coordinator_endpoint: String,
    topology: Arc<RwLock<TopologySnapshot>>,
    operation_timeout: Duration,
}

impl HashringClient {
    pub async fn connect(
        coordinator_endpoint: impl Into<String>,
        operation_timeout: Duration,
    ) -> Result<Self, ClientError> {
        let coordinator_endpoint = coordinator_endpoint.into();
        let topology =
            tokio::time::timeout(operation_timeout, fetch_topology(&coordinator_endpoint))
                .await
                .map_err(|_| ClientError::DeadlineExceeded {
                    unknown_write_outcome: false,
                })??;
        Ok(Self {
            coordinator_endpoint,
            topology: Arc::new(RwLock::new(topology)),
            operation_timeout,
        })
    }

    pub async fn topology(&self) -> TopologySnapshot {
        self.topology.read().await.clone()
    }

    pub async fn refresh_topology(&self) -> Result<TopologySnapshot, ClientError> {
        self.refresh_topology_for(self.operation_timeout, false)
            .await
    }

    pub async fn get(&self, key: Vec<u8>) -> Result<GetOutput, ClientError> {
        validate_key_size(&key)?;
        let deadline = Instant::now() + self.operation_timeout;
        let request_id = uuid::Uuid::new_v4().to_string();
        let mut attempt = 0_u32;

        loop {
            let topology = self.topology().await;
            let owner = topology.owner(&key).map_err(anyhow::Error::from)?.clone();
            let available = remaining(deadline, false)?;
            let mut client = match tokio::time::timeout(
                available,
                DataNodeClient::connect(owner.endpoint.clone()),
            )
            .await
            {
                Ok(Ok(client)) => configure_data_client(client),
                Ok(Err(_)) => {
                    self.retry_delay(deadline, &mut attempt, false).await?;
                    continue;
                }
                Err(_) => {
                    return Err(ClientError::DeadlineExceeded {
                        unknown_write_outcome: false,
                    });
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
                    .handle_retryable(error.clone(), deadline, &mut attempt, false)
                    .await?
                {
                    continue;
                }
                return Err(OperationFailure::from(error).into());
            }
            if response.current_epoch > topology.epoch {
                let _ = self.refresh_topology_before(deadline, false).await;
            }
            return Ok(GetOutput {
                value: response.value,
                version: response.version.ok_or(ClientError::MissingVersion)?,
                topology_epoch: response.current_epoch,
            });
        }
    }

    pub async fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<PutOutput, ClientError> {
        validate_key_size(&key)?;
        if value.len() > DEFAULT_MAX_VALUE_BYTES {
            return Err(size_failure(format!(
                "value exceeds {DEFAULT_MAX_VALUE_BYTES} bytes"
            )));
        }
        let deadline = Instant::now() + self.operation_timeout;
        let request_id = uuid::Uuid::new_v4().to_string();
        let mut attempt = 0_u32;
        let mut unknown_write_outcome = false;

        loop {
            let topology = self.topology().await;
            let owner = topology.owner(&key).map_err(anyhow::Error::from)?.clone();
            let available = remaining(deadline, unknown_write_outcome)?;
            let mut client = match tokio::time::timeout(
                available,
                DataNodeClient::connect(owner.endpoint.clone()),
            )
            .await
            {
                Ok(Ok(client)) => configure_data_client(client),
                Ok(Err(_)) => {
                    self.retry_delay(deadline, &mut attempt, unknown_write_outcome)
                        .await?;
                    continue;
                }
                Err(_) => {
                    return Err(ClientError::DeadlineExceeded {
                        unknown_write_outcome,
                    });
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
            if response.current_epoch > topology.epoch {
                let _ = self.refresh_topology_before(deadline, false).await;
            }
            return Ok(PutOutput {
                version: response.version.ok_or(ClientError::MissingVersion)?,
                topology_epoch: response.current_epoch,
            });
        }
    }

    async fn handle_retryable(
        &self,
        error: OperationError,
        deadline: Instant,
        attempt: &mut u32,
        unknown_write_outcome: bool,
    ) -> Result<bool, ClientError> {
        let code = ErrorCode::try_from(error.code).unwrap_or(ErrorCode::Unspecified);
        match code {
            ErrorCode::Moved => {
                self.refresh_topology_before(deadline, unknown_write_outcome)
                    .await?;
                self.retry_delay(deadline, attempt, unknown_write_outcome)
                    .await?;
                Ok(true)
            }
            ErrorCode::RangeBusy | ErrorCode::Unavailable if error.retryable => {
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
    ) -> Result<TopologySnapshot, ClientError> {
        let available = remaining(deadline, unknown_write_outcome)?;
        self.refresh_topology_for(available, unknown_write_outcome)
            .await
    }

    async fn refresh_topology_for(
        &self,
        timeout: Duration,
        unknown_write_outcome: bool,
    ) -> Result<TopologySnapshot, ClientError> {
        let topology = tokio::time::timeout(timeout, fetch_topology(&self.coordinator_endpoint))
            .await
            .map_err(|_| ClientError::DeadlineExceeded {
                unknown_write_outcome,
            })??;
        let mut current = self.topology.write().await;
        if topology.epoch >= current.epoch {
            *current = topology.clone();
        }
        Ok(current.clone())
    }

    async fn retry_delay(
        &self,
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
            return Err(ClientError::DeadlineExceeded {
                unknown_write_outcome,
            });
        }
        tokio::time::sleep(delay).await;
        Ok(())
    }
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
    use super::*;

    #[test]
    fn write_ambiguity_is_sticky_across_later_permanent_errors() {
        let unavailable = tonic::Status::unavailable("response was lost");
        let unimplemented = tonic::Status::unimplemented("wrong endpoint");

        let unknown = accumulated_write_ambiguity(false, &unavailable);
        assert!(unknown);
        assert!(accumulated_write_ambiguity(unknown, &unimplemented));
        assert!(!accumulated_write_ambiguity(false, &unimplemented));
    }
}
