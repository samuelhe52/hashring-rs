use std::{
    collections::{BTreeMap, HashMap},
    sync::{
        Arc, Weak,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
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
    limits::{
        DEFAULT_MAX_KEY_BYTES, MAX_DATA_MESSAGE_BYTES, MAX_MUTATION_ID_BYTES, MAX_VALUE_BYTES,
    },
    migration::TopologyChange,
    proto::{
        self, BeginTopologyChangeRequest, DeleteRequest, ErrorCode, ExecuteTopologyChangeRequest,
        GetRequest, OperationError, PutRequest, RecordVersion,
        coordinator_client::CoordinatorClient, data_node_client::DataNodeClient,
    },
    topology::{Member, TopologyConfig, TopologyError, TopologySnapshot, WriteAckPolicy},
    transport::{configure_coordinator_client, fetch_topology},
};

mod keyspace;

pub use keyspace::PER_KEY_OVERHEAD_BYTES;

#[derive(Clone, Debug)]
pub struct GetOutput {
    pub value: Vec<u8>,
    pub version: RecordVersion,
    pub topology_epoch: u64,
    /// Absolute Unix milliseconds at which the record stops being readable, or
    /// `None` when it never expires. Always `None` for a key that was not
    /// found, so a miss never looks like an expired hit.
    pub expires_at_unix_millis: Option<u64>,
}

#[derive(Clone, Debug)]
pub struct PutOutput {
    pub version: RecordVersion,
    pub topology_epoch: u64,
}

/// What a conditional write expects of the key's live record, checked by the
/// owner under the same lock that applies the write. A record that is deleted
/// or past its deadline is not live.
#[derive(Clone, Debug, PartialEq)]
pub enum Precondition {
    /// The key has no live record.
    Absent,
    /// The key's live record carries exactly this version, as returned by
    /// [`HashringClient::get`].
    Version(RecordVersion),
}

impl From<Precondition> for proto::Precondition {
    fn from(precondition: Precondition) -> Self {
        use proto::precondition::Expect;
        Self {
            expect: Some(match precondition {
                Precondition::Absent => Expect::Absent(proto::Empty {}),
                Precondition::Version(version) => Expect::Version(version),
            }),
        }
    }
}

/// The definite outcomes of one put: written, or not written for a reason the
/// node reported. Neither "not written" case keeps a receipt.
enum PutResult {
    Applied(PutOutput),
    KeyMissing,
    ConditionFailed,
}

/// What a write does to the record's deadline.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Expiry {
    /// Leave the record's current deadline alone.
    Keep,
    /// Store this absolute Unix millisecond deadline.
    At(u64),
    /// Remove the record's deadline.
    Clear,
}

/// On the wire an unset field means "keep what is there" and zero means "no
/// deadline", which is the same convention Redis uses for `KEEPTTL` against a
/// plain write.
impl From<Expiry> for Option<u64> {
    fn from(expiry: Expiry) -> Self {
        match expiry {
            Expiry::Keep => None,
            Expiry::At(at) => Some(at),
            Expiry::Clear => Some(0),
        }
    }
}

#[derive(Clone, Debug)]
pub struct DeleteOutput {
    pub topology_epoch: u64,
    /// The tombstone's version. It orders this delete against the writes and
    /// reads a caller observed.
    pub version: RecordVersion,
}

/// A conditional write rejected on a retry whose earlier attempt's outcome
/// is unknown. The rejection rules out the retry, not that earlier attempt.
fn rejected_after_ambiguity() -> ClientError {
    OperationFailure {
        code: ErrorCode::OutcomeUnknown,
        message: "a retried conditional write was rejected, but an earlier attempt may still apply"
            .into(),
        retryable: false,
        outcome: MutationOutcome::MayHaveApplied,
    }
    .into()
}

/// Whether a mutation can be ruled out after an unsuccessful attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MutationOutcome {
    /// The client knows the mutation was not applied.
    KnownNotApplied,
    /// The mutation may have been applied; retrying requires the same mutation ID.
    MayHaveApplied,
}

/// Why the client tried a request again.
///
/// The count is cumulative over the client's lifetime, like
/// [`HashringClient::retry_count`]. A driver reads it before and after a phase
/// to tell which failures the cluster produced and which were transport.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum RetryReason {
    Moved,
    RangeBusy,
    Unavailable,
    ResourceExhausted,
    TemporarilyUnavailable,
    ReplicaNotReady,
    OutcomeUnknown,
    LeaseExpired,
    Rpc,
    /// A data-node connection could not be established.
    Connect,
    TopologyRefresh,
}

impl RetryReason {
    pub const ALL: [RetryReason; 11] = [
        Self::Moved,
        Self::RangeBusy,
        Self::Unavailable,
        Self::ResourceExhausted,
        Self::TemporarilyUnavailable,
        Self::ReplicaNotReady,
        Self::OutcomeUnknown,
        Self::LeaseExpired,
        Self::Rpc,
        Self::Connect,
        Self::TopologyRefresh,
    ];

    pub fn name(self) -> &'static str {
        match self {
            Self::Moved => "moved",
            Self::RangeBusy => "range_busy",
            Self::Unavailable => "unavailable",
            Self::ResourceExhausted => "resource_exhausted",
            Self::TemporarilyUnavailable => "temporarily_unavailable",
            Self::ReplicaNotReady => "replica_not_ready",
            Self::OutcomeUnknown => "outcome_unknown",
            Self::LeaseExpired => "lease_expired",
            Self::Rpc => "rpc",
            Self::Connect => "connect",
            Self::TopologyRefresh => "topology_refresh",
        }
    }

    fn index(self) -> usize {
        match self {
            Self::Moved => 0,
            Self::RangeBusy => 1,
            Self::Unavailable => 2,
            Self::ResourceExhausted => 3,
            Self::TemporarilyUnavailable => 4,
            Self::ReplicaNotReady => 5,
            Self::OutcomeUnknown => 6,
            Self::LeaseExpired => 7,
            Self::Rpc => 8,
            Self::Connect => 9,
            Self::TopologyRefresh => 10,
        }
    }
}

/// One retryable error code with everything its retry attempt needs.
struct RetryCode<'a> {
    code: ErrorCode,
    reason: RetryReason,
    attempted_epoch: u64,
    deadline: Instant,
    attempt: &'a mut u32,
    unknown_write_outcome: bool,
}

impl From<bool> for MutationOutcome {
    fn from(unknown_write_outcome: bool) -> Self {
        if unknown_write_outcome {
            Self::MayHaveApplied
        } else {
            Self::KnownNotApplied
        }
    }
}

#[derive(Clone, Debug, Error)]
#[error("{code:?}: {message}")]
pub struct OperationFailure {
    pub code: ErrorCode,
    pub message: String,
    pub retryable: bool,
    pub outcome: MutationOutcome,
}

impl From<OperationError> for OperationFailure {
    fn from(error: OperationError) -> Self {
        let may_have_applied = operation_may_have_applied(&error);
        Self {
            code: ErrorCode::try_from(error.code).unwrap_or(ErrorCode::Unspecified),
            message: error.message,
            retryable: error.retryable,
            outcome: may_have_applied.into(),
        }
    }
}

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("failed to fetch or validate topology: {0}")]
    Topology(#[from] anyhow::Error),
    #[error("topology refresh failed: {source} (outcome={outcome:?})")]
    TopologyRefresh {
        source: anyhow::Error,
        outcome: MutationOutcome,
    },
    #[error(transparent)]
    Operation(#[from] OperationFailure),
    #[error("logical operation deadline exceeded (outcome={outcome:?})")]
    DeadlineExceeded { outcome: MutationOutcome },
    #[error("protocol response omitted record version")]
    MissingVersion,
    #[error("node RPC failed with {code:?}: {message} (outcome={outcome:?})")]
    Rpc {
        code: Code,
        message: String,
        outcome: MutationOutcome,
    },
}

impl ClientError {
    fn deadline(unknown_write_outcome: bool) -> Self {
        Self::DeadlineExceeded {
            outcome: unknown_write_outcome.into(),
        }
    }
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
    /// Fixed for the cluster's lifetime, so it is read once at connect.
    receipt_retention: Duration,
    topology: RwLock<TopologySnapshot>,
    channels: RwLock<HashMap<String, Channel>>,
    operation_timeout: Duration,
    topology_refresh_timeout: Duration,
    refresh_lock: Mutex<()>,
    refresh_generation: AtomicU64,
    background_refresh_running: AtomicBool,
    background_minimum_epoch: AtomicU64,
    background_trigger_generation: AtomicU64,
    /// Every internal retry this client made. A load driver reads it to tell
    /// a slow cluster apart from a client that had to try again.
    retries: AtomicU64,
    /// The retries by reason, cumulative like `retries`.
    retry_reasons: [AtomicU64; 11],
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
        .map_err(|_| ClientError::deadline(false))??;
        let inner = Arc::new(ClientInner {
            coordinator_endpoint,
            receipt_retention: topology.timings.receipt_retention(),
            topology: RwLock::new(topology),
            channels: RwLock::new(HashMap::new()),
            operation_timeout: config.operation_timeout,
            topology_refresh_timeout: config.topology_refresh_timeout,
            refresh_lock: Mutex::new(()),
            refresh_generation: AtomicU64::new(0),
            background_refresh_running: AtomicBool::new(false),
            background_minimum_epoch: AtomicU64::new(0),
            background_trigger_generation: AtomicU64::new(0),
            retries: AtomicU64::new(0),
            retry_reasons: std::array::from_fn(|_| AtomicU64::new(0)),
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

    /// The epoch of the topology the client is routing with.
    ///
    /// A caller that only wants the epoch should read it here rather than
    /// clone the whole snapshot.
    pub async fn epoch(&self) -> u64 {
        self.inner.topology.read().await.epoch
    }

    /// How many internal retries this client has made.
    ///
    /// A retry is one attempt a client made after a retryable failure, of a
    /// request or of a topology refresh. The count is cumulative over the
    /// client's lifetime, so a caller that wants the retries of one phase
    /// reads it before and after.
    pub fn retry_count(&self) -> u64 {
        self.inner.retries.load(Ordering::Relaxed)
    }

    /// The number of retries for one reason. A driver that samples a counter
    /// every few milliseconds needs one value, not the whole map.
    pub fn retry_reason_count(&self, reason: RetryReason) -> u64 {
        self.inner.retry_reasons[reason.index()].load(Ordering::Relaxed)
    }

    /// The internal retries by reason, cumulative over the client's lifetime.
    pub fn retry_reasons(&self) -> BTreeMap<&'static str, u64> {
        RetryReason::ALL
            .into_iter()
            .map(|reason| {
                (
                    reason.name(),
                    self.inner.retry_reasons[reason.index()].load(Ordering::Relaxed),
                )
            })
            .collect()
    }

    /// Counts one retry and its reason together.
    ///
    /// The two counters must move at the same point or they cannot be
    /// reconciled: recording the reason first and the total after a refresh
    /// that returns `Err` leaves `sum(retry_reasons) > retry_count`.
    fn record_retry(&self, reason: RetryReason) {
        self.inner.retries.fetch_add(1, Ordering::Relaxed);
        self.inner.retry_reasons[reason.index()].fetch_add(1, Ordering::Relaxed);
    }

    // Request routing needs only the epoch and selected endpoint. Keep them
    // from one snapshot without cloning every token assignment on each attempt.
    async fn route(&self, key: &[u8]) -> Result<(u64, String), ClientError> {
        let topology = self.inner.topology.read().await;
        let owner = topology.owner(key).map_err(anyhow::Error::from)?;
        Ok((topology.epoch, owner.endpoint.clone()))
    }

    pub async fn refresh_topology(&self) -> Result<TopologySnapshot, ClientError> {
        self.refresh_topology_for(self.inner.operation_timeout, false, None)
            .await
    }

    pub async fn begin_topology_change(
        &self,
        target_members: Vec<Member>,
    ) -> Result<TopologyChange, ClientError> {
        self.begin_topology_change_with_config(target_members, None)
            .await
    }

    pub async fn begin_write_policy_change(
        &self,
        target_policy: WriteAckPolicy,
    ) -> Result<TopologyChange, ClientError> {
        let members = self.topology().await.members;
        let mut config = self.topology().await.config();
        config.write_ack_policy = target_policy;
        self.begin_topology_change_with_config(members, Some(config))
            .await
    }

    pub async fn begin_topology_config_change(
        &self,
        target_config: TopologyConfig,
    ) -> Result<TopologyChange, ClientError> {
        let members = self.topology().await.members;
        self.begin_topology_change_with_config(members, Some(target_config))
            .await
    }

    async fn begin_topology_change_with_config(
        &self,
        target_members: Vec<Member>,
        target_config: Option<TopologyConfig>,
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
                return Err(ClientError::deadline(false));
            }
        };
        let request = BeginTopologyChangeRequest {
            target_write_ack_policy: target_config
                .as_ref()
                .map(|config| proto::WriteAckPolicy::from(config.write_ack_policy).into()),
            target_desired_replication_factor: target_config
                .as_ref()
                .map(|config| config.desired_replication_factor),
            target_write_availability_guard: target_config
                .as_ref()
                .map(|config| (&config.write_availability_guard).into()),
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
                return Err(ClientError::deadline(true));
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
        .map_err(|_| ClientError::deadline(false))??;
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
        .map_err(|_| ClientError::deadline(false))?
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
                return Err(ClientError::deadline(false));
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
                return Err(ClientError::deadline(true));
            }
        };
        Ok(response.try_into().map_err(anyhow::Error::from)?)
    }

    pub async fn get(&self, key: Vec<u8>) -> Result<GetOutput, ClientError> {
        validate_key_size(&key)?;
        let deadline = Instant::now() + self.inner.operation_timeout;
        let request_id = new_request_id();
        let mut attempt = 0_u32;

        loop {
            let (epoch, endpoint) = self.route(&key).await?;
            let mut client = match self.data_node_client(&endpoint, deadline).await {
                Ok(client) => client,
                Err(ClientError::DeadlineExceeded { .. }) => {
                    return Err(ClientError::deadline(false));
                }
                Err(_) => {
                    self.refresh_after_unavailable(epoch, deadline, false)
                        .await?;
                    self.retry_delay(deadline, &mut attempt, false, RetryReason::Connect)
                        .await?;
                    continue;
                }
            };

            let request = GetRequest {
                key: key.clone(),
                topology_epoch: epoch,
                request_id: request_id.clone(),
            };
            let rpc_timer = hashring_core::rpc_profile::CLIENT_GET_RPC.start();
            let response =
                tokio::time::timeout(remaining(deadline, false)?, client.get(request)).await;
            drop(rpc_timer);
            let response = match response {
                Ok(Ok(response)) => response.into_inner(),
                Ok(Err(status)) => {
                    if retryable_status(&status) {
                        self.refresh_after_unavailable(epoch, deadline, false)
                            .await?;
                        self.retry_delay(deadline, &mut attempt, false, RetryReason::Rpc)
                            .await?;
                        continue;
                    }
                    return Err(rpc_error(status, false));
                }
                Err(_) => {
                    return Err(ClientError::deadline(false));
                }
            };

            if let Some(error) = response.error {
                if self
                    .handle_retryable(error.clone(), epoch, deadline, &mut attempt, false)
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
                expires_at_unix_millis: response.expires_at_unix_millis,
            };
            if response.current_epoch > epoch {
                self.trigger_background_refresh(response.current_epoch);
            }
            return Ok(output);
        }
    }

    pub async fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<PutOutput, ClientError> {
        self.put_with_expiry(key, value, Expiry::Keep).await
    }

    /// Stores `value` under a caller-chosen mutation ID.
    ///
    /// Every internal retry of this call reuses the ID, and a later call with
    /// the same ID and payload is answered from the owner's receipt instead of
    /// being applied again. That is what lets a caller retry a request whose
    /// response it never saw.
    ///
    /// The ID must not be empty and must not exceed
    /// [`MAX_MUTATION_ID_BYTES`] bytes; a call that breaks either rule fails
    /// without contacting a node. The ID must also stay unique to this
    /// operation for at least the cluster's receipt retention period: a later
    /// call that reuses the ID with a different payload is rejected with
    /// `MutationIdConflict`, and a call that reuses it with the same payload
    /// is answered from the receipt instead of being applied again.
    pub async fn put_with_mutation_id(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
        mutation_id: impl Into<String>,
    ) -> Result<PutOutput, ClientError> {
        let mutation_id = mutation_id.into();
        validate_mutation_id(&mutation_id)?;
        match self
            .put_record(key, value, Expiry::Keep, false, None, mutation_id)
            .await?
        {
            PutResult::Applied(output) => Ok(output),
            PutResult::KeyMissing | PutResult::ConditionFailed => Err(ClientError::MissingVersion),
        }
    }

    /// Stores `value` and sets the record's deadline.
    ///
    /// The deadline is absolute and is fixed when this is called, so every
    /// retry of one logical write carries the deadline it was first sent with.
    /// A caller that wants a deadline relative to the moment the write is
    /// accepted computes it from its own clock.
    pub async fn put_with_expiry(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
        expiry: Expiry,
    ) -> Result<PutOutput, ClientError> {
        match self
            .put_record(key, value, expiry, false, None, new_request_id())
            .await?
        {
            PutResult::Applied(output) => Ok(output),
            // Only a conditional or expiry-only write is answered this way.
            PutResult::KeyMissing | PutResult::ConditionFailed => Err(ClientError::MissingVersion),
        }
    }

    /// Stores `value` only if `precondition` holds for the key's live record
    /// when the owner applies the write. `None` means it did not hold and
    /// nothing was written.
    ///
    /// A retry inside this call reuses the mutation ID, and the owner answers
    /// it from the first attempt's receipt before checking the condition
    /// again, so a write that was applied is never reported as failed.
    pub async fn put_if(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
        expiry: Expiry,
        precondition: Precondition,
    ) -> Result<Option<PutOutput>, ClientError> {
        match self
            .put_record(
                key,
                value,
                expiry,
                false,
                Some(precondition),
                new_request_id(),
            )
            .await?
        {
            PutResult::Applied(output) => Ok(Some(output)),
            PutResult::ConditionFailed => Ok(None),
            PutResult::KeyMissing => Err(ClientError::MissingVersion),
        }
    }

    /// Replaces the deadline of an existing record, keeping its value:
    /// `Some` stores that absolute deadline and `None` removes it.
    ///
    /// Returns `None` rather than an error when the key is absent, and writes
    /// nothing in that case: no record, no sequence and no receipt.
    pub async fn set_expiry(
        &self,
        key: Vec<u8>,
        expires_at_unix_millis: Option<u64>,
    ) -> Result<Option<PutOutput>, ClientError> {
        self.expiry_record(key, expires_at_unix_millis, None).await
    }

    /// [`HashringClient::set_expiry`] applied only if `precondition` holds;
    /// `None` when it did not or the key is absent, which the precondition
    /// already tells apart.
    pub async fn set_expiry_if(
        &self,
        key: Vec<u8>,
        expires_at_unix_millis: Option<u64>,
        precondition: Precondition,
    ) -> Result<Option<PutOutput>, ClientError> {
        self.expiry_record(key, expires_at_unix_millis, Some(precondition))
            .await
    }

    async fn expiry_record(
        &self,
        key: Vec<u8>,
        expires_at_unix_millis: Option<u64>,
        precondition: Option<Precondition>,
    ) -> Result<Option<PutOutput>, ClientError> {
        let expiry = expires_at_unix_millis.map_or(Expiry::Clear, Expiry::At);
        match self
            .put_record(
                key,
                Vec::new(),
                expiry,
                true,
                precondition,
                new_request_id(),
            )
            .await?
        {
            PutResult::Applied(output) => Ok(Some(output)),
            PutResult::KeyMissing | PutResult::ConditionFailed => Ok(None),
        }
    }

    async fn put_record(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
        expiry: Expiry,
        preserve_value: bool,
        precondition: Option<Precondition>,
        request_id: String,
    ) -> Result<PutResult, ClientError> {
        let _operation_timer = hashring_core::rpc_profile::CLIENT_PUT_OPERATION.start();
        validate_key_size(&key)?;
        // The transport, not the cluster's own settings, decides what one
        // message may carry, so this is the ceiling any cluster can hold.
        if value.len() > MAX_VALUE_BYTES {
            return Err(size_failure(format!(
                "value exceeds {MAX_VALUE_BYTES} bytes"
            )));
        }
        let deadline = self.mutation_deadline();
        let mut attempt = 0_u32;
        let mut unknown_write_outcome = false;

        loop {
            let (epoch, endpoint) = self.route(&key).await?;
            let mut client = match self.data_node_client(&endpoint, deadline).await {
                Ok(client) => client,
                Err(ClientError::DeadlineExceeded { .. }) => {
                    return Err(ClientError::deadline(unknown_write_outcome));
                }
                Err(_) => {
                    self.refresh_after_unavailable(epoch, deadline, unknown_write_outcome)
                        .await?;
                    self.retry_delay(
                        deadline,
                        &mut attempt,
                        unknown_write_outcome,
                        RetryReason::Connect,
                    )
                    .await?;
                    continue;
                }
            };

            let request = PutRequest {
                key: key.clone(),
                value: value.clone(),
                topology_epoch: epoch,
                request_id: request_id.clone(),
                expires_at_unix_millis: expiry.into(),
                preserve_value,
                precondition: precondition.clone().map(Into::into),
            };
            let rpc_timer = hashring_core::rpc_profile::CLIENT_PUT_RPC.start();
            let response = tokio::time::timeout(
                remaining(deadline, unknown_write_outcome)?,
                client.put(request),
            )
            .await;
            drop(rpc_timer);
            let response = match response {
                Ok(Ok(response)) => response.into_inner(),
                Ok(Err(status)) => {
                    unknown_write_outcome =
                        accumulated_write_ambiguity(unknown_write_outcome, &status);
                    if retryable_status(&status) {
                        tracing::debug!(operation = "put", request_id, attempt, epoch = epoch, code = ?status.code(), message = status.message(), "retrying node RPC status");
                        self.refresh_after_unavailable(epoch, deadline, unknown_write_outcome)
                            .await?;
                        self.retry_delay(
                            deadline,
                            &mut attempt,
                            unknown_write_outcome,
                            RetryReason::Rpc,
                        )
                        .await?;
                        continue;
                    }
                    return Err(rpc_error(status, unknown_write_outcome));
                }
                Err(_) => {
                    return Err(ClientError::deadline(true));
                }
            };

            if let Some(error) = response.error {
                let error_unknown_write_outcome =
                    unknown_write_outcome || operation_may_have_applied(&error);
                // Kept for later attempts: a retry the owner then rejects, or
                // fails permanently, still follows this ambiguous one.
                unknown_write_outcome = error_unknown_write_outcome;
                if error.retryable {
                    tracing::debug!(operation = "put", request_id, attempt, epoch = epoch, code = ?ErrorCode::try_from(error.code).unwrap_or(ErrorCode::Unspecified), message = error.message, "retrying operation response");
                }
                if self
                    .handle_retryable(
                        error.clone(),
                        epoch,
                        deadline,
                        &mut attempt,
                        error_unknown_write_outcome,
                    )
                    .await?
                {
                    continue;
                }
                let mut failure = OperationFailure::from(error);
                failure.outcome = error_unknown_write_outcome.into();
                return Err(failure.into());
            }
            if response.current_epoch > epoch {
                self.trigger_background_refresh(response.current_epoch);
            }
            if response.condition_failed || response.key_missing {
                // A rejection answers for this attempt only, and keeps no
                // receipt. An earlier attempt whose outcome is unknown may
                // still be on its way to the owner and apply afterwards.
                if unknown_write_outcome {
                    return Err(rejected_after_ambiguity());
                }
                return Ok(match response.condition_failed {
                    true => PutResult::ConditionFailed,
                    false => PutResult::KeyMissing,
                });
            }
            return Ok(PutResult::Applied(PutOutput {
                version: response.version.ok_or(ClientError::MissingVersion)?,
                topology_epoch: response.current_epoch,
            }));
        }
    }

    pub async fn delete(&self, key: Vec<u8>) -> Result<DeleteOutput, ClientError> {
        self.delete_record(key, None, new_request_id())
            .await?
            .ok_or(ClientError::MissingVersion)
    }

    /// Deletes the key under a caller-chosen mutation ID. Every internal retry
    /// reuses the ID, and a later call with the same ID is answered from the
    /// owner's receipt. See [`HashringClient::put_with_mutation_id`] for the
    /// rules the ID has to follow.
    pub async fn delete_with_mutation_id(
        &self,
        key: Vec<u8>,
        mutation_id: impl Into<String>,
    ) -> Result<DeleteOutput, ClientError> {
        let mutation_id = mutation_id.into();
        validate_mutation_id(&mutation_id)?;
        self.delete_record(key, None, mutation_id)
            .await?
            .ok_or(ClientError::MissingVersion)
    }

    /// Deletes the key only if `precondition` holds for its live record when
    /// the owner applies the delete. `None` means it did not hold and nothing
    /// was written. See [`HashringClient::put_if`] for how retries behave.
    pub async fn delete_if(
        &self,
        key: Vec<u8>,
        precondition: Precondition,
    ) -> Result<Option<DeleteOutput>, ClientError> {
        self.delete_record(key, Some(precondition), new_request_id())
            .await
    }

    async fn delete_record(
        &self,
        key: Vec<u8>,
        precondition: Option<Precondition>,
        request_id: String,
    ) -> Result<Option<DeleteOutput>, ClientError> {
        validate_key_size(&key)?;
        let deadline = self.mutation_deadline();
        let mut attempt = 0_u32;
        let mut unknown_write_outcome = false;

        loop {
            let (epoch, endpoint) = self.route(&key).await?;
            let mut client = match self.data_node_client(&endpoint, deadline).await {
                Ok(client) => client,
                Err(ClientError::DeadlineExceeded { .. }) => {
                    return Err(ClientError::deadline(unknown_write_outcome));
                }
                Err(_) => {
                    self.refresh_after_unavailable(epoch, deadline, unknown_write_outcome)
                        .await?;
                    self.retry_delay(
                        deadline,
                        &mut attempt,
                        unknown_write_outcome,
                        RetryReason::Connect,
                    )
                    .await?;
                    continue;
                }
            };

            let request = DeleteRequest {
                key: key.clone(),
                topology_epoch: epoch,
                request_id: request_id.clone(),
                precondition: precondition.clone().map(Into::into),
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
                        tracing::debug!(operation = "delete", request_id, attempt, epoch = epoch, code = ?status.code(), message = status.message(), "retrying node RPC status");
                        self.refresh_after_unavailable(epoch, deadline, unknown_write_outcome)
                            .await?;
                        self.retry_delay(
                            deadline,
                            &mut attempt,
                            unknown_write_outcome,
                            RetryReason::Rpc,
                        )
                        .await?;
                        continue;
                    }
                    return Err(rpc_error(status, unknown_write_outcome));
                }
                Err(_) => {
                    return Err(ClientError::deadline(true));
                }
            };

            if let Some(error) = response.error {
                let error_unknown_write_outcome =
                    unknown_write_outcome || operation_may_have_applied(&error);
                // Kept for later attempts: a retry the owner then rejects, or
                // fails permanently, still follows this ambiguous one.
                unknown_write_outcome = error_unknown_write_outcome;
                if error.retryable {
                    tracing::debug!(operation = "delete", request_id, attempt, epoch = epoch, code = ?ErrorCode::try_from(error.code).unwrap_or(ErrorCode::Unspecified), message = error.message, "retrying operation response");
                }
                if self
                    .handle_retryable(
                        error.clone(),
                        epoch,
                        deadline,
                        &mut attempt,
                        error_unknown_write_outcome,
                    )
                    .await?
                {
                    continue;
                }
                let mut failure = OperationFailure::from(error);
                failure.outcome = error_unknown_write_outcome.into();
                return Err(failure.into());
            }
            if response.current_epoch > epoch {
                self.trigger_background_refresh(response.current_epoch);
            }
            if response.condition_failed {
                // See `put_record`: an earlier ambiguous attempt may still
                // apply after this one was rejected.
                if unknown_write_outcome {
                    return Err(rejected_after_ambiguity());
                }
                return Ok(None);
            }
            return Ok(Some(DeleteOutput {
                topology_epoch: response.current_epoch,
                version: response.version.ok_or(ClientError::MissingVersion)?,
            }));
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
                self.retry_delay(deadline, attempt, unknown_write_outcome, RetryReason::Moved)
                    .await?;
                Ok(true)
            }
            ErrorCode::Unavailable if error.retryable => {
                self.refresh_after_unavailable(attempted_epoch, deadline, unknown_write_outcome)
                    .await?;
                self.retry_delay(
                    deadline,
                    attempt,
                    unknown_write_outcome,
                    RetryReason::Unavailable,
                )
                .await?;
                Ok(true)
            }
            ErrorCode::RangeBusy if error.retryable => {
                self.retry_code(
                    RetryCode {
                        code,
                        reason: RetryReason::RangeBusy,
                        attempted_epoch,
                        deadline,
                        attempt,
                        unknown_write_outcome,
                    },
                    &error,
                )
                .await
            }
            ErrorCode::ResourceExhausted if error.retryable => {
                self.retry_code(
                    RetryCode {
                        code,
                        reason: RetryReason::ResourceExhausted,
                        attempted_epoch,
                        deadline,
                        attempt,
                        unknown_write_outcome,
                    },
                    &error,
                )
                .await
            }
            ErrorCode::TemporarilyUnavailable if error.retryable => {
                self.retry_code(
                    RetryCode {
                        code,
                        reason: RetryReason::TemporarilyUnavailable,
                        attempted_epoch,
                        deadline,
                        attempt,
                        unknown_write_outcome,
                    },
                    &error,
                )
                .await
            }
            ErrorCode::ReplicaNotReady if error.retryable => {
                self.retry_code(
                    RetryCode {
                        code,
                        reason: RetryReason::ReplicaNotReady,
                        attempted_epoch,
                        deadline,
                        attempt,
                        unknown_write_outcome,
                    },
                    &error,
                )
                .await
            }
            ErrorCode::OutcomeUnknown if error.retryable => {
                self.retry_code(
                    RetryCode {
                        code,
                        reason: RetryReason::OutcomeUnknown,
                        attempted_epoch,
                        deadline,
                        attempt,
                        unknown_write_outcome,
                    },
                    &error,
                )
                .await
            }
            ErrorCode::LeaseExpired if error.retryable => {
                self.refresh_topology_before(deadline, unknown_write_outcome, None)
                    .await?;
                self.retry_delay(
                    deadline,
                    attempt,
                    unknown_write_outcome,
                    RetryReason::LeaseExpired,
                )
                .await?;
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    /// Retries a request whose error code names its own retry reason.
    ///
    /// Every code in the retryable group names the reason it is counted
    /// with, so a new code cannot join the group without one.
    async fn retry_code(
        &self,
        retry: RetryCode<'_>,
        error: &OperationError,
    ) -> Result<bool, ClientError> {
        let RetryCode {
            code,
            reason,
            attempted_epoch,
            deadline,
            attempt,
            unknown_write_outcome,
        } = retry;
        if error.current_epoch > attempted_epoch {
            self.refresh_topology_before(
                deadline,
                unknown_write_outcome,
                Some(error.current_epoch),
            )
            .await?;
        }
        self.retry_delay_with_hint(
            deadline,
            attempt,
            unknown_write_outcome,
            if code == ErrorCode::ResourceExhausted {
                Duration::from_millis(error.retry_after_millis)
            } else {
                Duration::ZERO
            },
            reason,
        )
        .await?;
        Ok(true)
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
        .map_err(|_| ClientError::deadline(unknown_write_outcome))?;
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
                        outcome: unknown_write_outcome.into(),
                    });
                }
                self.inner.refresh_generation.fetch_add(1, Ordering::SeqCst);
            }
            Ok(Err(_)) => {}
            Err(_) => {
                return Err(ClientError::deadline(unknown_write_outcome));
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

    /// Bounds the retries of one mutation ID by the cluster's receipt
    /// retention period: a receipt's clock starts when a replica applies the
    /// mutation, after the first attempt was sent, so retries before this
    /// deadline find it.
    fn mutation_deadline(&self) -> Instant {
        Instant::now()
            + self
                .inner
                .operation_timeout
                .min(self.inner.receipt_retention)
    }

    async fn retry_delay(
        &self,
        deadline: Instant,
        attempt: &mut u32,
        unknown_write_outcome: bool,
        reason: RetryReason,
    ) -> Result<(), ClientError> {
        self.record_retry(reason);
        retry_delay(deadline, attempt, unknown_write_outcome).await
    }

    async fn retry_delay_with_hint(
        &self,
        deadline: Instant,
        attempt: &mut u32,
        unknown_write_outcome: bool,
        minimum_delay: Duration,
        reason: RetryReason,
    ) -> Result<(), ClientError> {
        self.record_retry(reason);
        retry_delay_with_hint(deadline, attempt, unknown_write_outcome, minimum_delay).await
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
            .map_err(|_| ClientError::deadline(false))?
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
    .map_err(|_| ClientError::deadline(unknown_write_outcome))?;
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
                        outcome: unknown_write_outcome.into(),
                    });
                }
                inner.refresh_generation.fetch_add(1, Ordering::SeqCst);
                if minimum_epoch.is_none_or(|minimum| current.epoch >= minimum) {
                    return Ok(current.clone());
                }
                drop(current);
                refresh_retry_delay(inner, deadline, &mut attempt, unknown_write_outcome).await?;
            }
            Ok(Err(error)) if error.is_retryable() => {
                refresh_retry_delay(inner, deadline, &mut attempt, unknown_write_outcome).await?;
            }
            Ok(Err(error)) => {
                return Err(ClientError::TopologyRefresh {
                    source: error.into(),
                    outcome: unknown_write_outcome.into(),
                });
            }
            Err(_) => {
                return Err(ClientError::deadline(unknown_write_outcome));
            }
        }
    }
}

async fn retry_delay(
    deadline: Instant,
    attempt: &mut u32,
    unknown_write_outcome: bool,
) -> Result<(), ClientError> {
    retry_delay_with_hint(deadline, attempt, unknown_write_outcome, Duration::ZERO).await
}

/// Waits before the next attempt of a topology refresh, and counts it.
///
/// A refresh that has to try again is a retry of the client, like a request
/// that has to try again, so it is counted with them. A counter that left
/// these out would count a client that never had to try a request twice as a
/// client that never had to try anything twice.
async fn refresh_retry_delay(
    inner: &ClientInner,
    deadline: Instant,
    attempt: &mut u32,
    unknown_write_outcome: bool,
) -> Result<(), ClientError> {
    inner.retries.fetch_add(1, Ordering::Relaxed);
    inner.retry_reasons[RetryReason::TopologyRefresh.index()].fetch_add(1, Ordering::Relaxed);
    retry_delay(deadline, attempt, unknown_write_outcome).await
}

async fn retry_delay_with_hint(
    deadline: Instant,
    attempt: &mut u32,
    unknown_write_outcome: bool,
    minimum_delay: Duration,
) -> Result<(), ClientError> {
    let base_ms = 5_u64.saturating_mul(1_u64 << (*attempt).min(5));
    let jitter_ms = rand::random::<u64>() % (base_ms + 1);
    *attempt = attempt.saturating_add(1);
    let delay = Duration::from_millis((base_ms + jitter_ms).min(200)).max(minimum_delay);
    let available = remaining(deadline, unknown_write_outcome)?;
    if delay >= available {
        tokio::time::sleep(available).await;
        return Err(ClientError::deadline(unknown_write_outcome));
    }
    tokio::time::sleep(delay).await;
    Ok(())
}

/// A random 128-bit operation id, as unique as a v4 UUID. `rand`'s thread-local
/// generator is seeded from the OS once and reseeds only after 64 KiB of output,
/// where `Uuid::new_v4` makes a `getentropy` syscall per call, which cost 4.5-7%
/// of a load driver's CPU.
fn new_request_id() -> String {
    format!("{:032x}", rand::random::<u128>())
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

fn operation_may_have_applied(error: &OperationError) -> bool {
    error.unknown_write_outcome || error.code == ErrorCode::OutcomeUnknown as i32
}

fn rpc_error(status: tonic::Status, unknown_write_outcome: bool) -> ClientError {
    ClientError::Rpc {
        code: status.code(),
        message: status.message().to_owned(),
        outcome: unknown_write_outcome.into(),
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

/// Rejects a mutation ID a node would refuse anyway.
///
/// The check is here so a caller learns about a bad ID without a round trip,
/// and so the error names the rule instead of the transport.
fn validate_mutation_id(mutation_id: &str) -> Result<(), ClientError> {
    if mutation_id.is_empty() || mutation_id.len() > MAX_MUTATION_ID_BYTES {
        return Err(OperationFailure {
            code: ErrorCode::InvalidArgument,
            message: format!(
                "mutation id must contain 1 to {MAX_MUTATION_ID_BYTES} bytes, not {}",
                mutation_id.len()
            ),
            retryable: false,
            outcome: MutationOutcome::KnownNotApplied,
        }
        .into());
    }
    Ok(())
}

fn size_failure(message: String) -> ClientError {
    OperationFailure {
        code: ErrorCode::TooLarge,
        message,
        retryable: false,
        outcome: MutationOutcome::KnownNotApplied,
    }
    .into()
}

fn remaining(deadline: Instant, unknown_write_outcome: bool) -> Result<Duration, ClientError> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|remaining| !remaining.is_zero())
        .ok_or(ClientError::deadline(unknown_write_outcome))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use hashring_core::proto::coordinator_server::{Coordinator, CoordinatorServer};
    use hashring_core::topology::{ClusterTimings, TopologyConfig};
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
                .try_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
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
    fn outcome_unknown_code_is_ambiguous_even_without_the_wire_flag() {
        let failure = OperationFailure::from(OperationError {
            code: ErrorCode::OutcomeUnknown.into(),
            unknown_write_outcome: false,
            ..Default::default()
        });
        assert_eq!(failure.outcome, MutationOutcome::MayHaveApplied);
    }

    #[tokio::test]
    async fn write_deadline_before_owner_contact_is_known_not_applied() {
        let (endpoint, _, server) = start_fake_coordinator(Duration::ZERO).await;
        let mut config = ClientConfig::new(Duration::from_millis(250));
        config.topology_poll_interval = None;
        let client = HashringClient::connect_with_config(endpoint, config)
            .await
            .unwrap();

        assert!(matches!(
            client.put(b"key".to_vec(), b"value".to_vec()).await,
            Err(ClientError::DeadlineExceeded {
                outcome: MutationOutcome::KnownNotApplied
            })
        ));
        server.abort();
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
    async fn a_refresh_that_has_to_try_again_counts_as_a_retry() {
        let (endpoint, service, server) = start_fake_coordinator(Duration::ZERO).await;
        let mut config = ClientConfig::new(Duration::from_secs(1));
        config.topology_poll_interval = None;
        let client = HashringClient::connect_with_config(endpoint, config)
            .await
            .unwrap();
        // The first fetch of the refresh fails with a failure that can be
        // tried again, so the client has to try a second time.
        *service.topology_error.lock().unwrap() = Some(Code::Unavailable);
        service
            .topology_failures_remaining
            .store(1, Ordering::SeqCst);

        let before = client.retry_count();
        client.refresh_topology().await.unwrap();
        // The retry of a refresh is a retry of the client, like the retry of
        // a request. A counter that left it out would report a client that
        // had to try a request twice as one that never had to try anything
        // twice.
        assert_eq!(client.retry_count(), before + 1);
        server.abort();
    }

    #[tokio::test]
    async fn every_retry_is_counted_with_the_reason_that_caused_it() {
        let (endpoint, service, server) = start_fake_coordinator(Duration::ZERO).await;
        let mut config = ClientConfig::new(Duration::from_secs(1));
        config.topology_poll_interval = None;
        let client = HashringClient::connect_with_config(endpoint, config)
            .await
            .unwrap();
        // The first fetch of the refresh fails with a failure that can be
        // tried again, so the client has to try a second time.
        *service.topology_error.lock().unwrap() = Some(Code::Unavailable);
        service
            .topology_failures_remaining
            .store(1, Ordering::SeqCst);
        client.refresh_topology().await.unwrap();

        let reasons = client.retry_reasons();
        let counted: u64 = reasons.values().sum();
        // The total and the reasons have to move together, or they cannot be
        // reconciled and a driver cannot tell which failures the cluster
        // produced.
        assert_eq!(counted, client.retry_count());
        assert_eq!(reasons.get("topology_refresh").copied(), Some(1));
        server.abort();
    }

    #[tokio::test]
    async fn a_walk_that_cannot_reach_its_owner_counts_a_connect_retry() {
        let (endpoint, _service, server) = start_fake_coordinator(Duration::ZERO).await;
        let mut config = ClientConfig::new(Duration::from_millis(400));
        config.topology_poll_interval = None;
        let client = HashringClient::connect_with_config(endpoint, config)
            .await
            .unwrap();
        // The topology points at a closed port, so the walk cannot reach its
        // owner at all.
        let result = client.keys(|_| true, 1_000_000).await;
        assert!(
            result.is_err(),
            "the walk reached an owner that is not there"
        );

        let reasons = client.retry_reasons();
        let counted: u64 = reasons.values().sum();
        assert_eq!(counted, client.retry_count());
        // A connection that could not be made is not an rpc failure, and the
        // reasons have to say which one it was.
        assert!(
            reasons.get("connect").copied().unwrap_or(0) > 0,
            "the walk counted no connect retry: {reasons:?}"
        );
        assert_eq!(reasons.get("rpc").copied(), Some(0));
        server.abort();
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
                outcome: MutationOutcome::MayHaveApplied,
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
                outcome: MutationOutcome::MayHaveApplied,
                ..
            }
        ));
        server.abort();
    }

    /// A client whose coordinator and node endpoints are unreachable.
    fn unreachable_client(operation_timeout: Duration) -> HashringClient {
        unreachable_client_with(operation_timeout, ClusterTimings::default())
    }

    fn unreachable_client_with(
        operation_timeout: Duration,
        timings: ClusterTimings,
    ) -> HashringClient {
        let topology = TopologySnapshot::new_with_timings(
            1,
            1,
            1,
            vec![Member {
                node_id: "node-1".into(),
                endpoint: "http://127.0.0.1:1".into(),
            }],
            TopologyConfig::default(),
            timings,
        )
        .unwrap();
        HashringClient {
            inner: Arc::new(ClientInner {
                coordinator_endpoint: "http://127.0.0.1:1".into(),
                receipt_retention: topology.timings.receipt_retention(),
                topology: RwLock::new(topology),
                channels: RwLock::new(HashMap::new()),
                operation_timeout,
                topology_refresh_timeout: operation_timeout,
                refresh_lock: Mutex::new(()),
                refresh_generation: AtomicU64::new(0),
                background_refresh_running: AtomicBool::new(false),
                background_minimum_epoch: AtomicU64::new(0),
                background_trigger_generation: AtomicU64::new(0),
                retries: AtomicU64::new(0),
                retry_reasons: std::array::from_fn(|_| AtomicU64::new(0)),
            }),
        }
    }

    #[test]
    fn mutation_retries_stop_within_receipt_retention() {
        let timings = ClusterTimings {
            receipt_retention_millis: 5_000,
            ..ClusterTimings::default()
        };
        let retention = timings.receipt_retention();
        let before = Instant::now();
        let long = unreachable_client_with(retention * 2, timings).mutation_deadline();
        assert!(long <= Instant::now() + retention);
        assert!(long >= before + retention);

        let short = Duration::from_millis(100);
        let before = Instant::now();
        let deadline = unreachable_client(short).mutation_deadline();
        assert!(deadline >= before + short && deadline <= Instant::now() + short);
    }

    #[tokio::test]
    async fn moved_refresh_preserves_an_unknown_write_outcome_through_its_deadline() {
        let client = unreachable_client(Duration::from_millis(100));
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
                outcome: MutationOutcome::MayHaveApplied
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

    #[tokio::test]
    async fn capacity_retry_hint_waits_within_the_logical_deadline() {
        let (endpoint, _, server) = start_fake_coordinator(Duration::ZERO).await;
        let client = HashringClient::connect(endpoint, Duration::from_secs(1))
            .await
            .unwrap();
        let mut attempt = 0;
        let error = client
            .handle_retryable(
                OperationError {
                    code: ErrorCode::ResourceExhausted.into(),
                    retryable: true,
                    retry_after_millis: 100,
                    ..Default::default()
                },
                1,
                Instant::now() + Duration::from_millis(30),
                &mut attempt,
                false,
            )
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            ClientError::DeadlineExceeded {
                outcome: MutationOutcome::KnownNotApplied
            }
        ));
        assert_eq!(attempt, 1);
        server.abort();
    }

    #[tokio::test]
    async fn a_mutation_id_a_node_would_refuse_is_rejected_without_a_round_trip() {
        // The client's owner is unreachable, so an ID that reached a node
        // would fail with a transport error instead of the rule it broke.
        let client = unreachable_client(Duration::from_millis(100));

        for mutation_id in ["", &"x".repeat(MAX_MUTATION_ID_BYTES + 1)] {
            let error = client
                .put_with_mutation_id(b"key".to_vec(), b"value".to_vec(), mutation_id)
                .await
                .unwrap_err();
            assert!(
                matches!(
                    error,
                    ClientError::Operation(OperationFailure {
                        code: ErrorCode::InvalidArgument,
                        outcome: MutationOutcome::KnownNotApplied,
                        ..
                    })
                ),
                "the client accepted the mutation id {mutation_id:?}: {error}"
            );

            let error = client
                .delete_with_mutation_id(b"key".to_vec(), mutation_id)
                .await
                .unwrap_err();
            assert!(
                matches!(
                    error,
                    ClientError::Operation(OperationFailure {
                        code: ErrorCode::InvalidArgument,
                        ..
                    })
                ),
                "the client accepted the mutation id {mutation_id:?}: {error}"
            );
        }
    }

    #[test]
    fn a_mutation_id_at_the_limit_is_accepted() {
        assert!(validate_mutation_id(&"x".repeat(MAX_MUTATION_ID_BYTES)).is_ok());
        assert!(validate_mutation_id("x").is_ok());
        assert!(validate_mutation_id("").is_err());
        assert!(validate_mutation_id(&"x".repeat(MAX_MUTATION_ID_BYTES + 1)).is_err());
    }
}
