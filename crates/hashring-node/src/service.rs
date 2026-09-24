use super::*;

impl DataNodeService {
    pub(super) async fn ready_followers(
        &self,
        key: &[u8],
    ) -> Result<(u64, Vec<String>), OperationError> {
        let topology = self.state.read().await.topology.clone();
        let token = topology.key_token(key);
        let guard = &topology.write_availability_guard;
        let needs_status = guard.minimum_admitted_copies > 1
            || guard.minimum_healthy_followers > 0
            || topology.write_ack_policy != WriteAckPolicy::OwnerOnly;
        let status = if needs_status {
            let result = tokio::time::timeout(REPLICATION_RPC_TIMEOUT, async {
                let mut client = configure_coordinator_client(CoordinatorClient::new(
                    self.coordinator_channel.clone(),
                ));
                client
                    .get_replica_status(proto::Empty {})
                    .await
                    .map(Response::into_inner)
                    .map_err(|error| error.to_string())
            })
            .await;
            // Transport and RPC failures are deliberately indistinguishable at the
            // write gate: neither proves that a required copy is ready.
            match result {
                Ok(Ok(status)) => Some(status),
                _ => {
                    return Err(temporarily_unavailable(
                        topology.epoch,
                        "replica readiness could not be verified",
                    ));
                }
            }
        } else {
            None
        };
        let followers = required_followers(&topology, token, status.as_ref(), &self.node_id)?;
        Ok((topology.epoch, followers))
    }

    pub(super) async fn wait_required_acks(
        &self,
        required: &[RequiredAck],
        epoch: u64,
    ) -> Result<(), OperationError> {
        if !required.is_empty() {
            let mut receivers = {
                let state = self.state.read().await;
                required
                    .iter()
                    .map(|(stream_epoch, node_id, sequence)| {
                        state
                            .ack_progress
                            .get(&(*stream_epoch, node_id.clone()))
                            .map(|sender| (sender.subscribe(), *sequence))
                    })
                    .collect::<Option<Vec<_>>>()
            }
            .ok_or_else(|| outcome_unknown(epoch))?;
            let wait = async {
                for (receiver, sequence) in &mut receivers {
                    while *receiver.borrow_and_update() < *sequence {
                        receiver
                            .changed()
                            .await
                            .map_err(|_| outcome_unknown(epoch))?;
                    }
                }
                Ok(())
            };
            tokio::time::timeout(REQUIRED_ACK_TIMEOUT, wait)
                .await
                .unwrap_or_else(|_| Err(outcome_unknown(epoch)))?;
        }
        let state = self.state.read().await;
        if state.lease.is_none_or(|(lease_epoch, expires_at)| {
            lease_epoch != epoch || state.topology.epoch != epoch || Instant::now() >= expires_at
        }) {
            return Err(outcome_unknown(epoch));
        }
        Ok(())
    }

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
        // A Channel reconnects on demand and is cheap to clone. Guarded writes
        // must not create a fresh TCP connection for every readiness check.
        let coordinator_channel =
            Endpoint::from_shared(coordinator_endpoint.clone())?.connect_lazy();
        let (shutdown, _) = watch::channel(false);
        let state = Arc::new(RwLock::new(NodeState {
            topology,
            records: HashMap::new(),
            next_sequence: 0,
            owner_stream_sequences: HashMap::new(),
            owner_stream_unacked: HashMap::new(),
            ack_progress: HashMap::new(),
            follower_streams: HashMap::new(),
            sources: HashMap::new(),
            destinations: HashMap::new(),
            journal_bytes_total: 0,
            lease: None,
            policy_write_fence: None,
            dedup: HashMap::new(),
            dedup_expirations: BinaryHeap::new(),
            dedup_bytes: 0,
            ack_progress_needs_prune: false,
            next_ack_prune_at: Instant::now(),
        }));
        let replication_dispatch = start_replication_dispatch(state.clone());
        let service = Self {
            node_id,
            process_instance_id,
            coordinator_endpoint,
            coordinator_channel,
            state,
            replication_dispatch,
            refresh_lock: Arc::new(Mutex::new(())),
            max_key_bytes: DEFAULT_MAX_KEY_BYTES,
            max_value_bytes: DEFAULT_MAX_VALUE_BYTES,
            max_journal_bytes: DEFAULT_MAX_MIGRATION_JOURNAL_BYTES,
            max_dedup_bytes: MAX_DEDUP_BYTES,
            stop_response_delay: std::time::Duration::ZERO,
            stop_prepared: Arc::new(AtomicBool::new(false)),
            shutdown,
        };
        if is_committed_member {
            service.renew_lease_once().await?;
        }
        service.start_lease_renewal();
        service.start_peer_probes();
        Ok(service)
    }

    pub(super) async fn renew_lease_once(&self) -> anyhow::Result<()> {
        let epoch = self.state.read().await.topology.epoch;
        // Discount the entire RPC round trip from the granted lifetime. This
        // makes local authority expire no later than the coordinator's grant.
        let sent_at = Instant::now();
        let response = tokio::time::timeout(REPLICATION_RPC_TIMEOUT, async {
            let mut client = configure_coordinator_client(CoordinatorClient::new(
                self.coordinator_channel.clone(),
            ));
            Ok::<_, anyhow::Error>(
                client
                    .renew_node_lease(proto::RenewNodeLeaseRequest {
                        node_id: self.node_id.clone(),
                        process_instance_id: self.process_instance_id.clone(),
                        topology_epoch: epoch,
                    })
                    .await?
                    .into_inner(),
            )
        })
        .await??;
        if response.topology_epoch == epoch && response.lease_duration_millis > 0 {
            let mut state = self.state.write().await;
            if state.topology.epoch == epoch {
                state.lease = Some((
                    epoch,
                    sent_at + std::time::Duration::from_millis(response.lease_duration_millis),
                ));
            }
        }
        Ok(())
    }

    pub(super) fn start_lease_renewal(&self) {
        let service = self.clone();
        let mut shutdown = self.shutdown.subscribe();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        if service.renew_lease_once().await.is_err() {
                            // The coordinator may have published a new epoch.
                            let _ = service.refresh_topology().await;
                        }
                    }
                    _ = shutdown.changed() => {
                        if *shutdown.borrow() { break; }
                    }
                }
            }
        });
    }

    pub(super) fn start_peer_probes(&self) {
        let service = self.clone();
        let mut shutdown = self.shutdown.subscribe();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = interval.tick() => service.probe_peers().await,
                    _ = shutdown.changed() => {
                        if *shutdown.borrow() { break; }
                    }
                }
            }
        });
    }

    pub(super) async fn probe_peers(&self) {
        let (topology, leased) = {
            let state = self.state.read().await;
            let leased = state.lease.is_some_and(|(epoch, expires_at)| {
                epoch == state.topology.epoch && Instant::now() < expires_at
            });
            (state.topology.clone(), leased)
        };
        if !leased {
            return;
        }
        let selected = peer_probe_targets(&topology, &self.node_id);
        if selected.is_empty() {
            return;
        }
        let coordinator =
            configure_coordinator_client(CoordinatorClient::new(self.coordinator_channel.clone()));
        let mut probes = tokio::task::JoinSet::new();
        for member in selected {
            let mut coordinator = coordinator.clone();
            let reporter_node_id = self.node_id.clone();
            let reporter_process_instance_id = self.process_instance_id.clone();
            let topology_epoch = topology.epoch;
            probes.spawn(async move {
                let reachable = tokio::time::timeout(PEER_PROBE_TIMEOUT, async {
                    let mut client = configure_data_node_client(
                        DataNodeClient::connect(member.endpoint.clone())
                            .await
                            .ok()?,
                    );
                    let info = client
                        .get_process_info(proto::Empty {})
                        .await
                        .ok()?
                        .into_inner();
                    Some(info.node_id == member.node_id)
                })
                .await
                .ok()
                .flatten()
                .unwrap_or(false);
                let report = proto::ReportPeerHealthRequest {
                    reporter_node_id,
                    reporter_process_instance_id,
                    peer_node_id: member.node_id.clone(),
                    topology_epoch,
                    reachable,
                };
                let _ = tokio::time::timeout(
                    PEER_PROBE_TIMEOUT,
                    coordinator.report_peer_health(report),
                )
                .await;
            });
        }
        while probes.join_next().await.is_some() {}
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

    pub(super) async fn refresh_if_newer(&self, request_epoch: u64) -> Result<(), Status> {
        if self.state.read().await.topology.epoch >= request_epoch {
            return Ok(());
        }
        let _guard = self.refresh_lock.lock().await;
        if self.state.read().await.topology.epoch >= request_epoch {
            return Ok(());
        }
        self.refresh_topology().await
    }

    pub(super) async fn refresh_topology(&self) -> Result<(), Status> {
        let topology = fetch_topology(&self.coordinator_endpoint)
            .await
            .map_err(|error| Status::unavailable(error.to_string()))?;
        let mut state = self.state.write().await;
        if topology.epoch >= state.topology.epoch {
            let epoch = topology.epoch;
            if epoch != state.topology.epoch {
                state.lease = None;
                clear_replica_repair_staging(&mut state);
            }
            state.topology = topology;
            state
                .owner_stream_sequences
                .retain(|(stream_epoch, _), _| *stream_epoch == epoch);
            state
                .owner_stream_unacked
                .retain(|(stream_epoch, _), _| *stream_epoch == epoch);
            state
                .follower_streams
                .retain(|(stream_epoch, _), _| *stream_epoch == epoch);
            prune_ack_progress(&mut state);
            self.replication_dispatch.prune_epoch(epoch);
        }
        Ok(())
    }

    pub(super) fn validate_key(&self, key: &[u8]) -> Option<OperationError> {
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

    pub(super) fn owner_error(
        &self,
        state: &NodeState,
        key: &[u8],
    ) -> Result<Option<OperationError>, Status> {
        let owner = state
            .topology
            .owner(key)
            .map_err(|error| Status::internal(error.to_string()))?;
        if owner.node_id == self.node_id {
            if state.lease.is_none_or(|(epoch, expires_at)| {
                epoch != state.topology.epoch || Instant::now() >= expires_at
            }) {
                return Ok(Some(OperationError {
                    current_epoch: state.topology.epoch,
                    ..operation_error(ErrorCode::LeaseExpired, "owner lease has expired", true)
                }));
            }
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
