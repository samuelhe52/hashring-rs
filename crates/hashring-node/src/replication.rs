use super::*;

pub(super) fn required_followers(
    topology: &TopologySnapshot,
    token: u64,
    status: Option<&proto::ReplicaStatusResponse>,
    owner_node_id: &str,
) -> Result<Vec<String>, OperationError> {
    let epoch = topology.epoch;
    let replicas = topology
        .replica_node_ids_for_token(token)
        .map_err(|error| temporarily_unavailable(epoch, error.to_string()))?;
    let guard = &topology.write_availability_guard;
    if topology.write_ack_policy == WriteAckPolicy::FirstSuccessor && replicas.len() < 2 {
        return Err(temporarily_unavailable(
            epoch,
            "first successor is not in the desired placement",
        ));
    }
    if topology.write_ack_policy == WriteAckPolicy::AllReplicas
        && replicas.len() < topology.desired_replication_factor as usize
    {
        return Err(temporarily_unavailable(
            epoch,
            "complete desired replication factor is unavailable",
        ));
    }
    if guard.minimum_admitted_copies <= 1
        && guard.minimum_healthy_followers == 0
        && topology.write_ack_policy == WriteAckPolicy::OwnerOnly
    {
        return Ok(Vec::new());
    }
    let range = status
        .filter(|status| status.topology_epoch == epoch)
        .and_then(|status| {
            status.ranges.iter().find(|range| {
                range.owner_node_id == owner_node_id
                    && range_contains(range.start_exclusive, range.end_inclusive, token)
            })
        })
        .ok_or_else(|| temporarily_unavailable(epoch, "replica admission status is unavailable"))?;
    let admitted = range
        .followers
        .iter()
        .filter(|follower| follower.admitted)
        .count() as u32;
    let healthy = range
        .followers
        .iter()
        .filter(|follower| {
            follower.admitted
                && follower.lag_known
                && follower.lag_millis <= guard.max_replica_lag_millis
        })
        .count() as u32;
    if admitted.saturating_add(1) < guard.minimum_admitted_copies
        || healthy < guard.minimum_healthy_followers
    {
        return Err(temporarily_unavailable(
            epoch,
            "minimum admitted-copy or healthy-follower guard is not satisfied",
        ));
    }
    let desired: Vec<_> = match topology.write_ack_policy {
        WriteAckPolicy::OwnerOnly => Vec::new(),
        WriteAckPolicy::FirstSuccessor => replicas[1..2].to_vec(),
        WriteAckPolicy::AllReplicas => replicas[1..].to_vec(),
    };
    for node_id in &desired {
        if !range.followers.iter().any(|follower| {
            follower.node_id == *node_id
                && follower.admitted
                && follower.lag_known
                && follower.lag_millis <= guard.max_replica_lag_millis
        }) {
            return Err(OperationError {
                current_epoch: epoch,
                ..operation_error(
                    ErrorCode::ReplicaNotReady,
                    format!("required follower {node_id} is not healthy and admitted"),
                    true,
                )
            });
        }
    }
    Ok(desired.into_iter().map(str::to_owned).collect())
}

pub(super) fn prepare_replication_entries(
    state: &mut NodeState,
    key: &[u8],
    value: &[u8],
    deleted: bool,
    version: &RecordVersion,
    mutation_id: String,
) -> Result<Vec<PreparedReplication>, ()> {
    let token = state.topology.key_token(key);
    let follower_ids: Vec<_> = state
        .topology
        .replica_node_ids_for_token(token)
        .expect("installed topology was validated")
        .into_iter()
        .skip(1)
        .map(str::to_owned)
        .collect();
    let followers: Vec<_> = follower_ids
        .into_iter()
        .map(|node_id| {
            let endpoint = state
                .topology
                .members
                .iter()
                .find(|member| member.node_id == node_id)
                .expect("replica placement references a topology member")
                .endpoint
                .clone();
            (node_id, endpoint)
        })
        .collect();

    let per_entry_bytes = key
        .len()
        .checked_add(value.len())
        .and_then(|bytes| bytes.checked_add(version.owner_node_id.len()))
        .and_then(|bytes| bytes.checked_add(mutation_id.len()))
        .and_then(|bytes| bytes.checked_add(256))
        .ok_or(())?;
    if per_entry_bytes > MAX_PENDING_REPLICATION_BYTES {
        return Err(());
    }
    let mutation = Arc::new(ReplicationMutation {
        topology_epoch: state.topology.epoch,
        owner_node_id: version.owner_node_id.clone(),
        key: Arc::from(key),
        value: Arc::from(value),
        deleted,
        version: version.clone(),
        mutation_id,
    });

    Ok(followers
        .into_iter()
        .map(|(follower_node_id, follower_endpoint)| {
            state
                .ack_progress
                .entry((state.topology.epoch, follower_node_id.clone()))
                .or_insert_with(|| watch::channel(0).0);
            let sequence = state
                .owner_stream_sequences
                .entry((state.topology.epoch, follower_node_id.clone()))
                .or_default();
            *sequence += 1;
            PreparedReplication {
                follower_node_id,
                follower_endpoint,
                stream_sequence: *sequence,
                mutation: mutation.clone(),
            }
        })
        .collect())
}

pub(super) fn rollback_replication_sequences(
    state: &mut NodeState,
    entries: &[PreparedReplication],
) {
    for entry in entries {
        let key = (
            entry.mutation.topology_epoch,
            entry.follower_node_id.clone(),
        );
        let sequence = state
            .owner_stream_sequences
            .get_mut(&key)
            .expect("prepared replication initialized its owner stream");
        debug_assert_eq!(*sequence, entry.stream_sequence);
        *sequence -= 1;
        if *sequence == 0 {
            state.owner_stream_sequences.remove(&key);
        }
    }
}

pub(super) fn replication_backpressure_error(epoch: u64) -> OperationError {
    OperationError {
        current_epoch: epoch,
        ..operation_error(
            ErrorCode::ResourceExhausted,
            "replication queue is full or requires catch-up; retry with backoff",
            true,
        )
    }
}

pub(super) fn replication_fingerprint(entry: &ReplicationEntry) -> String {
    let mut digest = blake3::Hasher::new();
    digest.update(b"hashring-rs:replication-entry:v1\0");
    digest.update(&entry.topology_epoch.to_be_bytes());
    digest.update(&(entry.owner_node_id.len() as u64).to_be_bytes());
    digest.update(entry.owner_node_id.as_bytes());
    digest.update(&entry.stream_sequence.to_be_bytes());
    digest.update(&(entry.key.len() as u64).to_be_bytes());
    digest.update(&entry.key);
    digest.update(&(entry.value.len() as u64).to_be_bytes());
    digest.update(&entry.value);
    digest.update(&[u8::from(entry.deleted)]);
    if let Some(version) = &entry.version {
        digest.update(&version.topology_epoch.to_be_bytes());
        digest.update(&version.owner_sequence.to_be_bytes());
        digest.update(&(version.owner_node_id.len() as u64).to_be_bytes());
        digest.update(version.owner_node_id.as_bytes());
    }
    digest.update(&(entry.mutation_id.len() as u64).to_be_bytes());
    digest.update(entry.mutation_id.as_bytes());
    digest.finalize().to_hex().to_string()
}

pub(super) fn prune_ack_progress(state: &mut NodeState) {
    let started = diagnostics::enabled().then(Instant::now);
    if started.is_some() {
        state.cleanup_timing.ack_prune_calls += 1;
    }
    let current_epoch = state.topology.epoch;
    // Current-epoch streams must always remain available for replication and
    // ACK waiters. Receipt references matter only when retiring older streams.
    // Checking the small stream tables avoids scanning every live receipt on
    // each expiry tick during steady-state writes.
    if state
        .ack_progress
        .keys()
        .chain(state.ack_process_instances.keys())
        .all(|(epoch, _)| *epoch == current_epoch)
    {
        if let Some(started) = started {
            state.cleanup_timing.ack_prune_nanos += diagnostics::nanos(started.elapsed());
        }
        return;
    }
    if started.is_some() {
        state.cleanup_timing.ack_prune_scanned_receipts += state.dedup.len() as u64;
    }
    let live: HashSet<_> = state
        .dedup
        .values()
        .flat_map(|entry| {
            entry
                .required_acks
                .iter()
                .map(|(epoch, node_id, _)| (*epoch, node_id.clone()))
        })
        .collect();
    state
        .ack_progress
        .retain(|key, _| key.0 == current_epoch || live.contains(key));
    state
        .ack_process_instances
        .retain(|key, _| key.0 == current_epoch || live.contains(key));
    if let Some(started) = started {
        state.cleanup_timing.ack_prune_nanos += diagnostics::nanos(started.elapsed());
    }
}

pub(super) fn start_replication_dispatch(
    state: Arc<RwLock<NodeState>>,
    pressure: Arc<NodePressureStats>,
) -> ReplicationDispatcher {
    ReplicationDispatcher {
        state,
        streams: Arc::new(StdMutex::new(HashMap::new())),
        failed_streams: Arc::new(StdMutex::new(HashSet::new())),
        retained_budget: Arc::new(Semaphore::new(MAX_PENDING_REPLICATION_BYTES)),
        active_rpc_budget: Arc::new(Semaphore::new(MAX_PENDING_REPLICATION_BYTES)),
        pressure,
    }
}

impl ReplicationDispatcher {
    fn record_reservation_rejection(&self, reason: &'static str, stream: &ReplicationStreamKey) {
        let count = self
            .pressure
            .replication_reservation_rejections
            .fetch_add(1, AtomicOrdering::Relaxed)
            + 1;
        if count.is_power_of_two() {
            tracing::info!(
                owner = %stream.1,
                follower = %stream.2,
                reason,
                count,
                "replication reservation rejected"
            );
        }
    }

    pub(super) fn prune_epoch(&self, epoch: u64) {
        if let Ok(mut streams) = self.streams.lock() {
            streams.retain(|(stream_epoch, _, _), sender| {
                *stream_epoch == epoch && !sender.is_closed()
            });
        }
        if let Ok(mut failed) = self.failed_streams.lock() {
            failed.retain(|(stream_epoch, _, _)| *stream_epoch == epoch);
        }
    }

    pub(super) fn reserve(
        &self,
        entries: &[PreparedReplication],
    ) -> Result<Vec<ReplicationReservation>, ()> {
        let failed = self.failed_streams.lock().map_err(|_| ())?;
        let mut streams = self.streams.lock().map_err(|_| ())?;
        let mut reservations = Vec::with_capacity(entries.len());
        let budget = entries
            .first()
            .map(|entry| {
                let bytes =
                    u32::try_from(replication_mutation_bytes(&entry.mutation)).map_err(|_| ())?;
                self.retained_budget
                    .clone()
                    .try_acquire_many_owned(bytes)
                    .map(ReplicationBudget)
                    .map(Arc::new)
                    .map_err(|_| {
                        self.record_reservation_rejection(
                            "retained_budget",
                            &replication_stream_key(entry),
                        );
                    })
            })
            .transpose()?;
        for entry in entries {
            let stream_key = replication_stream_key(entry);
            if failed.contains(&stream_key) {
                self.record_reservation_rejection("failed_stream", &stream_key);
                return Err(());
            }
            let sender = streams.entry(stream_key.clone()).or_insert_with(|| {
                let (sender, receiver) = mpsc::channel(REPLICATION_STREAM_QUEUE_CAPACITY);
                tokio::spawn(deliver_replication_stream(
                    self.state.clone(),
                    self.failed_streams.clone(),
                    self.active_rpc_budget.clone(),
                    self.pressure.clone(),
                    stream_key.clone(),
                    receiver,
                ));
                sender
            });
            let queue = sender.clone().try_reserve_owned().map_err(|_| {
                self.record_reservation_rejection("stream_queue", &stream_key);
            })?;
            self.pressure.replication_stream_peak_pending.fetch_max(
                (sender.max_capacity() - sender.capacity()) as u64,
                AtomicOrdering::Relaxed,
            );
            reservations.push(ReplicationReservation {
                queue,
                budget: budget.clone().expect("non-empty replication has a budget"),
            });
        }
        Ok(reservations)
    }

    pub(super) fn dispatch(
        &self,
        entries: Vec<PreparedReplication>,
        reservations: Vec<ReplicationReservation>,
    ) {
        debug_assert_eq!(entries.len(), reservations.len());
        for (prepared, reservation) in entries.into_iter().zip(reservations) {
            reservation.queue.send(PendingReplication {
                prepared,
                _budget: reservation.budget,
            });
        }
    }
}

pub(super) fn replication_stream_key(entry: &PreparedReplication) -> ReplicationStreamKey {
    (
        entry.mutation.topology_epoch,
        entry.mutation.owner_node_id.clone(),
        entry.follower_node_id.clone(),
    )
}

pub(super) fn replication_mutation_bytes(mutation: &ReplicationMutation) -> usize {
    mutation.key.len()
        + mutation.value.len()
        + mutation.owner_node_id.len()
        + mutation.mutation_id.len()
        + 256
}

pub(super) async fn deliver_replication_stream(
    state: Arc<RwLock<NodeState>>,
    failed_streams: Arc<StdMutex<HashSet<ReplicationStreamKey>>>,
    active_rpc_budget: Arc<Semaphore>,
    pressure: Arc<NodePressureStats>,
    stream_key: ReplicationStreamKey,
    mut entries: mpsc::Receiver<PendingReplication>,
) {
    let mut client = None;
    while let Some(pending) = entries.recv().await {
        let prepared = &pending.prepared;
        let mut retry_delay = std::time::Duration::from_millis(25);
        loop {
            if state.read().await.topology.epoch != prepared.mutation.topology_epoch {
                return;
            }
            if client.is_none() {
                match tokio::time::timeout(
                    REPLICATION_RPC_TIMEOUT,
                    DataNodeClient::connect(prepared.follower_endpoint.clone()),
                )
                .await
                {
                    Ok(Ok(connected)) => client = Some(configure_data_node_client(connected)),
                    Ok(Err(_)) | Err(_) => {
                        tokio::time::sleep(retry_delay).await;
                        retry_delay = (retry_delay * 2).min(std::time::Duration::from_secs(1));
                        continue;
                    }
                }
            }
            let active_bytes = u32::try_from(replication_mutation_bytes(&prepared.mutation))
                .expect("validated replication size fits u32");
            let Ok(active_budget) = active_rpc_budget
                .clone()
                .acquire_many_owned(active_bytes)
                .await
            else {
                return;
            };
            let result = tokio::time::timeout(
                REPLICATION_RPC_TIMEOUT,
                client
                    .as_mut()
                    .expect("replication client was connected above")
                    .replicate_mutation(prepared.to_proto()),
            )
            .await
            .map(|result| result.map(Response::into_inner));
            drop(active_budget);
            match result {
                Ok(Ok(response))
                    if !response.process_instance_id.is_empty()
                        && response.applied_stream_sequence >= prepared.stream_sequence =>
                {
                    let mut state = diagnostics::write(&state, &pressure.ack_write_timing).await;
                    let stream = (
                        prepared.mutation.topology_epoch,
                        prepared.follower_node_id.clone(),
                    );
                    let previous_instance = state
                        .ack_process_instances
                        .insert(stream, response.process_instance_id.clone());
                    let instance_changed =
                        previous_instance.as_ref() != Some(&response.process_instance_id);
                    if let Some(progress) = state.ack_progress.get(&(
                        prepared.mutation.topology_epoch,
                        prepared.follower_node_id.clone(),
                    )) {
                        progress.send_if_modified(|sequence| {
                            if instance_changed || *sequence < response.applied_stream_sequence {
                                *sequence = response.applied_stream_sequence;
                                true
                            } else {
                                false
                            }
                        });
                    }
                    if let Some(queue) = state.owner_stream_unacked.get_mut(&(
                        prepared.mutation.topology_epoch,
                        prepared.follower_node_id.clone(),
                    )) {
                        while queue.front().is_some_and(|(sequence, _)| {
                            *sequence <= response.applied_stream_sequence
                        }) {
                            queue.pop_front();
                        }
                    }
                    break;
                }
                Ok(Err(status)) if retryable_replication_status(status.code()) => {
                    let count = pressure
                        .replication_rpc_retries
                        .fetch_add(1, AtomicOrdering::Relaxed)
                        + 1;
                    if count.is_power_of_two() {
                        tracing::info!(owner = %stream_key.1, follower = %stream_key.2, code = ?status.code(), message = status.message(), count, "replication RPC retrying");
                    }
                    client = None;
                    tokio::time::sleep(retry_delay).await;
                    retry_delay = (retry_delay * 2).min(std::time::Duration::from_secs(1));
                }
                Err(_) => {
                    pressure
                        .replication_rpc_retries
                        .fetch_add(1, AtomicOrdering::Relaxed);
                    client = None;
                    tokio::time::sleep(retry_delay).await;
                    retry_delay = (retry_delay * 2).min(std::time::Duration::from_secs(1));
                }
                Ok(Ok(_)) | Ok(Err(_)) => {
                    if let Ok(mut failed) = failed_streams.lock() {
                        failed.insert(stream_key);
                    }
                    return;
                }
            }
        }
    }
}

pub(super) fn retryable_replication_status(code: tonic::Code) -> bool {
    matches!(
        code,
        tonic::Code::Aborted
            | tonic::Code::Cancelled
            | tonic::Code::Unknown
            | tonic::Code::DeadlineExceeded
            | tonic::Code::ResourceExhausted
            | tonic::Code::Unavailable
    )
}
