use super::*;

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
        self.refresh_if_newer(request.topology_epoch).await?;
        let state = self.state.read().await;
        if let Some(error) = self.owner_error(&state, &request.key)? {
            return Ok(Response::new(GetResponse {
                current_epoch: error.current_epoch,
                error: Some(error),
                ..Default::default()
            }));
        }
        let Some(record) = state
            .records
            .get(&request.key)
            .filter(|record| !record.deleted)
        else {
            return Ok(Response::new(GetResponse {
                current_epoch: state.topology.epoch,
                error: Some(OperationError {
                    current_epoch: state.topology.epoch,
                    ..operation_error(ErrorCode::NotFound, "key not found", false)
                }),
                ..Default::default()
            }));
        };
        Ok(Response::new(GetResponse {
            value: record.value.to_vec(),
            version: Some(record.version.clone()),
            current_epoch: state.topology.epoch,
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
        if request.request_id.is_empty() || request.request_id.len() > MAX_MUTATION_ID_BYTES {
            return Ok(Response::new(PutResponse {
                error: Some(operation_error(
                    ErrorCode::InvalidArgument,
                    format!("mutation id must contain 1 to {MAX_MUTATION_ID_BYTES} bytes"),
                    false,
                )),
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
        self.refresh_if_newer(request.topology_epoch).await?;
        {
            let state = self.state.read().await;
            if let Some(error) = self.owner_error(&state, &request.key)? {
                return Ok(Response::new(PutResponse {
                    current_epoch: error.current_epoch,
                    error: Some(error),
                    ..Default::default()
                }));
            }
            if let Some(existing) = state
                .dedup
                .get(request.request_id.as_str())
                .filter(|entry| entry.expires_at > Instant::now())
            {
                if existing.fingerprint != mutation_fingerprint(&request.key, &request.value, false)
                    || existing.deleted
                {
                    return Ok(Response::new(PutResponse {
                        current_epoch: state.topology.epoch,
                        error: Some(operation_error(
                            ErrorCode::MutationIdConflict,
                            "mutation ID was reused with a different operation or payload",
                            false,
                        )),
                        ..Default::default()
                    }));
                }
                let version = existing.version.clone();
                let required = existing.required_acks.clone();
                let epoch = state.topology.epoch;
                drop(state);
                let error = self
                    .wait_required_acks(&required, epoch, &request.request_id, &version)
                    .await
                    .err();
                return Ok(Response::new(PutResponse {
                    version: error.is_none().then_some(version),
                    current_epoch: epoch,
                    error,
                }));
            }
        }
        let (readiness_epoch, ready_followers) = match self.ready_followers(&request.key).await {
            Ok(ready) => ready,
            Err(error) => {
                return Ok(Response::new(PutResponse {
                    current_epoch: error.current_epoch,
                    error: Some(error),
                    ..Default::default()
                }));
            }
        };
        let mut state = self.state.write().await;
        if let Some(error) = self.owner_error(&state, &request.key)? {
            return Ok(Response::new(PutResponse {
                current_epoch: error.current_epoch,
                error: Some(error),
                ..Default::default()
            }));
        }
        if state.topology.epoch != readiness_epoch {
            let error = temporarily_unavailable(
                state.topology.epoch,
                "topology changed while checking replica readiness",
            );
            return Ok(Response::new(PutResponse {
                current_epoch: error.current_epoch,
                error: Some(error),
                ..Default::default()
            }));
        }
        let dedup_now = Instant::now();
        purge_expired_dedup(&mut state, dedup_now);
        let fingerprint = mutation_fingerprint(&request.key, &request.value, false);
        if let Some(existing) = state.dedup.get(request.request_id.as_str()) {
            if existing.fingerprint == fingerprint && !existing.deleted {
                let version = existing.version.clone();
                let required = existing.required_acks.clone();
                let epoch = state.topology.epoch;
                drop(state);
                let error = self
                    .wait_required_acks(&required, epoch, &request.request_id, &version)
                    .await
                    .err();
                return Ok(Response::new(PutResponse {
                    version: error.is_none().then_some(version),
                    current_epoch: epoch,
                    error,
                }));
            }
            return Ok(Response::new(PutResponse {
                current_epoch: state.topology.epoch,
                error: Some(operation_error(
                    ErrorCode::MutationIdConflict,
                    "mutation ID was reused with a different operation or payload",
                    false,
                )),
                ..Default::default()
            }));
        }
        let ack_cost = required_ack_retained_bytes(&ready_followers);
        let dedup_cost =
            dedup_retained_bytes(&request.request_id, request.key.len(), self.node_id.len())
                .saturating_add(ack_cost);
        if state.dedup_bytes.saturating_add(dedup_cost) > self.max_dedup_bytes {
            return Ok(Response::new(PutResponse {
                current_epoch: state.topology.epoch,
                error: Some(self.owner_dedup_full_error(&state)),
                ..Default::default()
            }));
        }

        if state.policy_write_fence.is_some() {
            return Ok(Response::new(PutResponse {
                current_epoch: state.topology.epoch,
                error: Some(OperationError {
                    current_epoch: state.topology.epoch,
                    ..operation_error(
                        ErrorCode::RangeBusy,
                        "writes are fenced for ACK-policy transition",
                        true,
                    )
                }),
                ..Default::default()
            }));
        }
        let token = state.topology.key_token(&request.key);
        let journal_record_bytes =
            request.key.len() + request.value.len() + request.request_id.len() + 128;
        let matching_sources = state
            .sources
            .values()
            .filter(|source| source.range.contains(token))
            .count();
        for source in state
            .sources
            .values()
            .filter(|source| source.range.contains(token))
        {
            if source.writes_paused {
                return Ok(Response::new(PutResponse {
                    current_epoch: state.topology.epoch,
                    error: Some(OperationError {
                        current_epoch: state.topology.epoch,
                        ..operation_error(
                            ErrorCode::RangeBusy,
                            "range writes are fenced for topology cutover",
                            true,
                        )
                    }),
                    ..Default::default()
                }));
            }
        }
        if state
            .destinations
            .values()
            .any(|destination| !destination.writes_activated && destination.range.contains(token))
        {
            return Ok(Response::new(PutResponse {
                current_epoch: state.topology.epoch,
                error: Some(OperationError {
                    current_epoch: state.topology.epoch,
                    ..operation_error(
                        ErrorCode::RangeBusy,
                        "range writes are fenced for topology cutover",
                        true,
                    )
                }),
                ..Default::default()
            }));
        }
        let journal_growth = journal_record_bytes.saturating_mul(matching_sources);
        if state.journal_bytes_total.saturating_add(journal_growth) > self.max_journal_bytes {
            return Ok(Response::new(PutResponse {
                current_epoch: state.topology.epoch,
                error: Some(OperationError {
                    current_epoch: state.topology.epoch,
                    ..operation_error(
                        ErrorCode::ResourceExhausted,
                        "node migration journal budget is full; retry with backoff",
                        true,
                    )
                }),
                ..Default::default()
            }));
        }

        let next_sequence = state.next_sequence + 1;
        let version = RecordVersion {
            topology_epoch: state.topology.epoch,
            owner_sequence: next_sequence,
            owner_node_id: self.node_id.clone(),
        };
        let mutation_id = request.request_id.clone();
        let replications = match prepare_replication_entries(
            &mut state,
            &request.key,
            &request.value,
            false,
            &version,
            request.request_id,
        ) {
            Ok(replications) => replications,
            Err(()) => {
                return Ok(Response::new(PutResponse {
                    current_epoch: state.topology.epoch,
                    error: Some(replication_backpressure_error(state.topology.epoch)),
                    ..Default::default()
                }));
            }
        };
        let reservations = match self.replication_dispatch.reserve(&replications) {
            Ok(reservations) => reservations,
            Err(()) => {
                rollback_replication_sequences(&mut state, &replications);
                return Ok(Response::new(PutResponse {
                    current_epoch: state.topology.epoch,
                    error: Some(replication_backpressure_error(state.topology.epoch)),
                    ..Default::default()
                }));
            }
        };
        let required_acks: Vec<RequiredAck> = replications
            .iter()
            .filter(|entry| ready_followers.contains(&entry.follower_node_id))
            .map(|entry| {
                (
                    entry.mutation.topology_epoch,
                    entry.follower_node_id.clone(),
                    entry.stream_sequence,
                )
            })
            .collect();
        state.next_sequence = next_sequence;
        let now = now_unix_millis();
        for entry in &replications {
            state
                .owner_stream_unacked
                .entry((
                    entry.mutation.topology_epoch,
                    entry.follower_node_id.clone(),
                ))
                .or_default()
                .push_back((entry.stream_sequence, now));
        }
        let record = Record {
            value: request.value.into(),
            version: version.clone(),
            deleted: false,
        };
        state.records.insert(request.key.clone(), record.clone());
        for source in state
            .sources
            .values_mut()
            .filter(|source| source.range.contains(token))
        {
            source.watermark += 1;
            source.journal_bytes += journal_record_bytes;
            source.journal.push(JournalRecord {
                watermark: source.watermark,
                record: Some(MigrationRecord {
                    key: request.key.clone(),
                    value: record.value.to_vec(),
                    version: Some(record.version.clone()),
                    deleted: false,
                    mutation_id: mutation_id.clone(),
                    remaining_window_millis: 0,
                }),
            });
        }
        state.journal_bytes_total += journal_growth;
        insert_dedup(
            &mut state,
            mutation_id.clone(),
            Arc::from(request.key.as_slice()),
            fingerprint,
            version.clone(),
            false,
            Instant::now(),
        );
        let dedup = state
            .dedup
            .get_mut(mutation_id.as_str())
            .expect("new retry record exists");
        dedup.required_acks = required_acks.clone();
        dedup.retained_bytes += ack_cost;
        state.dedup_bytes += ack_cost;
        state.dedup_peak_bytes = state.dedup_peak_bytes.max(state.dedup_bytes);
        let current_epoch = state.topology.epoch;
        self.replication_dispatch
            .dispatch(replications, reservations);
        drop(state);
        let error = self
            .wait_required_acks(&required_acks, current_epoch, &mutation_id, &version)
            .await
            .err();
        Ok(Response::new(PutResponse {
            version: error.is_none().then_some(version),
            current_epoch,
            error,
        }))
    }

    async fn delete(
        &self,
        request: Request<DeleteRequest>,
    ) -> Result<Response<DeleteResponse>, Status> {
        let request = request.into_inner();
        if let Some(error) = self.validate_key(&request.key) {
            return Ok(Response::new(DeleteResponse {
                error: Some(error),
                ..Default::default()
            }));
        }
        if request.request_id.is_empty() || request.request_id.len() > MAX_MUTATION_ID_BYTES {
            return Ok(Response::new(DeleteResponse {
                error: Some(operation_error(
                    ErrorCode::InvalidArgument,
                    format!("mutation id must contain 1 to {MAX_MUTATION_ID_BYTES} bytes"),
                    false,
                )),
                ..Default::default()
            }));
        }
        self.refresh_if_newer(request.topology_epoch).await?;
        {
            let state = self.state.read().await;
            if let Some(error) = self.owner_error(&state, &request.key)? {
                return Ok(Response::new(DeleteResponse {
                    current_epoch: error.current_epoch,
                    error: Some(error),
                }));
            }
            if let Some(existing) = state
                .dedup
                .get(request.request_id.as_str())
                .filter(|entry| entry.expires_at > Instant::now())
            {
                if existing.fingerprint != mutation_fingerprint(&request.key, &[], true)
                    || !existing.deleted
                {
                    return Ok(Response::new(DeleteResponse {
                        current_epoch: state.topology.epoch,
                        error: Some(operation_error(
                            ErrorCode::MutationIdConflict,
                            "mutation ID was reused with a different operation or payload",
                            false,
                        )),
                    }));
                }
                let version = existing.version.clone();
                let required = existing.required_acks.clone();
                let epoch = state.topology.epoch;
                drop(state);
                let error = self
                    .wait_required_acks(&required, epoch, &request.request_id, &version)
                    .await
                    .err();
                return Ok(Response::new(DeleteResponse {
                    current_epoch: epoch,
                    error,
                }));
            }
        }
        let (readiness_epoch, ready_followers) = match self.ready_followers(&request.key).await {
            Ok(ready) => ready,
            Err(error) => {
                return Ok(Response::new(DeleteResponse {
                    current_epoch: error.current_epoch,
                    error: Some(error),
                }));
            }
        };
        let mut state = self.state.write().await;
        if let Some(error) = self.owner_error(&state, &request.key)? {
            return Ok(Response::new(DeleteResponse {
                current_epoch: error.current_epoch,
                error: Some(error),
            }));
        }
        if state.topology.epoch != readiness_epoch {
            let error = temporarily_unavailable(
                state.topology.epoch,
                "topology changed while checking replica readiness",
            );
            return Ok(Response::new(DeleteResponse {
                current_epoch: error.current_epoch,
                error: Some(error),
            }));
        }
        let dedup_now = Instant::now();
        purge_expired_dedup(&mut state, dedup_now);
        let fingerprint = mutation_fingerprint(&request.key, &[], true);
        if let Some(existing) = state.dedup.get(request.request_id.as_str()) {
            if existing.fingerprint == fingerprint && existing.deleted {
                let version = existing.version.clone();
                let required = existing.required_acks.clone();
                let epoch = state.topology.epoch;
                drop(state);
                let error = self
                    .wait_required_acks(&required, epoch, &request.request_id, &version)
                    .await
                    .err();
                return Ok(Response::new(DeleteResponse {
                    current_epoch: epoch,
                    error,
                }));
            }
            return Ok(Response::new(DeleteResponse {
                current_epoch: state.topology.epoch,
                error: Some(operation_error(
                    ErrorCode::MutationIdConflict,
                    "mutation ID was reused with a different operation or payload",
                    false,
                )),
            }));
        }
        let ack_cost = required_ack_retained_bytes(&ready_followers);
        let dedup_cost =
            dedup_retained_bytes(&request.request_id, request.key.len(), self.node_id.len())
                .saturating_add(ack_cost);
        if state.dedup_bytes.saturating_add(dedup_cost) > self.max_dedup_bytes {
            return Ok(Response::new(DeleteResponse {
                current_epoch: state.topology.epoch,
                error: Some(self.owner_dedup_full_error(&state)),
            }));
        }

        if state.policy_write_fence.is_some() {
            return Ok(Response::new(DeleteResponse {
                current_epoch: state.topology.epoch,
                error: Some(OperationError {
                    current_epoch: state.topology.epoch,
                    ..operation_error(
                        ErrorCode::RangeBusy,
                        "writes are fenced for ACK-policy transition",
                        true,
                    )
                }),
            }));
        }
        let token = state.topology.key_token(&request.key);
        for source in state
            .sources
            .values()
            .filter(|source| source.range.contains(token))
        {
            if source.writes_paused {
                return Ok(Response::new(DeleteResponse {
                    current_epoch: state.topology.epoch,
                    error: Some(OperationError {
                        current_epoch: state.topology.epoch,
                        ..operation_error(
                            ErrorCode::RangeBusy,
                            "range writes are fenced for topology cutover",
                            true,
                        )
                    }),
                }));
            }
        }
        if state
            .destinations
            .values()
            .any(|destination| !destination.writes_activated && destination.range.contains(token))
        {
            return Ok(Response::new(DeleteResponse {
                current_epoch: state.topology.epoch,
                error: Some(OperationError {
                    current_epoch: state.topology.epoch,
                    ..operation_error(
                        ErrorCode::RangeBusy,
                        "range writes are fenced for topology cutover",
                        true,
                    )
                }),
            }));
        }

        let journal_record_bytes = request.key.len() + request.request_id.len() + 128;
        let matching_sources = state
            .sources
            .values()
            .filter(|source| source.range.contains(token))
            .count();
        let journal_growth = journal_record_bytes.saturating_mul(matching_sources);
        if state.journal_bytes_total.saturating_add(journal_growth) > self.max_journal_bytes {
            return Ok(Response::new(DeleteResponse {
                current_epoch: state.topology.epoch,
                error: Some(OperationError {
                    current_epoch: state.topology.epoch,
                    ..operation_error(
                        ErrorCode::ResourceExhausted,
                        "node migration journal budget is full; retry with backoff",
                        true,
                    )
                }),
            }));
        }

        let next_sequence = state.next_sequence + 1;
        let version = RecordVersion {
            topology_epoch: state.topology.epoch,
            owner_sequence: next_sequence,
            owner_node_id: self.node_id.clone(),
        };
        let mutation_id = request.request_id.clone();
        let replications = match prepare_replication_entries(
            &mut state,
            &request.key,
            &[],
            true,
            &version,
            request.request_id,
        ) {
            Ok(replications) => replications,
            Err(()) => {
                return Ok(Response::new(DeleteResponse {
                    current_epoch: state.topology.epoch,
                    error: Some(replication_backpressure_error(state.topology.epoch)),
                }));
            }
        };
        let required_acks: Vec<RequiredAck> = replications
            .iter()
            .filter(|entry| ready_followers.contains(&entry.follower_node_id))
            .map(|entry| {
                (
                    entry.mutation.topology_epoch,
                    entry.follower_node_id.clone(),
                    entry.stream_sequence,
                )
            })
            .collect();
        let reservations = match self.replication_dispatch.reserve(&replications) {
            Ok(reservations) => reservations,
            Err(()) => {
                rollback_replication_sequences(&mut state, &replications);
                return Ok(Response::new(DeleteResponse {
                    current_epoch: state.topology.epoch,
                    error: Some(replication_backpressure_error(state.topology.epoch)),
                }));
            }
        };
        state.next_sequence = next_sequence;
        let now = now_unix_millis();
        for entry in &replications {
            state
                .owner_stream_unacked
                .entry((
                    entry.mutation.topology_epoch,
                    entry.follower_node_id.clone(),
                ))
                .or_default()
                .push_back((entry.stream_sequence, now));
        }
        apply_internal_record(
            &mut state.records,
            request.key.clone(),
            Record {
                value: Arc::from([]),
                version: version.clone(),
                deleted: true,
            },
        );
        for source in state
            .sources
            .values_mut()
            .filter(|source| source.range.contains(token))
        {
            source.watermark += 1;
            source.journal_bytes += journal_record_bytes;
            source.journal.push(JournalRecord {
                watermark: source.watermark,
                record: Some(MigrationRecord {
                    key: request.key.clone(),
                    value: Vec::new(),
                    version: Some(version.clone()),
                    deleted: true,
                    mutation_id: mutation_id.clone(),
                    remaining_window_millis: 0,
                }),
            });
        }
        state.journal_bytes_total += journal_growth;
        insert_dedup(
            &mut state,
            mutation_id.clone(),
            Arc::from(request.key.as_slice()),
            fingerprint,
            version.clone(),
            true,
            Instant::now(),
        );
        let dedup = state
            .dedup
            .get_mut(mutation_id.as_str())
            .expect("new retry record exists");
        dedup.required_acks = required_acks.clone();
        dedup.retained_bytes += ack_cost;
        state.dedup_bytes += ack_cost;
        state.dedup_peak_bytes = state.dedup_peak_bytes.max(state.dedup_bytes);
        let current_epoch = state.topology.epoch;
        self.replication_dispatch
            .dispatch(replications, reservations);
        drop(state);
        let error = self
            .wait_required_acks(&required_acks, current_epoch, &mutation_id, &version)
            .await
            .err();
        Ok(Response::new(DeleteResponse {
            current_epoch,
            error,
        }))
    }

    async fn get_replication_progress(
        &self,
        request: Request<ReplicationProgressRequest>,
    ) -> Result<Response<ReplicationProgressResponse>, Status> {
        let request = request.into_inner();
        let state = self.state.read().await;
        if request.topology_epoch != state.topology.epoch {
            return Err(Status::failed_precondition(
                "replication progress epoch is stale",
            ));
        }
        if request.owner_node_id.is_empty() || request.follower_node_id.is_empty() {
            return Err(Status::invalid_argument(
                "replication stream identity is empty",
            ));
        }
        let (stream_sequence, oldest_unacked_unix_millis) = if self.node_id == request.owner_node_id
        {
            if self
                .replication_dispatch
                .failed_streams
                .lock()
                .map_err(|_| Status::internal("replication stream state is poisoned"))?
                .contains(&(
                    request.topology_epoch,
                    request.owner_node_id.clone(),
                    request.follower_node_id.clone(),
                ))
            {
                return Err(Status::failed_precondition(
                    "owner replication stream is terminal",
                ));
            }
            let key = (request.topology_epoch, request.follower_node_id.clone());
            (
                state
                    .owner_stream_sequences
                    .get(&key)
                    .copied()
                    .unwrap_or_default(),
                state
                    .owner_stream_unacked
                    .get(&key)
                    .and_then(|queue| queue.front().map(|(_, at)| *at))
                    .unwrap_or_default(),
            )
        } else if self.node_id == request.follower_node_id {
            let key = (request.topology_epoch, request.owner_node_id.clone());
            state
                .follower_streams
                .get(&key)
                .map_or((0, 0), |stream| (stream.applied_sequence, 0))
        } else {
            return Err(Status::failed_precondition(
                "node is not part of this replication stream",
            ));
        };
        Ok(Response::new(ReplicationProgressResponse {
            process_instance_id: self.process_instance_id.clone(),
            stream_sequence,
            oldest_unacked_unix_millis,
            last_ack_sequence: if self.node_id == request.owner_node_id {
                state
                    .ack_progress
                    .get(&(request.topology_epoch, request.follower_node_id.clone()))
                    .map_or(0, |progress| *progress.borrow())
            } else {
                0
            },
            last_ack_known: self.node_id == request.owner_node_id
                && (stream_sequence == 0
                    || state
                        .ack_progress
                        .contains_key(&(request.topology_epoch, request.follower_node_id))),
        }))
    }

    async fn install_replication_checkpoint(
        &self,
        request: Request<ReplicationCheckpointRequest>,
    ) -> Result<Response<ReplicationProgressResponse>, Status> {
        let request = request.into_inner();
        let mut state = self.state.write().await;
        if request.topology_epoch != state.topology.epoch
            || request.follower_node_id != self.node_id
        {
            return Err(Status::failed_precondition(
                "checkpoint does not match follower epoch",
            ));
        }
        let expected: HashSet<_> = state
            .topology
            .derived_ranges()
            .map_err(|error| Status::internal(error.to_string()))?
            .into_iter()
            .filter(|range| {
                range.owner_node_id == request.owner_node_id
                    && range.follower_node_ids.contains(&self.node_id)
            })
            .map(|range| (range.start_exclusive, range.end_inclusive))
            .collect();
        if expected.is_empty() || request.verified_ranges.len() != expected.len() {
            return Err(Status::failed_precondition(
                "checkpoint does not cover the full follower stream",
            ));
        }
        let mut actual = HashSet::new();
        for control in &request.verified_ranges {
            let destination = state
                .destinations
                .get(&(control.change_id.clone(), control.range_id.clone()))
                .ok_or_else(|| Status::failed_precondition("checkpoint range was not prepared"))?;
            if !destination.committed
                || destination.range.source_node_id != request.owner_node_id
                || destination.range.destination_node_id != self.node_id
                || !actual.insert((
                    destination.range.start_exclusive,
                    destination.range.end_inclusive,
                ))
            {
                return Err(Status::failed_precondition(
                    "checkpoint contains an unverified or duplicate range",
                ));
            }
        }
        if actual != expected {
            return Err(Status::failed_precondition(
                "checkpoint has incomplete stream coverage",
            ));
        }
        let stream = state
            .follower_streams
            .entry((request.topology_epoch, request.owner_node_id))
            .or_default();
        if request.stream_sequence < stream.applied_sequence {
            return Err(Status::failed_precondition(
                "checkpoint would rewind the follower stream",
            ));
        }
        stream.applied_sequence = request.stream_sequence;
        stream.checkpoint_sequence = request.stream_sequence;
        Ok(Response::new(ReplicationProgressResponse {
            process_instance_id: self.process_instance_id.clone(),
            stream_sequence: stream.applied_sequence,
            oldest_unacked_unix_millis: 0,
            last_ack_sequence: 0,
            last_ack_known: false,
        }))
    }

    async fn replicate_mutation(
        &self,
        request: Request<ReplicationEntry>,
    ) -> Result<Response<ReplicateMutationResponse>, Status> {
        let entry = request.into_inner();
        if entry.stream_sequence == 0 {
            return Err(Status::invalid_argument(
                "replication stream sequence must be greater than zero",
            ));
        }
        if entry.mutation_id.is_empty() || entry.mutation_id.len() > MAX_MUTATION_ID_BYTES {
            return Err(Status::invalid_argument(format!(
                "mutation id must contain 1 to {MAX_MUTATION_ID_BYTES} bytes"
            )));
        }
        if entry.key.is_empty() || entry.key.len() > self.max_key_bytes {
            return Err(Status::invalid_argument("replication key size is invalid"));
        }
        if entry.value.len() > self.max_value_bytes {
            return Err(Status::resource_exhausted(
                "replication value exceeds the node limit",
            ));
        }
        if entry.deleted && !entry.value.is_empty() {
            return Err(Status::invalid_argument(
                "deleted replication entry must omit its value",
            ));
        }
        let version = entry
            .version
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("replication entry omitted version"))?;
        if version.topology_epoch != entry.topology_epoch
            || version.owner_node_id != entry.owner_node_id
        {
            return Err(Status::failed_precondition(
                "replication entry version does not match its stream authority",
            ));
        }

        let mut state = self.state.write().await;
        if entry.topology_epoch != state.topology.epoch {
            return Err(Status::failed_precondition(format!(
                "replication epoch {} does not match installed epoch {}",
                entry.topology_epoch, state.topology.epoch
            )));
        }
        let owner = state
            .topology
            .owner(&entry.key)
            .map_err(|error| Status::internal(error.to_string()))?;
        if owner.node_id != entry.owner_node_id {
            return Err(Status::failed_precondition(
                "replication entry was not issued by the current owner",
            ));
        }
        let replicas = state
            .topology
            .replica_node_ids_for_token(state.topology.key_token(&entry.key))
            .map_err(|error| Status::internal(error.to_string()))?;
        if !replicas[1..].contains(&self.node_id.as_str()) {
            return Err(Status::failed_precondition(
                "this node is not a desired follower for the mutation",
            ));
        }

        let fingerprint = replication_fingerprint(&entry);
        let stream_key = (entry.topology_epoch, entry.owner_node_id.clone());
        let stream = state.follower_streams.entry(stream_key).or_default();
        if entry.stream_sequence <= stream.applied_sequence {
            if stream
                .fingerprints
                .get(&entry.stream_sequence)
                .is_some_and(|existing| existing == &fingerprint)
            {
                return Ok(Response::new(ReplicateMutationResponse {
                    process_instance_id: self.process_instance_id.clone(),
                    applied_stream_sequence: stream.applied_sequence,
                }));
            }
            // The verified full-stream snapshot supersedes queued entries from
            // before its checkpoint. They cannot mutate data after the snapshot.
            if entry.stream_sequence <= stream.checkpoint_sequence
                && !stream.fingerprints.contains_key(&entry.stream_sequence)
            {
                return Ok(Response::new(ReplicateMutationResponse {
                    process_instance_id: self.process_instance_id.clone(),
                    applied_stream_sequence: stream.applied_sequence,
                }));
            }
            return Err(Status::already_exists(
                "replication stream sequence conflicts with an applied entry",
            ));
        }
        let expected = stream.applied_sequence.saturating_add(1);
        if entry.stream_sequence != expected {
            return Err(Status::aborted(format!(
                "replication gap: expected sequence {expected}, received {}",
                entry.stream_sequence
            )));
        }
        if version.owner_sequence <= stream.last_owner_sequence {
            return Err(Status::failed_precondition(
                "replication record version did not advance the owner sequence",
            ));
        }

        let dedup_now = Instant::now();
        purge_expired_dedup(&mut state, dedup_now);
        let mutation_fingerprint = mutation_fingerprint(&entry.key, &entry.value, entry.deleted);
        if state.dedup.contains_key(entry.mutation_id.as_str()) {
            return Err(Status::already_exists(
                "replication stream reused a live mutation ID",
            ));
        }
        if state.dedup_bytes.saturating_add(dedup_retained_bytes(
            &entry.mutation_id,
            entry.key.len(),
            version.owner_node_id.len(),
        )) > self.max_dedup_bytes
        {
            self.record_follower_dedup_rejection(&state);
            return Err(Status::resource_exhausted(
                "follower mutation retry window is full",
            ));
        }
        let mutation_id = entry.mutation_id.clone();
        let dedup_key: Arc<[u8]> = Arc::from(entry.key.as_slice());

        apply_internal_record(
            &mut state.records,
            entry.key,
            Record {
                value: entry.value.into(),
                version: version.clone(),
                deleted: entry.deleted,
            },
        );
        let stream = state
            .follower_streams
            .get_mut(&(entry.topology_epoch, entry.owner_node_id))
            .expect("replication stream was initialized above");
        stream.applied_sequence = entry.stream_sequence;
        stream.last_owner_sequence = version.owner_sequence;
        stream
            .fingerprints
            .insert(entry.stream_sequence, fingerprint);
        if entry.stream_sequence > MAX_REPLICATION_FINGERPRINTS {
            stream
                .fingerprints
                .remove(&(entry.stream_sequence - MAX_REPLICATION_FINGERPRINTS));
        }
        let applied_stream_sequence = stream.applied_sequence;
        insert_dedup(
            &mut state,
            mutation_id,
            dedup_key,
            mutation_fingerprint,
            version.clone(),
            entry.deleted,
            Instant::now(),
        );
        Ok(Response::new(ReplicateMutationResponse {
            process_instance_id: self.process_instance_id.clone(),
            applied_stream_sequence,
        }))
    }

    async fn prepare_source_range(
        &self,
        request: Request<PrepareRangeRequest>,
    ) -> Result<Response<PrepareSourceRangeResponse>, Status> {
        let range: RangeSpec = request
            .into_inner()
            .range
            .ok_or_else(|| Status::invalid_argument("missing range"))?
            .try_into()?;
        if range.source_node_id != self.node_id {
            return Err(Status::failed_precondition(
                "this node is not the range source",
            ));
        }
        let key = range.key();
        let (all_keys, dedup_keys, topology, ready) = {
            let mut state = self.state.write().await;
            if let Some(existing) = state.sources.get(&key) {
                if existing.range != range {
                    return Err(Status::failed_precondition(
                        "source range retry conflicts with the prepared specification",
                    ));
                }
                let mut ready = existing.snapshot_ready.subscribe();
                drop(state);
                if !*ready.borrow() {
                    ready.changed().await.map_err(|_| {
                        Status::aborted("source snapshot preparation was cancelled")
                    })?;
                }
                return Ok(Response::new(PrepareSourceRangeResponse {
                    process_instance_id: self.process_instance_id.clone(),
                }));
            }
            let all_keys = state.records.keys().cloned().collect::<Vec<_>>();
            let dedup_keys = state
                .dedup
                .iter()
                .map(|(id, entry)| (id.clone(), entry.key.clone()))
                .collect::<Vec<_>>();
            let topology = state.topology.clone();
            let (ready, _) = watch::channel(false);
            state.sources.insert(
                key.clone(),
                SourceMigration {
                    range: range.clone(),
                    snapshot_keys: None,
                    snapshot_dedup_ids: Vec::new(),
                    snapshot_ready: ready.clone(),
                    journal: Vec::new(),
                    journal_bytes: 0,
                    watermark: 0,
                    writes_paused: false,
                },
            );
            (all_keys, dedup_keys, topology, ready)
        };
        let mut preparation = SnapshotPreparationGuard {
            state: self.state.clone(),
            key: Some(key.clone()),
        };

        // Only keys are captured while traffic is locked. Filtering and sorting
        // happen outside the lock, and values are materialized one bounded page
        // at a time by `read_snapshot_page`.
        let mut snapshot_keys: Vec<_> = all_keys
            .into_iter()
            .filter(|record_key| range.contains(topology.key_token(record_key)))
            .collect();
        snapshot_keys.sort();
        let mut snapshot_dedup_ids: Vec<_> = dedup_keys
            .into_iter()
            .filter(|(_, record_key)| range.contains(topology.key_token(record_key)))
            .map(|(id, _)| id)
            .collect();
        snapshot_dedup_ids.sort();
        let mut state = self.state.write().await;
        let source = state
            .sources
            .get_mut(&key)
            .ok_or_else(|| Status::aborted("source migration was reset during preparation"))?;
        if source.range != range {
            return Err(Status::failed_precondition(
                "source range changed during preparation",
            ));
        }
        source.snapshot_keys = Some(snapshot_keys);
        source.snapshot_dedup_ids = snapshot_dedup_ids;
        ready.send_replace(true);
        preparation.disarm();
        Ok(Response::new(PrepareSourceRangeResponse {
            process_instance_id: self.process_instance_id.clone(),
        }))
    }

    async fn prepare_destination_range(
        &self,
        request: Request<PrepareRangeRequest>,
    ) -> Result<Response<PrepareDestinationRangeResponse>, Status> {
        let range: RangeSpec = request
            .into_inner()
            .range
            .ok_or_else(|| Status::invalid_argument("missing range"))?
            .try_into()?;
        if range.destination_node_id != self.node_id {
            return Err(Status::failed_precondition(
                "this node is not the range destination",
            ));
        }
        let mut state = self.state.write().await;
        if let Some(existing) = state.destinations.get(&range.key()) {
            if existing.range != range {
                return Err(Status::failed_precondition(
                    "destination range retry conflicts with the prepared specification",
                ));
            }
        } else {
            state.destinations.insert(
                range.key(),
                DestinationMigration {
                    range,
                    records: HashMap::new(),
                    dedup: HashMap::new(),
                    dedup_bytes: 0,
                    watermark: 0,
                    committed: false,
                    writes_activated: false,
                },
            );
        }
        Ok(Response::new(PrepareDestinationRangeResponse {
            process_instance_id: self.process_instance_id.clone(),
        }))
    }

    async fn get_process_info(
        &self,
        _request: Request<proto::Empty>,
    ) -> Result<Response<NodeInfoResponse>, Status> {
        let state = self.state.read().await;
        Ok(Response::new(NodeInfoResponse {
            node_id: self.node_id.clone(),
            process_instance_id: self.process_instance_id.clone(),
            dedup_bytes: state.dedup_bytes as u64,
            dedup_peak_bytes: state.dedup_peak_bytes as u64,
            dedup_capacity_bytes: self.max_dedup_bytes as u64,
            owner_dedup_rejections: self
                .pressure
                .owner_dedup_rejections
                .load(AtomicOrdering::Relaxed),
            follower_dedup_rejections: self
                .pressure
                .follower_dedup_rejections
                .load(AtomicOrdering::Relaxed),
            replication_reservation_rejections: self
                .pressure
                .replication_reservation_rejections
                .load(AtomicOrdering::Relaxed),
            replication_rpc_retries: self
                .pressure
                .replication_rpc_retries
                .load(AtomicOrdering::Relaxed),
            replication_stream_peak_pending: self
                .pressure
                .replication_stream_peak_pending
                .load(AtomicOrdering::Relaxed),
        }))
    }

    async fn read_snapshot_page(
        &self,
        request: Request<SnapshotPageRequest>,
    ) -> Result<Response<SnapshotPageResponse>, Status> {
        let request = request.into_inner();
        let state = self.state.read().await;
        let source = state
            .sources
            .get(&(request.change_id, request.range_id))
            .ok_or_else(|| Status::not_found("source migration not prepared"))?;
        let snapshot_keys = source
            .snapshot_keys
            .as_ref()
            .ok_or_else(|| Status::unavailable("source snapshot is still being prepared"))?;
        let start = usize::try_from(request.cursor)
            .map_err(|_| Status::invalid_argument("snapshot cursor is too large"))?;
        if start > snapshot_keys.len() {
            return Err(Status::out_of_range("snapshot cursor exceeds record count"));
        }
        let max_bytes = page_limit(request.max_bytes);
        let mut bytes = 0;
        let mut records = Vec::new();
        let mut next_cursor = start;
        for key in &snapshot_keys[start..] {
            let Some(record) = state.records.get(key) else {
                next_cursor += 1;
                continue;
            };
            let record = MigrationRecord {
                key: key.clone(),
                value: record.value.to_vec(),
                version: Some(record.version.clone()),
                deleted: record.deleted,
                mutation_id: String::new(),
                remaining_window_millis: 0,
            };
            let size = migration_record_size(&record);
            if !records.is_empty() && bytes + size > max_bytes {
                break;
            }
            bytes += size;
            records.push(record);
            next_cursor += 1;
        }
        Ok(Response::new(SnapshotPageResponse {
            records,
            next_cursor: next_cursor as u64,
            done: next_cursor == snapshot_keys.len(),
        }))
    }

    async fn read_dedup_snapshot_page(
        &self,
        request: Request<SnapshotPageRequest>,
    ) -> Result<Response<DedupSnapshotPageResponse>, Status> {
        let request = request.into_inner();
        let state = self.state.read().await;
        let source = state
            .sources
            .get(&(request.change_id, request.range_id))
            .ok_or_else(|| Status::not_found("source migration not prepared"))?;
        if source.snapshot_keys.is_none() {
            return Err(Status::unavailable(
                "source snapshot is still being prepared",
            ));
        }
        let ids = &source.snapshot_dedup_ids;
        let start = usize::try_from(request.cursor)
            .map_err(|_| Status::invalid_argument("dedup cursor is too large"))?;
        if start > ids.len() {
            return Err(Status::out_of_range("dedup cursor exceeds record count"));
        }
        let mut next_cursor = start;
        let mut bytes = 0;
        let mut records = Vec::new();
        let now = Instant::now();
        for id in &ids[start..] {
            next_cursor += 1;
            let Some(entry) = state
                .dedup
                .get(id.as_ref())
                .filter(|entry| entry.expires_at > now)
            else {
                continue;
            };
            let record = DeduplicationRecord {
                mutation_id: id.to_string(),
                key: entry.key.to_vec(),
                fingerprint: entry.fingerprint.to_vec(),
                version: Some(entry.version.clone()),
                deleted: entry.deleted,
                remaining_window_millis: remaining_window_millis(entry.expires_at, now),
            };
            let size = dedup_record_size(&record);
            if !records.is_empty() && bytes + size > page_limit(request.max_bytes) {
                next_cursor -= 1;
                break;
            }
            bytes += size;
            records.push(record);
        }
        Ok(Response::new(DedupSnapshotPageResponse {
            records,
            next_cursor: next_cursor as u64,
            done: next_cursor == ids.len(),
        }))
    }

    async fn read_changelog_page(
        &self,
        request: Request<ChangelogPageRequest>,
    ) -> Result<Response<ChangelogPageResponse>, Status> {
        let request = request.into_inner();
        let state = self.state.read().await;
        let source = state
            .sources
            .get(&(request.change_id, request.range_id))
            .ok_or_else(|| Status::not_found("source migration not prepared"))?;
        let max_bytes = page_limit(request.max_bytes);
        let mut bytes = 0;
        let mut records = Vec::new();
        for entry in source
            .journal
            .iter()
            .filter(|entry| entry.watermark > request.after_watermark)
        {
            let size = entry
                .record
                .as_ref()
                .map(migration_record_size)
                .unwrap_or_default()
                + 16;
            if !records.is_empty() && bytes + size > max_bytes {
                break;
            }
            bytes += size;
            let mut copied = entry.clone();
            if let Some(record) = copied.record.as_mut() {
                record.remaining_window_millis = state
                    .dedup
                    .get(record.mutation_id.as_str())
                    .filter(|dedup| {
                        dedup.fingerprint
                            == mutation_fingerprint(&record.key, &record.value, record.deleted)
                            && Some(&dedup.version) == record.version.as_ref()
                    })
                    .map_or(0, |dedup| {
                        remaining_window_millis(dedup.expires_at, Instant::now())
                    });
            }
            records.push(copied);
        }
        Ok(Response::new(ChangelogPageResponse {
            records,
            current_watermark: source.watermark,
        }))
    }

    async fn apply_migration_batch(
        &self,
        request: Request<ApplyMigrationBatchRequest>,
    ) -> Result<Response<proto::Empty>, Status> {
        let request = request.into_inner();
        let mut state = self.state.write().await;
        let topology = state.topology.clone();
        let destination = state
            .destinations
            .get_mut(&(request.change_id, request.range_id))
            .ok_or_else(|| Status::not_found("destination migration not prepared"))?;
        if destination.committed {
            return Err(Status::failed_precondition(
                "destination range is already committed",
            ));
        }
        purge_staged_dedup(destination, Instant::now());
        for record in request.snapshot_records {
            if !destination.range.contains(topology.key_token(&record.key)) {
                return Err(Status::invalid_argument(
                    "snapshot record is outside the prepared range",
                ));
            }
            apply_record(&mut destination.records, record)?;
        }
        for entry in request.journal_records {
            if entry.watermark <= destination.watermark {
                continue;
            }
            if entry.watermark != destination.watermark + 1 {
                return Err(Status::failed_precondition(format!(
                    "non-contiguous changelog: expected {}, received {}",
                    destination.watermark + 1,
                    entry.watermark
                )));
            }
            let record = entry
                .record
                .ok_or_else(|| Status::invalid_argument("journal entry omitted record"))?;
            if !destination.range.contains(topology.key_token(&record.key)) {
                return Err(Status::invalid_argument(
                    "journal record is outside the prepared range",
                ));
            }
            if !record.mutation_id.is_empty() {
                let dedup = DeduplicationRecord {
                    mutation_id: record.mutation_id.clone(),
                    key: record.key.clone(),
                    fingerprint: mutation_fingerprint(&record.key, &record.value, record.deleted)
                        .to_vec(),
                    version: record.version.clone(),
                    deleted: record.deleted,
                    remaining_window_millis: record.remaining_window_millis,
                };
                stage_dedup(destination, dedup, self.max_dedup_bytes)?;
            }
            apply_record(&mut destination.records, record)?;
            destination.watermark = entry.watermark;
        }
        Ok(Response::new(proto::Empty {}))
    }

    async fn apply_dedup_batch(
        &self,
        request: Request<ApplyDedupBatchRequest>,
    ) -> Result<Response<proto::Empty>, Status> {
        let request = request.into_inner();
        let mut state = self.state.write().await;
        let topology = state.topology.clone();
        let destination = state
            .destinations
            .get_mut(&(request.change_id, request.range_id))
            .ok_or_else(|| Status::not_found("destination migration not prepared"))?;
        if destination.committed {
            return Err(Status::failed_precondition(
                "destination range is already committed",
            ));
        }
        purge_staged_dedup(destination, Instant::now());
        for record in request.records {
            if !destination.range.contains(topology.key_token(&record.key)) {
                return Err(Status::invalid_argument(
                    "dedup record is outside the prepared range",
                ));
            }
            stage_dedup(destination, record, self.max_dedup_bytes)?;
        }
        Ok(Response::new(proto::Empty {}))
    }

    async fn pause_range_writes(
        &self,
        request: Request<RangeControlRequest>,
    ) -> Result<Response<PauseRangeResponse>, Status> {
        let request = request.into_inner();
        let mut state = self.state.write().await;
        let source = state
            .sources
            .get_mut(&(request.change_id, request.range_id))
            .ok_or_else(|| Status::not_found("source migration not prepared"))?;
        source.writes_paused = true;
        Ok(Response::new(PauseRangeResponse {
            final_watermark: source.watermark,
        }))
    }

    async fn source_range_digest(
        &self,
        request: Request<RangeControlRequest>,
    ) -> Result<Response<RangeDigestResponse>, Status> {
        let request = request.into_inner();
        let state = self.state.read().await;
        let source = state
            .sources
            .get(&(request.change_id, request.range_id))
            .ok_or_else(|| Status::not_found("source migration not prepared"))?;
        let watermark = source.watermark;
        let records: HashMap<_, _> = state
            .records
            .iter()
            .filter(|(key, _)| source.range.contains(state.topology.key_token(key)))
            .map(|(key, record)| (key.clone(), record.clone()))
            .collect();
        drop(state);
        Ok(Response::new(range_digest(&records, watermark)))
    }

    async fn destination_range_digest(
        &self,
        request: Request<RangeControlRequest>,
    ) -> Result<Response<RangeDigestResponse>, Status> {
        let request = request.into_inner();
        let state = self.state.read().await;
        let destination = state
            .destinations
            .get(&(request.change_id, request.range_id))
            .ok_or_else(|| Status::not_found("destination migration not prepared"))?;
        let records = destination.records.clone();
        let watermark = destination.watermark;
        drop(state);
        Ok(Response::new(range_digest(&records, watermark)))
    }

    async fn commit_destination_range(
        &self,
        request: Request<RangeControlRequest>,
    ) -> Result<Response<proto::Empty>, Status> {
        let request = request.into_inner();
        let mut state = self.state.write().await;
        let key = (request.change_id, request.range_id);
        let (range, records, dedup) = {
            let destination = state
                .destinations
                .get(&key)
                .ok_or_else(|| Status::not_found("destination migration not prepared"))?;
            if destination.committed {
                return Ok(Response::new(proto::Empty {}));
            }
            (
                destination.range.clone(),
                destination.records.clone(),
                destination
                    .dedup
                    .iter()
                    .filter(|(_, entry)| entry.expires_at > Instant::now())
                    .map(|(id, entry)| (id.clone(), entry.clone()))
                    .collect::<HashMap<_, _>>(),
            )
        };
        purge_expired_dedup(&mut state, Instant::now());
        let mut added_bytes = 0usize;
        for staged in dedup.values() {
            let record = &staged.record;
            if let Some(existing) = state.dedup.get(record.mutation_id.as_str()) {
                if existing.fingerprint.as_slice() != record.fingerprint
                    || Some(&existing.version) != record.version.as_ref()
                    || existing.deleted != record.deleted
                {
                    return Err(Status::failed_precondition(
                        "destination mutation ID conflicts with existing retry record",
                    ));
                }
            } else {
                let version = record
                    .version
                    .as_ref()
                    .expect("staged dedup version was validated");
                added_bytes = added_bytes.saturating_add(dedup_retained_bytes(
                    &record.mutation_id,
                    record.key.len(),
                    version.owner_node_id.len(),
                ));
            }
        }
        if state.dedup_bytes.saturating_add(added_bytes) > self.max_dedup_bytes {
            return Err(Status::resource_exhausted(
                "destination mutation retry window is full",
            ));
        }
        let topology = state.topology.clone();
        state
            .records
            .retain(|record_key, _| !range.contains(topology.key_token(record_key)));
        for (key, record) in records {
            apply_internal_record(&mut state.records, key, record);
        }
        for (id, staged) in dedup {
            if state.dedup.contains_key(id.as_str()) {
                continue;
            }
            let record = staged.record;
            insert_dedup_until(
                &mut state,
                id,
                Arc::from(record.key),
                record
                    .fingerprint
                    .try_into()
                    .expect("staged fingerprint was validated"),
                record.version.expect("staged version was validated"),
                record.deleted,
                staged.expires_at,
            );
        }
        state
            .destinations
            .get_mut(&key)
            .expect("destination was checked above")
            .committed = true;
        Ok(Response::new(proto::Empty {}))
    }

    async fn activate_destination_range(
        &self,
        request: Request<RangeControlRequest>,
    ) -> Result<Response<proto::Empty>, Status> {
        let request = request.into_inner();
        let mut state = self.state.write().await;
        let destination = state
            .destinations
            .get_mut(&(request.change_id, request.range_id))
            .ok_or_else(|| Status::not_found("destination migration not prepared"))?;
        if !destination.committed {
            return Err(Status::failed_precondition(
                "destination range is not committed",
            ));
        }
        destination.writes_activated = true;
        Ok(Response::new(proto::Empty {}))
    }

    async fn cleanup_source_range(
        &self,
        request: Request<RangeControlRequest>,
    ) -> Result<Response<proto::Empty>, Status> {
        let request = request.into_inner();
        let mut state = self.state.write().await;
        let key = (request.change_id, request.range_id);
        let Some(range) = state.sources.get(&key).map(|source| source.range.clone()) else {
            return Ok(Response::new(proto::Empty {}));
        };
        let topology = state.topology.clone();
        let keys: Vec<_> = state
            .records
            .keys()
            .filter(|record_key| {
                let token = topology.key_token(record_key);
                range.contains(token)
                    && topology
                        .replica_node_ids_for_token(token)
                        .is_ok_and(|replicas| !replicas.contains(&self.node_id.as_str()))
            })
            .cloned()
            .collect();
        for record_key in keys {
            state.records.remove(&record_key);
        }
        if let Some(source) = state.sources.remove(&key) {
            state.journal_bytes_total = state
                .journal_bytes_total
                .saturating_sub(source.journal_bytes);
        }
        Ok(Response::new(proto::Empty {}))
    }

    async fn abort_range_migration(
        &self,
        request: Request<RangeControlRequest>,
    ) -> Result<Response<proto::Empty>, Status> {
        let request = request.into_inner();
        let key = (request.change_id, request.range_id);
        let mut state = self.state.write().await;
        if let Some(source) = state.sources.remove(&key) {
            state.journal_bytes_total = state
                .journal_bytes_total
                .saturating_sub(source.journal_bytes);
        }
        state.destinations.remove(&key);
        Ok(Response::new(proto::Empty {}))
    }

    async fn abort_replica_repairs(
        &self,
        request: Request<proto::AbortReplicaRepairsRequest>,
    ) -> Result<Response<proto::Empty>, Status> {
        let epoch = request.into_inner().topology_epoch;
        let mut state = self.state.write().await;
        if state.topology.epoch != epoch {
            return Err(Status::failed_precondition(
                "repair cleanup epoch does not match installed topology",
            ));
        }
        clear_replica_repair_staging(&mut state);
        Ok(Response::new(proto::Empty {}))
    }

    async fn install_topology(
        &self,
        request: Request<InstallTopologyRequest>,
    ) -> Result<Response<proto::Empty>, Status> {
        let request = request.into_inner();
        let topology: TopologySnapshot = request
            .topology
            .ok_or_else(|| Status::invalid_argument("missing topology"))?
            .try_into()
            .map_err(|error: hashring_core::topology::TopologyError| {
                Status::invalid_argument(error.to_string())
            })?;
        let mut state = self.state.write().await;
        if topology.epoch < state.topology.epoch {
            return Err(Status::failed_precondition(
                "cannot install an older topology",
            ));
        }
        let epoch = topology.epoch;
        if epoch != state.topology.epoch {
            state.lease = None;
            state.admitted_followers.clear();
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
        drop(state);
        if request.require_lease
            && self
                .state
                .read()
                .await
                .topology
                .members
                .iter()
                .any(|member| member.node_id == self.node_id)
        {
            self.renew_lease_once().await.map_err(|error| {
                Status::unavailable(format!("new-epoch lease is unavailable: {error}"))
            })?;
        }
        Ok(Response::new(proto::Empty {}))
    }

    async fn pause_policy_writes(
        &self,
        request: Request<PolicyWriteFenceRequest>,
    ) -> Result<Response<proto::Empty>, Status> {
        let request = request.into_inner();
        if request.change_id.is_empty() {
            return Err(Status::invalid_argument("policy change ID is empty"));
        }
        let mut state = self.state.write().await;
        if state.topology.epoch != request.base_epoch {
            return Err(Status::failed_precondition(
                "policy fence base epoch does not match installed topology",
            ));
        }
        match &state.policy_write_fence {
            Some((change_id, epoch))
                if change_id != &request.change_id || *epoch != request.base_epoch =>
            {
                return Err(Status::failed_precondition(
                    "another policy change has fenced writes",
                ));
            }
            _ => state.policy_write_fence = Some((request.change_id, request.base_epoch)),
        }
        Ok(Response::new(proto::Empty {}))
    }

    async fn resume_policy_writes(
        &self,
        request: Request<PolicyWriteFenceRequest>,
    ) -> Result<Response<proto::Empty>, Status> {
        let request = request.into_inner();
        let mut state = self.state.write().await;
        if state.policy_write_fence.as_ref() == Some(&(request.change_id, request.base_epoch)) {
            state.policy_write_fence = None;
        }
        Ok(Response::new(proto::Empty {}))
    }

    async fn stop(&self, request: Request<StopRequest>) -> Result<Response<proto::Empty>, Status> {
        let request = request.into_inner();
        self.validate_stop_request(&request)?;
        if !self.stop_prepared.load(AtomicOrdering::Acquire) {
            return Err(Status::failed_precondition(
                "stop must be prepared before finalization",
            ));
        }
        let shutdown = self.shutdown.clone();
        tokio::spawn(async move {
            // Let the acknowledged response reach the coordinator so it can
            // durably record shutdown progress before the server exits.
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            let _ = shutdown.send(true);
        });
        if !self.stop_response_delay.is_zero() {
            tokio::time::sleep(self.stop_response_delay).await;
        }
        Ok(Response::new(proto::Empty {}))
    }

    async fn prepare_stop(
        &self,
        request: Request<StopRequest>,
    ) -> Result<Response<proto::Empty>, Status> {
        let request = request.into_inner();
        self.validate_stop_request(&request)?;
        if !self.stop_prepared.swap(true, AtomicOrdering::AcqRel) {
            let coordinator_endpoint = self.coordinator_endpoint.clone();
            let node_id = self.node_id.clone();
            let process_instance_id = self.process_instance_id.clone();
            let shutdown = self.shutdown.clone();
            tokio::spawn(async move {
                wait_for_durable_stop_confirmation(
                    coordinator_endpoint,
                    node_id,
                    process_instance_id,
                    shutdown,
                )
                .await;
            });
        }
        Ok(Response::new(proto::Empty {}))
    }
}
