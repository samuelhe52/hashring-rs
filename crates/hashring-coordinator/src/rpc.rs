use super::*;

#[tonic::async_trait]
impl Coordinator for CoordinatorService {
    async fn get_topology(
        &self,
        _request: Request<proto::Empty>,
    ) -> Result<Response<proto::TopologySnapshot>, Status> {
        let state = self.state.read().await;
        Ok(Response::new((&state.committed).into()))
    }

    async fn get_current_epoch(
        &self,
        _request: Request<proto::Empty>,
    ) -> Result<Response<proto::CurrentEpochResponse>, Status> {
        let epoch = self.state.read().await.committed.epoch;
        Ok(Response::new(proto::CurrentEpochResponse { epoch }))
    }

    async fn get_replica_status(
        &self,
        _request: Request<proto::Empty>,
    ) -> Result<Response<proto::ReplicaStatusResponse>, Status> {
        let state = self.state.read().await.clone();
        let epoch = state.committed.epoch;
        let now = Instant::now();
        let live_nodes: BTreeSet<_> = self
            .lease_grants
            .lock()
            .await
            .iter()
            .filter(|(node_id, grant)| {
                grant.epoch == epoch
                    && grant.expires_at > now
                    && state.process_instances.get(*node_id) == Some(&grant.process_instance_id)
                    && !state.fenced_nodes.contains(*node_id)
            })
            .map(|(node_id, _)| node_id.clone())
            .collect();
        let members: BTreeMap<_, _> = state
            .committed
            .members
            .iter()
            .map(|member| (member.node_id.clone(), member.endpoint.clone()))
            .collect();
        let channels = {
            let mut cache = self.status_channels.lock().await;
            let active: BTreeSet<_> = members.values().cloned().collect();
            cache.retain(|endpoint, _| active.contains(endpoint));
            for endpoint in &active {
                if !cache.contains_key(endpoint) {
                    let channel = Endpoint::from_shared(endpoint.clone())
                        .map_err(|error| Status::internal(error.to_string()))?
                        .connect_lazy();
                    cache.insert(endpoint.clone(), channel);
                }
            }
            cache.clone()
        };
        let pairs: BTreeSet<_> = state
            .replica_repairs
            .iter()
            .map(|repair| (repair.owner_node_id.clone(), repair.node_id.clone()))
            .collect();
        let progress = try_map_bounded(pairs, self.range_move_concurrency, |(owner, follower)| {
            let source_endpoint = members.get(&owner).cloned();
            let destination_endpoint = members.get(&follower).cloned();
            let source_channel = source_endpoint
                .as_ref()
                .and_then(|endpoint| channels.get(endpoint))
                .cloned();
            let destination_channel = destination_endpoint
                .as_ref()
                .and_then(|endpoint| channels.get(endpoint))
                .cloned();
            let owner_instance = live_nodes
                .contains(&owner)
                .then(|| state.process_instances.get(&owner).cloned())
                .flatten();
            let follower_instance = live_nodes
                .contains(&follower)
                .then(|| state.process_instances.get(&follower).cloned())
                .flatten();
            async move {
                let current = match (
                    source_channel,
                    destination_channel,
                    owner_instance,
                    follower_instance,
                ) {
                    (
                        Some(source_channel),
                        Some(destination_channel),
                        Some(owner_instance),
                        Some(follower_instance),
                    ) => {
                        read_replication_pair(
                            epoch,
                            &owner,
                            &follower,
                            source_channel,
                            destination_channel,
                            &owner_instance,
                            &follower_instance,
                        )
                        .await
                    }
                    _ => None,
                };
                Ok::<_, Status>(((owner, follower), current))
            }
        })
        .await?
        .into_iter()
        .collect::<BTreeMap<_, _>>();
        let admissions: BTreeMap<_, _> = state
            .replica_admissions
            .iter()
            .map(|admission| {
                (
                    (
                        admission.start_exclusive,
                        admission.end_inclusive,
                        admission.owner_node_id.as_str(),
                        admission.node_id.as_str(),
                    ),
                    admission,
                )
            })
            .collect();
        let repairs: BTreeMap<_, _> = state
            .replica_repairs
            .iter()
            .map(|repair| {
                (
                    (
                        repair.start_exclusive,
                        repair.end_inclusive,
                        repair.owner_node_id.as_str(),
                        repair.node_id.as_str(),
                    ),
                    repair,
                )
            })
            .collect();
        let activation_pending = state.active_change.as_ref().is_some_and(|change| {
            change.phase == MigrationPhase::Published && !change.activation_ready
        });
        let ranges = state
            .committed
            .derived_ranges()
            .map_err(|error| Status::internal(error.to_string()))?
            .into_iter()
            .map(|range| {
                let owner_leased = live_nodes.contains(&range.owner_node_id);
                let guard = &state.committed.write_availability_guard;
                let followers: Vec<_> = range
                    .follower_node_ids
                    .iter()
                    .map(|node_id| {
                        let key = (
                            range.start_exclusive,
                            range.end_inclusive,
                            range.owner_node_id.as_str(),
                            node_id.as_str(),
                        );
                        let admission = admissions.get(&key).copied().filter(|admission| {
                            state.process_instances.get(node_id)
                                == Some(&admission.process_instance_id)
                                && !state.fenced_nodes.contains(node_id)
                        });
                        let repair = repairs.get(&key).copied();
                        let live = progress
                            .get(&(range.owner_node_id.clone(), node_id.clone()))
                            .and_then(Option::as_ref);
                        let leased = live_nodes.contains(node_id);
                        let healthy = admission.is_some()
                            && leased
                            && live.is_some_and(|(_, _, lag)| *lag <= guard.max_replica_lag_millis);
                        proto::FollowerReplicaStatus {
                            node_id: node_id.clone(),
                            admitted: admission.is_some(),
                            process_instance_id: admission
                                .map(|admission| admission.process_instance_id.clone())
                                .unwrap_or_default(),
                            verified_watermark: admission
                                .map(|admission| admission.verified_watermark)
                                .unwrap_or_default(),
                            stream_cursor: live
                                .map(|(_, cursor, _)| *cursor)
                                .or_else(|| admission.map(|admission| admission.stream_cursor))
                                .unwrap_or_default(),
                            lag_millis: live.map(|(_, _, lag)| *lag).unwrap_or_default(),
                            lag_known: live.is_some(),
                            stream_head: live.map(|(head, _, _)| *head).unwrap_or_default(),
                            repair_state: repair
                                .map(|repair| format!("{:?}", repair.phase))
                                .unwrap_or_default(),
                            leased,
                            healthy,
                            repair_retry_count: repair.map_or(0, |repair| repair.retry_count),
                            repair_next_attempt_unix_millis: repair
                                .map_or(0, |repair| repair.next_attempt_unix_millis),
                            repair_last_error: repair
                                .map_or(String::new(), |repair| repair.last_error.clone()),
                        }
                    })
                    .collect();
                let admitted = followers
                    .iter()
                    .filter(|follower| follower.admitted)
                    .count() as u32;
                let healthy = followers.iter().filter(|follower| follower.healthy).count() as u32;
                let live_rf = u32::from(owner_leased)
                    + followers
                        .iter()
                        .filter(|follower| follower.admitted && follower.leased)
                        .count() as u32;
                let required = match state.committed.write_ack_policy {
                    WriteAckPolicy::OwnerOnly => Vec::new(),
                    WriteAckPolicy::FirstSuccessor => {
                        range.follower_node_ids.iter().take(1).collect()
                    }
                    WriteAckPolicy::AllReplicas => range.follower_node_ids.iter().collect(),
                };
                let transition_fenced = state.active_change.as_ref().is_some_and(|change| {
                    matches!(
                        change.phase,
                        MigrationPhase::PausingWrites
                            | MigrationPhase::Verifying
                            | MigrationPhase::ReadyToPublish
                    ) && (change.target_topology.write_ack_policy
                        != state.committed.write_ack_policy
                        || change.ranges.iter().any(|moving| {
                            intervals_overlap(
                                range.start_exclusive,
                                range.end_inclusive,
                                moving.start_exclusive,
                                moving.end_inclusive,
                            )
                        }))
                });
                let write_block_reason = if activation_pending {
                    if state.recovery_block_reason.is_empty() {
                        "topology activation pending".to_owned()
                    } else {
                        state.recovery_block_reason.clone()
                    }
                } else if transition_fenced {
                    "topology cutover write fence may be active".to_owned()
                } else if !owner_leased {
                    "owner lease unavailable".to_owned()
                } else if state.committed.write_ack_policy == WriteAckPolicy::FirstSuccessor
                    && range.follower_node_ids.is_empty()
                {
                    "first successor is not in desired placement".to_owned()
                } else if state.committed.write_ack_policy == WriteAckPolicy::AllReplicas
                    && range.follower_node_ids.len() + 1
                        < state.committed.desired_replication_factor as usize
                {
                    "complete desired replication factor is unavailable".to_owned()
                } else if admitted.saturating_add(1) < guard.minimum_admitted_copies
                    || healthy < guard.minimum_healthy_followers
                {
                    "minimum admitted-copy or healthy-follower guard is not satisfied".to_owned()
                } else if let Some(missing) = required.into_iter().find(|node_id| {
                    !followers
                        .iter()
                        .any(|follower| follower.node_id == **node_id && follower.healthy)
                }) {
                    format!("required follower {missing} is not healthy and admitted")
                } else {
                    String::new()
                };
                let repairing = followers
                    .iter()
                    .any(|follower| !follower.admitted || follower.repair_state != "Complete");
                proto::RangeReplicaStatus {
                    start_exclusive: range.start_exclusive,
                    end_inclusive: range.end_inclusive,
                    owner_node_id: range.owner_node_id.clone(),
                    desired_rf: state.committed.desired_replication_factor,
                    current_rf: u32::from(
                        state.process_instances.contains_key(&range.owner_node_id)
                            && !state.fenced_nodes.contains(&range.owner_node_id),
                    ) + admitted,
                    followers,
                    owner_leased,
                    live_rf,
                    writable: write_block_reason.is_empty(),
                    write_block_reason,
                    under_replicated: live_rf < state.committed.desired_replication_factor,
                    repairing,
                }
            })
            .collect();
        Ok(Response::new(proto::ReplicaStatusResponse {
            topology_epoch: epoch,
            ranges,
            activation_pending,
        }))
    }

    async fn begin_topology_change(
        &self,
        request: Request<proto::BeginTopologyChangeRequest>,
    ) -> Result<Response<proto::TopologyChangeSnapshot>, Status> {
        let request = request.into_inner();
        let target_policy = request
            .target_write_ack_policy
            .map(WriteAckPolicy::try_from)
            .transpose()
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let target_rf = request.target_desired_replication_factor;
        let target_guard = request.target_write_availability_guard.map(|guard| {
            hashring_core::topology::WriteAvailabilityGuard {
                minimum_admitted_copies: guard.minimum_admitted_copies,
                minimum_healthy_followers: guard.minimum_healthy_followers,
                max_replica_lag_millis: guard.max_replica_lag_millis,
            }
        });
        let target_members = request
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
        let mut config = state.committed.config();
        if let Some(policy) = target_policy {
            config.write_ack_policy = policy;
        }
        if let Some(rf) = target_rf {
            config.desired_replication_factor = rf;
        }
        if let Some(guard) = target_guard {
            config.write_availability_guard = guard;
        }
        if config != state.committed.config() && target_members != state.committed.members {
            return Err(Status::invalid_argument(
                "change membership and topology configuration in separate transitions",
            ));
        }
        let mut change = TopologyChange::plan_with_config(&state.committed, target_members, config)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        if can_direct_merge(&state, &change) {
            change.direct_merge = true;
            change.ranges.clear();
        }
        let target = &change.target_topology;
        let member_count = target.members.len();
        let guard = &target.write_availability_guard;
        if member_count < guard.minimum_admitted_copies as usize
            || member_count.saturating_sub(1) < guard.minimum_healthy_followers as usize
            || target.desired_replication_factor < guard.minimum_admitted_copies
            || target.desired_replication_factor.saturating_sub(1) < guard.minimum_healthy_followers
            || (target.write_ack_policy == WriteAckPolicy::FirstSuccessor && member_count < 2)
            || ((target.members != state.committed.members
                || target.desired_replication_factor != state.committed.desired_replication_factor)
                && target.write_ack_policy == WriteAckPolicy::AllReplicas
                && member_count < target.desired_replication_factor as usize)
        {
            return Err(Status::failed_precondition(
                "target membership cannot satisfy its write policy and availability guard",
            ));
        }
        let target_tasks: usize = change
            .target_topology
            .derived_ranges()
            .map_err(|error| Status::invalid_argument(error.to_string()))?
            .iter()
            .map(|range| range.follower_node_ids.len())
            .sum();
        if target_tasks > MAX_REPLICA_OBLIGATIONS {
            return Err(Status::resource_exhausted(format!(
                "target topology exceeds the {MAX_REPLICA_OBLIGATIONS} replica-task limit"
            )));
        }
        let mut next = state.clone();
        next.active_change = Some(change.clone());
        next.recovery_block_reason.clear();
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

    async fn execute_topology_change(
        &self,
        request: Request<proto::ExecuteTopologyChangeRequest>,
    ) -> Result<Response<proto::TopologyChangeSnapshot>, Status> {
        let request = request.into_inner();
        if request.change_id.is_empty() {
            return Err(Status::invalid_argument("change_id must not be empty"));
        }
        let expected = ChangeIdentity {
            change_id: request.change_id,
            base_epoch: request.base_epoch,
            target_epoch: request.target_epoch,
        };
        // Keep migration recovery running if the requesting client disconnects or
        // reaches its own deadline after the coordinator accepted the command.
        let service = self.clone();
        let change = tokio::spawn(async move { service.execute_change(expected, false).await })
            .await
            .map_err(|error| Status::internal(format!("migration task failed: {error}")))??;
        Ok(Response::new((&change).into()))
    }

    async fn register_node(
        &self,
        request: Request<proto::RegisterNodeRequest>,
    ) -> Result<Response<proto::Empty>, Status> {
        let request = request.into_inner();
        if request.node_id.is_empty() || request.process_instance_id.is_empty() {
            return Err(Status::invalid_argument(
                "node_id and process_instance_id must not be empty",
            ));
        }
        let mut state = self.state.write().await;
        let is_committed = state
            .committed
            .members
            .iter()
            .any(|member| member.node_id == request.node_id);
        let is_pending = state.active_change.as_ref().is_some_and(|change| {
            !change.phase.is_terminal()
                && change
                    .target_topology
                    .members
                    .iter()
                    .any(|member| member.node_id == request.node_id)
        });
        if !is_committed && !is_pending {
            return Err(Status::failed_precondition(
                "node is not present in committed or pending membership",
            ));
        }
        if is_committed && state.fenced_nodes.contains(&request.node_id) {
            return Err(Status::failed_precondition("node has been fenced"));
        }
        if let Some(existing) = state.process_instances.get(&request.node_id) {
            if existing == &request.process_instance_id {
                return Ok(Response::new(proto::Empty {}));
            }
            if is_committed {
                return Err(Status::failed_precondition(format!(
                    "committed node {} is fenced to another process instance",
                    request.node_id
                )));
            }
        }
        let mut next = state.clone();
        if !is_committed {
            next.fenced_nodes.remove(&request.node_id);
        }
        next.process_instances
            .insert(request.node_id, request.process_instance_id);
        self.repository
            .store_state(&next)
            .map_err(|error| Status::internal(error.to_string()))?;
        *state = next;
        Ok(Response::new(proto::Empty {}))
    }

    async fn renew_node_lease(
        &self,
        request: Request<proto::RenewNodeLeaseRequest>,
    ) -> Result<Response<proto::RenewNodeLeaseResponse>, Status> {
        let request = request.into_inner();
        let state = self.state.read().await;
        if request.node_id.is_empty()
            || request.process_instance_id.is_empty()
            || state.committed.epoch != request.topology_epoch
            || !state
                .committed
                .members
                .iter()
                .any(|member| member.node_id == request.node_id)
            || state.process_instances.get(&request.node_id) != Some(&request.process_instance_id)
            || state.fenced_nodes.contains(&request.node_id)
            || state.active_change.as_ref().is_some_and(|change| {
                change.phase == MigrationPhase::Published && !change.activation_ready
            })
        {
            return Err(Status::failed_precondition(
                "lease identity, epoch, or membership is no longer valid",
            ));
        }
        let epoch = state.committed.epoch;
        let mut grants = self.lease_grants.lock().await;
        grants.insert(
            request.node_id,
            NodeLeaseGrant {
                process_instance_id: request.process_instance_id,
                epoch,
                expires_at: Instant::now() + NODE_LEASE_DURATION,
            },
        );
        Ok(Response::new(proto::RenewNodeLeaseResponse {
            topology_epoch: epoch,
            lease_duration_millis: NODE_LEASE_DURATION.as_millis() as u64,
        }))
    }

    async fn report_peer_health(
        &self,
        request: Request<proto::ReportPeerHealthRequest>,
    ) -> Result<Response<proto::Empty>, Status> {
        let request = request.into_inner();
        let state = self.state.read().await;
        if request.reporter_node_id == request.peer_node_id
            || request.peer_node_id.is_empty()
            || request.topology_epoch != state.committed.epoch
            || state.process_instances.get(&request.reporter_node_id)
                != Some(&request.reporter_process_instance_id)
            || state.fenced_nodes.contains(&request.reporter_node_id)
            || !state
                .committed
                .members
                .iter()
                .any(|member| member.node_id == request.peer_node_id)
        {
            return Err(Status::failed_precondition(
                "peer report does not match active membership",
            ));
        }
        let now = Instant::now();
        let grants = self.lease_grants.lock().await;
        if grants.get(&request.reporter_node_id).is_none_or(|grant| {
            grant.epoch != request.topology_epoch
                || grant.process_instance_id != request.reporter_process_instance_id
                || grant.expires_at <= now
        }) {
            return Err(Status::failed_precondition("reporter has no valid lease"));
        }
        let mut failures = self.peer_failures.lock().await;
        let key = (request.peer_node_id, request.reporter_node_id);
        if request.reachable {
            failures.remove(&key);
        } else {
            failures
                .entry(key)
                .and_modify(|failure| {
                    if failure.last_seen + Duration::from_secs(2) < now {
                        failure.first_seen = now;
                    }
                    failure.last_seen = now;
                })
                .or_insert(PeerFailure {
                    first_seen: now,
                    last_seen: now,
                });
        }
        Ok(Response::new(proto::Empty {}))
    }

    async fn is_stop_confirmed(
        &self,
        request: Request<proto::StopRequest>,
    ) -> Result<Response<proto::StopConfirmationResponse>, Status> {
        let request = request.into_inner();
        if request.node_id.is_empty() || request.process_instance_id.is_empty() {
            return Err(Status::invalid_argument(
                "node_id and process_instance_id must not be empty",
            ));
        }
        let state = self.state.read().await;
        let confirmed = state
            .stop_confirmations
            .get(&request.node_id)
            .is_some_and(|instances| instances.contains(&request.process_instance_id));
        Ok(Response::new(proto::StopConfirmationResponse { confirmed }))
    }
}
