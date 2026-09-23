use super::*;

type StreamPair = (String, String);
type CandidateAdmission = (ReplicaAdmission, BTreeSet<StreamPair>);
type MergeProof = (BTreeSet<StreamPair>, Vec<CandidateAdmission>);

pub(super) fn can_direct_merge(state: &ClusterState, change: &TopologyChange) -> bool {
    let old = &state.committed;
    let target = &change.target_topology;
    if target.members.len() >= old.members.len()
        || target.config() != old.config()
        || target
            .members
            .iter()
            .any(|member| !old.members.contains(member))
    {
        return false;
    }
    merge_proof(state, change).is_ok()
}

fn merge_proof(state: &ClusterState, change: &TopologyChange) -> Result<MergeProof, Status> {
    prove_removal_owner_coverage(state, &change.target_topology)?;
    let old_ranges = state
        .committed
        .derived_ranges()
        .map_err(|error| Status::internal(error.to_string()))?;
    let target_ranges = change
        .target_topology
        .derived_ranges()
        .map_err(|error| Status::internal(error.to_string()))?;
    let mut required = BTreeSet::new();
    let mut candidates = Vec::new();
    for target_range in &target_ranges {
        let parts = constituents(&old_ranges, target_range);
        if parts.is_empty() {
            return Err(Status::data_loss("merged range has no old constituents"));
        }
        for part in &parts {
            if part.owner_node_id != target_range.owner_node_id {
                required.insert((
                    part.owner_node_id.clone(),
                    target_range.owner_node_id.clone(),
                ));
            }
        }
        for follower in &target_range.follower_node_ids {
            let Some(instance) = state.process_instances.get(follower) else {
                continue;
            };
            if state.fenced_nodes.contains(follower) {
                continue;
            }
            let mut pairs = BTreeSet::new();
            let mut digest = blake3::Hasher::new();
            digest.update(b"hashring-rs:merged-admission:v1\0");
            let mut complete = true;
            for part in &parts {
                digest.update(&part.start_exclusive.to_be_bytes());
                digest.update(&part.end_inclusive.to_be_bytes());
                if part.owner_node_id == *follower {
                    digest.update(b"owner");
                } else if let Some(admission) = admission_for(state, part, follower) {
                    digest.update(admission.digest.as_bytes());
                    pairs.insert((part.owner_node_id.clone(), follower.clone()));
                } else {
                    complete = false;
                    break;
                }
            }
            if complete {
                candidates.push((
                    ReplicaAdmission {
                        epoch: change.target_topology.epoch,
                        start_exclusive: target_range.start_exclusive,
                        end_inclusive: target_range.end_inclusive,
                        owner_node_id: target_range.owner_node_id.clone(),
                        node_id: follower.clone(),
                        process_instance_id: instance.clone(),
                        verified_watermark: 0,
                        stream_cursor: 0,
                        digest: digest.finalize().to_hex().to_string(),
                    },
                    pairs,
                ));
            }
        }
    }
    let guard = &change.target_topology.write_availability_guard;
    let policy_followers = match change.target_topology.write_ack_policy {
        WriteAckPolicy::OwnerOnly => 0,
        WriteAckPolicy::FirstSuccessor => 1,
        WriteAckPolicy::AllReplicas => {
            change.target_topology.desired_replication_factor as usize - 1
        }
    };
    let required_followers = policy_followers
        .max(guard.minimum_admitted_copies.saturating_sub(1) as usize)
        .max(guard.minimum_healthy_followers as usize);
    for range in &target_ranges {
        if range.follower_node_ids.len() < required_followers {
            return Err(Status::failed_precondition(
                "merged range cannot satisfy its write guard",
            ));
        }
        for follower in range.follower_node_ids.iter().take(required_followers) {
            let Some((_, pairs)) = candidates.iter().find(|(admission, _)| {
                admission.start_exclusive == range.start_exclusive
                    && admission.end_inclusive == range.end_inclusive
                    && admission.node_id == *follower
            }) else {
                return Err(Status::failed_precondition(
                    "required merged follower lacks admitted coverage",
                ));
            };
            required.extend(pairs.iter().cloned());
        }
    }
    Ok((required, candidates))
}

impl CoordinatorService {
    pub(super) async fn set_direct_merge_fence(
        &self,
        change: &TopologyChange,
        pause: bool,
        base_members: bool,
        deadline: Instant,
    ) -> Result<(), Status> {
        let base = change
            .base_topology
            .as_ref()
            .ok_or_else(|| Status::internal("direct merge omitted base topology"))?;
        let members = if base_members {
            &base.members
        } else {
            &change.target_topology.members
        };
        let request = proto::PolicyWriteFenceRequest {
            change_id: change.change_id.clone(),
            base_epoch: change.base_epoch,
        };
        let mut first_error = None;
        for member in members {
            let result = async {
                let mut client = connect_node(&member.endpoint, deadline).await?;
                if pause {
                    rpc_before(deadline, client.pause_policy_writes(request.clone())).await?;
                } else {
                    rpc_before(deadline, client.resume_policy_writes(request.clone())).await?;
                }
                Ok::<(), Status>(())
            }
            .await;
            if let Err(error) = result {
                if pause {
                    return Err(error);
                }
                first_error.get_or_insert(error);
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    async fn merge_pair_caught_up(
        &self,
        state: &ClusterState,
        (owner, follower): &StreamPair,
        deadline: Instant,
    ) -> Result<bool, Status> {
        let endpoint = |id: &str| {
            state
                .committed
                .members
                .iter()
                .find(|member| member.node_id == id)
                .map(|member| member.endpoint.as_str())
                .ok_or_else(|| Status::failed_precondition("replication stream member disappeared"))
        };
        let mut source = connect_node(endpoint(owner)?, deadline).await?;
        let mut destination = connect_node(endpoint(follower)?, deadline).await?;
        let request = proto::ReplicationProgressRequest {
            topology_epoch: state.committed.epoch,
            owner_node_id: owner.clone(),
            follower_node_id: follower.clone(),
        };
        let head = rpc_before(deadline, source.get_replication_progress(request.clone()))
            .await?
            .into_inner();
        let cursor = rpc_before(deadline, destination.get_replication_progress(request))
            .await?
            .into_inner();
        if state.process_instances.get(owner) != Some(&head.process_instance_id)
            || state.process_instances.get(follower) != Some(&cursor.process_instance_id)
        {
            return Err(Status::failed_precondition(
                "replication participant changed process",
            ));
        }
        Ok(cursor.stream_sequence >= head.stream_sequence)
    }

    pub(super) async fn execute_direct_merge(
        &self,
        mut change: TopologyChange,
        retry_transient: bool,
    ) -> Result<TopologyChange, Status> {
        let deadline = Instant::now() + self.migration_timeout;
        self.set_phase(MigrationPhase::PausingWrites).await?;
        change.phase = MigrationPhase::PausingWrites;
        let result = async {
            self.set_direct_merge_fence(&change, true, true, deadline)
                .await?;
            self.set_phase(MigrationPhase::Verifying).await?;
            change.phase = MigrationPhase::Verifying;
            let snapshot = self.state.read().await.clone();
            if !can_direct_merge(&snapshot, &change) {
                return Err(Status::failed_precondition(
                    "natural successor coverage was lost",
                ));
            }
            {
                let grants = self.lease_grants.lock().await;
                prove_removal_owner_leases(
                    &snapshot,
                    &change.target_topology,
                    &grants,
                    Instant::now(),
                )?;
            }
            let (required, candidates) = merge_proof(&snapshot, &change)?;
            let mut caught_up = BTreeSet::new();
            for pair in &required {
                loop {
                    if self.merge_pair_caught_up(&snapshot, pair, deadline).await? {
                        caught_up.insert(pair.clone());
                        break;
                    }
                    if Instant::now() >= deadline {
                        return Err(Status::deadline_exceeded(
                            "natural successor did not catch up",
                        ));
                    }
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
            }
            let optional: BTreeSet<_> = candidates
                .iter()
                .flat_map(|(_, pairs)| pairs.iter().cloned())
                .filter(|pair| !required.contains(pair))
                .collect();
            for pair in optional {
                if self
                    .merge_pair_caught_up(&snapshot, &pair, deadline)
                    .await
                    .unwrap_or(false)
                {
                    caught_up.insert(pair);
                }
            }
            let carried: Vec<_> = candidates
                .into_iter()
                .filter(|(_, pairs)| pairs.is_subset(&caught_up))
                .map(|(admission, _)| admission)
                .collect();
            self.set_phase(MigrationPhase::ReadyToPublish).await?;
            change.phase = MigrationPhase::ReadyToPublish;
            if !self.pre_publish_delay.is_zero() {
                tokio::time::sleep(self.pre_publish_delay).await;
            }
            // The write fence keeps the verified old streams stable until publication.
            let mut state = self.state.write().await;
            if state.committed != snapshot.committed
                || state.process_instances != snapshot.process_instances
                || state.replica_admissions != snapshot.replica_admissions
                || state.fenced_nodes != snapshot.fenced_nodes
                || state
                    .active_change
                    .as_ref()
                    .is_none_or(|active| change_identity(active) != change_identity(&change))
            {
                return Err(Status::failed_precondition(
                    "merge coverage changed before publication",
                ));
            }
            self.publish_removed_topology(
                &mut state,
                &change,
                RemovalPublication::Graceful {
                    admissions: carried,
                },
            )?;
            Ok::<(), Status>(())
        }
        .await;
        if let Err(error) = result {
            return self
                .abort_after_error(&change, error, retry_transient)
                .await;
        }
        change.phase = MigrationPhase::Published;
        change.activation_ready = true;
        if !self.post_publish_delay.is_zero() {
            tokio::time::sleep(self.post_publish_delay).await;
        }
        self.finish_direct_merge(change).await
    }

    pub(super) async fn finish_direct_merge(
        &self,
        mut change: TopologyChange,
    ) -> Result<TopologyChange, Status> {
        let deadline = Instant::now() + self.migration_timeout;
        let base = change
            .base_topology
            .as_ref()
            .ok_or_else(|| Status::internal("direct merge omitted base topology"))?;
        let target_ids: BTreeSet<_> = change
            .target_topology
            .members
            .iter()
            .map(|member| member.node_id.as_str())
            .collect();
        let removed: Vec<_> = base
            .members
            .iter()
            .filter(|member| !target_ids.contains(member.node_id.as_str()))
            .cloned()
            .collect();
        for member in &removed {
            if change.stopped_node_ids.contains(&member.node_id) {
                continue;
            }
            match self
                .install_on_members_with_lease(
                    &change.target_topology,
                    std::slice::from_ref(member),
                    deadline,
                    false,
                )
                .await
            {
                Ok(()) => {}
                Err(error) if is_absence_status(&error) => {
                    self.store_stopped_node(&member.node_id).await?;
                    change.stopped_node_ids.push(member.node_id.clone());
                }
                Err(error) => return Err(error),
            }
        }
        self.install_removed_topology_survivors(&change, deadline)
            .await?;
        self.set_direct_merge_fence(&change, false, false, deadline)
            .await?;
        self.set_phase(MigrationPhase::CleaningUp).await?;
        change.phase = MigrationPhase::CleaningUp;
        for member in removed {
            if change.stopped_node_ids.contains(&member.node_id) {
                continue;
            }
            let instance = self
                .state
                .read()
                .await
                .process_instances
                .get(&member.node_id)
                .cloned();
            let Some(instance) = instance else {
                self.store_stopped_node(&member.node_id).await?;
                change.stopped_node_ids.push(member.node_id);
                continue;
            };
            self.store_stopping_node(&member.node_id).await?;
            let mut node = match connect_node(&member.endpoint, deadline).await {
                Ok(node) => node,
                Err(error) if is_absence_status(&error) => {
                    self.store_stopped_node(&member.node_id).await?;
                    change.stopped_node_ids.push(member.node_id);
                    continue;
                }
                Err(error) => return Err(error),
            };
            let info = match rpc_before(deadline, node.get_process_info(proto::Empty {})).await {
                Ok(info) => info.into_inner(),
                Err(error) if is_absence_status(&error) => {
                    self.store_stopped_node(&member.node_id).await?;
                    change.stopped_node_ids.push(member.node_id);
                    continue;
                }
                Err(error) => return Err(error),
            };
            if info.node_id == member.node_id && info.process_instance_id == instance {
                rpc_before(
                    deadline,
                    node.prepare_stop(StopRequest {
                        node_id: member.node_id.clone(),
                        process_instance_id: instance.clone(),
                    }),
                )
                .await?;
                self.store_stop_prepared_node(&member.node_id, &instance)
                    .await?;
                match rpc_before(
                    deadline,
                    node.stop(StopRequest {
                        node_id: member.node_id.clone(),
                        process_instance_id: instance,
                    }),
                )
                .await
                {
                    Ok(_) => {}
                    Err(error) if is_absence_status(&error) => {}
                    Err(error) => return Err(error),
                }
            }
            self.store_stopped_node(&member.node_id).await?;
            change.stopped_node_ids.push(member.node_id);
        }
        self.set_phase(MigrationPhase::Complete).await?;
        change.phase = MigrationPhase::Complete;
        Ok(change)
    }
}
