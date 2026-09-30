use super::*;

impl CoordinatorService {
    pub fn new(
        state: ClusterState,
        repository: Arc<dyn CoordinatorRepository>,
        migration_timeout: Duration,
    ) -> Self {
        let (repair_interrupt, _) = watch::channel(0);
        Self {
            state: Arc::new(RwLock::new(state)),
            repository,
            execution_lock: Arc::new(Mutex::new(())),
            migration_timeout,
            range_move_concurrency: DEFAULT_RANGE_MOVE_CONCURRENCY,
            pre_publish_delay: Duration::ZERO,
            post_publish_delay: Duration::ZERO,
            lease_grants: Arc::new(Mutex::new(BTreeMap::new())),
            peer_failures: Arc::new(Mutex::new(BTreeMap::new())),
            pending_change_outages: Arc::new(Mutex::new(BTreeMap::new())),
            pending_change_probe_cursor: Arc::new(Mutex::new(0)),
            node_channels: Arc::new(Mutex::new(BTreeMap::new())),
            startup_at: Instant::now(),
            repair_interrupt: Arc::new(repair_interrupt),
        }
    }

    pub fn with_range_move_concurrency(mut self, concurrency: usize) -> Self {
        assert!(concurrency > 0, "range move concurrency must be positive");
        self.range_move_concurrency = concurrency;
        self
    }

    pub fn with_pre_publish_delay(mut self, delay: Duration) -> Self {
        self.pre_publish_delay = delay;
        self
    }

    pub fn with_post_publish_delay(mut self, delay: Duration) -> Self {
        self.post_publish_delay = delay;
        self
    }

    /// Run one failure-confirmation pass. A missed lease is sufficient evidence;
    /// otherwise two independent, continuously failing peer probes are needed.
    pub async fn run_failure_pass(&self) -> Result<(), Status> {
        let active_change = { self.state.read().await.active_change.clone() };
        if let Some(change) = active_change
            && !change.phase.is_terminal()
        {
            if change.failed_node_id.is_some() {
                self.execute_change(change_identity(&change), true).await?;
            } else {
                self.run_pending_change_failure_pass(&change).await?;
            }
            return Ok(());
        }
        let state = self.state.read().await.clone();
        if state.committed.members.len() <= 1 {
            return Ok(());
        }
        let now = Instant::now();
        if now < self.startup_at + NODE_LEASE_DURATION {
            return Ok(());
        }
        let grants = self.lease_grants.lock().await;
        let failures = self.peer_failures.lock().await;
        let candidate = state.committed.members.iter().find(|member| {
            if !state.process_instances.contains_key(&member.node_id)
                || state.fenced_nodes.contains(&member.node_id)
            {
                return false;
            }
            failure_confirmed(&state, &grants, &failures, &member.node_id, now)
        });
        let Some(failed_node_id) = candidate.map(|member| member.node_id.clone()) else {
            return Ok(());
        };
        drop(failures);
        drop(grants);
        let target_members = state
            .committed
            .members
            .iter()
            .filter(|member| member.node_id != failed_node_id)
            .cloned()
            .collect();
        let mut change = TopologyChange::plan(&state.committed, target_members)
            .map_err(|error| Status::failed_precondition(error.to_string()))?;
        change.failed_node_id = Some(failed_node_id.clone());
        let mut current = self.state.write().await;
        if current.committed != state.committed
            || current
                .active_change
                .as_ref()
                .is_some_and(|active| !active.phase.is_terminal())
        {
            return Ok(());
        }
        let mut next = current.clone();
        next.fenced_nodes.insert(failed_node_id);
        next.active_change = Some(change.clone());
        next.recovery_block_reason.clear();
        self.repository
            .store_state(&next)
            .map_err(|error| Status::internal(error.to_string()))?;
        *current = next;
        drop(current);
        self.repair_interrupt
            .send_modify(|generation| *generation += 1);
        self.execute_change(change_identity(&change), true).await?;
        Ok(())
    }

    async fn run_pending_change_failure_pass(&self, change: &TopologyChange) -> Result<(), Status> {
        let state = self.state.read().await.clone();
        let mut participants: BTreeMap<_, _> = change
            .target_topology
            .members
            .iter()
            .map(|member| (member.node_id.clone(), member.clone()))
            .collect();
        if let Some(base) = &change.base_topology
            && !matches!(
                change.phase,
                MigrationPhase::CleaningUp | MigrationPhase::Complete
            )
        {
            for member in &base.members {
                participants
                    .entry(member.node_id.clone())
                    .or_insert_with(|| member.clone());
            }
        }
        let mut candidates: Vec<_> = participants
            .into_values()
            .filter_map(|member| {
                state
                    .process_instances
                    .get(&member.node_id)
                    .map(|instance| (member, instance.clone()))
            })
            .collect();
        let candidate_ids: BTreeSet<_> = candidates
            .iter()
            .map(|(member, _)| member.node_id.clone())
            .collect();
        if !candidates.is_empty() {
            let mut cursor = self.pending_change_probe_cursor.lock().await;
            let start = *cursor % candidates.len();
            candidates.rotate_left(start);
            let scanned = candidates.len().min(MAX_PENDING_PROBES_PER_PASS);
            candidates.truncate(scanned);
            *cursor = (start + scanned) % candidate_ids.len();
        }
        let probes = try_map_bounded(candidates, 8, |(member, instance)| async move {
            let deadline = Instant::now() + Duration::from_millis(500);
            let healthy = match self.connect_node(&member.endpoint, deadline).await {
                Ok(mut node) => rpc_before(deadline, node.get_process_info(proto::Empty {}))
                    .await
                    .is_ok_and(|response| {
                        let info = response.into_inner();
                        info.node_id == member.node_id && info.process_instance_id == instance
                    }),
                Err(_) => false,
            };
            Ok::<_, Status>((member.node_id, healthy))
        })
        .await?;
        let now = Instant::now();
        let confirmation_delay = if matches!(
            change.phase,
            MigrationPhase::Published | MigrationPhase::CleaningUp
        ) {
            NODE_LEASE_DURATION
        } else {
            PREPUBLICATION_FAILURE_GRACE
        };
        let mut outages = self.pending_change_outages.lock().await;
        outages.retain(|node_id, _| candidate_ids.contains(node_id));
        let mut confirmed = None;
        for (node_id, healthy) in probes {
            if healthy {
                outages.remove(&node_id);
            } else {
                let first = outages.entry(node_id.clone()).or_insert(now);
                if confirmed.is_none() && *first + confirmation_delay <= now {
                    confirmed = Some(node_id);
                }
            }
        }
        drop(outages);
        if let Some(failed_node_id) = confirmed
            && let Err(error) = self
                .recover_pending_change_failure(change_identity(change), &failed_node_id)
                .await
        {
            self.set_recovery_block_reason(format!(
                "confirmed unavailable node {failed_node_id}; recovery is blocked: {error}"
            ))
            .await?;
            return Err(error);
        }
        Ok(())
    }

    /// Retry durable follower repairs when no topology transition is active.
    /// A failed task remains pending; the next call can safely start it again.
    pub async fn resume_replica_repairs(&self) -> Result<usize, Status> {
        let _execution = self.execution_lock.lock().await;
        let mut interrupt = self.repair_interrupt.subscribe();
        let tasks = {
            let mut state = self.state.write().await;
            if state
                .active_change
                .as_ref()
                .is_some_and(|change| !change.phase.is_terminal())
            {
                return Ok(0);
            }
            let mut next = state.clone();
            reconcile_replica_repairs(&mut next)
                .map_err(|error| Status::internal(error.to_string()))?;
            for repair in &mut next.replica_repairs {
                if repair.phase != ReplicaRepairPhase::Complete {
                    repair.phase = ReplicaRepairPhase::Pending;
                }
            }
            if next != *state {
                self.repository
                    .store_state(&next)
                    .map_err(|error| Status::internal(error.to_string()))?;
                *state = next;
            }
            let mut groups: BTreeMap<(String, String), Vec<ReplicaRepair>> = BTreeMap::new();
            let now_unix_millis = unix_millis_now();
            for repair in &state.replica_repairs {
                if state.process_instances.contains_key(&repair.owner_node_id)
                    && state.process_instances.contains_key(&repair.node_id)
                {
                    groups
                        .entry((repair.owner_node_id.clone(), repair.node_id.clone()))
                        .or_default()
                        .push(repair.clone());
                }
            }
            groups.retain(|_, repairs| {
                repairs
                    .iter()
                    .any(|repair| repair.phase == ReplicaRepairPhase::Pending)
                    && repairs.iter().all(|repair| {
                        repair.phase == ReplicaRepairPhase::Complete
                            || repair.next_attempt_unix_millis <= now_unix_millis
                    })
            });
            // Bound one pass as well as in-flight work. Remaining groups stay
            // durable and are considered on the next periodic pass.
            let mut due = groups.into_values().collect::<Vec<_>>();
            due.sort_by_key(|group| {
                group
                    .iter()
                    .map(|repair| repair.retry_count)
                    .max()
                    .unwrap_or(0)
            });
            due.truncate(MAX_REPAIR_GROUPS_PER_PASS);
            due
        };
        let work = try_map_bounded(
            tasks,
            self.range_move_concurrency.min(4),
            |tasks| async move {
                // Keep other ranges available if one destination is down. Failed
                // tasks retain durable Pending state for the next retry.
                match self.seed_replica_group(&tasks).await {
                    Ok(()) => Ok::<_, Status>(tasks.len()),
                    Err(error) => {
                        self.record_repair_failure(&tasks, &error).await?;
                        tracing::warn!(
                            owner = %tasks[0].owner_node_id,
                            follower = %tasks[0].node_id,
                            %error,
                            "replica seed remains pending"
                        );
                        Ok(0)
                    }
                }
            },
        );
        let results = tokio::select! {
            results = work => results?,
            changed = interrupt.changed() => {
                if changed.is_ok() {
                    self.cleanup_interrupted_repairs().await?;
                    return Ok(0);
                }
                return Err(Status::internal("repair interruption channel closed"));
            }
        };
        Ok(results.into_iter().sum())
    }

    pub(super) async fn record_repair_failure(
        &self,
        tasks: &[ReplicaRepair],
        error: &Status,
    ) -> Result<(), Status> {
        let mut state = self.state.write().await;
        let mut next = state.clone();
        let now = unix_millis_now();
        for task in tasks {
            if let Some(repair) = next.replica_repairs.iter_mut().find(|repair| {
                repair.epoch == task.epoch
                    && repair.start_exclusive == task.start_exclusive
                    && repair.end_inclusive == task.end_inclusive
                    && repair.owner_node_id == task.owner_node_id
                    && repair.node_id == task.node_id
                    && repair.phase != ReplicaRepairPhase::Complete
            }) {
                repair.retry_count = repair.retry_count.saturating_add(1);
                let delay_secs = 1u64 << repair.retry_count.min(6);
                repair.next_attempt_unix_millis = now.saturating_add(delay_secs.min(60) * 1000);
                repair.last_error = error.message().chars().take(256).collect();
            }
        }
        if next != *state {
            self.repository
                .store_state(&next)
                .map_err(|error| Status::internal(error.to_string()))?;
            *state = next;
        }
        Ok(())
    }

    pub(super) async fn cleanup_interrupted_repairs(&self) -> Result<(), Status> {
        let state = self.state.read().await.clone();
        let affected: BTreeSet<_> = state
            .replica_repairs
            .iter()
            .filter(|repair| {
                matches!(
                    repair.phase,
                    ReplicaRepairPhase::Copying | ReplicaRepairPhase::Verifying
                )
            })
            .flat_map(|repair| [repair.owner_node_id.clone(), repair.node_id.clone()])
            .filter(|node_id| !state.fenced_nodes.contains(node_id))
            .collect();
        let members: Vec<_> = state
            .committed
            .members
            .iter()
            .filter(|member| affected.contains(&member.node_id))
            .cloned()
            .collect();
        let epoch = state.committed.epoch;
        let deadline = Instant::now() + self.migration_timeout;
        try_map_bounded(members, self.range_move_concurrency, |member| async move {
            let mut node = self.connect_node(&member.endpoint, deadline).await?;
            rpc_before(
                deadline,
                node.abort_replica_repairs(proto::AbortReplicaRepairsRequest {
                    topology_epoch: epoch,
                }),
            )
            .await?;
            Ok::<(), Status>(())
        })
        .await?;
        Ok(())
    }

    // Called while the topology execution lock is held. Target-epoch owners
    // remain unleased until the policy and minimum-copy barrier is verified.
    pub(super) async fn seed_repairs_for_activation(&self) -> Result<(), Status> {
        self.cleanup_interrupted_repairs().await?;
        let (groups, required_count) = {
            let state = self.state.read().await;
            let topology = &state.committed;
            let guard = &topology.write_availability_guard;
            let policy_followers = match topology.write_ack_policy {
                WriteAckPolicy::OwnerOnly => 0,
                WriteAckPolicy::FirstSuccessor => 1,
                WriteAckPolicy::AllReplicas => topology.desired_replication_factor as usize - 1,
            };
            let needed = policy_followers
                .max(guard.minimum_admitted_copies.saturating_sub(1) as usize)
                .max(guard.minimum_healthy_followers as usize);
            let ranges = topology
                .derived_ranges()
                .map_err(|error| Status::internal(error.to_string()))?;
            for range in &ranges {
                if range.follower_node_ids.len() < needed {
                    return Err(Status::failed_precondition(
                        "target range cannot satisfy the activation policy and copy guard",
                    ));
                }
            }
            let admitted: BTreeSet<_> = state
                .replica_admissions
                .iter()
                .filter(|admission| {
                    admission.epoch == topology.epoch
                        && state.process_instances.get(&admission.node_id)
                            == Some(&admission.process_instance_id)
                })
                .map(|admission| {
                    (
                        admission.start_exclusive,
                        admission.end_inclusive,
                        admission.owner_node_id.clone(),
                        admission.node_id.clone(),
                    )
                })
                .collect();
            let mut groups: BTreeMap<(String, String), Vec<ReplicaRepair>> = BTreeMap::new();
            for repair in &state.replica_repairs {
                groups
                    .entry((repair.owner_node_id.clone(), repair.node_id.clone()))
                    .or_default()
                    .push(repair.clone());
            }
            let mut selected = BTreeSet::new();
            for range in &ranges {
                let owner = &range.owner_node_id;
                let is_admitted = |follower: &String| {
                    admitted.contains(&(
                        range.start_exclusive,
                        range.end_inclusive,
                        owner.clone(),
                        follower.clone(),
                    ))
                };
                // ACK policies name exact followers. The availability guard
                // only requires a count, so existing admissions and any
                // desired follower can satisfy its remaining quota.
                for follower in range.follower_node_ids.iter().take(policy_followers) {
                    if !is_admitted(follower) {
                        selected.insert((owner.clone(), follower.clone()));
                    }
                }
                let mut covered = range
                    .follower_node_ids
                    .iter()
                    .filter(|follower| {
                        is_admitted(follower)
                            || selected.contains(&(owner.clone(), (*follower).clone()))
                    })
                    .count();
                while covered < needed {
                    let candidate = range
                        .follower_node_ids
                        .iter()
                        .filter(|follower| {
                            !is_admitted(follower)
                                && !selected.contains(&(owner.clone(), (*follower).clone()))
                        })
                        .filter_map(|follower| {
                            let key = (owner.clone(), follower.clone());
                            groups.get(&key).map(|tasks| (key, tasks.len()))
                        })
                        .max_by_key(|(_, group_size)| *group_size)
                        .map(|(key, _)| key)
                        .ok_or_else(|| {
                            Status::unavailable("required target follower repair is unavailable")
                        })?;
                    selected.insert(candidate);
                    covered += 1;
                }
            }
            let pending = selected
                .into_iter()
                .map(|key| {
                    groups.remove(&key).ok_or_else(|| {
                        Status::unavailable("required target follower repair is unavailable")
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            (pending, ranges.len() * needed)
        };
        tracing::debug!(
            groups = groups.len(),
            required_followers = required_count,
            "replica activation seeding planned"
        );
        try_map_bounded(
            groups,
            self.range_move_concurrency.min(4),
            |group| async move { self.seed_replica_group(&group).await },
        )
        .await?;
        let state = self.state.read().await;
        let topology = &state.committed;
        let policy_followers = match topology.write_ack_policy {
            WriteAckPolicy::OwnerOnly => 0,
            WriteAckPolicy::FirstSuccessor => 1,
            WriteAckPolicy::AllReplicas => topology.desired_replication_factor as usize - 1,
        };
        let guard = &topology.write_availability_guard;
        let needed = policy_followers
            .max(guard.minimum_admitted_copies.saturating_sub(1) as usize)
            .max(guard.minimum_healthy_followers as usize);
        for range in topology
            .derived_ranges()
            .map_err(|error| Status::internal(error.to_string()))?
        {
            let admitted: BTreeSet<_> = state
                .replica_admissions
                .iter()
                .filter(|admission| {
                    admission.epoch == topology.epoch
                        && admission.start_exclusive == range.start_exclusive
                        && admission.end_inclusive == range.end_inclusive
                        && admission.owner_node_id == range.owner_node_id
                        && state.process_instances.get(&admission.node_id)
                            == Some(&admission.process_instance_id)
                })
                .map(|admission| admission.node_id.as_str())
                .collect();
            if range
                .follower_node_ids
                .iter()
                .filter(|follower| admitted.contains(follower.as_str()))
                .count()
                < needed
                || range
                    .follower_node_ids
                    .iter()
                    .take(policy_followers)
                    .any(|follower| !admitted.contains(follower.as_str()))
            {
                return Err(Status::unavailable(
                    "required target followers are not all admitted",
                ));
            }
        }
        Ok(())
    }

    pub(super) async fn copy_replica(
        &self,
        task: &ReplicaRepair,
    ) -> Result<CopiedReplicaSeed, Status> {
        let (topology, owner_instance, follower_instance) = {
            let state = self.state.read().await;
            if state.committed.epoch != task.epoch {
                return Err(Status::failed_precondition("replica task epoch is stale"));
            }
            let owner_instance = state
                .process_instances
                .get(&task.owner_node_id)
                .ok_or_else(|| Status::unavailable("owner process is not registered"))?
                .clone();
            let follower_instance = state
                .process_instances
                .get(&task.node_id)
                .ok_or_else(|| Status::unavailable("follower process is not registered"))?
                .clone();
            (state.committed.clone(), owner_instance, follower_instance)
        };
        let owner = topology
            .members
            .iter()
            .find(|member| member.node_id == task.owner_node_id)
            .ok_or_else(|| Status::internal("repair owner is absent from topology"))?;
        let follower = topology
            .members
            .iter()
            .find(|member| member.node_id == task.node_id)
            .ok_or_else(|| Status::internal("repair follower is absent from topology"))?;
        let control = replica_repair_control(task);
        let range = RangeMigration {
            range_id: control.range_id.clone(),
            start_exclusive: task.start_exclusive,
            end_inclusive: task.end_inclusive,
            source_node_id: task.owner_node_id.clone(),
            destination_node_id: task.node_id.clone(),
            source_endpoint: owner.endpoint.clone(),
            destination_endpoint: follower.endpoint.clone(),
            source_process_instance_id: String::new(),
            destination_process_instance_id: String::new(),
            source_cleaned: false,
            snapshot_records: 0,
            changelog_watermark: 0,
            verified: false,
        };
        let change = TopologyChange {
            change_id: control.change_id.clone(),
            base_epoch: task.epoch,
            base_topology: None,
            supersedes_change_id: None,
            target_topology: topology,
            phase: MigrationPhase::CopyingSnapshot,
            ranges: Vec::new(),
            replica_obligations: Vec::new(),
            stopped_node_ids: Vec::new(),
            stopping_node_ids: Vec::new(),
            stop_prepared_node_ids: Vec::new(),
            failed_node_id: None,
            activation_ready: false,
            direct_merge: false,
        };
        let deadline = Instant::now() + self.migration_timeout;
        let mut source = self.connect_node(&range.source_endpoint, deadline).await?;
        let mut destination = self
            .connect_node(&range.destination_endpoint, deadline)
            .await?;
        // A retry first releases any write fence left by an interrupted attempt.
        rpc_before(deadline, source.abort_range_migration(control.clone())).await?;
        rpc_before(deadline, destination.abort_range_migration(control.clone())).await?;
        let copied = self.copy_range(&change, &range, deadline).await?;
        if copied.source_process_instance_id != owner_instance
            || copied.destination_process_instance_id != follower_instance
        {
            return Err(Status::failed_precondition(
                "replica process changed during snapshot",
            ));
        }
        Ok(CopiedReplicaSeed {
            task: task.clone(),
            change,
            range: copied,
            control,
            follower_instance,
        })
    }

    pub(super) async fn finish_replica(
        &self,
        copied: CopiedReplicaSeed,
        final_watermark: u64,
        deadline: Instant,
    ) -> Result<PreparedReplicaSeed, Status> {
        let verified = self
            .finalize_range(&copied.change, &copied.range, final_watermark, deadline)
            .await?;
        let mut source = self
            .connect_node(&copied.range.source_endpoint, deadline)
            .await?;
        let digest = rpc_before(deadline, source.source_range_digest(copied.control.clone()))
            .await?
            .into_inner()
            .digest;
        Ok(PreparedReplicaSeed {
            task: copied.task.clone(),
            control: copied.control,
            admission: ReplicaAdmission {
                epoch: copied.task.epoch,
                start_exclusive: copied.task.start_exclusive,
                end_inclusive: copied.task.end_inclusive,
                owner_node_id: copied.task.owner_node_id,
                node_id: copied.task.node_id,
                process_instance_id: copied.follower_instance,
                verified_watermark: verified.changelog_watermark,
                stream_cursor: 0,
                digest,
            },
        })
    }

    pub(super) async fn seed_replica_group(&self, tasks: &[ReplicaRepair]) -> Result<(), Status> {
        let first = tasks
            .first()
            .ok_or_else(|| Status::invalid_argument("empty repair group"))?;
        let started = Instant::now();
        tracing::debug!(owner = %first.owner_node_id, follower = %first.node_id, ranges = tasks.len(), "replica seed group started");
        let (topology, owner_instance, follower_instance) = {
            let state = self.state.read().await;
            (
                state.committed.clone(),
                state.process_instances.get(&first.owner_node_id).cloned(),
                state.process_instances.get(&first.node_id).cloned(),
            )
        };
        if topology.epoch != first.epoch {
            return Err(Status::failed_precondition("repair stream epoch is stale"));
        }
        let expected: BTreeSet<_> = topology
            .derived_ranges()
            .map_err(|error| Status::internal(error.to_string()))?
            .into_iter()
            .filter(|range| {
                range.owner_node_id == first.owner_node_id
                    && range.follower_node_ids.contains(&first.node_id)
            })
            .map(|range| (range.start_exclusive, range.end_inclusive))
            .collect();
        let actual: BTreeSet<_> = tasks
            .iter()
            .map(|task| (task.start_exclusive, task.end_inclusive))
            .collect();
        if expected.is_empty()
            || expected != actual
            || tasks.iter().any(|task| {
                task.epoch != first.epoch
                    || task.owner_node_id != first.owner_node_id
                    || task.node_id != first.node_id
            })
        {
            return Err(Status::failed_precondition(
                "repair group does not cover its full stream",
            ));
        }
        let owner_endpoint = topology
            .members
            .iter()
            .find(|member| member.node_id == first.owner_node_id)
            .ok_or_else(|| Status::internal("repair owner is absent"))?
            .endpoint
            .clone();
        let follower_endpoint = topology
            .members
            .iter()
            .find(|member| member.node_id == first.node_id)
            .ok_or_else(|| Status::internal("repair follower is absent"))?
            .endpoint
            .clone();
        let mut prepared = Vec::with_capacity(tasks.len());
        let result = async {
            // Snapshot all ranges while writes continue. Only the short final
            // replay/checkpoint window fences writes for this stream.
            self.set_repair_phases(tasks, ReplicaRepairPhase::Copying)
                .await?;
            let copied = try_map_bounded(
                tasks.to_vec(),
                self.range_move_concurrency.min(4),
                |task| async move { self.copy_replica(&task).await },
            )
            .await?;
            let deadline = Instant::now() + self.migration_timeout;
            let paused =
                try_map_bounded(copied, self.range_move_concurrency, |copied| async move {
                    let watermark = self
                        .pause_range(&copied.change, &copied.range, deadline)
                        .await?;
                    Ok::<_, Status>((copied, watermark))
                })
                .await?;
            self.set_repair_phases(tasks, ReplicaRepairPhase::Verifying)
                .await?;
            prepared = try_map_bounded(
                paused,
                self.range_move_concurrency.min(4),
                |(copied, watermark)| async move {
                    self.finish_replica(copied, watermark, deadline).await
                },
            )
            .await?;
            let deadline = Instant::now() + self.migration_timeout;
            let mut source = self.connect_node(&owner_endpoint, deadline).await?;
            let mut destination = self.connect_node(&follower_endpoint, deadline).await?;
            let request = proto::ReplicationProgressRequest {
                topology_epoch: first.epoch,
                owner_node_id: first.owner_node_id.clone(),
                follower_node_id: first.node_id.clone(),
            };
            let head = rpc_before(deadline, source.get_replication_progress(request))
                .await?
                .into_inner();
            if Some(head.process_instance_id.as_str()) != owner_instance.as_deref() {
                return Err(Status::failed_precondition(
                    "owner process changed before checkpoint",
                ));
            }
            let checkpoint = rpc_before(
                deadline,
                destination.install_replication_checkpoint(proto::ReplicationCheckpointRequest {
                    topology_epoch: first.epoch,
                    owner_node_id: first.owner_node_id.clone(),
                    follower_node_id: first.node_id.clone(),
                    stream_sequence: head.stream_sequence,
                    verified_ranges: prepared.iter().map(|seed| seed.control.clone()).collect(),
                }),
            )
            .await?
            .into_inner();
            if Some(checkpoint.process_instance_id.as_str()) != follower_instance.as_deref()
                || checkpoint.stream_sequence < head.stream_sequence
            {
                return Err(Status::failed_precondition(
                    "follower process changed before checkpoint",
                ));
            }
            for seed in &mut prepared {
                seed.admission.stream_cursor = checkpoint.stream_sequence;
                rpc_before(deadline, source.cleanup_source_range(seed.control.clone())).await?;
                rpc_before(
                    deadline,
                    destination.abort_range_migration(seed.control.clone()),
                )
                .await?;
            }
            self.store_repair_admissions(&prepared).await?;
            Ok(())
        }
        .await;
        if result.is_err() {
            let cleanup_deadline = Instant::now() + self.migration_timeout;
            let controls: Vec<_> = tasks.iter().map(replica_repair_control).collect();
            if let Ok(mut source) = self.connect_node(&owner_endpoint, cleanup_deadline).await {
                for control in &controls {
                    let _ = rpc_before(
                        cleanup_deadline,
                        source.abort_range_migration(control.clone()),
                    )
                    .await;
                }
            }
            if let Ok(mut destination) = self
                .connect_node(&follower_endpoint, cleanup_deadline)
                .await
            {
                for control in &controls {
                    let _ = rpc_before(
                        cleanup_deadline,
                        destination.abort_range_migration(control.clone()),
                    )
                    .await;
                }
            }
            self.set_repair_phases(tasks, ReplicaRepairPhase::Pending)
                .await?;
        }
        tracing::debug!(owner = %first.owner_node_id, follower = %first.node_id, ranges = tasks.len(), elapsed_ms = started.elapsed().as_millis(), success = result.is_ok(), "replica seed group finished");
        result
    }

    pub(super) async fn set_repair_phases(
        &self,
        tasks: &[ReplicaRepair],
        phase: ReplicaRepairPhase,
    ) -> Result<(), Status> {
        let mut state = self.state.write().await;
        let mut next = state.clone();
        for task in tasks {
            let repair = next
                .replica_repairs
                .iter_mut()
                .find(|repair| {
                    repair == &task
                        || (repair.epoch == task.epoch
                            && repair.start_exclusive == task.start_exclusive
                            && repair.end_inclusive == task.end_inclusive
                            && repair.owner_node_id == task.owner_node_id
                            && repair.node_id == task.node_id)
                })
                .ok_or_else(|| Status::failed_precondition("replica repair was invalidated"))?;
            repair.phase = phase;
        }
        self.repository
            .store_state(&next)
            .map_err(|error| Status::internal(error.to_string()))?;
        *state = next;
        Ok(())
    }

    pub(super) async fn store_repair_admissions(
        &self,
        seeds: &[PreparedReplicaSeed],
    ) -> Result<(), Status> {
        let mut state = self.state.write().await;
        let mut next = state.clone();
        for seed in seeds {
            let task = &seed.task;
            let admission = &seed.admission;
            if next.committed.epoch != task.epoch
                || next.process_instances.get(&task.node_id) != Some(&admission.process_instance_id)
            {
                return Err(Status::failed_precondition(
                    "replica changed before durable admission",
                ));
            }
            let repair = next
                .replica_repairs
                .iter_mut()
                .find(|repair| {
                    repair.epoch == task.epoch
                        && repair.start_exclusive == task.start_exclusive
                        && repair.end_inclusive == task.end_inclusive
                        && repair.owner_node_id == task.owner_node_id
                        && repair.node_id == task.node_id
                })
                .ok_or_else(|| Status::failed_precondition("replica repair was invalidated"))?;
            repair.phase = ReplicaRepairPhase::Complete;
            repair.retry_count = 0;
            repair.next_attempt_unix_millis = 0;
            repair.last_error.clear();
            next.replica_admissions.retain(|existing| {
                !(existing.epoch == task.epoch
                    && existing.start_exclusive == task.start_exclusive
                    && existing.end_inclusive == task.end_inclusive
                    && existing.owner_node_id == task.owner_node_id
                    && existing.node_id == task.node_id)
            });
            next.replica_admissions.push(admission.clone());
        }
        self.repository
            .store_state(&next)
            .map_err(|error| Status::internal(error.to_string()))?;
        *state = next;
        Ok(())
    }
}
