use super::*;

impl CoordinatorService {
    pub async fn resume_interrupted_change(&self) -> Result<Option<TopologyChange>, Status> {
        let change = self.state.read().await.active_change.clone();
        match change {
            Some(change)
                if matches!(
                    change.phase,
                    MigrationPhase::Planned | MigrationPhase::Complete | MigrationPhase::Aborted
                ) =>
            {
                Ok(None)
            }
            None => Ok(None),
            Some(change) => {
                let result = self.execute_change(change_identity(&change), true).await;
                if let Err(error) = &result
                    && change.supersedes_change_id.is_some()
                {
                    self.set_recovery_block_reason(format!(
                        "published recovery is blocked: {error}"
                    ))
                    .await?;
                }
                result.map(Some)
            }
        }
    }

    pub(super) async fn execute_change(
        &self,
        expected: ChangeIdentity,
        retry_transient: bool,
    ) -> Result<TopologyChange, Status> {
        let _execution = self.execution_lock.lock().await;
        let mut change = self
            .state
            .read()
            .await
            .active_change
            .clone()
            .ok_or_else(|| Status::failed_precondition("no topology change is active"))?;
        if change_identity(&change) != expected {
            return Err(Status::failed_precondition(
                "active topology change does not match the execute request",
            ));
        }
        if change.failed_node_id.is_some() {
            return self.execute_failed_change(change).await;
        }
        match change.phase {
            MigrationPhase::Complete => return Ok(change),
            MigrationPhase::Aborted => {
                return Err(Status::failed_precondition(
                    "the topology change was aborted",
                ));
            }
            MigrationPhase::Aborting => {
                self.finish_abort(&change).await?;
                change.phase = MigrationPhase::Aborted;
                return Ok(change);
            }
            MigrationPhase::Published | MigrationPhase::CleaningUp => {
                if change.direct_merge {
                    return self.finish_direct_merge(change).await;
                }
                if change.supersedes_change_id.is_some() {
                    return self.finish_recovered_change(change).await;
                }
                return self
                    .finish_published_change(change, Instant::now() + self.migration_timeout)
                    .await;
            }
            MigrationPhase::Planned => {}
            MigrationPhase::Resetting
            | MigrationPhase::CopyingSnapshot
            | MigrationPhase::ReplayingChangelog
            | MigrationPhase::PausingWrites
            | MigrationPhase::Verifying
            | MigrationPhase::ReadyToPublish => {
                change = self.reset_prepublication_attempt(&change).await?;
            }
        }

        if change.direct_merge {
            return self.execute_direct_merge(change, retry_transient).await;
        }
        if change.ranges.is_empty()
            && change.replica_obligations.is_empty()
            && change.target_topology.members == self.state.read().await.committed.members
            && change.target_topology.config() != self.state.read().await.committed.config()
        {
            return self.execute_policy_change(change, false).await;
        }

        let deadline = Instant::now() + self.migration_timeout;
        self.set_phase(MigrationPhase::CopyingSnapshot).await?;
        change.phase = MigrationPhase::CopyingSnapshot;
        let change_ref = &change;
        let progresses = match try_map_bounded(
            change.ranges.clone(),
            self.range_move_concurrency,
            |range| async move {
                let progress = self.copy_range(change_ref, &range, deadline).await?;
                self.store_range_progress(&progress).await?;
                Ok(progress)
            },
        )
        .await
        {
            Ok(progresses) => progresses,
            Err(error) => {
                return self
                    .abort_after_error(&change, error, retry_transient)
                    .await;
            }
        };
        for progress in progresses {
            replace_range_progress(&mut change, progress);
        }

        self.set_phase(MigrationPhase::ReplayingChangelog).await?;
        change.phase = MigrationPhase::ReplayingChangelog;
        self.set_phase(MigrationPhase::PausingWrites).await?;
        change.phase = MigrationPhase::PausingWrites;
        let change_ref = &change;
        let final_watermarks = match try_map_bounded(
            change.ranges.clone(),
            self.range_move_concurrency,
            |range| async move {
                let watermark = self.pause_range(change_ref, &range, deadline).await?;
                Ok((range.range_id, watermark))
            },
        )
        .await
        {
            Ok(watermarks) => watermarks.into_iter().collect::<BTreeMap<_, _>>(),
            Err(error) => {
                return self
                    .abort_after_error(&change, error, retry_transient)
                    .await;
            }
        };

        if let Err(error) = self.set_phase(MigrationPhase::Verifying).await {
            return self
                .abort_after_error(&change, error, retry_transient)
                .await;
        }
        change.phase = MigrationPhase::Verifying;
        let change_ref = &change;
        let final_watermarks = &final_watermarks;
        let progresses = match try_map_bounded(
            change.ranges.clone(),
            self.range_move_concurrency,
            |range| async move {
                let final_watermark = final_watermarks[&range.range_id];
                let progress = self
                    .finalize_range(change_ref, &range, final_watermark, deadline)
                    .await?;
                self.store_range_progress(&progress).await?;
                Ok(progress)
            },
        )
        .await
        {
            Ok(progresses) => progresses,
            Err(error) => {
                return self
                    .abort_after_error(&change, error, retry_transient)
                    .await;
            }
        };
        for progress in progresses {
            replace_range_progress(&mut change, progress);
        }

        self.set_phase(MigrationPhase::ReadyToPublish).await?;
        change.phase = MigrationPhase::ReadyToPublish;
        if !self.pre_publish_delay.is_zero() {
            tokio::time::sleep(self.pre_publish_delay).await;
        }
        if let Err(error) = self.verify_destination_instances(&change, deadline).await {
            return self
                .abort_after_error(&change, error, retry_transient)
                .await;
        }
        {
            let destinations = destination_instances(&change)?;
            let mut state = self.state.write().await;
            let mut next = state.clone();
            if let Some(node_id) =
                destinations
                    .iter()
                    .find_map(|(node_id, (_, expected_instance))| {
                        (next.process_instances.get(node_id) != Some(expected_instance))
                            .then_some(node_id)
                    })
            {
                let error = Status::failed_precondition(format!(
                    "destination process changed before publication: {node_id}"
                ));
                drop(state);
                return self
                    .abort_after_error(&change, error, retry_transient)
                    .await;
            }
            let active = next
                .active_change
                .as_mut()
                .ok_or_else(|| Status::internal("active change disappeared"))?;
            if change_identity(active) != expected {
                return Err(Status::failed_precondition(
                    "active topology change changed before publication",
                ));
            }
            active.phase = MigrationPhase::Published;
            next.committed = active.target_topology.clone();
            reconcile_replica_repairs(&mut next)
                .map_err(|error| Status::internal(error.to_string()))?;
            self.repository
                .store_state(&next)
                .map_err(|error| Status::internal(error.to_string()))?;
            *state = next;
        }
        tracing::debug!(change_id = %change.change_id, epoch = change.target_topology.epoch, moved_ranges = change.ranges.len(), replica_obligations = change.replica_obligations.len(), "topology published; activation pending");
        change.phase = MigrationPhase::Published;
        if !self.post_publish_delay.is_zero() {
            tokio::time::sleep(self.post_publish_delay).await;
        }
        self.finish_published_change(change, deadline).await
    }

    pub(super) async fn set_recovery_block_reason(&self, reason: String) -> Result<(), Status> {
        let mut state = self.state.write().await;
        if state.recovery_block_reason == reason {
            return Ok(());
        }
        let mut next = state.clone();
        next.recovery_block_reason = reason;
        self.repository
            .store_state(&next)
            .map_err(|error| Status::internal(error.to_string()))?;
        *state = next;
        Ok(())
    }

    pub(super) async fn recover_pending_change_failure(
        &self,
        expected: ChangeIdentity,
        failed_node_id: &str,
    ) -> Result<(), Status> {
        let _execution = self.execution_lock.lock().await;
        let state = self.state.read().await.clone();
        let Some(change) = state.active_change.clone() else {
            return Ok(());
        };
        if change_identity(&change) != expected || change.phase.is_terminal() {
            return Ok(());
        }
        if change.phase != MigrationPhase::Published || change.activation_ready {
            if !change.activation_ready
                && !matches!(
                    change.phase,
                    MigrationPhase::Aborting | MigrationPhase::CleaningUp
                )
                && change.base_topology.as_ref().is_some_and(|base| {
                    !base
                        .members
                        .iter()
                        .any(|member| member.node_id == failed_node_id)
                })
            {
                self.abort_prepublication_change(&change).await?;
                self.set_recovery_block_reason(String::new()).await?;
                self.pending_change_outages.lock().await.clear();
                return Ok(());
            }
            self.set_recovery_block_reason(format!(
                "confirmed unavailable node {failed_node_id} during {:?}; automatic recovery requires a published, unactivated pure join",
                change.phase
            ))
            .await?;
            return Ok(());
        }
        let Some(base) = change.base_topology.as_ref() else {
            self.set_recovery_block_reason(format!(
                "confirmed unavailable node {failed_node_id}; original topology is unavailable for recovery"
            ))
            .await?;
            return Ok(());
        };
        let surviving: BTreeSet<_> = state
            .committed
            .members
            .iter()
            .filter(|member| member.node_id != failed_node_id)
            .map(|member| (member.node_id.clone(), member.endpoint.clone()))
            .collect();
        let original: BTreeSet<_> = base
            .members
            .iter()
            .map(|member| (member.node_id.clone(), member.endpoint.clone()))
            .collect();
        let recoverable = !original.is_empty()
            && surviving == original
            && !original
                .iter()
                .any(|(node_id, _)| node_id == failed_node_id)
            && change.ranges.iter().all(|range| {
                range.destination_node_id == failed_node_id
                    && range.verified
                    && !range.source_cleaned
                    && !range.source_process_instance_id.is_empty()
                    && state.process_instances.get(&range.source_node_id)
                        == Some(&range.source_process_instance_id)
            });
        if !recoverable {
            self.set_recovery_block_reason(format!(
                "confirmed unavailable node {failed_node_id}; surviving original owners cannot prove complete frozen-range coverage"
            ))
            .await?;
            return Ok(());
        }
        let deadline = Instant::now() + self.migration_timeout;
        for member in &base.members {
            let expected_instance =
                state
                    .process_instances
                    .get(&member.node_id)
                    .ok_or_else(|| {
                        Status::failed_precondition(format!(
                            "original owner {} has no process identity",
                            member.node_id
                        ))
                    })?;
            let mut node = connect_node(&member.endpoint, deadline).await?;
            let info = rpc_before(deadline, node.get_process_info(proto::Empty {}))
                .await?
                .into_inner();
            if info.node_id != member.node_id || info.process_instance_id != *expected_instance {
                return Err(Status::failed_precondition(format!(
                    "original owner {} changed process instance",
                    member.node_id
                )));
            }
        }
        for range in &change.ranges {
            let mut source = connect_node(&range.source_endpoint, deadline).await?;
            let digest = rpc_before(
                deadline,
                source.source_range_digest(RangeControlRequest {
                    change_id: change.change_id.clone(),
                    range_id: range.range_id.clone(),
                }),
            )
            .await?
            .into_inner();
            if digest.changelog_watermark != range.changelog_watermark || digest.digest.is_empty() {
                return Err(Status::failed_precondition(format!(
                    "frozen source range {} no longer matches the verified watermark",
                    range.range_id
                )));
            }
        }
        let mut recovery = TopologyChange::plan(&state.committed, base.members.clone())
            .map_err(|error| Status::failed_precondition(error.to_string()))?;
        recovery.ranges.clear();
        recovery.phase = MigrationPhase::Published;
        recovery.supersedes_change_id = Some(change.change_id.clone());
        let mut current = self.state.write().await;
        if current
            .active_change
            .as_ref()
            .is_none_or(|active| change_identity(active) != expected)
            || current.committed != state.committed
        {
            return Ok(());
        }
        let mut next = current.clone();
        next.committed = recovery.target_topology.clone();
        next.active_change = Some(recovery.clone());
        next.superseded_change = Some(change);
        next.fenced_nodes.insert(failed_node_id.to_owned());
        next.process_instances.remove(failed_node_id);
        next.replica_admissions.clear();
        next.replica_repairs.clear();
        next.recovery_block_reason.clear();
        reconcile_replica_repairs(&mut next)
            .map_err(|error| Status::internal(error.to_string()))?;
        self.repository
            .store_state(&next)
            .map_err(|error| Status::internal(error.to_string()))?;
        *current = next;
        drop(current);
        self.pending_change_outages.lock().await.clear();
        self.finish_recovered_change(recovery).await?;
        Ok(())
    }

    async fn finish_recovered_change(
        &self,
        mut change: TopologyChange,
    ) -> Result<TopologyChange, Status> {
        let original = self
            .state
            .read()
            .await
            .superseded_change
            .clone()
            .ok_or_else(|| {
                Status::failed_precondition("superseded change is missing from durable state")
            })?;
        let deadline = Instant::now() + self.migration_timeout;
        self.install_on_members_with_lease(
            &change.target_topology,
            &change.target_topology.members,
            deadline,
            false,
        )
        .await?;
        let last_grant = self
            .lease_grants
            .lock()
            .await
            .values()
            .map(|grant| grant.expires_at)
            .max()
            .unwrap_or(self.startup_at);
        tokio::time::sleep_until(last_grant.max(self.startup_at + NODE_LEASE_DURATION)).await;
        let deadline = Instant::now() + self.migration_timeout;
        for range in &original.ranges {
            let mut source = connect_node(&range.source_endpoint, deadline).await?;
            rpc_before(
                deadline,
                source.abort_range_migration(RangeControlRequest {
                    change_id: original.change_id.clone(),
                    range_id: range.range_id.clone(),
                }),
            )
            .await?;
        }
        let guard = &change.target_topology.write_availability_guard;
        if change.target_topology.write_ack_policy != WriteAckPolicy::OwnerOnly
            || guard.minimum_admitted_copies > 1
            || guard.minimum_healthy_followers > 0
        {
            self.seed_repairs_for_activation().await?;
        }
        self.set_activation_ready().await?;
        change.activation_ready = true;
        self.install_on_members(
            &change.target_topology,
            &change.target_topology.members,
            Instant::now() + self.migration_timeout,
        )
        .await?;
        self.set_phase(MigrationPhase::Complete).await?;
        change.phase = MigrationPhase::Complete;
        Ok(change)
    }

    pub(super) async fn abort_after_error(
        &self,
        change: &TopologyChange,
        original: Status,
        retry_transient: bool,
    ) -> Result<TopologyChange, Status> {
        if retry_transient
            && matches!(
                original.code(),
                tonic::Code::Unavailable | tonic::Code::DeadlineExceeded
            )
        {
            // Keep a network-interrupted change recoverable. Resetting is durable,
            // and the next pass repeats cleanup before it copies anything.
            self.set_phase(MigrationPhase::Resetting).await?;
            if let Err(cleanup) = self
                .clear_prepublication_nodes(change, Instant::now() + self.migration_timeout)
                .await
            {
                return Err(Status::unavailable(format!(
                    "migration interrupted ({original}); cleanup remains pending ({cleanup})"
                )));
            }
            return Err(original);
        }
        match self.abort_prepublication_change(change).await {
            Ok(()) => Err(original),
            Err(cleanup) => Err(Status::internal(format!(
                "migration failed ({original}); cleanup remains pending ({cleanup})"
            ))),
        }
    }

    pub(super) async fn execute_failed_change(
        &self,
        mut change: TopologyChange,
    ) -> Result<TopologyChange, Status> {
        let failed_node_id = change
            .failed_node_id
            .as_ref()
            .ok_or_else(|| Status::internal("failure transition omitted node identity"))?;
        if change.phase == MigrationPhase::Complete {
            return Ok(change);
        }
        if matches!(
            change.phase,
            MigrationPhase::Published | MigrationPhase::CleaningUp
        ) {
            return self.finish_failed_change(change).await;
        }
        if change.phase == MigrationPhase::Aborted {
            return Err(Status::failed_precondition(
                "failure transition was aborted",
            ));
        }
        self.cleanup_interrupted_repairs().await?;
        // A coordinator restart loses the old in-memory grant deadline. A full
        // startup quarantine covers a grant issued immediately before restart.
        let last_grant = self
            .lease_grants
            .lock()
            .await
            .get(failed_node_id)
            .map(|grant| grant.expires_at)
            .unwrap_or(self.startup_at);
        let safe_at = last_grant.max(self.startup_at + NODE_LEASE_DURATION);
        tokio::time::sleep_until(safe_at).await;

        let publication = {
            let mut state = self.state.write().await;
            let grants = self.lease_grants.lock().await;
            prove_failure_coverage(&state, &change.target_topology, &grants, Instant::now())?;
            if !state.fenced_nodes.contains(failed_node_id)
                || state
                    .active_change
                    .as_ref()
                    .is_none_or(|active| change_identity(active) != change_identity(&change))
            {
                return Err(Status::failed_precondition(
                    "failure transition lost its durable fence",
                ));
            }
            // Old admissions are tied to old exact bounds and stream epoch.
            // Deterministic repairs will verify and re-admit new followers.
            self.publish_removed_topology(&mut state, &change, RemovalPublication::Failed)?;
            Ok::<(), Status>(())
        };
        publication?;
        change.phase = MigrationPhase::Published;
        self.finish_failed_change(change).await
    }

    pub(super) async fn finish_failed_change(
        &self,
        mut change: TopologyChange,
    ) -> Result<TopologyChange, Status> {
        let deadline = Instant::now() + self.migration_timeout;
        self.install_removed_topology_survivors(&change, deadline)
            .await?;
        self.set_phase(MigrationPhase::Complete).await?;
        change.phase = MigrationPhase::Complete;
        if let Some(failed_node_id) = &change.failed_node_id {
            self.lease_grants.lock().await.remove(failed_node_id);
            self.peer_failures
                .lock()
                .await
                .retain(|(target, reporter), _| {
                    target != failed_node_id && reporter != failed_node_id
                });
        }
        Ok(change)
    }

    pub(super) async fn copy_range(
        &self,
        change: &TopologyChange,
        range: &RangeMigration,
        deadline: Instant,
    ) -> Result<RangeMigration, Status> {
        let mut source = connect_node(&range.source_endpoint, deadline).await?;
        let mut destination = connect_node(&range.destination_endpoint, deadline).await?;
        let spec = proto::RangeSpec {
            change_id: change.change_id.clone(),
            range_id: range.range_id.clone(),
            start_exclusive: range.start_exclusive,
            end_inclusive: range.end_inclusive,
            source_node_id: range.source_node_id.clone(),
            destination_node_id: range.destination_node_id.clone(),
        };
        let source_process_instance_id = rpc_before(
            deadline,
            source.prepare_source_range(PrepareRangeRequest {
                range: Some(spec.clone()),
            }),
        )
        .await?
        .into_inner()
        .process_instance_id;
        if source_process_instance_id.is_empty() {
            return Err(Status::data_loss(
                "source omitted its process instance identifier",
            ));
        }
        let destination_process_instance_id = rpc_before(
            deadline,
            destination.prepare_destination_range(PrepareRangeRequest { range: Some(spec) }),
        )
        .await?
        .into_inner()
        .process_instance_id;
        if destination_process_instance_id.is_empty() {
            return Err(Status::data_loss(
                "destination omitted its process instance identifier",
            ));
        }

        let mut cursor = 0;
        let mut snapshot_records = 0;
        loop {
            let page = rpc_before(
                deadline,
                source.read_snapshot_page(SnapshotPageRequest {
                    change_id: change.change_id.clone(),
                    range_id: range.range_id.clone(),
                    cursor,
                    max_bytes: MAX_MIGRATION_PAGE_BYTES as u64,
                }),
            )
            .await?
            .into_inner();
            snapshot_records += page.records.len() as u64;
            if !page.records.is_empty() {
                rpc_before(
                    deadline,
                    destination.apply_migration_batch(ApplyMigrationBatchRequest {
                        change_id: change.change_id.clone(),
                        range_id: range.range_id.clone(),
                        snapshot_records: page.records,
                        journal_records: Vec::new(),
                    }),
                )
                .await?;
            }
            cursor = page.next_cursor;
            if page.done {
                break;
            }
        }

        let mut dedup_cursor = 0;
        loop {
            let page = rpc_before(
                deadline,
                source.read_dedup_snapshot_page(SnapshotPageRequest {
                    change_id: change.change_id.clone(),
                    range_id: range.range_id.clone(),
                    cursor: dedup_cursor,
                    max_bytes: MAX_MIGRATION_PAGE_BYTES as u64,
                }),
            )
            .await?
            .into_inner();
            if !page.records.is_empty() {
                rpc_before(
                    deadline,
                    destination.apply_dedup_batch(ApplyDedupBatchRequest {
                        change_id: change.change_id.clone(),
                        range_id: range.range_id.clone(),
                        records: page.records,
                    }),
                )
                .await?;
            }
            dedup_cursor = page.next_cursor;
            if page.done {
                break;
            }
        }

        let watermark = replay_changelog(
            &mut source,
            &mut destination,
            change,
            range,
            0,
            None,
            deadline,
        )
        .await?;
        let mut progress = range.clone();
        progress.source_process_instance_id = source_process_instance_id;
        progress.destination_process_instance_id = destination_process_instance_id;
        progress.snapshot_records = snapshot_records;
        progress.changelog_watermark = watermark;
        Ok(progress)
    }

    pub(super) async fn verify_destination_instances(
        &self,
        change: &TopologyChange,
        deadline: Instant,
    ) -> Result<(), Status> {
        let destinations = destination_instances(change)?;
        for (node_id, (endpoint, expected_instance)) in destinations {
            let mut client = connect_node(&endpoint, deadline).await?;
            let actual = rpc_before(deadline, client.get_process_info(proto::Empty {}))
                .await?
                .into_inner();
            if actual.node_id != node_id || actual.process_instance_id != expected_instance {
                return Err(Status::failed_precondition(format!(
                    "destination process changed before publication: {node_id}"
                )));
            }
        }
        Ok(())
    }

    pub(super) async fn pause_range(
        &self,
        change: &TopologyChange,
        range: &RangeMigration,
        deadline: Instant,
    ) -> Result<u64, Status> {
        let mut source = connect_node(&range.source_endpoint, deadline).await?;
        Ok(rpc_before(
            deadline,
            source.pause_range_writes(RangeControlRequest {
                change_id: change.change_id.clone(),
                range_id: range.range_id.clone(),
            }),
        )
        .await?
        .into_inner()
        .final_watermark)
    }

    pub(super) async fn finalize_range(
        &self,
        change: &TopologyChange,
        range: &RangeMigration,
        final_watermark: u64,
        deadline: Instant,
    ) -> Result<RangeMigration, Status> {
        let mut source = connect_node(&range.source_endpoint, deadline).await?;
        let mut destination = connect_node(&range.destination_endpoint, deadline).await?;
        let watermark = replay_changelog(
            &mut source,
            &mut destination,
            change,
            range,
            range.changelog_watermark,
            Some(final_watermark),
            deadline,
        )
        .await?;
        let control = RangeControlRequest {
            change_id: change.change_id.clone(),
            range_id: range.range_id.clone(),
        };
        let source_digest = rpc_before(deadline, source.source_range_digest(control.clone()))
            .await?
            .into_inner();
        let destination_digest = rpc_before(
            deadline,
            destination.destination_range_digest(control.clone()),
        )
        .await?
        .into_inner();
        if source_digest.record_count != destination_digest.record_count
            || source_digest.digest != destination_digest.digest
            || source_digest.changelog_watermark != destination_digest.changelog_watermark
            || destination_digest.changelog_watermark != final_watermark
            || watermark != final_watermark
        {
            return Err(Status::data_loss(format!(
                "range verification failed for {}",
                range.range_id
            )));
        }
        rpc_before(deadline, destination.commit_destination_range(control)).await?;

        let mut progress = range.clone();
        progress.changelog_watermark = final_watermark;
        progress.verified = true;
        Ok(progress)
    }

    pub(super) async fn finish_published_change(
        &self,
        mut change: TopologyChange,
        _deadline: Instant,
    ) -> Result<TopologyChange, Status> {
        let deadline = Instant::now() + self.migration_timeout;
        let needs_activation = !change.activation_ready;
        if needs_activation {
            let old_sources: Vec<_> = change
                .ranges
                .iter()
                .map(|range| (range.source_node_id.clone(), range.source_endpoint.clone()))
                .collect::<BTreeMap<_, _>>()
                .into_iter()
                .map(|(node_id, endpoint)| Member { node_id, endpoint })
                .collect();
            self.install_on_members_with_lease(
                &change.target_topology,
                &old_sources,
                deadline,
                false,
            )
            .await?;
        }
        let destination_ids: BTreeSet<_> = change
            .ranges
            .iter()
            .map(|range| range.destination_node_id.as_str())
            .collect();
        let destinations: Vec<_> = change
            .target_topology
            .members
            .iter()
            .filter(|member| destination_ids.contains(member.node_id.as_str()))
            .cloned()
            .collect();
        let other_targets: Vec<_> = change
            .target_topology
            .members
            .iter()
            .filter(|member| !destination_ids.contains(member.node_id.as_str()))
            .cloned()
            .collect();
        self.install_on_members_with_lease(
            &change.target_topology,
            &destinations,
            deadline,
            !needs_activation,
        )
        .await?;
        self.install_on_members_with_lease(
            &change.target_topology,
            &other_targets,
            deadline,
            !needs_activation,
        )
        .await?;
        let target_ids: BTreeSet<String> = change
            .target_topology
            .members
            .iter()
            .map(|member| member.node_id.clone())
            .collect();
        let stopping_or_stopped: BTreeSet<_> = change
            .stopping_node_ids
            .iter()
            .chain(&change.stop_prepared_node_ids)
            .chain(&change.stopped_node_ids)
            .map(String::as_str)
            .collect();
        let removed_members: Vec<_> = change
            .ranges
            .iter()
            .filter(|range| {
                !target_ids.contains(&range.source_node_id)
                    && !stopping_or_stopped.contains(range.source_node_id.as_str())
            })
            .map(|range| (range.source_node_id.clone(), range.source_endpoint.clone()))
            .collect::<BTreeMap<_, _>>()
            .into_iter()
            .map(|(node_id, endpoint)| Member { node_id, endpoint })
            .collect();
        self.install_on_members(&change.target_topology, &removed_members, deadline)
            .await?;
        let change_id = &change.change_id;
        try_map_bounded(
            change.ranges.clone(),
            self.range_move_concurrency,
            |range| async move {
                let mut destination = connect_node(&range.destination_endpoint, deadline).await?;
                rpc_before(
                    deadline,
                    destination.activate_destination_range(RangeControlRequest {
                        change_id: change_id.clone(),
                        range_id: range.range_id,
                    }),
                )
                .await?;
                Ok::<(), Status>(())
            },
        )
        .await?;
        if needs_activation {
            let guard = &change.target_topology.write_availability_guard;
            let needs_replica_barrier = change.target_topology.write_ack_policy
                != WriteAckPolicy::OwnerOnly
                || guard.minimum_admitted_copies > 1
                || guard.minimum_healthy_followers > 0;
            if needs_replica_barrier
                && (!change.ranges.is_empty() || !change.replica_obligations.is_empty())
            {
                tracing::debug!(change_id = %change.change_id, "starting replica activation barrier");
                self.seed_repairs_for_activation().await?;
                tracing::debug!(change_id = %change.change_id, "replica activation barrier satisfied");
            }
            self.set_activation_ready().await?;
            tracing::debug!(change_id = %change.change_id, "topology activation ready");
            change.activation_ready = true;
            self.install_on_members(
                &change.target_topology,
                &change.target_topology.members,
                Instant::now() + self.migration_timeout,
            )
            .await?;
        }
        if change.ranges.is_empty() && change.replica_obligations.is_empty() {
            self.policy_fence_members(&change, false, deadline).await?;
        }
        self.set_phase(MigrationPhase::CleaningUp).await?;
        change.phase = MigrationPhase::CleaningUp;
        let ranges_to_clean: Vec<_> = change
            .ranges
            .iter()
            .filter(|range| {
                !range.source_cleaned
                    && !change.stopping_node_ids.contains(&range.source_node_id)
                    && !change
                        .stop_prepared_node_ids
                        .contains(&range.source_node_id)
                    && !change.stopped_node_ids.contains(&range.source_node_id)
            })
            .cloned()
            .collect();
        let change_id = &change.change_id;
        let cleaned = try_map_bounded(
            ranges_to_clean,
            self.range_move_concurrency,
            |mut range| async move {
                let mut source = connect_node(&range.source_endpoint, deadline).await?;
                rpc_before(
                    deadline,
                    source.cleanup_source_range(RangeControlRequest {
                        change_id: change_id.clone(),
                        range_id: range.range_id.clone(),
                    }),
                )
                .await?;
                range.source_cleaned = true;
                self.store_range_progress(&range).await?;
                Ok::<RangeMigration, Status>(range)
            },
        )
        .await?;
        for progress in cleaned {
            replace_range_progress(&mut change, progress);
        }
        let mut removed_sources: BTreeMap<String, (String, String)> = BTreeMap::new();
        for range in change
            .ranges
            .iter()
            .filter(|range| !target_ids.contains(&range.source_node_id))
        {
            if range.source_process_instance_id.is_empty() {
                return Err(Status::data_loss(format!(
                    "missing process instance for removed node {}",
                    range.source_node_id
                )));
            }
            let value = (
                range.source_endpoint.clone(),
                range.source_process_instance_id.clone(),
            );
            if removed_sources
                .insert(range.source_node_id.clone(), value.clone())
                .is_some_and(|previous| previous != value)
            {
                return Err(Status::data_loss(format!(
                    "removed node {} changed process instance during migration",
                    range.source_node_id
                )));
            }
        }
        for (node_id, (endpoint, process_instance_id)) in removed_sources {
            if change.stopped_node_ids.contains(&node_id) {
                continue;
            }
            if !change.stopping_node_ids.contains(&node_id)
                && !change.stop_prepared_node_ids.contains(&node_id)
            {
                self.store_stopping_node(&node_id).await?;
                change.stopping_node_ids.push(node_id.clone());
            }
            if !change.stop_prepared_node_ids.contains(&node_id) {
                let mut client = connect_node(&endpoint, deadline).await?;
                let actual = rpc_before(deadline, client.get_process_info(proto::Empty {}))
                    .await?
                    .into_inner();
                if actual.node_id != node_id || actual.process_instance_id != process_instance_id {
                    self.store_stopped_node(&node_id).await?;
                    change
                        .stopping_node_ids
                        .retain(|stopping| stopping != &node_id);
                    change.stopped_node_ids.push(node_id);
                    continue;
                }
                rpc_before(
                    deadline,
                    client.prepare_stop(StopRequest {
                        node_id: node_id.clone(),
                        process_instance_id: process_instance_id.clone(),
                    }),
                )
                .await?;
                self.store_stop_prepared_node(&node_id, &process_instance_id)
                    .await?;
                change
                    .stopping_node_ids
                    .retain(|stopping| stopping != &node_id);
                change.stop_prepared_node_ids.push(node_id.clone());
            }
            let mut client = match connect_node(&endpoint, deadline).await {
                Ok(client) => client,
                Err(error) if is_absence_status(&error) => {
                    self.store_stopped_node(&node_id).await?;
                    change
                        .stop_prepared_node_ids
                        .retain(|prepared| prepared != &node_id);
                    change.stopped_node_ids.push(node_id);
                    continue;
                }
                Err(error) => return Err(error),
            };
            let actual = match rpc_before(deadline, client.get_process_info(proto::Empty {})).await
            {
                Ok(response) => response.into_inner(),
                Err(error) if is_absence_status(&error) => {
                    self.store_stopped_node(&node_id).await?;
                    change
                        .stop_prepared_node_ids
                        .retain(|prepared| prepared != &node_id);
                    change.stopped_node_ids.push(node_id);
                    continue;
                }
                Err(error) => return Err(error),
            };
            if actual.node_id == node_id && actual.process_instance_id == process_instance_id {
                match rpc_before(
                    deadline,
                    client.stop(StopRequest {
                        node_id: node_id.clone(),
                        process_instance_id,
                    }),
                )
                .await
                {
                    Ok(_) => {}
                    Err(error) if is_absence_status(&error) => {}
                    Err(error) => return Err(error),
                }
            }
            self.store_stopped_node(&node_id).await?;
            change
                .stop_prepared_node_ids
                .retain(|prepared| prepared != &node_id);
            change.stopped_node_ids.push(node_id);
        }
        self.set_phase(MigrationPhase::Complete).await?;
        change.phase = MigrationPhase::Complete;
        Ok(change)
    }

    pub(super) async fn install_on_members(
        &self,
        topology: &TopologySnapshot,
        members: &[Member],
        deadline: Instant,
    ) -> Result<(), Status> {
        self.install_on_members_with_lease(topology, members, deadline, true)
            .await
    }

    pub(super) async fn install_on_members_with_lease(
        &self,
        topology: &TopologySnapshot,
        members: &[Member],
        deadline: Instant,
        require_lease: bool,
    ) -> Result<(), Status> {
        for member in members {
            let mut client = connect_node(&member.endpoint, deadline).await?;
            rpc_before(
                deadline,
                client.install_topology(InstallTopologyRequest {
                    topology: Some(topology.into()),
                    require_lease,
                }),
            )
            .await?;
        }
        Ok(())
    }

    pub(super) async fn set_activation_ready(&self) -> Result<(), Status> {
        let mut state = self.state.write().await;
        let mut next = state.clone();
        let change = next
            .active_change
            .as_mut()
            .ok_or_else(|| Status::failed_precondition("no topology change is active"))?;
        if change.phase != MigrationPhase::Published {
            return Err(Status::failed_precondition(
                "topology is not waiting for owner activation",
            ));
        }
        change.activation_ready = true;
        next.recovery_block_reason.clear();
        self.repository
            .store_state(&next)
            .map_err(|error| Status::internal(error.to_string()))?;
        *state = next;
        Ok(())
    }

    pub(super) async fn reset_prepublication_attempt(
        &self,
        change: &TopologyChange,
    ) -> Result<TopologyChange, Status> {
        self.set_phase(MigrationPhase::Resetting).await?;
        let deadline = Instant::now() + self.migration_timeout;
        self.clear_prepublication_nodes(change, deadline).await?;
        let mut state = self.state.write().await;
        let mut next = state.clone();
        let active = next
            .active_change
            .as_mut()
            .ok_or_else(|| Status::internal("active change disappeared"))?;
        for range in &mut active.ranges {
            range.source_process_instance_id.clear();
            range.destination_process_instance_id.clear();
            range.source_cleaned = false;
            range.snapshot_records = 0;
            range.changelog_watermark = 0;
            range.verified = false;
        }
        active.phase = MigrationPhase::CopyingSnapshot;
        self.repository
            .store_state(&next)
            .map_err(|error| Status::internal(error.to_string()))?;
        let reset = next
            .active_change
            .clone()
            .expect("active change was checked above");
        *state = next;
        Ok(reset)
    }

    pub(super) async fn clear_prepublication_nodes(
        &self,
        change: &TopologyChange,
        deadline: Instant,
    ) -> Result<(), Status> {
        let change_id = &change.change_id;
        try_map_bounded(
            change.ranges.clone(),
            self.range_move_concurrency,
            |range| async move {
                let control = RangeControlRequest {
                    change_id: change_id.clone(),
                    range_id: range.range_id,
                };
                let mut source = connect_node(&range.source_endpoint, deadline).await?;
                rpc_before(deadline, source.abort_range_migration(control.clone())).await?;
                // Destination staging is never client-visible before publication,
                // so inability to discard it is not an ownership safety failure.
                if let Ok(mut destination) =
                    connect_node(&range.destination_endpoint, deadline).await
                {
                    let _ = rpc_before(deadline, destination.abort_range_migration(control)).await;
                }
                Ok::<(), Status>(())
            },
        )
        .await
        .map(|_| ())
    }

    pub(super) async fn abort_prepublication_change(
        &self,
        change: &TopologyChange,
    ) -> Result<(), Status> {
        self.set_phase(MigrationPhase::Aborting).await?;
        self.finish_abort(change).await
    }

    pub(super) async fn finish_abort(&self, change: &TopologyChange) -> Result<(), Status> {
        let deadline = Instant::now() + self.migration_timeout;
        self.clear_prepublication_nodes(change, deadline).await?;
        if change.direct_merge {
            self.set_direct_merge_fence(change, false, true, deadline)
                .await?;
        } else if change.ranges.is_empty() && change.replica_obligations.is_empty() {
            self.policy_fence_members(change, false, deadline).await?;
        }
        self.set_phase(MigrationPhase::Aborted).await
    }

    pub(super) async fn store_stopped_node(&self, node_id: &str) -> Result<(), Status> {
        let mut state = self.state.write().await;
        let mut next = state.clone();
        let change = next
            .active_change
            .as_mut()
            .ok_or_else(|| Status::internal("active change disappeared"))?;
        change
            .stopping_node_ids
            .retain(|stopping| stopping != node_id);
        change
            .stop_prepared_node_ids
            .retain(|prepared| prepared != node_id);
        if !change
            .stopped_node_ids
            .iter()
            .any(|stopped| stopped == node_id)
        {
            change.stopped_node_ids.push(node_id.to_owned());
            change.stopped_node_ids.sort();
        }
        next.process_instances.remove(node_id);
        self.repository
            .store_state(&next)
            .map_err(|error| Status::internal(error.to_string()))?;
        *state = next;
        Ok(())
    }

    pub(super) async fn store_stopping_node(&self, node_id: &str) -> Result<(), Status> {
        let mut state = self.state.write().await;
        let mut next = state.clone();
        let change = next
            .active_change
            .as_mut()
            .ok_or_else(|| Status::internal("active change disappeared"))?;
        if !change
            .stopping_node_ids
            .iter()
            .any(|stopping| stopping == node_id)
        {
            change.stopping_node_ids.push(node_id.to_owned());
            change.stopping_node_ids.sort();
            self.repository
                .store_state(&next)
                .map_err(|error| Status::internal(error.to_string()))?;
            *state = next;
        }
        Ok(())
    }

    pub(super) async fn store_stop_prepared_node(
        &self,
        node_id: &str,
        process_instance_id: &str,
    ) -> Result<(), Status> {
        let mut state = self.state.write().await;
        let mut next = state.clone();
        let change = next
            .active_change
            .as_mut()
            .ok_or_else(|| Status::internal("active change disappeared"))?;
        change
            .stopping_node_ids
            .retain(|stopping| stopping != node_id);
        if !change
            .stop_prepared_node_ids
            .iter()
            .any(|prepared| prepared == node_id)
        {
            change.stop_prepared_node_ids.push(node_id.to_owned());
            change.stop_prepared_node_ids.sort();
        }
        next.stop_confirmations
            .entry(node_id.to_owned())
            .or_default()
            .insert(process_instance_id.to_owned());
        self.repository
            .store_state(&next)
            .map_err(|error| Status::internal(error.to_string()))?;
        *state = next;
        Ok(())
    }

    pub(super) async fn set_phase(&self, phase: MigrationPhase) -> Result<(), Status> {
        let mut state = self.state.write().await;
        let mut next = state.clone();
        let change = next
            .active_change
            .as_mut()
            .ok_or_else(|| Status::internal("active change disappeared"))?;
        change.phase = phase;
        let change_id = change.change_id.clone();
        if phase.is_terminal() {
            next.recovery_block_reason.clear();
        }
        self.repository
            .store_state(&next)
            .map_err(|error| Status::internal(error.to_string()))?;
        *state = next;
        tracing::debug!(%change_id, ?phase, "topology change phase updated");
        Ok(())
    }

    pub(super) async fn store_range_progress(
        &self,
        progress: &RangeMigration,
    ) -> Result<(), Status> {
        let mut state = self.state.write().await;
        let mut next = state.clone();
        let range = next
            .active_change
            .as_mut()
            .and_then(|change| {
                change
                    .ranges
                    .iter_mut()
                    .find(|range| range.range_id == progress.range_id)
            })
            .ok_or_else(|| Status::internal("migration range disappeared"))?;
        *range = progress.clone();
        self.repository
            .store_state(&next)
            .map_err(|error| Status::internal(error.to_string()))?;
        *state = next;
        Ok(())
    }
}
