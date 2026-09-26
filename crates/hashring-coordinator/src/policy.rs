use super::*;

impl CoordinatorService {
    pub(super) async fn execute_policy_change(
        &self,
        mut change: TopologyChange,
        retry_transient: bool,
    ) -> Result<TopologyChange, Status> {
        let deadline = Instant::now() + self.migration_timeout;
        self.set_phase(MigrationPhase::PausingWrites).await?;
        change.phase = MigrationPhase::PausingWrites;
        if let Err(error) = self.policy_fence_members(&change, true, deadline).await {
            return self
                .abort_after_error(&change, error, retry_transient)
                .await;
        }
        if let Err(error) = self.set_phase(MigrationPhase::Verifying).await {
            return self
                .abort_after_error(&change, error, retry_transient)
                .await;
        }
        change.phase = MigrationPhase::Verifying;
        {
            loop {
                let status = tokio::time::timeout_at(
                    deadline,
                    self.get_replica_status(Request::new(proto::Empty {})),
                )
                .await
                .map_err(|_| Status::deadline_exceeded("policy readiness barrier timed out"))
                .and_then(|response| response.map(Response::into_inner));
                let status = match status {
                    Ok(status) => status,
                    Err(error) => {
                        return self
                            .abort_after_error(&change, error, retry_transient)
                            .await;
                    }
                };
                if policy_readiness_verified(&change.target_topology, &status) {
                    break;
                }
                if Instant::now() >= deadline {
                    return self
                        .abort_after_error(
                            &change,
                            Status::deadline_exceeded(
                                "required followers did not catch up before policy publication",
                            ),
                            false,
                        )
                        .await;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        }
        if let Err(error) = self.set_phase(MigrationPhase::ReadyToPublish).await {
            return self
                .abort_after_error(&change, error, retry_transient)
                .await;
        }
        change.phase = MigrationPhase::ReadyToPublish;
        if !self.pre_publish_delay.is_zero() {
            tokio::time::sleep(self.pre_publish_delay).await;
        }
        // Recheck after the optional pre-publication delay while all old owners are fenced.
        {
            let status = tokio::time::timeout_at(
                deadline,
                self.get_replica_status(Request::new(proto::Empty {})),
            )
            .await
            .map_err(|_| Status::deadline_exceeded("policy readiness recheck timed out"))
            .and_then(|response| response.map(Response::into_inner));
            let status = match status {
                Ok(status) => status,
                Err(error) => {
                    return self
                        .abort_after_error(&change, error, retry_transient)
                        .await;
                }
            };
            if !policy_readiness_verified(&change.target_topology, &status) {
                return self
                    .abort_after_error(
                        &change,
                        Status::failed_precondition("policy readiness was lost before publication"),
                        retry_transient,
                    )
                    .await;
            }
        }
        let publication = {
            let mut state = self.state.write().await;
            let mut next = state.clone();
            (|| -> Result<(), Status> {
                if next
                    .active_change
                    .as_ref()
                    .is_none_or(|active| change_identity(active) != change_identity(&change))
                {
                    return Err(Status::failed_precondition(
                        "policy transition changed before publication",
                    ));
                }
                next.active_change
                    .as_mut()
                    .expect("active transition was checked")
                    .phase = MigrationPhase::Published;
                next.committed = change.target_topology.clone();
                // Identical placements plus the catch-up barrier preserve admission proof.
                for admission in &mut next.replica_admissions {
                    admission.epoch = next.committed.epoch;
                }
                for repair in &mut next.replica_repairs {
                    repair.epoch = next.committed.epoch;
                }
                reconcile_replica_repairs(&mut next)
                    .map_err(|error| Status::internal(error.to_string()))?;
                self.repository
                    .store_state(&next)
                    .map_err(|error| Status::internal(error.to_string()))?;
                *state = next;
                Ok(())
            })()
        };
        if let Err(error) = publication {
            return self
                .abort_after_error(&change, error, retry_transient)
                .await;
        }
        change.phase = MigrationPhase::Published;
        self.finish_published_change(change, deadline).await
    }

    pub(super) async fn policy_fence_members(
        &self,
        change: &TopologyChange,
        pause: bool,
        deadline: Instant,
    ) -> Result<(), Status> {
        let request = PolicyWriteFenceRequest {
            change_id: change.change_id.clone(),
            base_epoch: change.base_epoch,
        };
        let mut first_error = None;
        for member in &change.target_topology.members {
            let result = async {
                let mut client = self.connect_node(&member.endpoint, deadline).await?;
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
}

pub(super) fn policy_readiness_verified(
    topology: &TopologySnapshot,
    status: &proto::ReplicaStatusResponse,
) -> bool {
    if status.topology_epoch != topology.epoch.saturating_sub(1) {
        return false;
    }
    let Ok(ranges) = topology.derived_ranges() else {
        return false;
    };
    let guard = &topology.write_availability_guard;
    ranges.iter().all(|range| {
        let Some(actual) = status.ranges.iter().find(|actual| {
            actual.start_exclusive == range.start_exclusive
                && actual.end_inclusive == range.end_inclusive
                && actual.owner_node_id == range.owner_node_id
        }) else {
            return false;
        };
        let admitted = actual
            .followers
            .iter()
            .filter(|follower| follower.admitted)
            .count() as u32;
        let healthy = actual
            .followers
            .iter()
            .filter(|follower| {
                follower.admitted
                    && follower.lag_known
                    && follower.lag_millis <= guard.max_replica_lag_millis
                    && follower.stream_cursor == follower.stream_head
            })
            .count() as u32;
        if actual.followers.iter().any(|follower| {
            follower.admitted
                && (!follower.lag_known || follower.stream_cursor != follower.stream_head)
        }) {
            return false;
        }
        if admitted.saturating_add(1) < guard.minimum_admitted_copies
            || healthy < guard.minimum_healthy_followers
        {
            return false;
        }
        let needed: Vec<_> = match topology.write_ack_policy {
            WriteAckPolicy::OwnerOnly => Vec::new(),
            WriteAckPolicy::FirstSuccessor => range.follower_node_ids.first().into_iter().collect(),
            WriteAckPolicy::AllReplicas => {
                if range.follower_node_ids.len() + 1 < topology.desired_replication_factor as usize
                {
                    return false;
                }
                range.follower_node_ids.iter().collect()
            }
        };
        needed.into_iter().all(|node_id| {
            actual.followers.iter().any(|follower| {
                &follower.node_id == node_id
                    && follower.admitted
                    && follower.lag_known
                    && follower.lag_millis <= guard.max_replica_lag_millis
                    && follower.stream_cursor == follower.stream_head
            })
        })
    })
}
