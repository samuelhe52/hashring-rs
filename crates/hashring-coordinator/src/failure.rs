use super::*;

pub(super) fn token_in_range(start: u64, end: u64, token: u64) -> bool {
    if start < end {
        token > start && token <= end
    } else if start > end {
        token > start || token <= end
    } else {
        true
    }
}

pub(super) fn failure_confirmed(
    state: &ClusterState,
    grants: &BTreeMap<String, NodeLeaseGrant>,
    reports: &BTreeMap<(String, String), PeerFailure>,
    target: &str,
    now: Instant,
) -> bool {
    let has_lease = |node_id: &str| {
        grants.get(node_id).is_some_and(|grant| {
            grant.epoch == state.committed.epoch
                && state.process_instances.get(node_id) == Some(&grant.process_instance_id)
                && grant.expires_at > now
                && !state.fenced_nodes.contains(node_id)
        })
    };
    if !has_lease(target) {
        return true;
    }
    reports
        .iter()
        .filter(|((reported_target, reporter), report)| {
            reported_target == target
                && reporter != reported_target
                && has_lease(reporter)
                && report.first_seen + NODE_LEASE_DURATION <= now
                && report.last_seen + Duration::from_secs(2) >= now
        })
        .count()
        >= 2
}

pub(super) fn prove_failure_coverage(
    state: &ClusterState,
    target: &TopologySnapshot,
    grants: &BTreeMap<String, NodeLeaseGrant>,
    now: Instant,
) -> Result<(), Status> {
    let old_ranges = state
        .committed
        .derived_ranges()
        .map_err(|error| Status::internal(error.to_string()))?;
    let target_ranges = target
        .derived_ranges()
        .map_err(|error| Status::internal(error.to_string()))?;
    for target_range in target_ranges {
        let owner = &target_range.owner_node_id;
        let process_instance_id = state.process_instances.get(owner).ok_or_else(|| {
            Status::failed_precondition(format!("promoted owner {owner} is not registered"))
        })?;
        if state.fenced_nodes.contains(owner)
            || grants.get(owner).is_none_or(|grant| {
                grant.epoch != state.committed.epoch
                    || grant.process_instance_id != *process_instance_id
                    || grant.expires_at <= now
            })
        {
            return Err(Status::failed_precondition(format!(
                "promoted owner {owner} has no current lease"
            )));
        }
        let constituents: Vec<_> = old_ranges
            .iter()
            .filter(|range| {
                token_in_range(
                    target_range.start_exclusive,
                    target_range.end_inclusive,
                    range.end_inclusive,
                )
            })
            .collect();
        if constituents.is_empty() {
            return Err(Status::data_loss("merged range has no old constituents"));
        }
        for old_range in constituents {
            if old_range.owner_node_id == *owner {
                continue;
            }
            if old_range.follower_node_ids.first() != Some(owner)
                || !state.replica_admissions.iter().any(|admission| {
                    admission.epoch == state.committed.epoch
                        && admission.start_exclusive == old_range.start_exclusive
                        && admission.end_inclusive == old_range.end_inclusive
                        && admission.owner_node_id == old_range.owner_node_id
                        && admission.node_id == *owner
                        && admission.process_instance_id == *process_instance_id
                })
            {
                return Err(Status::failed_precondition(format!(
                    "promoted owner {owner} lacks first-successor coverage for ({}, {}]",
                    old_range.start_exclusive, old_range.end_inclusive
                )));
            }
        }
    }
    Ok(())
}

pub(super) async fn read_replication_pair(
    epoch: u64,
    owner: &str,
    follower: &str,
    owner_channel: Channel,
    follower_channel: Channel,
    owner_instance: &str,
    follower_instance: &str,
) -> Option<(u64, u64, u64)> {
    let deadline = Instant::now() + Duration::from_millis(500);
    let mut source = configure_data_node_client(DataNodeClient::new(owner_channel));
    let mut destination = configure_data_node_client(DataNodeClient::new(follower_channel));
    let request = proto::ReplicationProgressRequest {
        topology_epoch: epoch,
        owner_node_id: owner.to_owned(),
        follower_node_id: follower.to_owned(),
    };
    let head = rpc_before(deadline, source.get_replication_progress(request.clone()))
        .await
        .ok()?
        .into_inner();
    let applied = rpc_before(deadline, destination.get_replication_progress(request))
        .await
        .ok()?
        .into_inner();
    if head.process_instance_id != owner_instance
        || applied.process_instance_id != follower_instance
    {
        return None;
    }
    let lag = if applied.stream_sequence >= head.stream_sequence {
        0
    } else {
        let last = head.oldest_unacked_unix_millis;
        if last == 0 {
            return None;
        }
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()?
            .as_millis()
            .saturating_sub(u128::from(last))
            .try_into()
            .unwrap_or(u64::MAX)
    };
    Some((head.stream_sequence, applied.stream_sequence, lag))
}
