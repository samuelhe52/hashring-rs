use super::*;

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
    prove_removal_owner_leases(state, target, grants, now)?;
    prove_removal_owner_coverage(state, target)
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
