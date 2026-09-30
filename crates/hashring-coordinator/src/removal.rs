use super::*;
use hashring_core::topology::DerivedRange;

pub(super) enum RemovalPublication {
    Graceful { admissions: Vec<ReplicaAdmission> },
    Failed,
}

pub(super) fn token_in_range(start: u64, end: u64, token: u64) -> bool {
    if start < end {
        token > start && token <= end
    } else if start > end {
        token > start || token <= end
    } else {
        true
    }
}

pub(super) fn constituents<'a>(
    old: &'a [DerivedRange],
    target: &DerivedRange,
) -> Vec<&'a DerivedRange> {
    old.iter()
        .filter(|range| {
            token_in_range(
                target.start_exclusive,
                target.end_inclusive,
                range.end_inclusive,
            )
        })
        .collect()
}

pub(super) fn admission_for<'a>(
    state: &'a ClusterState,
    range: &DerivedRange,
    follower: &str,
) -> Option<&'a ReplicaAdmission> {
    let instance = state.process_instances.get(follower)?;
    state.replica_admissions.iter().find(|admission| {
        admission.epoch == state.committed.epoch
            && admission.start_exclusive == range.start_exclusive
            && admission.end_inclusive == range.end_inclusive
            && admission.owner_node_id == range.owner_node_id
            && admission.node_id == follower
            && admission.process_instance_id == *instance
            && !state.fenced_nodes.contains(follower)
    })
}

pub(super) fn prove_removal_owner_coverage(
    state: &ClusterState,
    target: &TopologySnapshot,
) -> Result<(), Status> {
    let old_ranges = state
        .committed
        .derived_ranges()
        .map_err(|error| Status::internal(error.to_string()))?;
    let target_ranges = target
        .derived_ranges()
        .map_err(|error| Status::internal(error.to_string()))?;
    for target_range in &target_ranges {
        let owner = &target_range.owner_node_id;
        let parts = constituents(&old_ranges, target_range);
        if parts.is_empty() {
            return Err(Status::data_loss("merged range has no old constituents"));
        }
        for part in parts {
            if part.owner_node_id != *owner
                && (part.follower_node_ids.first() != Some(owner)
                    || admission_for(state, part, owner).is_none())
            {
                return Err(Status::failed_precondition(format!(
                    "merged owner {owner} lacks first-successor coverage for ({}, {}]",
                    part.start_exclusive, part.end_inclusive
                )));
            }
        }
    }
    Ok(())
}

pub(super) fn prove_removal_owner_leases(
    state: &ClusterState,
    target: &TopologySnapshot,
    grants: &BTreeMap<String, NodeLeaseGrant>,
    now: Instant,
) -> Result<(), Status> {
    for range in target
        .derived_ranges()
        .map_err(|error| Status::internal(error.to_string()))?
    {
        let owner = &range.owner_node_id;
        let instance = state.process_instances.get(owner).ok_or_else(|| {
            Status::failed_precondition(format!("merged owner {owner} is not registered"))
        })?;
        if state.fenced_nodes.contains(owner)
            || grants.get(owner).is_none_or(|grant| {
                grant.epoch != state.committed.epoch
                    || grant.process_instance_id != *instance
                    || grant.expires_at <= now
            })
        {
            return Err(Status::failed_precondition(format!(
                "merged owner {owner} has no current lease"
            )));
        }
    }
    Ok(())
}

impl CoordinatorService {
    pub(super) fn publish_removed_topology(
        &self,
        state: &mut ClusterState,
        change: &TopologyChange,
        publication: RemovalPublication,
    ) -> Result<(), Status> {
        if state
            .active_change
            .as_ref()
            .is_none_or(|active| change_identity(active) != change_identity(change))
        {
            return Err(Status::failed_precondition(
                "removal transition changed before publication",
            ));
        }
        let mut next = state.clone();
        next.committed = change.target_topology.clone();
        next.replica_admissions = match publication {
            RemovalPublication::Graceful { admissions } => admissions,
            RemovalPublication::Failed => {
                let node_id = change
                    .failed_node_id
                    .as_ref()
                    .ok_or_else(|| Status::internal("failure transition omitted node identity"))?;
                next.process_instances.remove(node_id);
                Vec::new()
            }
        };
        next.replica_repairs.clear();
        let active = next.active_change.as_mut().expect("checked above");
        active.phase = MigrationPhase::Published;
        active.activation_ready = true;
        reconcile_replica_repairs(&mut next)
            .map_err(|error| Status::internal(error.to_string()))?;
        self.repository
            .store_state(&next)
            .map_err(|error| Status::internal(error.to_string()))?;
        *state = next;
        Ok(())
    }

    pub(super) async fn install_removed_topology_survivors(
        &self,
        change: &TopologyChange,
        deadline: Instant,
    ) -> Result<(), Status> {
        self.install_on_members(
            &change.target_topology,
            &change.target_topology.members,
            deadline,
        )
        .await
    }
}
