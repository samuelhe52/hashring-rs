use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::topology::{Member, TopologyError, TopologySnapshot};

pub const MAX_MIGRATION_RANGES: usize = 16_384;
pub const MAX_REPLICA_OBLIGATIONS: usize = 16_384;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum MigrationPhase {
    Planned,
    Resetting,
    CopyingSnapshot,
    ReplayingChangelog,
    PausingWrites,
    Verifying,
    ReadyToPublish,
    Published,
    CleaningUp,
    Complete,
    Aborting,
    Aborted,
}

impl MigrationPhase {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Complete | Self::Aborted)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RangeMigration {
    pub range_id: String,
    /// Hash interval `(start_exclusive, end_inclusive]`, wrapping at `u64::MAX`.
    pub start_exclusive: u64,
    pub end_inclusive: u64,
    pub source_node_id: String,
    pub destination_node_id: String,
    #[serde(default)]
    pub source_endpoint: String,
    #[serde(default)]
    pub destination_endpoint: String,
    #[serde(default)]
    pub source_process_instance_id: String,
    #[serde(default)]
    pub destination_process_instance_id: String,
    #[serde(default)]
    pub source_cleaned: bool,
    pub snapshot_records: u64,
    pub changelog_watermark: u64,
    pub verified: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReplicaObligation {
    /// Hash interval `(start_exclusive, end_inclusive]`, wrapping at `u64::MAX`.
    pub start_exclusive: u64,
    pub end_inclusive: u64,
    pub source_node_id: String,
    pub destination_node_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TopologyChange {
    pub change_id: String,
    pub base_epoch: u64,
    pub target_topology: TopologySnapshot,
    pub phase: MigrationPhase,
    pub ranges: Vec<RangeMigration>,
    #[serde(default)]
    pub replica_obligations: Vec<ReplicaObligation>,
    #[serde(default)]
    pub stopped_node_ids: Vec<String>,
    #[serde(default)]
    pub stopping_node_ids: Vec<String>,
    #[serde(default)]
    pub stop_prepared_node_ids: Vec<String>,
}

#[derive(Debug, Error)]
pub enum MigrationError {
    #[error("invalid target topology: {0}")]
    Topology(#[from] TopologyError),
    #[error("target membership is identical to committed membership")]
    NoMembershipChange,
    #[error("changing the endpoint of existing node {0} is not supported for in-memory nodes")]
    EndpointChangeUnsupported(String),
    #[error("topology epoch overflow")]
    EpochOverflow,
    #[error("topology change exceeds the {MAX_MIGRATION_RANGES} moving-range limit")]
    TooManyRanges,
    #[error("topology change exceeds the {MAX_REPLICA_OBLIGATIONS} new-follower obligation limit")]
    TooManyReplicaObligations,
    #[error("migration payload omitted target topology")]
    MissingTargetTopology,
    #[error("unknown migration phase: {0}")]
    UnknownPhase(i32),
}

impl TopologyChange {
    pub fn plan(
        committed: &TopologySnapshot,
        target_members: Vec<Member>,
    ) -> Result<Self, MigrationError> {
        for current in &committed.members {
            if let Some(target) = target_members
                .iter()
                .find(|target| target.node_id == current.node_id)
                && target.endpoint != current.endpoint
            {
                return Err(MigrationError::EndpointChangeUnsupported(
                    current.node_id.clone(),
                ));
            }
        }
        let target_epoch = committed
            .epoch
            .checked_add(1)
            .ok_or(MigrationError::EpochOverflow)?;
        let target_topology = TopologySnapshot::new_with_config(
            target_epoch,
            committed.hash_seed,
            committed.virtual_nodes,
            target_members,
            committed.config(),
        )?;
        if target_topology.members == committed.members {
            return Err(MigrationError::NoMembershipChange);
        }

        let (ranges, replica_obligations) = topology_delta(committed, &target_topology)?;
        Ok(Self {
            change_id: uuid::Uuid::new_v4().to_string(),
            base_epoch: committed.epoch,
            ranges,
            replica_obligations,
            target_topology,
            phase: MigrationPhase::Planned,
            stopped_node_ids: Vec::new(),
            stopping_node_ids: Vec::new(),
            stop_prepared_node_ids: Vec::new(),
        })
    }
}

pub fn moving_ranges(
    committed: &TopologySnapshot,
    target: &TopologySnapshot,
) -> Result<Vec<RangeMigration>, MigrationError> {
    Ok(topology_delta(committed, target)?.0)
}

pub fn topology_delta(
    committed: &TopologySnapshot,
    target: &TopologySnapshot,
) -> Result<(Vec<RangeMigration>, Vec<ReplicaObligation>), MigrationError> {
    let boundaries: BTreeSet<_> = committed
        .assignments
        .iter()
        .chain(&target.assignments)
        .map(|assignment| assignment.token)
        .collect();
    if boundaries.is_empty() {
        return Err(TopologyError::NoAssignments.into());
    }

    let ordered: Vec<_> = boundaries.into_iter().collect();
    let mut previous = *ordered
        .last()
        .expect("boundaries were checked as non-empty");
    let mut ranges = Vec::new();
    let mut replica_obligations = Vec::new();
    for end in ordered {
        let committed_replicas = committed.replica_node_ids_for_token(end)?;
        let target_replicas = target.replica_node_ids_for_token(end)?;
        let source_node_id = committed_replicas[0];
        let destination_node_id = target_replicas[0];
        if source_node_id != destination_node_id {
            if ranges.len() == MAX_MIGRATION_RANGES {
                return Err(MigrationError::TooManyRanges);
            }
            let source = committed.owner_for_token(end)?;
            let destination = target.owner_for_token(end)?;
            let range_id = range_id(previous, end, source_node_id, destination_node_id);
            ranges.push(RangeMigration {
                range_id,
                start_exclusive: previous,
                end_inclusive: end,
                source_node_id: source.node_id.clone(),
                destination_node_id: destination.node_id.clone(),
                source_endpoint: source.endpoint.clone(),
                destination_endpoint: destination.endpoint.clone(),
                source_process_instance_id: String::new(),
                destination_process_instance_id: String::new(),
                source_cleaned: false,
                snapshot_records: 0,
                changelog_watermark: 0,
                verified: false,
            });
        }
        for follower_node_id in &target_replicas[1..] {
            if !committed_replicas.contains(follower_node_id) {
                if replica_obligations.len() == MAX_REPLICA_OBLIGATIONS {
                    return Err(MigrationError::TooManyReplicaObligations);
                }
                replica_obligations.push(ReplicaObligation {
                    start_exclusive: previous,
                    end_inclusive: end,
                    source_node_id: source_node_id.to_owned(),
                    destination_node_id: (*follower_node_id).to_owned(),
                });
            }
        }
        previous = end;
    }
    Ok((ranges, replica_obligations))
}

fn range_id(start: u64, end: u64, source: &str, destination: &str) -> String {
    let mut digest = blake3::Hasher::new();
    digest.update(b"hashring-rs:range:v1\0");
    digest.update(&start.to_be_bytes());
    digest.update(&end.to_be_bytes());
    digest.update(&(source.len() as u64).to_be_bytes());
    digest.update(source.as_bytes());
    digest.update(&(destination.len() as u64).to_be_bytes());
    digest.update(destination.as_bytes());
    digest.finalize().to_hex().to_string()
}

impl From<MigrationPhase> for crate::proto::MigrationPhase {
    fn from(phase: MigrationPhase) -> Self {
        match phase {
            MigrationPhase::Planned => Self::Planned,
            MigrationPhase::Resetting => Self::Resetting,
            MigrationPhase::CopyingSnapshot => Self::CopyingSnapshot,
            MigrationPhase::ReplayingChangelog => Self::ReplayingChangelog,
            MigrationPhase::PausingWrites => Self::PausingWrites,
            MigrationPhase::Verifying => Self::Verifying,
            MigrationPhase::ReadyToPublish => Self::ReadyToPublish,
            MigrationPhase::Published => Self::Published,
            MigrationPhase::CleaningUp => Self::CleaningUp,
            MigrationPhase::Complete => Self::Complete,
            MigrationPhase::Aborting => Self::Aborting,
            MigrationPhase::Aborted => Self::Aborted,
        }
    }
}

impl TryFrom<i32> for MigrationPhase {
    type Error = MigrationError;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        let phase = crate::proto::MigrationPhase::try_from(value)
            .map_err(|_| MigrationError::UnknownPhase(value))?;
        match phase {
            crate::proto::MigrationPhase::Unspecified => Err(MigrationError::UnknownPhase(value)),
            crate::proto::MigrationPhase::Planned => Ok(Self::Planned),
            crate::proto::MigrationPhase::Resetting => Ok(Self::Resetting),
            crate::proto::MigrationPhase::CopyingSnapshot => Ok(Self::CopyingSnapshot),
            crate::proto::MigrationPhase::ReplayingChangelog => Ok(Self::ReplayingChangelog),
            crate::proto::MigrationPhase::PausingWrites => Ok(Self::PausingWrites),
            crate::proto::MigrationPhase::Verifying => Ok(Self::Verifying),
            crate::proto::MigrationPhase::ReadyToPublish => Ok(Self::ReadyToPublish),
            crate::proto::MigrationPhase::Published => Ok(Self::Published),
            crate::proto::MigrationPhase::CleaningUp => Ok(Self::CleaningUp),
            crate::proto::MigrationPhase::Complete => Ok(Self::Complete),
            crate::proto::MigrationPhase::Aborting => Ok(Self::Aborting),
            crate::proto::MigrationPhase::Aborted => Ok(Self::Aborted),
        }
    }
}

impl From<&RangeMigration> for crate::proto::RangeMigration {
    fn from(range: &RangeMigration) -> Self {
        Self {
            range_id: range.range_id.clone(),
            start_exclusive: range.start_exclusive,
            end_inclusive: range.end_inclusive,
            source_node_id: range.source_node_id.clone(),
            destination_node_id: range.destination_node_id.clone(),
            source_endpoint: range.source_endpoint.clone(),
            destination_endpoint: range.destination_endpoint.clone(),
            source_process_instance_id: range.source_process_instance_id.clone(),
            destination_process_instance_id: range.destination_process_instance_id.clone(),
            source_cleaned: range.source_cleaned,
            snapshot_records: range.snapshot_records,
            changelog_watermark: range.changelog_watermark,
            verified: range.verified,
        }
    }
}

impl From<&ReplicaObligation> for crate::proto::ReplicaObligation {
    fn from(obligation: &ReplicaObligation) -> Self {
        Self {
            start_exclusive: obligation.start_exclusive,
            end_inclusive: obligation.end_inclusive,
            source_node_id: obligation.source_node_id.clone(),
            destination_node_id: obligation.destination_node_id.clone(),
        }
    }
}

impl From<crate::proto::ReplicaObligation> for ReplicaObligation {
    fn from(obligation: crate::proto::ReplicaObligation) -> Self {
        Self {
            start_exclusive: obligation.start_exclusive,
            end_inclusive: obligation.end_inclusive,
            source_node_id: obligation.source_node_id,
            destination_node_id: obligation.destination_node_id,
        }
    }
}

impl From<crate::proto::RangeMigration> for RangeMigration {
    fn from(range: crate::proto::RangeMigration) -> Self {
        Self {
            range_id: range.range_id,
            start_exclusive: range.start_exclusive,
            end_inclusive: range.end_inclusive,
            source_node_id: range.source_node_id,
            destination_node_id: range.destination_node_id,
            source_endpoint: range.source_endpoint,
            destination_endpoint: range.destination_endpoint,
            source_process_instance_id: range.source_process_instance_id,
            destination_process_instance_id: range.destination_process_instance_id,
            source_cleaned: range.source_cleaned,
            snapshot_records: range.snapshot_records,
            changelog_watermark: range.changelog_watermark,
            verified: range.verified,
        }
    }
}

impl From<&TopologyChange> for crate::proto::TopologyChangeSnapshot {
    fn from(change: &TopologyChange) -> Self {
        Self {
            change_id: change.change_id.clone(),
            base_epoch: change.base_epoch,
            target_topology: Some((&change.target_topology).into()),
            phase: crate::proto::MigrationPhase::from(change.phase).into(),
            ranges: change.ranges.iter().map(Into::into).collect(),
            replica_obligations: change.replica_obligations.iter().map(Into::into).collect(),
            stopped_node_ids: change.stopped_node_ids.clone(),
            stopping_node_ids: change.stopping_node_ids.clone(),
            stop_prepared_node_ids: change.stop_prepared_node_ids.clone(),
        }
    }
}

impl TryFrom<crate::proto::TopologyChangeSnapshot> for TopologyChange {
    type Error = MigrationError;

    fn try_from(change: crate::proto::TopologyChangeSnapshot) -> Result<Self, Self::Error> {
        Ok(Self {
            change_id: change.change_id,
            base_epoch: change.base_epoch,
            target_topology: change
                .target_topology
                .ok_or(MigrationError::MissingTargetTopology)?
                .try_into()?,
            phase: change.phase.try_into()?,
            ranges: change.ranges.into_iter().map(Into::into).collect(),
            replica_obligations: change
                .replica_obligations
                .into_iter()
                .map(Into::into)
                .collect(),
            stopped_node_ids: change.stopped_node_ids,
            stopping_node_ids: change.stopping_node_ids,
            stop_prepared_node_ids: change.stop_prepared_node_ids,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn member(id: &str, port: u16) -> Member {
        Member {
            node_id: id.into(),
            endpoint: format!("http://127.0.0.1:{port}"),
        }
    }

    #[test]
    fn scale_out_plan_moves_only_ranges_with_changed_owners() {
        let committed = TopologySnapshot::new(1, 42, 16, vec![member("a", 1)]).unwrap();
        let change =
            TopologyChange::plan(&committed, vec![member("a", 1), member("b", 2)]).unwrap();

        assert_eq!(change.base_epoch, 1);
        assert_eq!(change.target_topology.epoch, 2);
        assert_eq!(change.phase, MigrationPhase::Planned);
        assert!(!change.ranges.is_empty());
        assert!(
            change
                .ranges
                .iter()
                .all(|range| { range.source_node_id == "a" && range.destination_node_id == "b" })
        );
        assert!(
            change
                .replica_obligations
                .iter()
                .all(|obligation| obligation.source_node_id == "a"
                    && obligation.destination_node_id == "b")
        );
        assert!(!change.replica_obligations.is_empty());
    }

    #[test]
    fn topology_delta_reports_only_new_follower_coverage() {
        let committed =
            TopologySnapshot::new(1, 42, 16, vec![member("a", 1), member("b", 2)]).unwrap();
        let target = TopologySnapshot::new_with_config(
            2,
            42,
            16,
            vec![member("a", 1), member("b", 2), member("c", 3)],
            committed.config(),
        )
        .unwrap();

        let (_, obligations) = topology_delta(&committed, &target).unwrap();
        assert!(!obligations.is_empty());
        for obligation in obligations {
            let old = committed
                .replica_node_ids_for_token(obligation.end_inclusive)
                .unwrap();
            let new = target
                .replica_node_ids_for_token(obligation.end_inclusive)
                .unwrap();
            assert!(!old.contains(&obligation.destination_node_id.as_str()));
            assert!(new[1..].contains(&obligation.destination_node_id.as_str()));
            assert_eq!(old[0], obligation.source_node_id);
        }
    }

    #[test]
    fn identical_membership_is_not_a_change() {
        let committed = TopologySnapshot::new(1, 42, 16, vec![member("a", 1)]).unwrap();
        assert!(matches!(
            TopologyChange::plan(&committed, committed.members.clone()),
            Err(MigrationError::NoMembershipChange)
        ));
    }

    #[test]
    fn endpoint_only_replacement_is_rejected() {
        let committed = TopologySnapshot::new(1, 42, 16, vec![member("a", 1)]).unwrap();
        assert!(matches!(
            TopologyChange::plan(&committed, vec![member("a", 2)]),
            Err(MigrationError::EndpointChangeUnsupported(node)) if node == "a"
        ));
    }
}
