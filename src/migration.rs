use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::topology::{Member, TopologyError, TopologySnapshot};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum MigrationPhase {
    Planned,
    CopyingSnapshot,
    ReplayingChangelog,
    PausingWrites,
    Verifying,
    ReadyToPublish,
    Published,
    CleaningUp,
    Complete,
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
    pub snapshot_records: u64,
    pub changelog_watermark: u64,
    pub verified: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TopologyChange {
    pub change_id: String,
    pub base_epoch: u64,
    pub target_topology: TopologySnapshot,
    pub phase: MigrationPhase,
    pub ranges: Vec<RangeMigration>,
}

#[derive(Debug, Error)]
pub enum MigrationError {
    #[error("invalid target topology: {0}")]
    Topology(#[from] TopologyError),
    #[error("target membership is identical to committed membership")]
    NoMembershipChange,
    #[error("topology epoch overflow")]
    EpochOverflow,
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
        let target_epoch = committed
            .epoch
            .checked_add(1)
            .ok_or(MigrationError::EpochOverflow)?;
        let target_topology = TopologySnapshot::new(
            target_epoch,
            committed.hash_seed,
            committed.virtual_nodes,
            target_members,
        )?;
        if target_topology.members == committed.members {
            return Err(MigrationError::NoMembershipChange);
        }

        Ok(Self {
            change_id: uuid::Uuid::new_v4().to_string(),
            base_epoch: committed.epoch,
            ranges: moving_ranges(committed, &target_topology)?,
            target_topology,
            phase: MigrationPhase::Planned,
        })
    }
}

pub fn moving_ranges(
    committed: &TopologySnapshot,
    target: &TopologySnapshot,
) -> Result<Vec<RangeMigration>, TopologyError> {
    let boundaries: BTreeSet<_> = committed
        .assignments
        .iter()
        .chain(&target.assignments)
        .map(|assignment| assignment.token)
        .collect();
    if boundaries.is_empty() {
        return Err(TopologyError::NoAssignments);
    }

    let ordered: Vec<_> = boundaries.into_iter().collect();
    let mut previous = *ordered
        .last()
        .expect("boundaries were checked as non-empty");
    let mut ranges = Vec::new();
    for end in ordered {
        let source = committed.owner_for_token(end)?;
        let destination = target.owner_for_token(end)?;
        if source.node_id != destination.node_id {
            let range_id = range_id(previous, end, &source.node_id, &destination.node_id);
            ranges.push(RangeMigration {
                range_id,
                start_exclusive: previous,
                end_inclusive: end,
                source_node_id: source.node_id.clone(),
                destination_node_id: destination.node_id.clone(),
                snapshot_records: 0,
                changelog_watermark: 0,
                verified: false,
            });
        }
        previous = end;
    }
    Ok(ranges)
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
            MigrationPhase::CopyingSnapshot => Self::CopyingSnapshot,
            MigrationPhase::ReplayingChangelog => Self::ReplayingChangelog,
            MigrationPhase::PausingWrites => Self::PausingWrites,
            MigrationPhase::Verifying => Self::Verifying,
            MigrationPhase::ReadyToPublish => Self::ReadyToPublish,
            MigrationPhase::Published => Self::Published,
            MigrationPhase::CleaningUp => Self::CleaningUp,
            MigrationPhase::Complete => Self::Complete,
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
            crate::proto::MigrationPhase::CopyingSnapshot => Ok(Self::CopyingSnapshot),
            crate::proto::MigrationPhase::ReplayingChangelog => Ok(Self::ReplayingChangelog),
            crate::proto::MigrationPhase::PausingWrites => Ok(Self::PausingWrites),
            crate::proto::MigrationPhase::Verifying => Ok(Self::Verifying),
            crate::proto::MigrationPhase::ReadyToPublish => Ok(Self::ReadyToPublish),
            crate::proto::MigrationPhase::Published => Ok(Self::Published),
            crate::proto::MigrationPhase::CleaningUp => Ok(Self::CleaningUp),
            crate::proto::MigrationPhase::Complete => Ok(Self::Complete),
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
            snapshot_records: range.snapshot_records,
            changelog_watermark: range.changelog_watermark,
            verified: range.verified,
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
    }

    #[test]
    fn identical_membership_is_not_a_change() {
        let committed = TopologySnapshot::new(1, 42, 16, vec![member("a", 1)]).unwrap();
        assert!(matches!(
            TopologyChange::plan(&committed, committed.members.clone()),
            Err(MigrationError::NoMembershipChange)
        ));
    }
}
