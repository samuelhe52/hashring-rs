use std::collections::HashSet;

use serde::{Deserialize, Serialize};
use thiserror::Error;
use xxhash_rust::xxh3::xxh3_64_with_seed;

pub const HASH_ALGORITHM: &str = "xxh3-64";
pub const ENCODING_VERSION: u32 = 2;
pub const MAX_MEMBERS: usize = 1_024;
pub const MAX_VIRTUAL_NODES: u32 = 4_096;
pub const MAX_TOKEN_ASSIGNMENTS: usize = 1_048_576;
pub const MAX_NODE_ID_BYTES: usize = 32;
pub const MAX_ENDPOINT_BYTES: usize = 256;
pub const DEFAULT_DESIRED_REPLICATION_FACTOR: u32 = 3;
pub const DEFAULT_MINIMUM_ADMITTED_COPIES: u32 = 1;
pub const DEFAULT_MINIMUM_HEALTHY_FOLLOWERS: u32 = 0;
pub const DEFAULT_MAX_REPLICA_LAG_MILLIS: u64 = 5_000;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum WriteAckPolicy {
    #[default]
    OwnerOnly,
    FirstSuccessor,
    AllReplicas,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WriteAvailabilityGuard {
    pub minimum_admitted_copies: u32,
    pub minimum_healthy_followers: u32,
    pub max_replica_lag_millis: u64,
}

impl Default for WriteAvailabilityGuard {
    fn default() -> Self {
        Self {
            minimum_admitted_copies: DEFAULT_MINIMUM_ADMITTED_COPIES,
            minimum_healthy_followers: DEFAULT_MINIMUM_HEALTHY_FOLLOWERS,
            max_replica_lag_millis: DEFAULT_MAX_REPLICA_LAG_MILLIS,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TopologyConfig {
    pub desired_replication_factor: u32,
    pub write_ack_policy: WriteAckPolicy,
    pub write_availability_guard: WriteAvailabilityGuard,
}

impl Default for TopologyConfig {
    fn default() -> Self {
        Self {
            desired_replication_factor: DEFAULT_DESIRED_REPLICATION_FACTOR,
            write_ack_policy: WriteAckPolicy::OwnerOnly,
            write_availability_guard: WriteAvailabilityGuard::default(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Member {
    pub node_id: String,
    pub endpoint: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TokenAssignment {
    pub token: u64,
    pub node_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DerivedRange {
    /// Hash interval `(start_exclusive, end_inclusive]`, wrapping at `u64::MAX`.
    pub start_exclusive: u64,
    pub end_inclusive: u64,
    pub owner_node_id: String,
    pub follower_node_ids: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TopologySnapshot {
    pub epoch: u64,
    pub hash_seed: u64,
    pub hash_algorithm: String,
    pub encoding_version: u32,
    pub virtual_nodes: u32,
    pub desired_replication_factor: u32,
    pub write_ack_policy: WriteAckPolicy,
    pub write_availability_guard: WriteAvailabilityGuard,
    pub members: Vec<Member>,
    pub assignments: Vec<TokenAssignment>,
    pub digest: String,
}

#[derive(Debug, Error)]
pub enum TopologyError {
    #[error("a topology must contain at least one member")]
    NoMembers,
    #[error("virtual node count must be greater than zero")]
    NoVirtualNodes,
    #[error("member count exceeds {MAX_MEMBERS}")]
    TooManyMembers,
    #[error("virtual node count exceeds {MAX_VIRTUAL_NODES}")]
    TooManyVirtualNodes,
    #[error("token assignment count exceeds {MAX_TOKEN_ASSIGNMENTS}")]
    TooManyAssignments,
    #[error("node id must not be empty")]
    EmptyNodeId,
    #[error("node id exceeds {MAX_NODE_ID_BYTES} bytes")]
    NodeIdTooLong,
    #[error("endpoint must not be empty")]
    EmptyEndpoint,
    #[error("endpoint exceeds {MAX_ENDPOINT_BYTES} bytes")]
    EndpointTooLong,
    #[error("duplicate node id: {0}")]
    DuplicateNodeId(String),
    #[error("duplicate endpoint: {0}")]
    DuplicateEndpoint(String),
    #[error("topology has no token assignments")]
    NoAssignments,
    #[error("unsupported hash configuration: {algorithm}, encoding version {encoding_version}")]
    UnsupportedHashConfiguration {
        algorithm: String,
        encoding_version: u32,
    },
    #[error("topology digest mismatch")]
    DigestMismatch,
    #[error("members or token assignments are not in canonical form")]
    NonCanonical,
    #[error("token assignment references unknown node: {0}")]
    UnknownAssignedNode(String),
    #[error("desired replication factor must be greater than zero")]
    NoReplicas,
    #[error("desired replication factor exceeds {MAX_MEMBERS}")]
    TooManyReplicas,
    #[error("FirstSuccessor requires a desired replication factor of at least two")]
    FirstSuccessorRequiresFollower,
    #[error("minimum admitted copies must be greater than zero")]
    NoMinimumAdmittedCopies,
    #[error("maximum replica lag must be greater than zero")]
    NoMaximumReplicaLag,
    #[error("minimum admitted copies exceeds the desired replication factor")]
    MinimumAdmittedCopiesExceedReplicationFactor,
    #[error("minimum healthy followers exceeds the desired follower count")]
    MinimumHealthyFollowersExceedReplicationFactor,
    #[error("unknown write acknowledgement policy: {0}")]
    UnknownWriteAckPolicy(i32),
}

impl TopologySnapshot {
    pub fn new(
        epoch: u64,
        hash_seed: u64,
        virtual_nodes: u32,
        members: Vec<Member>,
    ) -> Result<Self, TopologyError> {
        Self::new_with_config(
            epoch,
            hash_seed,
            virtual_nodes,
            members,
            TopologyConfig::default(),
        )
    }

    pub fn new_with_config(
        epoch: u64,
        hash_seed: u64,
        virtual_nodes: u32,
        mut members: Vec<Member>,
        config: TopologyConfig,
    ) -> Result<Self, TopologyError> {
        validate_members(&members, virtual_nodes)?;
        validate_config(&config)?;
        members.sort_by(|left, right| left.node_id.cmp(&right.node_id));

        let assignment_count = members
            .len()
            .checked_mul(virtual_nodes as usize)
            .ok_or(TopologyError::TooManyAssignments)?;
        let mut assignments = Vec::with_capacity(assignment_count);
        for member in &members {
            for vnode in 0..virtual_nodes {
                let mut encoded = Vec::with_capacity(member.node_id.len() + 24);
                encoded.extend_from_slice(b"hashring-rs:vnode:v1\0");
                encoded.extend_from_slice(&(member.node_id.len() as u64).to_be_bytes());
                encoded.extend_from_slice(member.node_id.as_bytes());
                encoded.extend_from_slice(&vnode.to_be_bytes());
                assignments.push(TokenAssignment {
                    token: xxh3_64_with_seed(&encoded, hash_seed),
                    node_id: member.node_id.clone(),
                });
            }
        }
        assignments.sort_by(|left, right| {
            left.token
                .cmp(&right.token)
                .then_with(|| left.node_id.cmp(&right.node_id))
        });

        let mut snapshot = Self {
            epoch,
            hash_seed,
            hash_algorithm: HASH_ALGORITHM.to_owned(),
            encoding_version: ENCODING_VERSION,
            virtual_nodes,
            desired_replication_factor: config.desired_replication_factor,
            write_ack_policy: config.write_ack_policy,
            write_availability_guard: config.write_availability_guard,
            members,
            assignments,
            digest: String::new(),
        };
        snapshot.digest = snapshot.calculate_digest();
        Ok(snapshot)
    }

    pub fn validate(&self) -> Result<(), TopologyError> {
        validate_members(&self.members, self.virtual_nodes)?;
        validate_config(&self.config())?;
        if self.hash_algorithm != HASH_ALGORITHM || self.encoding_version != ENCODING_VERSION {
            return Err(TopologyError::UnsupportedHashConfiguration {
                algorithm: self.hash_algorithm.clone(),
                encoding_version: self.encoding_version,
            });
        }
        if self.assignments.is_empty() {
            return Err(TopologyError::NoAssignments);
        }
        let member_ids: HashSet<_> = self.members.iter().map(|member| &member.node_id).collect();
        for assignment in &self.assignments {
            if !member_ids.contains(&assignment.node_id) {
                return Err(TopologyError::UnknownAssignedNode(
                    assignment.node_id.clone(),
                ));
            }
        }
        let canonical = Self::new_with_config(
            self.epoch,
            self.hash_seed,
            self.virtual_nodes,
            self.members.clone(),
            self.config(),
        )?;
        if canonical.digest != self.digest {
            return Err(TopologyError::DigestMismatch);
        }
        if canonical.members != self.members || canonical.assignments != self.assignments {
            return Err(TopologyError::NonCanonical);
        }
        Ok(())
    }

    pub fn key_token(&self, key: &[u8]) -> u64 {
        xxh3_64_with_seed(key, self.hash_seed)
    }

    pub fn owner(&self, key: &[u8]) -> Result<&Member, TopologyError> {
        self.owner_for_token(self.key_token(key))
    }

    pub fn owner_for_token(&self, token: u64) -> Result<&Member, TopologyError> {
        let index = self
            .assignments
            .partition_point(|assignment| assignment.token < token);
        let assignment = &self.assignments[index % self.assignments.len()];
        self.members
            .iter()
            .find(|member| member.node_id == assignment.node_id)
            .ok_or_else(|| TopologyError::UnknownAssignedNode(assignment.node_id.clone()))
    }

    pub fn config(&self) -> TopologyConfig {
        TopologyConfig {
            desired_replication_factor: self.desired_replication_factor,
            write_ack_policy: self.write_ack_policy,
            write_availability_guard: self.write_availability_guard.clone(),
        }
    }

    pub fn replica_node_ids_for_token(&self, token: u64) -> Result<Vec<&str>, TopologyError> {
        let owner_index = self
            .assignments
            .partition_point(|assignment| assignment.token < token)
            % self.assignments.len();
        let target_count = usize::min(self.desired_replication_factor as usize, self.members.len());
        let mut seen = HashSet::with_capacity(target_count);
        let mut replicas = Vec::with_capacity(target_count);
        for offset in 0..self.assignments.len() {
            let assignment = &self.assignments[(owner_index + offset) % self.assignments.len()];
            if seen.insert(assignment.node_id.as_str()) {
                replicas.push(assignment.node_id.as_str());
                if replicas.len() == target_count {
                    break;
                }
            }
        }
        if replicas.is_empty() {
            return Err(TopologyError::NoAssignments);
        }
        Ok(replicas)
    }

    pub fn derived_ranges(&self) -> Result<Vec<DerivedRange>, TopologyError> {
        let mut boundaries: Vec<_> = self
            .assignments
            .iter()
            .map(|assignment| assignment.token)
            .collect();
        boundaries.dedup();
        let mut previous = *boundaries.last().ok_or(TopologyError::NoAssignments)?;
        let mut ranges = Vec::with_capacity(boundaries.len());
        for end in boundaries {
            let replicas = self.replica_node_ids_for_token(end)?;
            ranges.push(DerivedRange {
                start_exclusive: previous,
                end_inclusive: end,
                owner_node_id: replicas[0].to_owned(),
                follower_node_ids: replicas[1..]
                    .iter()
                    .map(|node_id| (*node_id).to_owned())
                    .collect(),
            });
            previous = end;
        }
        Ok(ranges)
    }

    fn calculate_digest(&self) -> String {
        let mut unsigned = self.clone();
        unsigned.digest.clear();
        let canonical =
            serde_json::to_vec(&unsigned).expect("serializing an in-memory topology cannot fail");
        blake3::hash(&canonical).to_hex().to_string()
    }
}

fn validate_config(config: &TopologyConfig) -> Result<(), TopologyError> {
    if config.desired_replication_factor == 0 {
        return Err(TopologyError::NoReplicas);
    }
    if config.desired_replication_factor as usize > MAX_MEMBERS {
        return Err(TopologyError::TooManyReplicas);
    }
    if config.write_ack_policy == WriteAckPolicy::FirstSuccessor
        && config.desired_replication_factor < 2
    {
        return Err(TopologyError::FirstSuccessorRequiresFollower);
    }
    if config.write_availability_guard.minimum_admitted_copies == 0 {
        return Err(TopologyError::NoMinimumAdmittedCopies);
    }
    if config.write_availability_guard.max_replica_lag_millis == 0 {
        return Err(TopologyError::NoMaximumReplicaLag);
    }
    let guard = &config.write_availability_guard;
    // Before the lenient default, RF=1 with the 2/1 default guard was a
    // supported read-only topology. Keep stored snapshots loadable.
    let legacy_rf_one_read_only = config.desired_replication_factor == 1
        && guard.minimum_admitted_copies == 2
        && guard.minimum_healthy_followers == 1;
    if !legacy_rf_one_read_only && guard.minimum_admitted_copies > config.desired_replication_factor
    {
        return Err(TopologyError::MinimumAdmittedCopiesExceedReplicationFactor);
    }
    if !legacy_rf_one_read_only
        && guard.minimum_healthy_followers > config.desired_replication_factor.saturating_sub(1)
    {
        return Err(TopologyError::MinimumHealthyFollowersExceedReplicationFactor);
    }
    Ok(())
}

fn validate_members(members: &[Member], virtual_nodes: u32) -> Result<(), TopologyError> {
    if members.is_empty() {
        return Err(TopologyError::NoMembers);
    }
    if virtual_nodes == 0 {
        return Err(TopologyError::NoVirtualNodes);
    }
    if members.len() > MAX_MEMBERS {
        return Err(TopologyError::TooManyMembers);
    }
    if virtual_nodes > MAX_VIRTUAL_NODES {
        return Err(TopologyError::TooManyVirtualNodes);
    }
    if members
        .len()
        .checked_mul(virtual_nodes as usize)
        .is_none_or(|assignments| assignments > MAX_TOKEN_ASSIGNMENTS)
    {
        return Err(TopologyError::TooManyAssignments);
    }
    let mut node_ids = HashSet::new();
    let mut endpoints = HashSet::new();
    for member in members {
        if member.node_id.is_empty() {
            return Err(TopologyError::EmptyNodeId);
        }
        if member.node_id.len() > MAX_NODE_ID_BYTES {
            return Err(TopologyError::NodeIdTooLong);
        }
        if member.endpoint.is_empty() {
            return Err(TopologyError::EmptyEndpoint);
        }
        if member.endpoint.len() > MAX_ENDPOINT_BYTES {
            return Err(TopologyError::EndpointTooLong);
        }
        if !node_ids.insert(&member.node_id) {
            return Err(TopologyError::DuplicateNodeId(member.node_id.clone()));
        }
        if !endpoints.insert(&member.endpoint) {
            return Err(TopologyError::DuplicateEndpoint(member.endpoint.clone()));
        }
    }
    Ok(())
}

impl From<&Member> for crate::proto::Member {
    fn from(member: &Member) -> Self {
        Self {
            node_id: member.node_id.clone(),
            endpoint: member.endpoint.clone(),
        }
    }
}

impl From<&TopologySnapshot> for crate::proto::TopologySnapshot {
    fn from(snapshot: &TopologySnapshot) -> Self {
        Self {
            epoch: snapshot.epoch,
            hash_seed: snapshot.hash_seed,
            hash_algorithm: snapshot.hash_algorithm.clone(),
            encoding_version: snapshot.encoding_version,
            virtual_nodes: snapshot.virtual_nodes,
            desired_replication_factor: snapshot.desired_replication_factor,
            write_ack_policy: crate::proto::WriteAckPolicy::from(snapshot.write_ack_policy).into(),
            write_availability_guard: Some((&snapshot.write_availability_guard).into()),
            members: snapshot.members.iter().map(Into::into).collect(),
            assignments: snapshot
                .assignments
                .iter()
                .map(|assignment| crate::proto::TokenAssignment {
                    token: assignment.token,
                    node_id: assignment.node_id.clone(),
                })
                .collect(),
            digest: snapshot.digest.clone(),
        }
    }
}

impl From<WriteAckPolicy> for crate::proto::WriteAckPolicy {
    fn from(policy: WriteAckPolicy) -> Self {
        match policy {
            WriteAckPolicy::OwnerOnly => Self::OwnerOnly,
            WriteAckPolicy::FirstSuccessor => Self::FirstSuccessor,
            WriteAckPolicy::AllReplicas => Self::AllReplicas,
        }
    }
}

impl TryFrom<i32> for WriteAckPolicy {
    type Error = TopologyError;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match crate::proto::WriteAckPolicy::try_from(value)
            .map_err(|_| TopologyError::UnknownWriteAckPolicy(value))?
        {
            crate::proto::WriteAckPolicy::Unspecified => {
                Err(TopologyError::UnknownWriteAckPolicy(value))
            }
            crate::proto::WriteAckPolicy::OwnerOnly => Ok(Self::OwnerOnly),
            crate::proto::WriteAckPolicy::FirstSuccessor => Ok(Self::FirstSuccessor),
            crate::proto::WriteAckPolicy::AllReplicas => Ok(Self::AllReplicas),
        }
    }
}

impl From<&WriteAvailabilityGuard> for crate::proto::WriteAvailabilityGuard {
    fn from(guard: &WriteAvailabilityGuard) -> Self {
        Self {
            minimum_admitted_copies: guard.minimum_admitted_copies,
            minimum_healthy_followers: guard.minimum_healthy_followers,
            max_replica_lag_millis: guard.max_replica_lag_millis,
        }
    }
}

impl TryFrom<crate::proto::TopologySnapshot> for TopologySnapshot {
    type Error = TopologyError;

    fn try_from(snapshot: crate::proto::TopologySnapshot) -> Result<Self, Self::Error> {
        let snapshot = Self {
            epoch: snapshot.epoch,
            hash_seed: snapshot.hash_seed,
            hash_algorithm: snapshot.hash_algorithm,
            encoding_version: snapshot.encoding_version,
            virtual_nodes: snapshot.virtual_nodes,
            desired_replication_factor: snapshot.desired_replication_factor,
            write_ack_policy: snapshot.write_ack_policy.try_into()?,
            write_availability_guard: snapshot
                .write_availability_guard
                .map(|guard| WriteAvailabilityGuard {
                    minimum_admitted_copies: guard.minimum_admitted_copies,
                    minimum_healthy_followers: guard.minimum_healthy_followers,
                    max_replica_lag_millis: guard.max_replica_lag_millis,
                })
                .ok_or(TopologyError::NoMinimumAdmittedCopies)?,
            members: snapshot
                .members
                .into_iter()
                .map(|member| Member {
                    node_id: member.node_id,
                    endpoint: member.endpoint,
                })
                .collect(),
            assignments: snapshot
                .assignments
                .into_iter()
                .map(|assignment| TokenAssignment {
                    token: assignment.token,
                    node_id: assignment.node_id,
                })
                .collect(),
            digest: snapshot.digest,
        };
        snapshot.validate()?;
        Ok(snapshot)
    }
}

#[cfg(test)]
mod tests {
    use prost::Message;

    use super::*;

    fn members() -> Vec<Member> {
        vec![
            Member {
                node_id: "node-b".into(),
                endpoint: "http://127.0.0.1:5002".into(),
            },
            Member {
                node_id: "node-a".into(),
                endpoint: "http://127.0.0.1:5001".into(),
            },
        ]
    }

    #[test]
    fn construction_is_canonical() {
        let first = TopologySnapshot::new(1, 42, 16, members()).unwrap();
        let mut reversed = members();
        reversed.reverse();
        let second = TopologySnapshot::new(1, 42, 16, reversed).unwrap();
        assert_eq!(first, second);
        assert_eq!(first.assignments.len(), 32);
        first.validate().unwrap();
    }

    #[test]
    fn every_key_has_a_member_owner() {
        let topology = TopologySnapshot::new(1, 42, 16, members()).unwrap();
        for key in 0_u64..10_000 {
            let owner = topology.owner(&key.to_be_bytes()).unwrap();
            assert!(owner.node_id == "node-a" || owner.node_id == "node-b");
        }
    }

    fn configured_topology(
        desired_replication_factor: u32,
        write_ack_policy: WriteAckPolicy,
    ) -> TopologySnapshot {
        TopologySnapshot::new_with_config(
            1,
            42,
            16,
            vec![
                Member {
                    node_id: "node-a".into(),
                    endpoint: "http://127.0.0.1:5001".into(),
                },
                Member {
                    node_id: "node-b".into(),
                    endpoint: "http://127.0.0.1:5002".into(),
                },
                Member {
                    node_id: "node-c".into(),
                    endpoint: "http://127.0.0.1:5003".into(),
                },
            ],
            TopologyConfig {
                desired_replication_factor,
                write_ack_policy,
                write_availability_guard: WriteAvailabilityGuard::default(),
            },
        )
        .unwrap()
    }

    #[test]
    fn derived_ranges_cover_wraparound_and_skip_repeated_nodes() {
        let topology = configured_topology(3, WriteAckPolicy::OwnerOnly);
        let ranges = topology.derived_ranges().unwrap();

        assert_eq!(ranges.len(), topology.assignments.len());
        assert_eq!(
            ranges[0].start_exclusive,
            topology.assignments.last().unwrap().token
        );
        assert_eq!(ranges[0].end_inclusive, topology.assignments[0].token);
        for range in ranges {
            let replicas = topology
                .replica_node_ids_for_token(range.end_inclusive)
                .unwrap();
            assert_eq!(replicas[0], range.owner_node_id);
            assert_eq!(replicas[1..], range.follower_node_ids);
            assert_eq!(replicas.len(), 3);
            assert_eq!(replicas.iter().copied().collect::<HashSet<_>>().len(), 3);
        }
    }

    #[test]
    fn desired_replication_factor_above_membership_is_visibly_under_replicated() {
        let topology = TopologySnapshot::new_with_config(
            1,
            42,
            16,
            members(),
            TopologyConfig {
                desired_replication_factor: 5,
                ..TopologyConfig::default()
            },
        )
        .unwrap();

        assert_eq!(topology.desired_replication_factor, 5);
        assert!(topology.derived_ranges().unwrap().iter().all(|range| {
            1 + range.follower_node_ids.len() < topology.desired_replication_factor as usize
        }));
    }

    #[test]
    fn first_successor_becomes_owner_when_the_owner_is_removed() {
        let topology = configured_topology(3, WriteAckPolicy::OwnerOnly);
        for range in topology.derived_ranges().unwrap() {
            let expected_successor = range.follower_node_ids[0].clone();
            let remaining = topology
                .members
                .iter()
                .filter(|member| member.node_id != range.owner_node_id)
                .cloned()
                .collect();
            let after_removal = TopologySnapshot::new_with_config(
                topology.epoch + 1,
                topology.hash_seed,
                topology.virtual_nodes,
                remaining,
                topology.config(),
            )
            .unwrap();
            assert_eq!(
                after_removal
                    .owner_for_token(range.end_inclusive)
                    .unwrap()
                    .node_id,
                expected_successor
            );
        }
    }

    #[test]
    fn topology_configuration_is_part_of_digest_and_wire_encoding() {
        let base = configured_topology(3, WriteAckPolicy::OwnerOnly);
        let policy = configured_topology(3, WriteAckPolicy::AllReplicas);
        let rf = configured_topology(2, WriteAckPolicy::OwnerOnly);
        let mut guard_config = base.config();
        guard_config.write_availability_guard.max_replica_lag_millis += 1;
        let guard = TopologySnapshot::new_with_config(
            base.epoch,
            base.hash_seed,
            base.virtual_nodes,
            base.members.clone(),
            guard_config,
        )
        .unwrap();

        assert_ne!(base.digest, policy.digest);
        assert_ne!(base.digest, rf.digest);
        assert_ne!(base.digest, guard.digest);
        let first = crate::proto::TopologySnapshot::from(&base).encode_to_vec();
        let second = crate::proto::TopologySnapshot::from(&base).encode_to_vec();
        assert_eq!(first, second);
        assert_eq!(
            TopologySnapshot::try_from(crate::proto::TopologySnapshot::from(&base)).unwrap(),
            base
        );
    }

    #[test]
    fn first_successor_policy_requires_a_follower() {
        let error = TopologySnapshot::new_with_config(
            1,
            42,
            16,
            members(),
            TopologyConfig {
                desired_replication_factor: 1,
                write_ack_policy: WriteAckPolicy::FirstSuccessor,
                write_availability_guard: WriteAvailabilityGuard::default(),
            },
        )
        .unwrap_err();

        assert!(matches!(
            error,
            TopologyError::FirstSuccessorRequiresFollower
        ));
    }

    #[test]
    fn impossible_write_guards_are_rejected_and_legacy_rf_one_loads() {
        let mut config = TopologyConfig::default();
        config.write_availability_guard.minimum_admitted_copies = 4;
        assert!(matches!(
            TopologySnapshot::new_with_config(1, 42, 16, members(), config),
            Err(TopologyError::MinimumAdmittedCopiesExceedReplicationFactor)
        ));

        let mut config = TopologyConfig::default();
        config.write_availability_guard.minimum_healthy_followers = 3;
        assert!(matches!(
            TopologySnapshot::new_with_config(1, 42, 16, members(), config),
            Err(TopologyError::MinimumHealthyFollowersExceedReplicationFactor)
        ));

        let rf_one = TopologyConfig {
            desired_replication_factor: 1,
            ..TopologyConfig::default()
        };
        TopologySnapshot::new_with_config(1, 42, 16, members(), rf_one).unwrap();

        let impossible_rf_one = TopologyConfig {
            desired_replication_factor: 1,
            write_availability_guard: WriteAvailabilityGuard {
                minimum_admitted_copies: 2,
                minimum_healthy_followers: 0,
                ..WriteAvailabilityGuard::default()
            },
            ..TopologyConfig::default()
        };
        assert!(matches!(
            TopologySnapshot::new_with_config(1, 42, 16, members(), impossible_rf_one),
            Err(TopologyError::MinimumAdmittedCopiesExceedReplicationFactor)
        ));

        let legacy_rf_one = TopologyConfig {
            desired_replication_factor: 1,
            write_availability_guard: WriteAvailabilityGuard {
                minimum_admitted_copies: 2,
                minimum_healthy_followers: 1,
                ..WriteAvailabilityGuard::default()
            },
            ..TopologyConfig::default()
        };
        let legacy =
            TopologySnapshot::new_with_config(1, 42, 16, members(), legacy_rf_one).unwrap();
        legacy.validate().unwrap();
    }

    #[test]
    fn maximum_assignment_topology_fits_control_plane_limit() {
        let members = (0..256)
            .map(|index| Member {
                node_id: format!("n{index:03}{}", "x".repeat(MAX_NODE_ID_BYTES - 4)),
                endpoint: format!(
                    "http://127.0.0.1/{index:03}{}",
                    "x".repeat(MAX_ENDPOINT_BYTES - 21)
                ),
            })
            .collect();
        let topology = TopologySnapshot::new(1, 42, MAX_VIRTUAL_NODES, members).unwrap();
        assert_eq!(topology.assignments.len(), MAX_TOKEN_ASSIGNMENTS);
        let wire = crate::proto::TopologySnapshot::from(&topology);
        assert!(wire.encoded_len() <= crate::limits::MAX_CONTROL_MESSAGE_BYTES);

        let source = &topology.members[0];
        let destination = &topology.members[1];
        let range = crate::migration::RangeMigration {
            range_id: "f".repeat(64),
            start_exclusive: u64::MAX - 1,
            end_inclusive: u64::MAX,
            source_node_id: source.node_id.clone(),
            destination_node_id: destination.node_id.clone(),
            source_endpoint: "s".repeat(MAX_ENDPOINT_BYTES),
            destination_endpoint: "d".repeat(MAX_ENDPOINT_BYTES),
            source_process_instance_id: "s".repeat(36),
            destination_process_instance_id: "d".repeat(36),
            source_cleaned: false,
            snapshot_records: u64::MAX,
            changelog_watermark: u64::MAX,
            verified: true,
        };
        let change = crate::migration::TopologyChange {
            change_id: "c".repeat(36),
            base_epoch: 0,
            base_topology: None,
            supersedes_change_id: None,
            target_topology: topology,
            phase: crate::migration::MigrationPhase::Planned,
            ranges: vec![range; crate::migration::MAX_MIGRATION_RANGES],
            replica_obligations: Vec::new(),
            stopped_node_ids: Vec::new(),
            stopping_node_ids: Vec::new(),
            stop_prepared_node_ids: Vec::new(),
            failed_node_id: None,
            activation_ready: false,
            direct_merge: false,
        };
        let wire = crate::proto::TopologyChangeSnapshot::from(&change);
        assert!(wire.encoded_len() <= crate::limits::MAX_CONTROL_MESSAGE_BYTES);
    }

    #[test]
    fn tampering_is_detected() {
        let mut topology = TopologySnapshot::new(1, 42, 16, members()).unwrap();
        topology.epoch = 2;
        assert!(matches!(
            topology.validate(),
            Err(TopologyError::DigestMismatch)
        ));
    }
}
