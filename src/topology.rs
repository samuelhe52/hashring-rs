use std::collections::HashSet;

use serde::{Deserialize, Serialize};
use thiserror::Error;
use xxhash_rust::xxh3::xxh3_64_with_seed;

pub const HASH_ALGORITHM: &str = "xxh3-64";
pub const ENCODING_VERSION: u32 = 1;
pub const MAX_MEMBERS: usize = 1_024;
pub const MAX_VIRTUAL_NODES: u32 = 4_096;
pub const MAX_TOKEN_ASSIGNMENTS: usize = 1_048_576;
pub const MAX_NODE_ID_BYTES: usize = 32;
pub const MAX_ENDPOINT_BYTES: usize = 256;

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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TopologySnapshot {
    pub epoch: u64,
    pub hash_seed: u64,
    pub hash_algorithm: String,
    pub encoding_version: u32,
    pub virtual_nodes: u32,
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
}

impl TopologySnapshot {
    pub fn new(
        epoch: u64,
        hash_seed: u64,
        virtual_nodes: u32,
        mut members: Vec<Member>,
    ) -> Result<Self, TopologyError> {
        validate_members(&members, virtual_nodes)?;
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
            members,
            assignments,
            digest: String::new(),
        };
        snapshot.digest = snapshot.calculate_digest();
        Ok(snapshot)
    }

    pub fn validate(&self) -> Result<(), TopologyError> {
        validate_members(&self.members, self.virtual_nodes)?;
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
        let canonical = Self::new(
            self.epoch,
            self.hash_seed,
            self.virtual_nodes,
            self.members.clone(),
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

    fn calculate_digest(&self) -> String {
        let mut unsigned = self.clone();
        unsigned.digest.clear();
        let canonical =
            serde_json::to_vec(&unsigned).expect("serializing an in-memory topology cannot fail");
        blake3::hash(&canonical).to_hex().to_string()
    }
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

impl TryFrom<crate::proto::TopologySnapshot> for TopologySnapshot {
    type Error = TopologyError;

    fn try_from(snapshot: crate::proto::TopologySnapshot) -> Result<Self, Self::Error> {
        let snapshot = Self {
            epoch: snapshot.epoch,
            hash_seed: snapshot.hash_seed,
            hash_algorithm: snapshot.hash_algorithm,
            encoding_version: snapshot.encoding_version,
            virtual_nodes: snapshot.virtual_nodes,
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
        assert!(wire.encoded_len() <= crate::node::MAX_CONTROL_MESSAGE_BYTES);

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
            target_topology: topology,
            phase: crate::migration::MigrationPhase::Planned,
            ranges: vec![range; crate::migration::MAX_MIGRATION_RANGES],
            stopped_node_ids: Vec::new(),
            stopping_node_ids: Vec::new(),
            stop_prepared_node_ids: Vec::new(),
        };
        let wire = crate::proto::TopologyChangeSnapshot::from(&change);
        assert!(wire.encoded_len() <= crate::node::MAX_CONTROL_MESSAGE_BYTES);
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
