use super::*;
use hashring_core::topology::{Member, TopologyConfig, WriteAvailabilityGuard};

fn service() -> DataNodeService {
    let topology = TopologySnapshot::new_with_config(
        1,
        42,
        8,
        vec![Member {
            node_id: "node-1".into(),
            endpoint: "http://127.0.0.1:5001".into(),
        }],
        TopologyConfig {
            desired_replication_factor: 1,
            write_availability_guard: WriteAvailabilityGuard {
                minimum_admitted_copies: 1,
                minimum_healthy_followers: 0,
                ..WriteAvailabilityGuard::default()
            },
            ..TopologyConfig::default()
        },
    )
    .unwrap();
    service_for("node-1", topology)
}

fn service_for(node_id: &str, topology: TopologySnapshot) -> DataNodeService {
    let (shutdown, _) = watch::channel(false);
    let test_epoch = topology.epoch;
    let state = Arc::new(RwLock::new(NodeState {
        topology,
        records: HashMap::new(),
        next_sequence: 0,
        owner_stream_sequences: HashMap::new(),
        owner_stream_unacked: HashMap::new(),
        ack_progress: HashMap::new(),
        ack_process_instances: HashMap::new(),
        admitted_followers: HashMap::new(),
        follower_streams: HashMap::new(),
        sources: HashMap::new(),
        destinations: HashMap::new(),
        journal_bytes_total: 0,
        lease: Some((
            test_epoch,
            Instant::now() + std::time::Duration::from_secs(3600),
        )),
        policy_write_fence: None,
        dedup: HashMap::new(),
        dedup_expirations: BinaryHeap::new(),
        dedup_bytes: 0,
        dedup_peak_bytes: 0,
        ack_progress_needs_prune: false,
        next_ack_prune_at: Instant::now(),
    }));
    let pressure = Arc::new(NodePressureStats::default());
    let replication_dispatch = start_replication_dispatch(state.clone(), pressure.clone());
    DataNodeService {
        node_id: node_id.into(),
        process_instance_id: "instance-1".into(),
        coordinator_endpoint: "http://127.0.0.1:5000".into(),
        coordinator_channel: Endpoint::from_static("http://127.0.0.1:5000").connect_lazy(),
        state,
        replication_dispatch,
        pressure,
        refresh_lock: Arc::new(Mutex::new(())),
        max_key_bytes: DEFAULT_MAX_KEY_BYTES,
        max_value_bytes: DEFAULT_MAX_VALUE_BYTES,
        max_journal_bytes: DEFAULT_MAX_MIGRATION_JOURNAL_BYTES,
        max_dedup_bytes: MAX_DEDUP_BYTES,
        stop_response_delay: std::time::Duration::ZERO,
        stop_prepared: Arc::new(AtomicBool::new(false)),
        shutdown,
    }
}

fn replication_topology(desired_replication_factor: u32) -> TopologySnapshot {
    TopologySnapshot::new_with_config(
        1,
        42,
        8,
        vec![
            Member {
                node_id: "node-1".into(),
                endpoint: "http://127.0.0.1:5001".into(),
            },
            Member {
                node_id: "node-2".into(),
                endpoint: "http://127.0.0.1:5002".into(),
            },
            Member {
                node_id: "node-3".into(),
                endpoint: "http://127.0.0.1:5003".into(),
            },
        ],
        TopologyConfig {
            desired_replication_factor,
            write_availability_guard: WriteAvailabilityGuard {
                minimum_admitted_copies: 1,
                minimum_healthy_followers: 0,
                ..WriteAvailabilityGuard::default()
            },
            ..TopologyConfig::default()
        },
    )
    .unwrap()
}

#[test]
fn peer_probes_keep_two_independent_reporters_at_large_membership() {
    let members = (0..128)
        .map(|index| Member {
            node_id: format!("node-{index:03}"),
            endpoint: format!("http://127.0.0.1:{}", 6000 + index),
        })
        .collect();
    let topology = TopologySnapshot::new(1, 42, 1, members).unwrap();
    let mut reporters: HashMap<String, HashSet<String>> = HashMap::new();
    for member in &topology.members {
        let targets = peer_probe_targets(&topology, &member.node_id);
        assert_eq!(targets.len(), 4);
        for target in targets {
            reporters
                .entry(target.node_id)
                .or_default()
                .insert(member.node_id.clone());
        }
    }
    assert!(reporters.values().all(|reporters| reporters.len() == 4));
}

fn key_with_placement(topology: &TopologySnapshot, predicate: impl Fn(&[&str]) -> bool) -> Vec<u8> {
    (0_u64..100_000)
        .map(|value| value.to_be_bytes().to_vec())
        .find(|key| {
            topology
                .replica_node_ids_for_token(topology.key_token(key))
                .is_ok_and(|replicas| predicate(&replicas))
        })
        .expect("test topology should contain a matching placement")
}

fn replication_entry(
    topology: &TopologySnapshot,
    key: Vec<u8>,
    stream_sequence: u64,
    owner_sequence: u64,
    value: &[u8],
    deleted: bool,
) -> ReplicationEntry {
    let owner = topology.owner(&key).unwrap();
    ReplicationEntry {
        topology_epoch: topology.epoch,
        owner_node_id: owner.node_id.clone(),
        stream_sequence,
        key,
        value: value.to_vec(),
        deleted,
        version: Some(RecordVersion {
            topology_epoch: topology.epoch,
            owner_sequence,
            owner_node_id: owner.node_id.clone(),
        }),
        mutation_id: format!("mutation-{stream_sequence}"),
    }
}

fn range(end_inclusive: u64) -> proto::RangeSpec {
    proto::RangeSpec {
        change_id: "change-1".into(),
        range_id: "range-1".into(),
        start_exclusive: 0,
        end_inclusive,
        source_node_id: "node-1".into(),
        destination_node_id: "node-2".into(),
    }
}

mod migration;
mod replication;
