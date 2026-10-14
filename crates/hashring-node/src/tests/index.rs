use super::*;
use hashring_core::topology::{Member, TopologyConfig, WriteAvailabilityGuard};

/// The index is only worth having if it answers exactly what hashing every key
/// would have answered, including for the wrapping range that spans the end of
/// the token space.
fn ranges() -> Vec<RangeSpec> {
    vec![
        spec(0, 0),
        spec(0, u64::MAX / 3),
        spec(u64::MAX / 3, u64::MAX / 2),
        // Wraps past the end of the token space.
        spec(u64::MAX / 2, u64::MAX / 3),
        spec(u64::MAX - 1, 0),
        spec(u64::MAX, 1),
        // start == end means every token.
        spec(42, 42),
    ]
}

fn spec(start_exclusive: u64, end_inclusive: u64) -> RangeSpec {
    RangeSpec {
        change_id: "change-1".into(),
        range_id: "range-1".into(),
        start_exclusive,
        end_inclusive,
        source_node_id: "node-1".into(),
        destination_node_id: "node-2".into(),
    }
}

fn keys(count: u32) -> Vec<Vec<u8>> {
    (0..count)
        .map(|index| format!("key-{index:04}").into_bytes())
        .collect()
}

fn record(owner_sequence: u64) -> Record {
    Record {
        value: Arc::from(format!("value-{owner_sequence}").as_bytes()),
        version: RecordVersion {
            topology_epoch: 1,
            owner_sequence,
            owner_node_id: "node-1".into(),
        },
        deleted: false,
    }
}

/// What the previous implementation produced: hash every key on the node and
/// keep the ones whose token the range claims.
fn brute_force(state: &NodeState, range: &RangeSpec) -> Vec<Vec<u8>> {
    let mut found: Vec<Vec<u8>> = state
        .records
        .keys()
        .filter(|key| range.contains(state.topology.key_token(key)))
        .map(|key| key.to_vec())
        .collect();
    found.sort();
    found
}

fn indexed(state: &NodeState, range: &RangeSpec) -> Vec<Vec<u8>> {
    let mut found: Vec<Vec<u8>> = state
        .record_keys_in_range(range)
        .into_iter()
        .map(|key| key.to_vec())
        .collect();
    found.sort();
    found
}

#[tokio::test]
async fn record_index_matches_hashing_every_key() {
    let service = service();
    let mut state = service.state.write().await;
    for (index, key) in keys(64).into_iter().enumerate() {
        seed_record(&mut state, key.as_slice(), record(index as u64 + 1));
    }
    state.debug_assert_index();
    assert_eq!(state.records_by_token.len(), 64);

    for range in ranges() {
        assert_eq!(
            indexed(&state, &range),
            brute_force(&state, &range),
            "range {}..= disagrees with a full scan",
            range.start_exclusive,
        );
    }
}

#[tokio::test]
async fn record_index_tracks_insert_replace_apply_and_remove() {
    let service = service();
    let mut state = service.state.write().await;
    let owned = keys(32);

    for key in &owned {
        seed_record(&mut state, key.as_slice(), record(1));
    }
    state.debug_assert_index();

    // Replacing a key must not leave its old index entry behind.
    let token = state.token_of(&owned[0]);
    state.insert_record(Arc::from(owned[0].as_slice()), token, record(9));
    state.debug_assert_index();
    assert_eq!(state.records_by_token.len(), 32);
    assert_eq!(
        state.records[owned[0].as_slice()].value.as_ref(),
        b"value-9"
    );

    // A stale version is ignored, and must not index the key twice.
    let token = state.token_of(&owned[1]);
    assert!(
        !state.apply_record(Arc::from(owned[1].as_slice()), token, record(1)),
        "an older version must not overwrite the stored record",
    );
    state.debug_assert_index();
    assert_eq!(state.records_by_token.len(), 32);

    for key in &owned {
        assert!(state.remove_record(key).is_some());
    }
    state.debug_assert_index();
    assert_eq!(state.records_by_token.len(), 0);
    assert!(state.records.is_empty());

    // Removals of keys this node never held must not disturb the index.
    assert!(state.remove_record(b"absent").is_none());
    state.debug_assert_index();
}

#[tokio::test]
async fn record_index_survives_a_topology_change() {
    let service = service();
    let mut state = service.state.write().await;
    for (index, key) in keys(48).into_iter().enumerate() {
        seed_record(&mut state, key.as_slice(), record(index as u64 + 1));
    }
    let before: Vec<Vec<Vec<u8>>> = ranges()
        .iter()
        .map(|range| indexed(&state, range))
        .collect();

    // A topology change carries the committed hash seed forward, so no key
    // moves: the index must keep answering the same ranges without a rebuild.
    let seed = state.topology.hash_seed;
    let topology = TopologySnapshot::new_with_config(
        2,
        seed,
        16,
        (0..4)
            .map(|index| Member {
                node_id: format!("node-{index}"),
                endpoint: format!("http://127.0.0.1:500{}", index + 1),
            })
            .collect(),
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
    state.topology = topology;
    state.debug_assert_index();

    for (index, range) in ranges().iter().enumerate() {
        assert_eq!(
            indexed(&state, range),
            before[index],
            "range {}..= changed across an epoch bump",
            range.start_exclusive,
        );
        assert_eq!(indexed(&state, range), brute_force(&state, range));
    }

    // Writes at the new epoch still index correctly.
    seed_record(&mut state, b"at-epoch-2", record(100));
    state.debug_assert_index();
    let mut at_epoch_two = brute_force(&state, &spec(0, u64::MAX));
    assert_eq!(at_epoch_two.len(), 49);
    at_epoch_two.sort();
    assert_eq!(indexed(&state, &spec(0, u64::MAX)), at_epoch_two);
}

#[tokio::test]
async fn dedup_index_matches_hashing_every_receipt_key() {
    let service = service();
    let mut state = service.state.write().await;
    let now = Instant::now();
    let owned = keys(48);
    for (index, key) in owned.iter().enumerate() {
        insert_dedup(
            &mut state,
            format!("mutation-{index}"),
            Arc::from(key.as_slice()),
            [index as u8; 32],
            RecordVersion {
                topology_epoch: 1,
                owner_sequence: index as u64 + 1,
                owner_node_id: "node-1".into(),
            },
            false,
            now,
        );
    }
    state.debug_assert_index();
    assert_eq!(state.dedup_by_token.len(), 48);

    for range in ranges() {
        let mut brute: Vec<String> = state
            .dedup
            .iter()
            .filter(|(_, entry)| range.contains(state.topology.key_token(&entry.key)))
            .map(|(id, _)| id.to_string())
            .collect();
        brute.sort();
        let mut indexed: Vec<String> = state
            .dedup_ids_in_range(&range)
            .into_iter()
            .map(|id| id.to_string())
            .collect();
        indexed.sort();
        assert_eq!(indexed, brute, "receipt index disagrees with a scan");
    }

    // Expiration must drop the index entry as well as the receipt.
    purge_expired_dedup(
        &mut state,
        now + RECEIPT_RETENTION_PERIOD + std::time::Duration::from_secs(1),
    );
    state.debug_assert_index();
    assert!(state.dedup.is_empty());
    assert_eq!(state.dedup_by_token.len(), 0);
}
