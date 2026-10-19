//! Paging through the keys a node owns.
//!
//! A walk over every owner has to see each live key exactly once: the node
//! must skip its follower copies, deleted and expired records, and resume a
//! page exactly where the last one stopped, even inside a token bucket.

use super::*;

fn record(deleted: bool, expires_at_unix_millis: Option<u64>) -> Record {
    Record {
        value: Arc::from(b"value".as_slice()),
        version: RecordVersion {
            topology_epoch: 1,
            owner_sequence: 1,
            owner_node_id: "node-1".into(),
        },
        deleted,
        expires_at_unix_millis,
    }
}

fn limits(max_keys: usize, count_only: bool) -> crate::keyspace::PageLimits {
    crate::keyspace::PageLimits {
        max_keys,
        max_bytes: usize::MAX,
        max_examined: crate::keyspace::MAX_EXAMINED_PER_PAGE,
        count_only,
    }
}

/// Every page from the start until the node says it is done.
fn walk(
    state: &NodeState,
    node_id: &str,
    limits: &crate::keyspace::PageLimits,
) -> (Vec<Vec<u8>>, u64) {
    let mut cursor = crate::keyspace::KeyCursor::default();
    let (mut keys, mut count) = (Vec::new(), 0);
    for _ in 0..10_000 {
        let page = state.owned_keys_page(node_id, cursor, now_unix_millis(), limits);
        keys.extend(page.keys);
        count += page.count;
        match page.next {
            Some(next) => cursor = next,
            None => return (keys, count),
        }
    }
    panic!("the walk did not finish");
}

fn list_request(epoch: u64) -> ListKeysRequest {
    ListKeysRequest {
        topology_epoch: epoch,
        max_keys: 100,
        max_bytes: 1 << 20,
        ..Default::default()
    }
}

#[tokio::test]
async fn a_page_lists_live_keys_only() {
    let service = service();
    let mut state = service.state.write().await;
    seed_record(&mut state, b"live", record(false, None));
    seed_record(
        &mut state,
        b"expiring",
        record(false, Some(now_unix_millis() + 3_600_000)),
    );
    seed_record(&mut state, b"deleted", record(true, None));
    seed_record(
        &mut state,
        b"expired",
        record(false, Some(now_unix_millis() - 1)),
    );

    let (mut keys, count) = walk(&state, "node-1", &limits(100, false));
    keys.sort();
    assert_eq!(keys, [b"expiring".to_vec(), b"live".to_vec()]);
    assert_eq!(count, 2);

    let (keys, count) = walk(&state, "node-1", &limits(100, true));
    assert!(keys.is_empty());
    assert_eq!(count, 2);
}

#[tokio::test]
async fn single_key_pages_visit_every_key_once_including_a_shared_token() {
    let service = service();
    let mut state = service.state.write().await;
    for index in 0..50 {
        seed_record(
            &mut state,
            format!("key-{index}").as_bytes(),
            record(false, None),
        );
    }
    // Two keys that land on the same token share a bucket. A page that stops
    // between them must resume at the second, not skip it or repeat the first.
    for key in [b"twin-a".as_slice(), b"twin-b"] {
        state.records.insert(Arc::from(key), record(false, None));
        state.records_by_token.insert(7, Arc::from(key));
    }

    let (keys, count) = walk(&state, "node-1", &limits(1, false));
    assert_eq!(count, 52);
    let unique: HashSet<_> = keys.iter().collect();
    assert_eq!(unique.len(), 52);
    assert!(unique.contains(&b"twin-a".to_vec()) && unique.contains(&b"twin-b".to_vec()));
}

#[tokio::test]
async fn a_page_resumed_inside_a_shared_token_starts_just_past_its_cursor() {
    let service = service();
    let mut state = service.state.write().await;
    let keys: Vec<Vec<u8>> = (0..100)
        .map(|index| format!("twin-{index:03}").into_bytes())
        .collect();
    for key in &keys {
        state
            .records
            .insert(Arc::from(key.as_slice()), record(false, None));
        state.records_by_token.insert(7, Arc::from(key.as_slice()));
    }
    // Resuming after the 90th key, a budget of a handful of entries still
    // reaches the keys after it: the 90 before it are not walked again.
    let tight = crate::keyspace::PageLimits {
        max_examined: 5,
        ..limits(100, false)
    };
    let page = state.owned_keys_page(
        "node-1",
        crate::keyspace::KeyCursor {
            token: 7,
            after_key: Some(keys[89].clone()),
        },
        now_unix_millis(),
        &tight,
    );
    assert_eq!(page.keys, keys[90..94]);
}

#[tokio::test]
async fn a_page_skips_the_follower_copies_a_node_holds() {
    let topology = replication_topology(3);
    let service = service_for("node-1", topology.clone());
    let mut state = service.state.write().await;
    let keys: Vec<Vec<u8>> = (0..300)
        .map(|index| format!("key-{index}").into_bytes())
        .collect();
    for key in &keys {
        seed_record(&mut state, key, record(false, None));
    }

    for node in ["node-1", "node-2", "node-3"] {
        let (listed, _) = walk(&state, node, &limits(7, false));
        let mut expected: Vec<_> = keys
            .iter()
            .filter(|key| topology.owner(key).unwrap().node_id == node)
            .cloned()
            .collect();
        let mut listed = listed;
        expected.sort();
        listed.sort();
        assert!(!expected.is_empty());
        assert_eq!(listed, expected, "{node}");
    }
}

#[tokio::test]
async fn a_page_stops_at_its_examination_budget() {
    let service = service();
    let mut state = service.state.write().await;
    for index in 0..20 {
        seed_record(
            &mut state,
            format!("gone-{index}").as_bytes(),
            record(true, None),
        );
    }
    seed_record(&mut state, b"live", record(false, None));

    let tight = crate::keyspace::PageLimits {
        max_examined: 5,
        ..limits(100, false)
    };
    let first = state.owned_keys_page("node-1", Default::default(), now_unix_millis(), &tight);
    assert!(
        first.next.is_some(),
        "twenty dead entries do not fit in five"
    );
    let (keys, _) = walk(&state, "node-1", &tight);
    assert_eq!(keys, [b"live".to_vec()]);
}

#[tokio::test]
async fn a_walk_from_an_older_epoch_is_told_to_start_over() {
    let service = service();
    let stale = service
        .list_keys(Request::new(list_request(0)))
        .await
        .unwrap()
        .into_inner();
    let error = stale.error.expect("an older epoch is refused");
    assert_eq!(error.code, ErrorCode::Moved as i32);
    assert_eq!(error.current_epoch, 1);
}

#[tokio::test]
async fn an_owner_whose_lease_ran_out_does_not_answer() {
    let service = service();
    service.state.write().await.lease = None;
    let response = service
        .list_keys(Request::new(list_request(1)))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        response.error.map(|error| error.code),
        Some(ErrorCode::LeaseExpired as i32)
    );
}
