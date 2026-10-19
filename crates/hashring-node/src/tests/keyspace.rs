//! Paging through the keys a node owns.
//!
//! A walk over every owner has to see each live key exactly once: the node
//! must skip its follower copies, deleted and expired records, and end every
//! page between tokens so the next one can start from a token alone.

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
        one_stretch: false,
    }
}

/// Every page from the start until the node says it is done.
fn walk(
    state: &NodeState,
    node_id: &str,
    limits: &crate::keyspace::PageLimits,
) -> (Vec<Vec<u8>>, u64) {
    let mut cursor = 0;
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

/// A state holding every key, under a three-node topology, so that each node's
/// pages can be taken from it as that node would take them from its own.
async fn three_owners_with_keys(count: usize) -> (TopologySnapshot, DataNodeService, Vec<Vec<u8>>) {
    let topology = replication_topology(3);
    let service = service_for("node-1", topology.clone());
    let keys: Vec<Vec<u8>> = (0..count)
        .map(|index| format!("key-{index}").into_bytes())
        .collect();
    {
        let mut state = service.state.write().await;
        for key in &keys {
            seed_record(&mut state, key, record(false, None));
        }
    }
    (topology, service, keys)
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
async fn single_key_pages_visit_every_key_once_and_never_split_a_token() {
    let service = service();
    let mut state = service.state.write().await;
    for index in 0..50 {
        seed_record(
            &mut state,
            format!("key-{index}").as_bytes(),
            record(false, None),
        );
    }
    // Two keys that land on the same token share a bucket. A page takes both,
    // even past its key limit, so that the next page can start at a token.
    for key in [b"twin-a".as_slice(), b"twin-b"] {
        state.records.insert(Arc::from(key), record(false, None));
        state.records_by_token.insert(7, Arc::from(key));
    }
    let twins = state.owned_keys_page("node-1", 7, now_unix_millis(), &limits(1, false));
    assert_eq!(twins.keys, [b"twin-a".to_vec(), b"twin-b".to_vec()]);
    assert!(twins.next.is_some_and(|next| next > 7));

    let (keys, count) = walk(&state, "node-1", &limits(1, false));
    assert_eq!(count, 52);
    let unique: HashSet<_> = keys.iter().collect();
    assert_eq!(unique.len(), 52);
}

#[tokio::test]
async fn an_owner_refuses_one_key_too_many_on_a_token() {
    let service = service();
    let key = b"newcomer";
    let token = service.state.read().await.token_of(key);
    {
        // Keys built to collide with `newcomer`, as anyone who knows the
        // hash seed could build them.
        let mut state = service.state.write().await;
        for index in 0..crate::keyspace::MAX_KEYS_PER_TOKEN {
            let colliding: Arc<[u8]> = Arc::from(format!("colliding-{index}").as_bytes());
            state.records.insert(colliding.clone(), record(false, None));
            state.records_by_token.insert(token, colliding);
        }
    }
    let put = |key: &[u8], request_id: &str| PutRequest {
        key: key.to_vec(),
        value: b"value".to_vec(),
        topology_epoch: 1,
        request_id: request_id.into(),
        ..Default::default()
    };
    let refused = service
        .put(Request::new(put(key, "new")))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        refused.error.map(|error| error.code),
        Some(ErrorCode::ResourceExhausted as i32)
    );
    assert_eq!(
        service.state.read().await.records_by_token.len_at(token),
        crate::keyspace::MAX_KEYS_PER_TOKEN
    );

    // A key already on the token is still written.
    let rewritten = service
        .put(Request::new(put(b"colliding-0", "rewrite")))
        .await
        .unwrap()
        .into_inner();
    assert!(rewritten.error.is_none(), "{:?}", rewritten.error);
}

#[tokio::test]
async fn a_page_skips_the_follower_copies_a_node_holds() {
    let (topology, service, keys) = three_owners_with_keys(300).await;
    let state = service.state.read().await;
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
async fn one_stretch_at_a_time_round_the_ring_visits_every_key_once() {
    let (topology, service, keys) = three_owners_with_keys(300).await;
    let state = service.state.read().await;
    let one_stretch = crate::keyspace::PageLimits {
        one_stretch: true,
        ..limits(5, false)
    };
    // What a scan does: ask whoever owns the cursor's token, and carry on
    // from where its stretch ended.
    let mut position = 0;
    let mut seen = Vec::new();
    for _ in 0..10_000 {
        let owner = topology.owner_for_token(position).unwrap().node_id.clone();
        let page = state.owned_keys_page(&owner, position, now_unix_millis(), &one_stretch);
        for key in &page.keys {
            assert_eq!(topology.owner(key).unwrap().node_id, owner);
            assert!(topology.key_token(key) >= position);
        }
        seen.extend(page.keys);
        match page.next {
            Some(next) => {
                assert!(next > position, "a stretch made no progress");
                position = next;
            }
            None => break,
        }
    }
    let mut expected = keys;
    expected.sort();
    seen.sort();
    assert_eq!(seen, expected);
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
    let first = state.owned_keys_page("node-1", 0, now_unix_millis(), &tight);
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
async fn a_stretch_is_held_to_ownership_rather_than_to_the_callers_epoch() {
    let (topology, service, _) = three_owners_with_keys(30).await;
    let owned = topology
        .assignments
        .iter()
        .find(|assignment| assignment.node_id == "node-1")
        .unwrap()
        .token;
    let foreign = topology
        .assignments
        .iter()
        .find(|assignment| assignment.node_id != "node-1")
        .unwrap();
    let stretch = |start_token| ListKeysRequest {
        start_token,
        one_stretch: true,
        ..list_request(0)
    };

    // A cursor from an older epoch is still answered by the token's owner.
    let answered = service
        .list_keys(Request::new(stretch(owned)))
        .await
        .unwrap()
        .into_inner();
    assert!(answered.error.is_none(), "{:?}", answered.error);

    // A token another node owns is sent there.
    let moved = service
        .list_keys(Request::new(stretch(foreign.token)))
        .await
        .unwrap()
        .into_inner()
        .error
        .expect("a token owned elsewhere is refused");
    assert_eq!(moved.code, ErrorCode::Moved as i32);
    let endpoint = &topology
        .members
        .iter()
        .find(|member| member.node_id == foreign.node_id)
        .unwrap()
        .endpoint;
    assert_eq!(&moved.owner_endpoint, endpoint);
}

#[tokio::test]
async fn an_owner_whose_lease_ran_out_does_not_answer() {
    let service = service();
    service.state.write().await.lease = None;
    for one_stretch in [false, true] {
        let response = service
            .list_keys(Request::new(ListKeysRequest {
                one_stretch,
                ..list_request(1)
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            response.error.map(|error| error.code),
            Some(ErrorCode::LeaseExpired as i32),
            "one_stretch={one_stretch}"
        );
    }
}
