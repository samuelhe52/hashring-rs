//! Per-key expiry on a node.
//!
//! The properties these check are the ones a promoted replica and a migration
//! destination have to agree on: a record past its deadline reads as absent
//! wherever it lives, and nothing about it depends on a delete having travelled
//! through the cluster.

use super::*;

/// A migration range that contains one key's token, whatever it is.
fn range_covering(token: u64) -> proto::RangeSpec {
    proto::RangeSpec {
        start_exclusive: token.saturating_sub(1),
        end_inclusive: token,
        change_id: "change-1".into(),
        range_id: "range-1".into(),
        source_node_id: "node-1".into(),
        destination_node_id: "node-2".into(),
    }
}

/// A deadline a little way ahead, in the form the wire carries.
fn deadline_in(secs: u64) -> u64 {
    now_unix_millis() + secs * 1000
}

fn put(key: &[u8], value: &[u8], request_id: &str) -> PutRequest {
    PutRequest {
        key: key.to_vec(),
        value: value.to_vec(),
        topology_epoch: 1,
        request_id: request_id.into(),
        expires_at_unix_millis: None,
        preserve_value: false,
    }
}

async fn get(service: &DataNodeService, key: &[u8]) -> GetResponse {
    service
        .get(Request::new(GetRequest {
            key: key.to_vec(),
            topology_epoch: 1,
            request_id: "read".into(),
        }))
        .await
        .unwrap()
        .into_inner()
}

async fn write(service: &DataNodeService, request: PutRequest) -> PutResponse {
    service
        .put(Request::new(request))
        .await
        .unwrap()
        .into_inner()
}

#[tokio::test]
async fn a_record_past_its_deadline_reads_as_absent() {
    let service = service();
    let stored = write(
        &service,
        PutRequest {
            expires_at_unix_millis: Some(deadline_in(0)),
            ..put(b"key", b"value", "first")
        },
    )
    .await;
    assert!(stored.error.is_none());

    let read = get(&service, b"key").await;
    assert_eq!(
        read.error.map(|error| ErrorCode::try_from(error.code)),
        Some(Ok(ErrorCode::NotFound)),
        "a deadline of zero is a key that is already gone"
    );
    assert_eq!(read.expires_at_unix_millis, None);
}

#[tokio::test]
async fn a_record_reports_the_deadline_it_was_given() {
    let service = service();
    let expires_at = deadline_in(600);
    write(
        &service,
        PutRequest {
            expires_at_unix_millis: Some(expires_at),
            ..put(b"key", b"value", "first")
        },
    )
    .await;
    let read = get(&service, b"key").await;
    assert_eq!(read.value, b"value");
    assert_eq!(read.expires_at_unix_millis, Some(expires_at));

    // A miss never carries a deadline, or a client could not tell the two apart.
    let missing = get(&service, b"absent").await;
    assert_eq!(missing.expires_at_unix_millis, None);
    assert!(missing.error.is_some());
}

#[tokio::test]
async fn a_write_without_a_deadline_keeps_the_one_it_finds_and_zero_clears_it() {
    let service = service();
    let expires_at = deadline_in(600);
    write(
        &service,
        PutRequest {
            expires_at_unix_millis: Some(expires_at),
            ..put(b"key", b"first", "first")
        },
    )
    .await;

    write(&service, put(b"key", b"second", "second")).await;
    assert_eq!(
        get(&service, b"key").await.expires_at_unix_millis,
        Some(expires_at)
    );

    write(
        &service,
        PutRequest {
            expires_at_unix_millis: Some(0),
            ..put(b"key", b"third", "third")
        },
    )
    .await;
    assert_eq!(get(&service, b"key").await.expires_at_unix_millis, None);
}

#[tokio::test]
async fn an_expiry_only_write_keeps_the_value_and_reports_a_missing_key() {
    let service = service();
    write(&service, put(b"key", b"value", "first")).await;

    let expires_at = deadline_in(600);
    let applied = write(
        &service,
        PutRequest {
            key: b"key".to_vec(),
            value: Vec::new(),
            topology_epoch: 1,
            request_id: "extend".into(),
            expires_at_unix_millis: Some(expires_at),
            preserve_value: true,
        },
    )
    .await;
    assert!(applied.error.is_none());
    assert!(!applied.key_missing);
    let read = get(&service, b"key").await;
    assert_eq!(
        read.value, b"value",
        "only the deadline was meant to change"
    );
    assert_eq!(read.expires_at_unix_millis, Some(expires_at));

    // A key that is not there is reported rather than created, and nothing is
    // stored for it: no record, no sequence, no receipt.
    let sequences_before = service.state.read().await.next_sequence;
    let missing = write(
        &service,
        PutRequest {
            key: b"absent".to_vec(),
            value: Vec::new(),
            topology_epoch: 1,
            request_id: "extend-absent".into(),
            expires_at_unix_millis: Some(expires_at),
            preserve_value: true,
        },
    )
    .await;
    assert!(missing.key_missing);
    assert!(missing.error.is_none());
    assert!(missing.version.is_none());
    assert!(
        !service
            .state
            .read()
            .await
            .dedup
            .contains_key("extend-absent")
    );
    assert_eq!(service.state.read().await.next_sequence, sequences_before);

    // An expired record counts as missing too.
    write(
        &service,
        PutRequest {
            expires_at_unix_millis: Some(deadline_in(0)),
            ..put(b"gone", b"value", "gone")
        },
    )
    .await;
    let expired = write(
        &service,
        PutRequest {
            key: b"gone".to_vec(),
            value: Vec::new(),
            topology_epoch: 1,
            request_id: "extend-gone".into(),
            expires_at_unix_millis: Some(expires_at),
            preserve_value: true,
        },
    )
    .await;
    assert!(expired.key_missing);
}

#[tokio::test]
async fn an_expiry_only_write_needs_a_deadline_and_no_value() {
    let service = service();
    let with_value = write(
        &service,
        PutRequest {
            key: b"key".to_vec(),
            value: b"value".to_vec(),
            topology_epoch: 1,
            request_id: "bad".into(),
            expires_at_unix_millis: Some(deadline_in(60)),
            preserve_value: true,
        },
    )
    .await;
    assert_eq!(
        with_value
            .error
            .map(|error| ErrorCode::try_from(error.code)),
        Some(Ok(ErrorCode::InvalidArgument))
    );

    let without_deadline = write(
        &service,
        PutRequest {
            key: b"key".to_vec(),
            value: Vec::new(),
            topology_epoch: 1,
            request_id: "bad".into(),
            expires_at_unix_millis: None,
            preserve_value: true,
        },
    )
    .await;
    assert_eq!(
        without_deadline
            .error
            .map(|error| ErrorCode::try_from(error.code)),
        Some(Ok(ErrorCode::InvalidArgument))
    );
}

#[tokio::test]
async fn a_retried_expiry_only_write_returns_the_first_answer() {
    let service = service();
    write(&service, put(b"key", b"value", "first")).await;
    let expires_at = deadline_in(600);
    let request = PutRequest {
        key: b"key".to_vec(),
        value: Vec::new(),
        topology_epoch: 1,
        request_id: "extend".into(),
        expires_at_unix_millis: Some(expires_at),
        preserve_value: true,
    };
    let first = write(&service, request.clone()).await;
    let sequence_after_first = service.state.read().await.next_sequence;
    let retried = write(&service, request).await;
    assert_eq!(
        retried.version, first.version,
        "the retry reused the result"
    );
    assert_eq!(
        service.state.read().await.next_sequence,
        sequence_after_first,
        "the retry did not apply a second write"
    );
    assert_eq!(get(&service, b"key").await.value, b"value");

    // The same mutation ID with a different deadline is a different operation.
    let conflict = write(
        &service,
        PutRequest {
            key: b"key".to_vec(),
            value: Vec::new(),
            topology_epoch: 1,
            request_id: "extend".into(),
            expires_at_unix_millis: Some(deadline_in(900)),
            preserve_value: true,
        },
    )
    .await;
    assert_eq!(
        conflict.error.map(|error| ErrorCode::try_from(error.code)),
        Some(Ok(ErrorCode::MutationIdConflict))
    );
}

#[tokio::test]
async fn the_sweep_reclaims_deadlines_nobody_asks_about() {
    let service = service();
    {
        let mut state = service.state.write().await;
        for index in 0..16_u32 {
            seed_record(
                &mut state,
                format!("key-{index}").as_bytes(),
                Record {
                    value: Arc::from(b"value".as_slice()),
                    version: RecordVersion {
                        topology_epoch: 1,
                        owner_sequence: u64::from(index) + 1,
                        owner_node_id: "node-1".into(),
                    },
                    deleted: false,
                    expires_at_unix_millis: Some(now_unix_millis() - index as u64 - 1),
                },
            );
        }
        // A write that pushes a key's deadline out leaves the earlier entry in
        // the heap, and the sweep has to notice rather than remove the key.
        seed_record(
            &mut state,
            b"kept",
            Record {
                value: Arc::from(b"value".as_slice()),
                version: RecordVersion {
                    topology_epoch: 1,
                    owner_sequence: 1,
                    owner_node_id: "node-1".into(),
                },
                deleted: false,
                expires_at_unix_millis: Some(now_unix_millis() + 3_600_000),
            },
        );
    }
    assert_eq!(service.state.read().await.records.len(), 17);

    // Every key but `kept` is already past its deadline.
    let removed = {
        let mut state = service.state.write().await;
        sweep_expired_records(&mut state, Instant::now(), EXPIRY_SWEEP_LIMIT)
    };
    assert_eq!(removed, 16);
    {
        let state = service.state.read().await;
        assert_eq!(state.records.len(), 1);
        assert!(state.records.contains_key(b"kept".as_slice()));
        state.debug_assert_index();
    }

    // The heap is drained of expired entries, so the next pass costs nothing.
    let mut state = service.state.write().await;
    assert_eq!(
        sweep_expired_records(&mut state, Instant::now(), EXPIRY_SWEEP_LIMIT),
        0
    );
    drop(state);
}

#[tokio::test]
async fn a_deadline_reaches_a_follower_and_a_replica_read_agrees() {
    let topology = replication_topology(2);
    let owner = service_for("node-1", topology.clone());
    let follower = service_for("node-2", topology.clone());
    let key = key_with_placement(&topology, |replicas| {
        replicas[0] == "node-1" && replicas[1] == "node-2"
    });

    let expires_at = deadline_in(600);
    let stored = write(
        &owner,
        PutRequest {
            expires_at_unix_millis: Some(expires_at),
            ..put(&key, b"value", "first")
        },
    )
    .await;
    let version = stored.version.expect("the write was applied");

    let applied = follower
        .replicate_mutation(Request::new(replication_entry(
            &topology,
            key.to_vec(),
            1,
            1,
            b"value",
            false,
            Some(expires_at),
        )))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(applied.applied_stream_sequence, 1);

    // The follower stores the deadline the owner sent, so both read the key the
    // same way and both would agree if one of them were promoted.
    {
        let state = follower.state.read().await;
        let record = state.records.get(key.as_slice()).unwrap();
        assert_eq!(record.expires_at_unix_millis, Some(expires_at));
        assert_eq!(record.version, version);
    }

    // A record that arrives already expired is not readable either.
    let expired = write(
        &owner,
        PutRequest {
            expires_at_unix_millis: Some(now_unix_millis()),
            ..put(&key, b"second", "second")
        },
    )
    .await;
    assert!(expired.error.is_none());
    follower
        .replicate_mutation(Request::new(replication_entry(
            &topology,
            key.clone(),
            2,
            2,
            b"second",
            false,
            Some(now_unix_millis()),
        )))
        .await
        .unwrap();
    let read = get(&follower, &key).await;
    assert!(read.error.is_some(), "the follower read an expired record");
}

#[tokio::test]
async fn a_range_digest_covers_the_deadline() {
    let topology = replication_topology(2);
    let source = service_for("node-1", topology.clone());
    let destination = service_for("node-2", topology.clone());
    let key = key_with_placement(&topology, |replicas| replicas[0] == "node-1");
    let token = topology.key_token(&key);
    let expires_at = deadline_in(600);

    write(
        &source,
        PutRequest {
            expires_at_unix_millis: Some(expires_at),
            ..put(&key, b"value", "first")
        },
    )
    .await;
    source
        .prepare_source_range(Request::new(PrepareRangeRequest {
            range: Some(range_covering(token)),
        }))
        .await
        .unwrap();
    let page = source
        .read_snapshot_page(Request::new(SnapshotPageRequest {
            change_id: "change-1".into(),
            range_id: "range-1".into(),
            cursor: 0,
            max_bytes: u64::MAX,
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(page.records.len(), 1);
    assert_eq!(
        page.records[0].expires_at_unix_millis,
        Some(expires_at),
        "the deadline travels with the record"
    );

    // A destination that was handed a different deadline must not verify: the
    // two would stop being readable at different times.
    destination
        .prepare_destination_range(Request::new(PrepareRangeRequest {
            range: Some(range_covering(token)),
        }))
        .await
        .unwrap();
    for expires_at in [Some(deadline_in(900)), None] {
        destination
            .apply_migration_batch(Request::new(ApplyMigrationBatchRequest {
                change_id: "change-1".into(),
                range_id: "range-1".into(),
                snapshot_records: vec![MigrationRecord {
                    key: key.clone(),
                    value: b"value".to_vec(),
                    version: page.records[0].version.clone(),
                    deleted: false,
                    mutation_id: String::new(),
                    remaining_retention_millis: 0,
                    expires_at_unix_millis: expires_at,
                    preserve_value: false,
                }],
                journal_records: Vec::new(),
            }))
            .await
            .unwrap();
        let source_digest = source
            .source_range_digest(Request::new(RangeControlRequest {
                change_id: "change-1".into(),
                range_id: "range-1".into(),
            }))
            .await
            .unwrap()
            .into_inner();
        let destination_digest = destination
            .destination_range_digest(Request::new(RangeControlRequest {
                change_id: "change-1".into(),
                range_id: "range-1".into(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_ne!(
            source_digest.digest, destination_digest.digest,
            "{expires_at:?} was accepted as the same record"
        );
    }
}
