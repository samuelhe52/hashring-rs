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
        ..Default::default()
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
            ..Default::default()
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
            ..Default::default()
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
            ..Default::default()
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
            ..Default::default()
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
            ..Default::default()
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
async fn an_expiry_only_write_of_zero_clears_the_deadline() {
    let service = service();
    write(
        &service,
        PutRequest {
            expires_at_unix_millis: Some(deadline_in(600)),
            ..put(b"key", b"value", "first")
        },
    )
    .await;
    let cleared = write(
        &service,
        PutRequest {
            key: b"key".to_vec(),
            topology_epoch: 1,
            request_id: "persist".into(),
            expires_at_unix_millis: Some(0),
            preserve_value: true,
            ..Default::default()
        },
    )
    .await;
    assert!(cleared.error.is_none() && !cleared.key_missing);
    let read = get(&service, b"key").await;
    assert_eq!(read.value, b"value");
    assert_eq!(read.expires_at_unix_millis, None);
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
        ..Default::default()
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
            ..Default::default()
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
        // A key whose deadline is still ahead is left alone.
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
        sweep_expired_records(&mut state, now_unix_millis(), EXPIRY_SWEEP_LIMIT)
    };
    assert_eq!(removed, 16);
    {
        let state = service.state.read().await;
        assert_eq!(state.records.len(), 1);
        assert!(state.records.contains_key(b"kept".as_slice()));
        state.debug_assert_index();
    }

    // The index is drained of expired entries, so the next pass costs nothing.
    let mut state = service.state.write().await;
    assert_eq!(
        sweep_expired_records(&mut state, now_unix_millis(), EXPIRY_SWEEP_LIMIT),
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
            (1, 1),
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
            (2, 2),
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
                    request_fingerprint: Vec::new(),
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

#[tokio::test]
async fn a_kept_deadline_travels_as_the_deadline_the_owner_stored() {
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
            ..put(&key, b"first", "first")
        },
    )
    .await;
    source
        .prepare_source_range(Request::new(PrepareRangeRequest {
            range: Some(range_covering(token)),
        }))
        .await
        .unwrap();

    // The request says "keep", which a follower or destination has no way to
    // resolve the same way the owner did, so the journal must carry the result.
    let kept = write(&source, put(&key, b"second", "second")).await;
    assert!(kept.error.is_none());
    let page = source
        .read_changelog_page(Request::new(ChangelogPageRequest {
            change_id: "change-1".into(),
            range_id: "range-1".into(),
            after_watermark: 0,
            max_bytes: u64::MAX,
        }))
        .await
        .unwrap()
        .into_inner();
    let record = page.records[0].record.clone().unwrap();
    assert_eq!(record.expires_at_unix_millis, Some(expires_at));
    assert!(
        record.remaining_retention_millis > 0,
        "the carried fingerprint matches the owner's receipt"
    );

    // The destination stages the receipt of the request, not of the record it
    // resolved to, so a retry of the same "keep" write is still recognized.
    destination
        .prepare_destination_range(Request::new(PrepareRangeRequest {
            range: Some(range_covering(token)),
        }))
        .await
        .unwrap();
    destination
        .apply_migration_batch(Request::new(ApplyMigrationBatchRequest {
            change_id: "change-1".into(),
            range_id: "range-1".into(),
            snapshot_records: Vec::new(),
            journal_records: page.records,
        }))
        .await
        .unwrap();
    let state = destination.state.read().await;
    let staged = state
        .destinations
        .values()
        .next()
        .unwrap()
        .dedup
        .get("second")
        .expect("the receipt was staged");
    assert_eq!(
        staged.record.fingerprint,
        mutation_fingerprint(&key, MutationShape::value(b"second", false, None)).to_vec()
    );
}

#[tokio::test]
async fn a_kept_deadline_is_not_inherited_from_an_expired_record() {
    let service = service();
    write(
        &service,
        PutRequest {
            expires_at_unix_millis: Some(now_unix_millis()),
            ..put(b"key", b"expired", "first")
        },
    )
    .await;
    // The record is past its deadline but no sweep has removed it yet.
    let rewritten = write(&service, put(b"key", b"fresh", "second")).await;
    assert!(rewritten.error.is_none());
    let read = get(&service, b"key").await;
    assert!(read.error.is_none(), "an acknowledged write read as absent");
    assert_eq!(read.value, b"fresh");
    assert_eq!(read.expires_at_unix_millis, None);
}

#[tokio::test]
async fn rewriting_a_key_keeps_one_expiry_entry_for_it() {
    let service = service();
    // A hot key rewritten with a long TTL, as `SET k v EX` or `INCR` on a key
    // with a deadline would do, must not leave one entry behind per write.
    for index in 0..100 {
        write(
            &service,
            PutRequest {
                expires_at_unix_millis: Some(deadline_in(600 + index)),
                ..put(b"hot", b"value", &format!("write-{index}"))
            },
        )
        .await;
        write(&service, put(b"hot", b"kept", &format!("keep-{index}"))).await;
    }
    let state = service.state.read().await;
    assert_eq!(state.record_expirations.len(), 1);
    assert_eq!(
        state
            .record_expirations
            .first()
            .map(|(deadline, _)| *deadline),
        state.records[b"hot".as_slice()].expires_at_unix_millis
    );
    state.debug_assert_index();
}

#[tokio::test]
async fn a_sweep_pass_examines_at_most_its_limit() {
    let service = service();
    {
        let mut state = service.state.write().await;
        for index in 0..10_u32 {
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
                    expires_at_unix_millis: Some(now_unix_millis() - 1),
                },
            );
        }
    }
    let mut state = service.state.write().await;
    assert_eq!(sweep_expired_records(&mut state, now_unix_millis(), 4), 4);
    assert_eq!(state.records.len(), 6);
    assert_eq!(sweep_expired_records(&mut state, now_unix_millis(), 100), 6);
    state.debug_assert_index();
}

#[tokio::test]
async fn an_expired_record_outlives_the_migration_that_copied_it() {
    let topology = replication_topology(2);
    let source = service_for("node-1", topology.clone());
    let destination = service_for("node-2", topology.clone());
    let key = key_with_placement(&topology, |replicas| replicas[0] == "node-1");
    let token = topology.key_token(&key);
    write(
        &source,
        PutRequest {
            expires_at_unix_millis: Some(deadline_in(600)),
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
    destination
        .prepare_destination_range(Request::new(PrepareRangeRequest {
            range: Some(range_covering(token)),
        }))
        .await
        .unwrap();
    destination
        .apply_migration_batch(Request::new(ApplyMigrationBatchRequest {
            change_id: "change-1".into(),
            range_id: "range-1".into(),
            snapshot_records: page.records,
            journal_records: Vec::new(),
        }))
        .await
        .unwrap();

    // The record's deadline passes after the snapshot reached the destination.
    let later = deadline_in(601);
    {
        let mut state = source.state.write().await;
        assert_eq!(
            sweep_expired_records(&mut state, later, EXPIRY_SWEEP_LIMIT),
            0
        );
        assert!(state.records.contains_key(key.as_slice()));
        state.debug_assert_index();
    }
    let control = || RangeControlRequest {
        change_id: "change-1".into(),
        range_id: "range-1".into(),
    };
    let source_digest = source
        .source_range_digest(Request::new(control()))
        .await
        .unwrap()
        .into_inner();
    let destination_digest = destination
        .destination_range_digest(Request::new(control()))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(source_digest.record_count, destination_digest.record_count);
    assert_eq!(source_digest.digest, destination_digest.digest);

    // Once the migration ends the record is reclaimed like any other.
    source
        .abort_range_migration(Request::new(control()))
        .await
        .unwrap();
    let mut state = source.state.write().await;
    assert_eq!(
        sweep_expired_records(&mut state, later, EXPIRY_SWEEP_LIMIT),
        1
    );
    assert!(!state.records.contains_key(key.as_slice()));
    state.debug_assert_index();
}

#[tokio::test]
async fn ending_one_migration_releases_only_its_own_deferred_records() {
    let topology = replication_topology(2);
    let source = service_for("node-1", topology.clone());
    let owned: Vec<Vec<u8>> = (0..1_000)
        .map(|index| format!("deferred-{index}").into_bytes())
        .filter(|key| topology.owner(key).unwrap().node_id == "node-1")
        .take(2)
        .collect();
    for (index, key) in owned.iter().enumerate() {
        write(
            &source,
            PutRequest {
                expires_at_unix_millis: Some(deadline_in(600)),
                ..put(key, b"value", &format!("write-{index}"))
            },
        )
        .await;
        let token = topology.key_token(key);
        source
            .prepare_source_range(Request::new(PrepareRangeRequest {
                range: Some(proto::RangeSpec {
                    range_id: format!("range-{index}"),
                    ..range_covering(token)
                }),
            }))
            .await
            .unwrap();
    }
    let later = deadline_in(601);
    {
        let mut state = source.state.write().await;
        assert_eq!(
            sweep_expired_records(&mut state, later, EXPIRY_SWEEP_LIMIT),
            0
        );
        assert!(state.record_expirations.is_empty());
        state.debug_assert_index();
    }
    source
        .abort_range_migration(Request::new(RangeControlRequest {
            change_id: "change-1".into(),
            range_id: "range-0".into(),
        }))
        .await
        .unwrap();
    let mut state = source.state.write().await;
    assert_eq!(
        state.record_expirations.len(),
        1,
        "another range's record was requeued"
    );
    assert_eq!(
        sweep_expired_records(&mut state, later, EXPIRY_SWEEP_LIMIT),
        1
    );
    assert!(!state.records.contains_key(owned[0].as_slice()));
    assert!(state.records.contains_key(owned[1].as_slice()));
    state.debug_assert_index();
}
