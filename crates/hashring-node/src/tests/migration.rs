use super::*;

#[tokio::test]
async fn prepare_rejects_conflicting_retry_specification() {
    let service = service();
    service
        .prepare_source_range(Request::new(PrepareRangeRequest {
            range: Some(range(10)),
        }))
        .await
        .unwrap();
    let error = service
        .prepare_source_range(Request::new(PrepareRangeRequest {
            range: Some(range(11)),
        }))
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
}

#[tokio::test]
async fn paused_source_range_keeps_reads_available_and_blocks_writes() {
    let service = service();
    service
        .put(Request::new(PutRequest {
            key: b"key".to_vec(),
            value: b"value".to_vec(),
            topology_epoch: 1,
            request_id: "put-1".into(),
        }))
        .await
        .unwrap();
    service
        .prepare_source_range(Request::new(PrepareRangeRequest {
            range: Some(range(0)),
        }))
        .await
        .unwrap();
    let control = RangeControlRequest {
        change_id: "change-1".into(),
        range_id: "range-1".into(),
    };
    service
        .pause_range_writes(Request::new(control.clone()))
        .await
        .unwrap();
    let readable = service
        .get(Request::new(GetRequest {
            key: b"key".to_vec(),
            topology_epoch: 1,
            request_id: "get-1".into(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(readable.error.is_none());
    assert_eq!(readable.value, b"value");
    let paused_put = service
        .put(Request::new(PutRequest {
            key: b"key".to_vec(),
            value: b"replacement".to_vec(),
            topology_epoch: 1,
            request_id: "put-2".into(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(paused_put.error.unwrap().code, ErrorCode::RangeBusy as i32);
    let paused_delete = service
        .delete(Request::new(DeleteRequest {
            key: b"key".to_vec(),
            topology_epoch: 1,
            request_id: "delete-1".into(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        paused_delete.error.unwrap().code,
        ErrorCode::RangeBusy as i32
    );
    service
        .abort_range_migration(Request::new(control))
        .await
        .unwrap();
    let resumed = service
        .put(Request::new(PutRequest {
            key: b"key".to_vec(),
            value: b"replacement".to_vec(),
            topology_epoch: 1,
            request_id: "put-3".into(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(resumed.error.is_none());
}

#[tokio::test]
async fn interrupted_repair_cleanup_unpauses_only_repair_ranges() {
    let service = service();
    let mut repair = range(0);
    repair.change_id = "repair-interrupted".into();
    repair.range_id = "repair-range".into();
    service
        .prepare_source_range(Request::new(PrepareRangeRequest {
            range: Some(repair),
        }))
        .await
        .unwrap();
    service
        .pause_range_writes(Request::new(RangeControlRequest {
            change_id: "repair-interrupted".into(),
            range_id: "repair-range".into(),
        }))
        .await
        .unwrap();
    let busy = service
        .put(Request::new(PutRequest {
            key: b"key".to_vec(),
            value: b"value".to_vec(),
            topology_epoch: 1,
            request_id: "before-cleanup".into(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(busy.error.unwrap().code, ErrorCode::RangeBusy as i32);
    service
        .abort_replica_repairs(Request::new(proto::AbortReplicaRepairsRequest {
            topology_epoch: 1,
        }))
        .await
        .unwrap();
    assert!(service.state.read().await.sources.is_empty());
    let write = service
        .put(Request::new(PutRequest {
            key: b"key".to_vec(),
            value: b"value".to_vec(),
            topology_epoch: 1,
            request_id: "after-cleanup".into(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(write.error.is_none());
}

#[tokio::test]
async fn committed_destination_fences_writes_until_idempotent_activation() {
    let mut destination = service();
    destination.node_id = "node-2".into();
    destination
        .prepare_destination_range(Request::new(PrepareRangeRequest {
            range: Some(range(0)),
        }))
        .await
        .unwrap();
    destination
        .apply_migration_batch(Request::new(ApplyMigrationBatchRequest {
            change_id: "change-1".into(),
            range_id: "range-1".into(),
            snapshot_records: vec![MigrationRecord {
                key: b"key".to_vec(),
                value: b"migrated".to_vec(),
                version: Some(RecordVersion {
                    topology_epoch: 1,
                    owner_sequence: 1,
                    owner_node_id: "node-1".into(),
                }),
                deleted: false,
                mutation_id: String::new(),
                remaining_window_millis: 0,
            }],
            journal_records: Vec::new(),
        }))
        .await
        .unwrap();
    let control = RangeControlRequest {
        change_id: "change-1".into(),
        range_id: "range-1".into(),
    };
    let error = destination
        .activate_destination_range(Request::new(control.clone()))
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    let error = destination
        .activate_destination_range(Request::new(RangeControlRequest {
            change_id: "change-1".into(),
            range_id: "unknown".into(),
        }))
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::NotFound);

    destination
        .commit_destination_range(Request::new(control.clone()))
        .await
        .unwrap();
    destination
        .commit_destination_range(Request::new(control.clone()))
        .await
        .unwrap();
    let topology = TopologySnapshot::new_with_config(
        2,
        42,
        8,
        vec![Member {
            node_id: "node-2".into(),
            endpoint: "http://127.0.0.1:5002".into(),
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
    destination
        .install_topology(Request::new(InstallTopologyRequest {
            topology: Some((&topology).into()),
            require_lease: false,
        }))
        .await
        .unwrap();
    destination.state.write().await.lease =
        Some((2, Instant::now() + std::time::Duration::from_secs(3600)));

    let readable = destination
        .get(Request::new(GetRequest {
            key: b"key".to_vec(),
            topology_epoch: 2,
            request_id: "get-1".into(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(readable.error.is_none());
    assert_eq!(readable.value, b"migrated");
    let fenced_put = destination
        .put(Request::new(PutRequest {
            key: b"key".to_vec(),
            value: b"new".to_vec(),
            topology_epoch: 2,
            request_id: "put-1".into(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(fenced_put.error.unwrap().code, ErrorCode::RangeBusy as i32);
    let fenced_delete = destination
        .delete(Request::new(DeleteRequest {
            key: b"key".to_vec(),
            topology_epoch: 2,
            request_id: "delete-1".into(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        fenced_delete.error.unwrap().code,
        ErrorCode::RangeBusy as i32
    );

    destination
        .activate_destination_range(Request::new(control.clone()))
        .await
        .unwrap();
    destination
        .activate_destination_range(Request::new(control.clone()))
        .await
        .unwrap();
    destination
        .put(Request::new(PutRequest {
            key: b"key".to_vec(),
            value: b"new".to_vec(),
            topology_epoch: 2,
            request_id: "put-2".into(),
        }))
        .await
        .unwrap();
    destination
        .commit_destination_range(Request::new(control))
        .await
        .unwrap();
    let after_retry = destination
        .get(Request::new(GetRequest {
            key: b"key".to_vec(),
            topology_epoch: 2,
            request_id: "get-2".into(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(after_retry.value, b"new");
    let deleted = destination
        .delete(Request::new(DeleteRequest {
            key: b"key".to_vec(),
            topology_epoch: 2,
            request_id: "delete-2".into(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(deleted.error.is_none());
}

#[tokio::test]
async fn owner_retries_reuse_original_result_and_reject_mutation_id_conflicts() {
    let service = service();
    let first = PutRequest {
        key: b"key".to_vec(),
        value: b"original".to_vec(),
        topology_epoch: 1,
        request_id: "same-id".into(),
    };
    let original = service
        .put(Request::new(first.clone()))
        .await
        .unwrap()
        .into_inner();
    assert!(original.error.is_none());
    let retry = service
        .put(Request::new(first.clone()))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(retry.version, original.version);
    assert_eq!(service.state.read().await.next_sequence, 1);

    let mut conflicting = first;
    conflicting.value = b"different".to_vec();
    let conflict = service
        .put(Request::new(conflicting))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        conflict.error.unwrap().code,
        ErrorCode::MutationIdConflict as i32
    );
    let delete_conflict = service
        .delete(Request::new(DeleteRequest {
            key: b"key".to_vec(),
            topology_epoch: 1,
            request_id: "same-id".into(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        delete_conflict.error.unwrap().code,
        ErrorCode::MutationIdConflict as i32
    );
    assert_eq!(service.state.read().await.next_sequence, 1);
    assert_eq!(
        service.state.read().await.records[b"key".as_slice()]
            .value
            .as_ref(),
        b"original"
    );

    let delete = DeleteRequest {
        key: b"key".to_vec(),
        topology_epoch: 1,
        request_id: "delete-id".into(),
    };
    assert!(
        service
            .delete(Request::new(delete.clone()))
            .await
            .unwrap()
            .into_inner()
            .error
            .is_none()
    );
    assert!(
        service
            .delete(Request::new(delete))
            .await
            .unwrap()
            .into_inner()
            .error
            .is_none()
    );
    assert_eq!(service.state.read().await.next_sequence, 2);
}

#[tokio::test]
async fn dedup_budget_rejects_before_apply_and_expired_records_free_capacity() {
    let mut service = service();
    service.max_dedup_bytes = dedup_retained_bytes("first", b"key-1".len(), "node-1".len());
    let first = PutRequest {
        key: b"key-1".to_vec(),
        value: b"one".to_vec(),
        topology_epoch: 1,
        request_id: "first".into(),
    };
    assert!(
        service
            .put(Request::new(first))
            .await
            .unwrap()
            .into_inner()
            .error
            .is_none()
    );
    let second = PutRequest {
        key: b"key-2".to_vec(),
        value: b"two".to_vec(),
        topology_epoch: 1,
        request_id: "other".into(),
    };
    let rejected = service
        .put(Request::new(second.clone()))
        .await
        .unwrap()
        .into_inner();
    let error = rejected.error.unwrap();
    assert_eq!(error.code, ErrorCode::ResourceExhausted as i32);
    assert!(error.retry_after_millis > 0);
    assert!(error.retry_after_millis <= 60_000);
    let pressure = service
        .get_process_info(Request::new(proto::Empty {}))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(pressure.owner_dedup_rejections, 1);
    assert_eq!(
        pressure.dedup_capacity_bytes,
        service.max_dedup_bytes as u64
    );
    assert!(pressure.dedup_peak_bytes > 0);
    assert_eq!(service.state.read().await.next_sequence, 1);
    assert!(
        !service
            .state
            .read()
            .await
            .records
            .contains_key(b"key-2".as_slice())
    );
    let mut state = service.state.write().await;
    state.dedup.get_mut("first").unwrap().expires_at =
        Instant::now() - std::time::Duration::from_millis(1);
    state.dedup_expirations.push(Reverse((
        Instant::now() - std::time::Duration::from_millis(1),
        "first".into(),
    )));
    drop(state);
    assert!(
        service
            .put(Request::new(second))
            .await
            .unwrap()
            .into_inner()
            .error
            .is_none()
    );
    assert_eq!(service.state.read().await.next_sequence, 2);
}

#[tokio::test]
async fn migration_carries_snapshot_and_journal_mutation_ids() {
    let source = service();
    let before = PutRequest {
        key: b"before".to_vec(),
        value: b"one".to_vec(),
        topology_epoch: 1,
        request_id: "before-id".into(),
    };
    let first_version = source
        .put(Request::new(before.clone()))
        .await
        .unwrap()
        .into_inner()
        .version
        .unwrap();
    source
        .prepare_source_range(Request::new(PrepareRangeRequest {
            range: Some(range(0)),
        }))
        .await
        .unwrap();
    let after = PutRequest {
        key: b"after".to_vec(),
        value: b"two".to_vec(),
        topology_epoch: 1,
        request_id: "after-id".into(),
    };
    let second_version = source
        .put(Request::new(after.clone()))
        .await
        .unwrap()
        .into_inner()
        .version
        .unwrap();
    let snapshot = source
        .read_snapshot_page(Request::new(SnapshotPageRequest {
            change_id: "change-1".into(),
            range_id: "range-1".into(),
            cursor: 0,
            max_bytes: MAX_MIGRATION_PAGE_BYTES as u64,
        }))
        .await
        .unwrap()
        .into_inner();
    let dedup = source
        .read_dedup_snapshot_page(Request::new(SnapshotPageRequest {
            change_id: "change-1".into(),
            range_id: "range-1".into(),
            cursor: 0,
            max_bytes: MAX_MIGRATION_PAGE_BYTES as u64,
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(dedup.records.len(), 1);
    assert_eq!(dedup.records[0].mutation_id, "before-id");
    let journal = source
        .read_changelog_page(Request::new(ChangelogPageRequest {
            change_id: "change-1".into(),
            range_id: "range-1".into(),
            after_watermark: 0,
            max_bytes: MAX_MIGRATION_PAGE_BYTES as u64,
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(journal.records.len(), 1);
    assert_eq!(
        journal.records[0].record.as_ref().unwrap().mutation_id,
        "after-id"
    );

    let mut destination = service();
    destination.node_id = "node-2".into();
    destination
        .prepare_destination_range(Request::new(PrepareRangeRequest {
            range: Some(range(0)),
        }))
        .await
        .unwrap();
    destination
        .apply_migration_batch(Request::new(ApplyMigrationBatchRequest {
            change_id: "change-1".into(),
            range_id: "range-1".into(),
            snapshot_records: snapshot.records,
            journal_records: Vec::new(),
        }))
        .await
        .unwrap();
    destination
        .apply_dedup_batch(Request::new(ApplyDedupBatchRequest {
            change_id: "change-1".into(),
            range_id: "range-1".into(),
            records: dedup.records,
        }))
        .await
        .unwrap();
    destination
        .apply_migration_batch(Request::new(ApplyMigrationBatchRequest {
            change_id: "change-1".into(),
            range_id: "range-1".into(),
            snapshot_records: Vec::new(),
            journal_records: journal.records,
        }))
        .await
        .unwrap();
    destination
        .commit_destination_range(Request::new(RangeControlRequest {
            change_id: "change-1".into(),
            range_id: "range-1".into(),
        }))
        .await
        .unwrap();
    destination.node_id = "node-1".into(); // The test topology still routes ownership to node-1.
    let retry_before = destination
        .put(Request::new(before))
        .await
        .unwrap()
        .into_inner();
    let retry_after = destination
        .put(Request::new(after))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(retry_before.version, Some(first_version));
    assert_eq!(retry_after.version, Some(second_version));
    assert_eq!(destination.state.read().await.next_sequence, 0);
}

#[tokio::test]
async fn expired_staged_ids_free_migration_budget_before_commit() {
    let mut destination = service();
    destination.node_id = "node-2".into();
    destination.max_dedup_bytes =
        staged_dedup_retained_bytes("first", b"key-1".len(), "node-1".len()).max(
            dedup_retained_bytes("other", b"key-2".len(), "node-1".len()),
        );
    destination
        .prepare_destination_range(Request::new(PrepareRangeRequest {
            range: Some(range(0)),
        }))
        .await
        .unwrap();
    let make_record = |id: &str, key: &[u8]| DeduplicationRecord {
        mutation_id: id.into(),
        key: key.to_vec(),
        fingerprint: mutation_fingerprint(key, b"value", false).to_vec(),
        version: Some(RecordVersion {
            topology_epoch: 1,
            owner_sequence: 1,
            owner_node_id: "node-1".into(),
        }),
        deleted: false,
        remaining_window_millis: 60_000,
    };
    let request = |record| ApplyDedupBatchRequest {
        change_id: "change-1".into(),
        range_id: "range-1".into(),
        records: vec![record],
    };
    destination
        .apply_dedup_batch(Request::new(request(make_record("first", b"key-1"))))
        .await
        .unwrap();
    destination
        .state
        .write()
        .await
        .destinations
        .get_mut(&("change-1".into(), "range-1".into()))
        .unwrap()
        .dedup
        .get_mut("first")
        .unwrap()
        .expires_at = Instant::now() - std::time::Duration::from_millis(1);
    destination
        .apply_dedup_batch(Request::new(request(make_record("other", b"key-2"))))
        .await
        .unwrap();
    let staged_expiry = {
        let mut state = destination.state.write().await;
        let entry = state
            .destinations
            .get_mut(&("change-1".into(), "range-1".into()))
            .unwrap()
            .dedup
            .get_mut("other")
            .unwrap();
        entry.expires_at = Instant::now() + std::time::Duration::from_secs(5);
        entry.expires_at
    };
    destination
        .commit_destination_range(Request::new(RangeControlRequest {
            change_id: "change-1".into(),
            range_id: "range-1".into(),
        }))
        .await
        .unwrap();
    let state = destination.state.read().await;
    assert!(!state.dedup.contains_key("first"));
    assert_eq!(state.dedup["other"].expires_at, staged_expiry);
}

#[tokio::test]
async fn delete_is_idempotent_and_put_restores_an_empty_value() {
    let service = service();
    service
        .put(Request::new(PutRequest {
            key: b"key".to_vec(),
            value: Vec::new(),
            topology_epoch: 1,
            request_id: "put-1".into(),
        }))
        .await
        .unwrap();

    let first = service
        .delete(Request::new(DeleteRequest {
            key: b"key".to_vec(),
            topology_epoch: 1,
            request_id: "delete-1".into(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(first.error.is_none());
    assert_eq!(service.state.read().await.next_sequence, 2);
    assert!(service.state.read().await.records[b"key".as_slice()].deleted);

    let second = service
        .delete(Request::new(DeleteRequest {
            key: b"key".to_vec(),
            topology_epoch: 1,
            request_id: "delete-2".into(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(second.error.is_none());
    assert_eq!(service.state.read().await.next_sequence, 3);

    service
        .put(Request::new(PutRequest {
            key: b"key".to_vec(),
            value: b"restored".to_vec(),
            topology_epoch: 1,
            request_id: "put-2".into(),
        }))
        .await
        .unwrap();
    let restored = service
        .get(Request::new(GetRequest {
            key: b"key".to_vec(),
            topology_epoch: 1,
            request_id: "get-1".into(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(restored.value, b"restored");
    assert_eq!(restored.version.unwrap().owner_sequence, 4);
}

#[tokio::test]
async fn delete_validates_empty_and_oversized_keys() {
    let service = service();
    let empty = service
        .delete(Request::new(DeleteRequest {
            key: Vec::new(),
            topology_epoch: 1,
            request_id: "delete-empty".into(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(empty.error.unwrap().code, ErrorCode::InvalidArgument as i32);

    let oversized = service
        .delete(Request::new(DeleteRequest {
            key: vec![0; DEFAULT_MAX_KEY_BYTES + 1],
            topology_epoch: 1,
            request_id: "delete-oversized".into(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(oversized.error.unwrap().code, ErrorCode::TooLarge as i32);

    let oversized_mutation_id = service
        .delete(Request::new(DeleteRequest {
            key: b"key".to_vec(),
            topology_epoch: 1,
            request_id: "x".repeat(MAX_MUTATION_ID_BYTES + 1),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        oversized_mutation_id.error.unwrap().code,
        ErrorCode::InvalidArgument as i32
    );
    assert_eq!(service.state.read().await.next_sequence, 0);
}

#[tokio::test]
async fn snapshot_and_changelog_preserve_a_deleted_key_tombstone() {
    let source = service();
    source
        .put(Request::new(PutRequest {
            key: b"key".to_vec(),
            value: b"value".to_vec(),
            topology_epoch: 1,
            request_id: "put-1".into(),
        }))
        .await
        .unwrap();
    source
        .prepare_source_range(Request::new(PrepareRangeRequest {
            range: Some(range(0)),
        }))
        .await
        .unwrap();
    source
        .delete(Request::new(DeleteRequest {
            key: b"key".to_vec(),
            topology_epoch: 1,
            request_id: "delete-1".into(),
        }))
        .await
        .unwrap();

    let snapshot = source
        .read_snapshot_page(Request::new(SnapshotPageRequest {
            change_id: "change-1".into(),
            range_id: "range-1".into(),
            cursor: 0,
            max_bytes: MAX_MIGRATION_PAGE_BYTES as u64,
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(snapshot.records.len(), 1);
    assert!(snapshot.records[0].deleted);
    assert_eq!(snapshot.next_cursor, 1);
    assert!(snapshot.done);

    let changelog = source
        .read_changelog_page(Request::new(ChangelogPageRequest {
            change_id: "change-1".into(),
            range_id: "range-1".into(),
            after_watermark: 0,
            max_bytes: MAX_MIGRATION_PAGE_BYTES as u64,
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(changelog.current_watermark, 1);
    assert!(changelog.records[0].record.as_ref().unwrap().deleted);

    let digest = source
        .source_range_digest(Request::new(RangeControlRequest {
            change_id: "change-1".into(),
            range_id: "range-1".into(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(digest.record_count, 1);
    assert_eq!(digest.changelog_watermark, 1);

    let mut destination = service();
    destination.node_id = "node-2".into();
    destination
        .prepare_destination_range(Request::new(PrepareRangeRequest {
            range: Some(range(0)),
        }))
        .await
        .unwrap();
    destination
        .apply_migration_batch(Request::new(ApplyMigrationBatchRequest {
            change_id: "change-1".into(),
            range_id: "range-1".into(),
            snapshot_records: snapshot.records,
            journal_records: Vec::new(),
        }))
        .await
        .unwrap();
    let destination_digest = destination
        .destination_range_digest(Request::new(RangeControlRequest {
            change_id: "change-1".into(),
            range_id: "range-1".into(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(destination_digest.digest, digest.digest);
    destination
        .commit_destination_range(Request::new(RangeControlRequest {
            change_id: "change-1".into(),
            range_id: "range-1".into(),
        }))
        .await
        .unwrap();
    destination.node_id = "node-1".into();
    let read = destination
        .get(Request::new(GetRequest {
            key: b"key".to_vec(),
            topology_epoch: 1,
            request_id: "get-deleted".into(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(read.error.unwrap().code, ErrorCode::NotFound as i32);
}

#[tokio::test]
async fn deletion_replay_and_commit_remove_stale_destination_data() {
    let mut destination = service();
    destination.node_id = "node-2".into();
    destination.state.write().await.records.insert(
        b"key".to_vec(),
        Record {
            value: Arc::from(b"stale".as_slice()),
            version: RecordVersion {
                topology_epoch: 0,
                owner_sequence: 1,
                owner_node_id: "node-2".into(),
            },
            deleted: false,
        },
    );
    destination
        .prepare_destination_range(Request::new(PrepareRangeRequest {
            range: Some(range(0)),
        }))
        .await
        .unwrap();
    let deletion = JournalRecord {
        watermark: 1,
        record: Some(MigrationRecord {
            key: b"key".to_vec(),
            value: Vec::new(),
            version: Some(RecordVersion {
                topology_epoch: 1,
                owner_sequence: 2,
                owner_node_id: "node-1".into(),
            }),
            deleted: true,
            mutation_id: String::new(),
            remaining_window_millis: 0,
        }),
    };
    let batch = ApplyMigrationBatchRequest {
        change_id: "change-1".into(),
        range_id: "range-1".into(),
        snapshot_records: Vec::new(),
        journal_records: vec![deletion],
    };
    destination
        .apply_migration_batch(Request::new(batch.clone()))
        .await
        .unwrap();
    destination
        .apply_migration_batch(Request::new(batch))
        .await
        .unwrap();

    let control = RangeControlRequest {
        change_id: "change-1".into(),
        range_id: "range-1".into(),
    };
    let digest = destination
        .destination_range_digest(Request::new(control.clone()))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(digest.record_count, 1);
    assert_eq!(digest.changelog_watermark, 1);
    destination
        .commit_destination_range(Request::new(control))
        .await
        .unwrap();
    assert!(destination.state.read().await.records[b"key".as_slice()].deleted);
}

#[tokio::test]
async fn migration_replay_preserves_delete_and_put_order() {
    let mut destination = service();
    destination.node_id = "node-2".into();
    let version = |owner_sequence| RecordVersion {
        topology_epoch: 1,
        owner_sequence,
        owner_node_id: "node-1".into(),
    };
    let deletion = |watermark, owner_sequence| JournalRecord {
        watermark,
        record: Some(MigrationRecord {
            key: b"key".to_vec(),
            value: Vec::new(),
            version: Some(version(owner_sequence)),
            deleted: true,
            mutation_id: String::new(),
            remaining_window_millis: 0,
        }),
    };
    let put = |watermark, owner_sequence, value: &[u8]| JournalRecord {
        watermark,
        record: Some(MigrationRecord {
            key: b"key".to_vec(),
            value: value.to_vec(),
            version: Some(version(owner_sequence)),
            deleted: false,
            mutation_id: String::new(),
            remaining_window_millis: 0,
        }),
    };

    destination
        .prepare_destination_range(Request::new(PrepareRangeRequest {
            range: Some(range(0)),
        }))
        .await
        .unwrap();
    destination
        .apply_migration_batch(Request::new(ApplyMigrationBatchRequest {
            change_id: "change-1".into(),
            range_id: "range-1".into(),
            snapshot_records: Vec::new(),
            journal_records: vec![deletion(1, 1), put(2, 2, b"restored")],
        }))
        .await
        .unwrap();
    let key = ("change-1".to_owned(), "range-1".to_owned());
    assert_eq!(
        destination.state.read().await.destinations[&key].records[b"key".as_slice()]
            .value
            .as_ref(),
        b"restored"
    );

    destination
        .abort_range_migration(Request::new(RangeControlRequest {
            change_id: "change-1".into(),
            range_id: "range-1".into(),
        }))
        .await
        .unwrap();
    destination
        .prepare_destination_range(Request::new(PrepareRangeRequest {
            range: Some(range(0)),
        }))
        .await
        .unwrap();
    destination
        .apply_migration_batch(Request::new(ApplyMigrationBatchRequest {
            change_id: "change-1".into(),
            range_id: "range-1".into(),
            snapshot_records: Vec::new(),
            journal_records: vec![put(1, 3, b"temporary"), deletion(2, 4)],
        }))
        .await
        .unwrap();
    assert!(destination.state.read().await.destinations[&key].records[b"key".as_slice()].deleted);
}

#[tokio::test]
async fn stop_is_fenced_to_the_process_instance() {
    let service = service();
    let receiver = service.shutdown_receiver();
    let error = service
        .stop(Request::new(StopRequest {
            node_id: "node-1".into(),
            process_instance_id: "replacement".into(),
        }))
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(!*receiver.borrow());

    service
        .prepare_stop(Request::new(StopRequest {
            node_id: "node-1".into(),
            process_instance_id: "instance-1".into(),
        }))
        .await
        .unwrap();
    service
        .stop(Request::new(StopRequest {
            node_id: "node-1".into(),
            process_instance_id: "instance-1".into(),
        }))
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(120)).await;
    assert!(*receiver.borrow());
}

#[tokio::test]
async fn journal_budget_is_aggregate_across_ranges() {
    let mut service = service();
    service.max_journal_bytes = 200;
    let mut first = range(0);
    first.range_id = "range-1".into();
    let mut second = first.clone();
    second.range_id = "range-2".into();
    service
        .prepare_source_range(Request::new(PrepareRangeRequest { range: Some(first) }))
        .await
        .unwrap();
    service
        .prepare_source_range(Request::new(PrepareRangeRequest {
            range: Some(second),
        }))
        .await
        .unwrap();
    let response = service
        .put(Request::new(PutRequest {
            key: b"key".to_vec(),
            value: b"value".to_vec(),
            topology_epoch: 1,
            request_id: "put-1".into(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        response.error.unwrap().code,
        ErrorCode::ResourceExhausted as i32
    );
    assert_eq!(service.state.read().await.journal_bytes_total, 0);
}

#[tokio::test]
async fn delete_leaves_the_value_when_the_journal_budget_is_full() {
    let mut service = service();
    service
        .put(Request::new(PutRequest {
            key: b"key".to_vec(),
            value: b"value".to_vec(),
            topology_epoch: 1,
            request_id: "put-1".into(),
        }))
        .await
        .unwrap();
    service.max_journal_bytes = 1;
    service
        .prepare_source_range(Request::new(PrepareRangeRequest {
            range: Some(range(0)),
        }))
        .await
        .unwrap();
    let response = service
        .delete(Request::new(DeleteRequest {
            key: b"key".to_vec(),
            topology_epoch: 1,
            request_id: "delete-1".into(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        response.error.unwrap().code,
        ErrorCode::ResourceExhausted as i32
    );
    assert_eq!(
        service
            .state
            .read()
            .await
            .records
            .get(b"key".as_slice())
            .unwrap()
            .value
            .as_ref(),
        b"value"
    );
    assert_eq!(service.state.read().await.next_sequence, 1);
}
