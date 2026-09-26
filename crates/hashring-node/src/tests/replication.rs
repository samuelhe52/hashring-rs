use super::*;

#[tokio::test]
async fn replication_progress_reports_owner_ack_only_when_known() {
    let topology = replication_topology(3);
    let service = service_for("node-1", topology.clone());
    let request = ReplicationProgressRequest {
        topology_epoch: topology.epoch,
        owner_node_id: "node-1".into(),
        follower_node_id: "node-2".into(),
    };
    {
        let mut state = service.state.write().await;
        state
            .owner_stream_sequences
            .insert((topology.epoch, "node-2".into()), 3);
    }
    let unknown = service
        .get_replication_progress(Request::new(request.clone()))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(unknown.stream_sequence, 3);
    assert!(!unknown.last_ack_known);

    let (ack, _) = watch::channel(2);
    service
        .state
        .write()
        .await
        .ack_progress
        .insert((topology.epoch, "node-2".into()), ack);
    let known = service
        .get_replication_progress(Request::new(request))
        .await
        .unwrap()
        .into_inner();
    assert!(known.last_ack_known);
    assert_eq!(known.last_ack_sequence, 2);
}

#[tokio::test]
async fn follower_applies_only_contiguous_authorized_replication() {
    let topology = replication_topology(3);
    let key = key_with_placement(&topology, |replicas| replicas[1..].contains(&"node-2"));
    let service = service_for("node-2", topology.clone());
    let put = replication_entry(&topology, key.clone(), 1, 10, b"value", false);

    let applied = service
        .replicate_mutation(Request::new(put.clone()))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(applied.applied_stream_sequence, 1);
    assert_eq!(
        service.state.read().await.records[&key].value.as_ref(),
        b"value"
    );
    let client_read = service
        .get(Request::new(GetRequest {
            key: key.clone(),
            topology_epoch: topology.epoch,
            request_id: "get".into(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(client_read.error.unwrap().code, ErrorCode::Moved as i32);

    let duplicate = service
        .replicate_mutation(Request::new(put.clone()))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(duplicate.applied_stream_sequence, 1);
    let mut conflict = put;
    conflict.value = b"different".to_vec();
    assert_eq!(
        service
            .replicate_mutation(Request::new(conflict))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::AlreadyExists
    );

    let gap = replication_entry(&topology, key.clone(), 3, 12, b"later", false);
    assert_eq!(
        service
            .replicate_mutation(Request::new(gap))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Aborted
    );
    let stale_version = replication_entry(&topology, key.clone(), 2, 10, b"later", false);
    assert_eq!(
        service
            .replicate_mutation(Request::new(stale_version))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );

    let delete = replication_entry(&topology, key.clone(), 2, 11, b"", true);
    let applied = service
        .replicate_mutation(Request::new(delete))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(applied.applied_stream_sequence, 2);
    assert!(service.state.read().await.records[&key].deleted);
}

#[tokio::test]
async fn full_stream_checkpoint_repairs_a_gap_without_replaying_old_entries() {
    let topology = replication_topology(3);
    let follower = service_for("node-2", topology.clone());
    let ranges: Vec<_> = topology
        .derived_ranges()
        .unwrap()
        .into_iter()
        .filter(|range| {
            range.owner_node_id == "node-1"
                && range.follower_node_ids.iter().any(|node| node == "node-2")
        })
        .collect();
    assert!(ranges.len() > 1);
    let controls: Vec<_> = ranges
        .iter()
        .enumerate()
        .map(|(index, range)| {
            let control = RangeControlRequest {
                change_id: "repair-test".into(),
                range_id: format!("range-{index}"),
            };
            let spec = proto::RangeSpec {
                change_id: control.change_id.clone(),
                range_id: control.range_id.clone(),
                start_exclusive: range.start_exclusive,
                end_inclusive: range.end_inclusive,
                source_node_id: "node-1".into(),
                destination_node_id: "node-2".into(),
            };
            (control, spec)
        })
        .collect();
    for (control, spec) in &controls {
        follower
            .prepare_destination_range(Request::new(PrepareRangeRequest {
                range: Some(spec.clone()),
            }))
            .await
            .unwrap();
        follower
            .commit_destination_range(Request::new(control.clone()))
            .await
            .unwrap();
    }
    let checkpoint = ReplicationCheckpointRequest {
        topology_epoch: 1,
        owner_node_id: "node-1".into(),
        follower_node_id: "node-2".into(),
        stream_sequence: 5,
        verified_ranges: controls
            .iter()
            .map(|(control, _)| control.clone())
            .collect(),
    };
    let mut incomplete = checkpoint.clone();
    incomplete.verified_ranges.pop();
    assert_eq!(
        follower
            .install_replication_checkpoint(Request::new(incomplete))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    let installed = follower
        .install_replication_checkpoint(Request::new(checkpoint))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(installed.stream_sequence, 5);
    let key = key_with_placement(&topology, |replicas| {
        replicas[0] == "node-1" && replicas.contains(&"node-2")
    });
    let old = replication_entry(&topology, key.clone(), 1, 1, b"stale", false);
    assert_eq!(
        follower
            .replicate_mutation(Request::new(old))
            .await
            .unwrap()
            .into_inner()
            .applied_stream_sequence,
        5
    );
    assert!(!follower.state.read().await.records.contains_key(&key));
    let gap = replication_entry(&topology, key.clone(), 7, 7, b"gap", false);
    assert_eq!(
        follower
            .replicate_mutation(Request::new(gap))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Aborted
    );
    let next = replication_entry(&topology, key.clone(), 6, 6, b"current", false);
    follower
        .replicate_mutation(Request::new(next))
        .await
        .unwrap();
    assert_eq!(
        follower.state.read().await.records[&key].value.as_ref(),
        b"current"
    );
}

#[tokio::test]
async fn follower_rejects_stale_wrong_owner_and_out_of_coverage_entries() {
    let topology = replication_topology(2);
    let follower_key = key_with_placement(&topology, |replicas| {
        replicas[0] != "node-2" && replicas[1..].contains(&"node-2")
    });
    let outside_key = key_with_placement(&topology, |replicas| !replicas.contains(&"node-2"));
    let service = service_for("node-2", topology.clone());

    let mut stale = replication_entry(&topology, follower_key.clone(), 1, 1, b"value", false);
    stale.topology_epoch = 0;
    stale.version.as_mut().unwrap().topology_epoch = 0;
    assert_eq!(
        service
            .replicate_mutation(Request::new(stale))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );

    let mut oversized_id =
        replication_entry(&topology, follower_key.clone(), 1, 1, b"value", false);
    oversized_id.mutation_id = "x".repeat(MAX_MUTATION_ID_BYTES + 1);
    assert_eq!(
        service
            .replicate_mutation(Request::new(oversized_id))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::InvalidArgument
    );

    let mut wrong_owner = replication_entry(&topology, follower_key, 1, 1, b"value", false);
    let incorrect_node_id = topology
        .members
        .iter()
        .find(|member| member.node_id != wrong_owner.owner_node_id)
        .unwrap()
        .node_id
        .clone();
    wrong_owner.owner_node_id = incorrect_node_id.clone();
    wrong_owner.version.as_mut().unwrap().owner_node_id = incorrect_node_id;
    assert_eq!(
        service
            .replicate_mutation(Request::new(wrong_owner))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );

    let outside = replication_entry(&topology, outside_key, 1, 1, b"value", false);
    assert_eq!(
        service
            .replicate_mutation(Request::new(outside))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
}

#[tokio::test]
async fn expired_mutation_ids_eventually_release_old_ack_progress() {
    let service = service();
    let mut state = service.state.write().await;
    let now = Instant::now();
    let (sender, _) = watch::channel(0);
    state
        .ack_progress
        .insert((0, "old-follower".into()), sender);
    insert_dedup_until(
        &mut state,
        "expired".into(),
        Arc::from(&b"key"[..]),
        [0; 32],
        RecordVersion {
            topology_epoch: 0,
            owner_sequence: 1,
            owner_node_id: "node-1".into(),
        },
        false,
        now - std::time::Duration::from_millis(1),
    );
    state.next_ack_prune_at = now + std::time::Duration::from_secs(1);

    purge_expired_dedup(&mut state, now);
    assert!(state.dedup.is_empty());
    assert!(state.ack_progress.contains_key(&(0, "old-follower".into())));

    purge_expired_dedup(&mut state, now + std::time::Duration::from_secs(1));
    assert!(!state.ack_progress.contains_key(&(0, "old-follower".into())));
}

#[tokio::test]
async fn acknowledged_mutation_releases_ack_metadata_but_keeps_retry_result() {
    let service = service();
    let version = RecordVersion {
        topology_epoch: 1,
        owner_sequence: 1,
        owner_node_id: "node-1".into(),
    };
    let required = vec![(1, "node-2".into(), 1)];
    let ack_bytes = required_ack_retained_bytes(&["node-2".into()]);
    let base_bytes;
    {
        let mut state = service.state.write().await;
        insert_dedup(
            &mut state,
            "mutation-1".into(),
            Arc::from(&b"key"[..]),
            [0; 32],
            version.clone(),
            false,
            Instant::now(),
        );
        let heap_id = &state.dedup_expirations.peek().unwrap().0.1;
        let map_id = state.dedup.keys().next().unwrap();
        assert!(Arc::ptr_eq(heap_id, map_id));
        base_bytes = state.dedup_bytes;
        let entry = state.dedup.get_mut("mutation-1").unwrap();
        entry.required_acks = required.clone();
        entry.retained_bytes += ack_bytes;
        state.dedup_bytes += ack_bytes;
        let (sender, _) = watch::channel(1);
        state.ack_progress.insert((1, "node-2".into()), sender);
        state
            .admitted_followers
            .insert("node-2".into(), "follower-process".into());
        state
            .ack_process_instances
            .insert((1, "node-2".into()), "follower-process".into());
    }

    service
        .wait_required_acks(&required, 1, "mutation-1", &version)
        .await
        .unwrap();
    let state = service.state.read().await;
    let entry = &state.dedup["mutation-1"];
    assert!(entry.required_acks.is_empty());
    assert_eq!(state.dedup_bytes, base_bytes);
    assert_eq!(entry.version, version);
    drop(state);
    service
        .wait_required_acks(&[], 1, "mutation-1", &version)
        .await
        .unwrap();
}

#[tokio::test]
async fn unacknowledged_mutation_retains_ack_requirements_for_retry() {
    let service = service();
    let version = RecordVersion {
        topology_epoch: 1,
        owner_sequence: 1,
        owner_node_id: "node-1".into(),
    };
    let required = vec![(1, "node-2".into(), 1)];
    let (sender, _) = watch::channel(0);
    {
        let mut state = service.state.write().await;
        insert_dedup(
            &mut state,
            "mutation-1".into(),
            Arc::from(&b"key"[..]),
            [0; 32],
            version.clone(),
            false,
            Instant::now(),
        );
        let ack_bytes = required_ack_retained_bytes(&["node-2".into()]);
        let entry = state.dedup.get_mut("mutation-1").unwrap();
        entry.required_acks = required.clone();
        entry.retained_bytes += ack_bytes;
        state.dedup_bytes += ack_bytes;
        state
            .ack_progress
            .insert((1, "node-2".into()), sender.clone());
    }
    assert!(
        service
            .wait_required_acks(&required, 1, "mutation-1", &version)
            .await
            .is_err()
    );
    assert_eq!(
        service.state.read().await.dedup["mutation-1"].required_acks,
        required
    );
    sender.send_replace(1);
    {
        let mut state = service.state.write().await;
        state
            .admitted_followers
            .insert("node-2".into(), "follower-process".into());
        state
            .ack_process_instances
            .insert((1, "node-2".into()), "follower-process".into());
    }
    service
        .wait_required_acks(&required, 1, "mutation-1", &version)
        .await
        .unwrap();
    assert!(
        service.state.read().await.dedup["mutation-1"]
            .required_acks
            .is_empty()
    );
}

#[test]
fn owner_assigns_independent_contiguous_sequences_per_follower() {
    let topology = replication_topology(3);
    let key = key_with_placement(&topology, |replicas| replicas[0] == "node-1");
    let mut state = NodeState {
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
        lease: Some((1, Instant::now() + std::time::Duration::from_secs(3600))),
        policy_write_fence: None,
        dedup: HashMap::new(),
        dedup_expirations: BinaryHeap::new(),
        dedup_bytes: 0,
        dedup_peak_bytes: 0,
        ack_progress_needs_prune: false,
        next_ack_prune_at: Instant::now(),
        cleanup_timing: proto::ReceiptCleanupTiming::default(),
    };
    let version = RecordVersion {
        topology_epoch: 1,
        owner_sequence: 1,
        owner_node_id: "node-1".into(),
    };
    let first = prepare_replication_entries(
        &mut state,
        &key,
        b"one",
        false,
        &version,
        "mutation-1".into(),
    )
    .unwrap();
    let second = prepare_replication_entries(
        &mut state,
        &key,
        b"two",
        false,
        &RecordVersion {
            owner_sequence: 2,
            ..version
        },
        "mutation-2".into(),
    )
    .unwrap();

    assert_eq!(first.len(), 2);
    assert_eq!(second.len(), 2);
    for follower in first {
        let next = second
            .iter()
            .find(|candidate| candidate.follower_node_id == follower.follower_node_id)
            .unwrap();
        assert_eq!(follower.stream_sequence, 1);
        assert_eq!(next.stream_sequence, 2);
    }
}

#[tokio::test]
async fn owner_only_delivers_put_and_delete_to_follower_in_order() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let follower_endpoint = format!("http://{}", listener.local_addr().unwrap());
    let topology = TopologySnapshot::new_with_config(
        1,
        42,
        8,
        vec![
            Member {
                node_id: "node-1".into(),
                endpoint: "http://127.0.0.1:1".into(),
            },
            Member {
                node_id: "node-2".into(),
                endpoint: follower_endpoint,
            },
        ],
        TopologyConfig {
            desired_replication_factor: 2,
            write_availability_guard: WriteAvailabilityGuard {
                minimum_admitted_copies: 1,
                minimum_healthy_followers: 0,
                ..WriteAvailabilityGuard::default()
            },
            ..TopologyConfig::default()
        },
    )
    .unwrap();
    let follower = service_for("node-2", topology.clone());
    let follower_state = follower.state.clone();
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(proto::data_node_server::DataNodeServer::new(follower))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );
    let owner = service_for("node-1", topology.clone());
    let key = key_with_placement(&topology, |replicas| replicas[0] == "node-1");

    let put = owner
        .put(Request::new(PutRequest {
            key: key.clone(),
            value: b"replicated".to_vec(),
            topology_epoch: 1,
            request_id: "put-1".into(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(put.error.is_none());
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            if follower_state
                .read()
                .await
                .records
                .get(&key)
                .is_some_and(|record| record.value.as_ref() == b"replicated")
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        follower_state.read().await.dedup["put-1"]
            .version
            .owner_sequence,
        1
    );

    let deleted = owner
        .delete(Request::new(DeleteRequest {
            key: key.clone(),
            topology_epoch: 1,
            request_id: "delete-1".into(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(deleted.error.is_none());
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let state = follower_state.read().await;
            if state.records.get(&key).is_some_and(|record| record.deleted)
                && state
                    .follower_streams
                    .values()
                    .any(|stream| stream.applied_sequence == 2)
            {
                break;
            }
            drop(state);
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    let mut concurrent = Vec::new();
    for index in 0..6_u8 {
        let owner = owner.clone();
        let key = key.clone();
        concurrent.push(tokio::spawn(async move {
            owner
                .put(Request::new(PutRequest {
                    key,
                    value: vec![index],
                    topology_epoch: 1,
                    request_id: format!("concurrent-{index}"),
                }))
                .await
                .unwrap()
                .into_inner()
        }));
    }
    for write in concurrent {
        assert!(write.await.unwrap().error.is_none());
    }
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let state = follower_state.read().await;
            if state
                .follower_streams
                .values()
                .any(|stream| stream.applied_sequence == 8)
                && state.records[&key].version.owner_sequence == 8
            {
                break;
            }
            drop(state);
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    server.abort();
}

#[tokio::test]
async fn dispatcher_resumes_after_a_verified_gap_checkpoint() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let topology = TopologySnapshot::new_with_config(
        1,
        42,
        4,
        vec![
            Member {
                node_id: "node-1".into(),
                endpoint: "http://127.0.0.1:1".into(),
            },
            Member {
                node_id: "node-2".into(),
                endpoint: format!("http://{}", listener.local_addr().unwrap()),
            },
        ],
        TopologyConfig {
            desired_replication_factor: 2,
            write_availability_guard: WriteAvailabilityGuard {
                minimum_admitted_copies: 1,
                minimum_healthy_followers: 0,
                ..WriteAvailabilityGuard::default()
            },
            ..TopologyConfig::default()
        },
    )
    .unwrap();
    let follower = service_for("node-2", topology.clone());
    let follower_rpc = follower.clone();
    let follower_state = follower.state.clone();
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(proto::data_node_server::DataNodeServer::new(follower))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );
    let owner = service_for("node-1", topology.clone());
    let key = key_with_placement(&topology, |replicas| replicas[0] == "node-1");
    for (id, value) in [("first", b"one".as_slice())] {
        let result = owner
            .put(Request::new(PutRequest {
                key: key.clone(),
                value: value.to_vec(),
                topology_epoch: 1,
                request_id: id.into(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(result.error.is_none());
    }
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            if follower_state
                .read()
                .await
                .follower_streams
                .values()
                .any(|stream| stream.applied_sequence == 1)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    {
        let mut state = follower_state.write().await;
        state.follower_streams.clear();
        state.records.clear();
    }
    let result = owner
        .put(Request::new(PutRequest {
            key: key.clone(),
            value: b"two".to_vec(),
            topology_epoch: 1,
            request_id: "second".into(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(result.error.is_none());
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert!(
        follower_state
            .read()
            .await
            .follower_streams
            .values()
            .all(|stream| stream.applied_sequence == 0)
    );
    let source_records = owner.state.read().await.records.clone();
    let mut controls = Vec::new();
    for (index, range) in topology
        .derived_ranges()
        .unwrap()
        .into_iter()
        .filter(|range| {
            range.owner_node_id == "node-1" && range.follower_node_ids.contains(&"node-2".into())
        })
        .enumerate()
    {
        let control = RangeControlRequest {
            change_id: "repair-dispatch".into(),
            range_id: format!("range-{index}"),
        };
        let spec = RangeSpec {
            change_id: control.change_id.clone(),
            range_id: control.range_id.clone(),
            start_exclusive: range.start_exclusive,
            end_inclusive: range.end_inclusive,
            source_node_id: "node-1".into(),
            destination_node_id: "node-2".into(),
        };
        let records = source_records
            .iter()
            .filter(|(key, _)| spec.contains(topology.key_token(key)))
            .map(|(key, record)| (key.clone(), record.clone()))
            .collect();
        follower_state.write().await.destinations.insert(
            spec.key(),
            DestinationMigration {
                range: spec,
                records,
                dedup: HashMap::new(),
                dedup_bytes: 0,
                watermark: 0,
                committed: false,
                writes_activated: false,
            },
        );
        follower_rpc
            .commit_destination_range(Request::new(control.clone()))
            .await
            .unwrap();
        controls.push(control);
    }
    let checkpoint = follower_rpc
        .install_replication_checkpoint(Request::new(ReplicationCheckpointRequest {
            topology_epoch: 1,
            owner_node_id: "node-1".into(),
            follower_node_id: "node-2".into(),
            stream_sequence: 2,
            verified_ranges: controls,
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(checkpoint.stream_sequence, 2);
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let state = owner.state.read().await;
            if state.owner_stream_unacked.values().all(VecDeque::is_empty) {
                break;
            }
            drop(state);
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let result = owner
        .put(Request::new(PutRequest {
            key: key.clone(),
            value: b"three".to_vec(),
            topology_epoch: 1,
            request_id: "third".into(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(result.error.is_none());
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let state = follower_state.read().await;
            if state
                .follower_streams
                .values()
                .any(|stream| stream.applied_sequence == 3)
                && state.records[&key].value.as_ref() == b"three"
            {
                break;
            }
            drop(state);
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    server.abort();
}

#[tokio::test]
async fn unavailable_follower_backpressures_before_unbounded_queue_growth() {
    let topology = TopologySnapshot::new_with_config(
        1,
        42,
        8,
        vec![
            Member {
                node_id: "node-1".into(),
                endpoint: "http://127.0.0.1:1".into(),
            },
            Member {
                node_id: "node-2".into(),
                endpoint: "http://127.0.0.1:2".into(),
            },
        ],
        TopologyConfig {
            desired_replication_factor: 2,
            write_availability_guard: WriteAvailabilityGuard {
                minimum_admitted_copies: 1,
                minimum_healthy_followers: 0,
                ..WriteAvailabilityGuard::default()
            },
            ..TopologyConfig::default()
        },
    )
    .unwrap();
    let owner = service_for("node-1", topology.clone());
    let key = key_with_placement(&topology, |replicas| replicas[0] == "node-1");
    let mut successful = 0;
    let rejected = loop {
        let response = owner
            .put(Request::new(PutRequest {
                key: key.clone(),
                value: vec![successful as u8],
                topology_epoch: 1,
                request_id: format!("put-{successful}"),
            }))
            .await
            .unwrap()
            .into_inner();
        if let Some(error) = response.error {
            break error;
        }
        successful += 1;
        assert!(successful <= REPLICATION_STREAM_QUEUE_CAPACITY + 1);
    };

    assert_eq!(rejected.code, ErrorCode::ResourceExhausted as i32);
    let pressure = owner
        .get_process_info(Request::new(proto::Empty {}))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(pressure.replication_reservation_rejections, 1);
    assert_eq!(
        pressure.replication_stream_peak_pending,
        REPLICATION_STREAM_QUEUE_CAPACITY as u64
    );
    let state = owner.state.read().await;
    assert_eq!(state.next_sequence, successful as u64);
    assert_eq!(
        state.records[&key].version.owner_sequence,
        successful as u64
    );
    assert_eq!(
        state.owner_stream_sequences.values().copied().next(),
        Some(successful as u64)
    );
}

#[tokio::test]
async fn older_replicated_delete_cannot_erase_newer_seeded_value() {
    let topology = replication_topology(3);
    let key = key_with_placement(&topology, |replicas| replicas[1..].contains(&"node-2"));
    let service = service_for("node-2", topology.clone());
    let owner_node_id = topology.owner(&key).unwrap().node_id.clone();
    service.state.write().await.records.insert(
        key.clone(),
        Record {
            value: Arc::from(b"newer".as_slice()),
            version: RecordVersion {
                topology_epoch: topology.epoch,
                owner_sequence: 11,
                owner_node_id,
            },
            deleted: false,
        },
    );

    let delete = replication_entry(&topology, key.clone(), 1, 10, b"", true);
    service
        .replicate_mutation(Request::new(delete))
        .await
        .unwrap();
    let state = service.state.read().await;
    assert_eq!(state.records[&key].value.as_ref(), b"newer");
    assert!(!state.records[&key].deleted);
}

#[tokio::test]
async fn source_cleanup_retains_records_needed_as_follower_copies() {
    let topology = TopologySnapshot::new_with_config(
        2,
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
        ],
        TopologyConfig {
            desired_replication_factor: 2,
            ..TopologyConfig::default()
        },
    )
    .unwrap();
    let key = key_with_placement(&topology, |replicas| {
        replicas[0] == "node-2" && replicas[1] == "node-1"
    });
    let service = service_for("node-1", topology);
    let (snapshot_ready, _) = watch::channel(true);
    {
        let mut state = service.state.write().await;
        state.records.insert(
            key.clone(),
            Record {
                value: Arc::from(b"replica".as_slice()),
                version: RecordVersion {
                    topology_epoch: 1,
                    owner_sequence: 1,
                    owner_node_id: "node-1".into(),
                },
                deleted: false,
            },
        );
        state.sources.insert(
            ("change-1".into(), "range-1".into()),
            SourceMigration {
                range: RangeSpec {
                    change_id: "change-1".into(),
                    range_id: "range-1".into(),
                    start_exclusive: 0,
                    end_inclusive: 0,
                    source_node_id: "node-1".into(),
                    destination_node_id: "node-2".into(),
                },
                snapshot_keys: Some(Vec::new()),
                snapshot_dedup_ids: Vec::new(),
                snapshot_ready,
                journal: Vec::new(),
                journal_bytes: 0,
                watermark: 0,
                writes_paused: true,
            },
        );
    }

    service
        .cleanup_source_range(Request::new(RangeControlRequest {
            change_id: "change-1".into(),
            range_id: "range-1".into(),
        }))
        .await
        .unwrap();
    assert!(service.state.read().await.records.contains_key(&key));
}

#[tokio::test]
async fn strong_ack_policies_fail_writes_closed_until_replication_is_active() {
    for policy in [WriteAckPolicy::FirstSuccessor, WriteAckPolicy::AllReplicas] {
        let service = service();
        let topology = TopologySnapshot::new_with_config(
            2,
            42,
            8,
            vec![Member {
                node_id: "node-1".into(),
                endpoint: "http://127.0.0.1:5001".into(),
            }],
            TopologyConfig {
                write_ack_policy: policy,
                ..TopologyConfig::default()
            },
        )
        .unwrap();
        service
            .install_topology(Request::new(InstallTopologyRequest {
                topology: Some((&topology).into()),
                require_lease: false,
            }))
            .await
            .unwrap();
        service.state.write().await.lease =
            Some((2, Instant::now() + std::time::Duration::from_secs(3600)));

        let put = service
            .put(Request::new(PutRequest {
                key: b"key".to_vec(),
                value: b"value".to_vec(),
                topology_epoch: 2,
                request_id: "put".into(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            put.error.unwrap().code,
            ErrorCode::TemporarilyUnavailable as i32
        );
        let delete = service
            .delete(Request::new(DeleteRequest {
                key: b"key".to_vec(),
                topology_epoch: 2,
                request_id: "delete".into(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            delete.error.unwrap().code,
            ErrorCode::TemporarilyUnavailable as i32
        );
        assert!(service.state.read().await.records.is_empty());
    }
}

#[tokio::test]
async fn first_successor_requires_the_exact_healthy_follower_and_its_ack() {
    let base = replication_topology(3);
    let key = key_with_placement(&base, |replicas| replicas[0] == "node-1");
    let token = base.key_token(&key);
    let range = base
        .derived_ranges()
        .unwrap()
        .into_iter()
        .find(|range| {
            range.owner_node_id == "node-1"
                && range_contains(range.start_exclusive, range.end_inclusive, token)
        })
        .unwrap();
    let first = range.follower_node_ids[0].clone();
    let second = range.follower_node_ids[1].clone();
    let mut topology = TopologySnapshot::new_with_config(
        1,
        42,
        8,
        base.members.clone(),
        TopologyConfig {
            desired_replication_factor: 3,
            write_ack_policy: WriteAckPolicy::FirstSuccessor,
            write_availability_guard: WriteAvailabilityGuard {
                minimum_admitted_copies: 2,
                minimum_healthy_followers: 1,
                ..WriteAvailabilityGuard::default()
            },
        },
    )
    .unwrap();
    let mut status = proto::ReplicaStatusResponse {
        topology_epoch: 1,
        ranges: vec![proto::RangeReplicaStatus {
            start_exclusive: range.start_exclusive,
            end_inclusive: range.end_inclusive,
            owner_node_id: "node-1".into(),
            desired_rf: 3,
            current_rf: 3,
            followers: vec![
                proto::FollowerReplicaStatus {
                    node_id: first.clone(),
                    admitted: true,
                    lag_known: false,
                    ..Default::default()
                },
                proto::FollowerReplicaStatus {
                    node_id: second.clone(),
                    admitted: true,
                    lag_known: true,
                    ..Default::default()
                },
            ],
            ..Default::default()
        }],
        ..Default::default()
    };
    assert!(required_followers(&topology, token, Some(&status), "node-1").is_err());
    status.ranges[0].followers[0].lag_known = true;
    assert_eq!(
        required_followers(&topology, token, Some(&status), "node-1").unwrap(),
        vec![first.clone()]
    );
    topology.write_ack_policy = WriteAckPolicy::AllReplicas;
    assert_eq!(
        required_followers(&topology, token, Some(&status), "node-1").unwrap(),
        vec![first.clone(), second.clone()]
    );
    status.ranges[0].followers[1].admitted = false;
    assert!(required_followers(&topology, token, Some(&status), "node-1").is_err());
    topology.write_ack_policy = WriteAckPolicy::OwnerOnly;
    assert!(
        required_followers(&topology, token, Some(&status), "node-1")
            .unwrap()
            .is_empty()
    );

    let service = service();
    let (first_sender, _) = watch::channel(0);
    let (second_sender, _) = watch::channel(0);
    {
        let mut state = service.state.write().await;
        state
            .admitted_followers
            .insert(first.clone(), "follower-process".into());
        state
            .ack_process_instances
            .insert((1, first.clone()), "follower-process".into());
        state
            .ack_progress
            .insert((1, first.clone()), first_sender.clone());
        state
            .ack_progress
            .insert((1, second), second_sender.clone());
    }
    let waiter = tokio::spawn(async move {
        service
            .wait_required_acks(
                &[(1, first, 1)],
                1,
                "mutation-1",
                &RecordVersion {
                    topology_epoch: 1,
                    owner_sequence: 1,
                    owner_node_id: "node-1".into(),
                },
            )
            .await
    });
    second_sender.send_replace(1);
    tokio::task::yield_now().await;
    assert!(!waiter.is_finished());
    first_sender.send_replace(1);
    assert!(waiter.await.unwrap().is_ok());
}

#[tokio::test]
async fn required_ack_rejects_a_restarted_follower_until_its_new_process_is_admitted() {
    let service = service();
    let required = vec![(1, "node-2".into(), 1)];
    let version = RecordVersion {
        topology_epoch: 1,
        owner_sequence: 1,
        owner_node_id: "node-1".into(),
    };
    {
        let mut state = service.state.write().await;
        state
            .ack_progress
            .insert((1, "node-2".into()), watch::channel(1).0);
        state
            .admitted_followers
            .insert("node-2".into(), "old-process".into());
        state
            .ack_process_instances
            .insert((1, "node-2".into()), "new-process".into());
    }
    assert!(
        service
            .wait_required_acks(&required, 1, "id", &version)
            .await
            .is_err()
    );
    service
        .state
        .write()
        .await
        .admitted_followers
        .insert("node-2".into(), "new-process".into());
    service
        .wait_required_acks(&required, 1, "id", &version)
        .await
        .unwrap();
    // A late waiter cannot use a sequence acknowledged by the previous process.
    service
        .state
        .write()
        .await
        .ack_progress
        .get(&(1, "node-2".into()))
        .unwrap()
        .send_replace(0);
    assert!(
        service
            .wait_required_acks(&required, 1, "id", &version)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn local_readiness_requires_admission_and_bounds_unacknowledged_lag() {
    let topology = TopologySnapshot::new(
        1,
        7,
        4,
        (1..=3)
            .map(|i| Member {
                node_id: format!("node-{i}"),
                endpoint: format!("http://127.0.0.1:500{i}"),
            })
            .collect(),
    )
    .unwrap();
    let service = service_for("node-1", topology);
    let key = b"local-readiness";
    let follower;
    {
        let mut state = service.state.write().await;
        state.topology.write_ack_policy = WriteAckPolicy::FirstSuccessor;
        follower = state
            .topology
            .replica_node_ids_for_token(state.topology.key_token(key))
            .unwrap()[1]
            .to_owned();
    }
    assert!(service.ready_followers(key).await.is_err());
    {
        let mut state = service.state.write().await;
        state
            .admitted_followers
            .insert(follower.clone(), "process".into());
    }
    assert_eq!(
        service.ready_followers(key).await.unwrap().1,
        std::slice::from_ref(&follower)
    );
    {
        let mut state = service.state.write().await;
        let old = now_unix_millis().saturating_sub(
            state
                .topology
                .write_availability_guard
                .max_replica_lag_millis
                + 1,
        );
        state
            .owner_stream_unacked
            .insert((1, follower.clone()), [(1, old)].into());
    }
    assert!(service.ready_followers(key).await.is_err());
    service.state.write().await.owner_stream_unacked.clear();
    assert_eq!(service.ready_followers(key).await.unwrap().1, [follower]);
}

#[tokio::test]
async fn ack_pruning_preserves_live_old_requirements_and_skips_current_epoch_scans() {
    let service = service();
    let mut state = service.state.write().await;
    let current_epoch = state.topology.epoch;
    let old = (0, "old-follower".to_owned());
    let orphan = (0, "orphan-instance".to_owned());
    let current = (current_epoch, "current-follower".to_owned());
    for stream in [&old, &current] {
        state
            .ack_progress
            .insert(stream.clone(), watch::channel(1).0);
        state
            .ack_process_instances
            .insert(stream.clone(), "process".into());
    }
    // An identity can require retirement even without a corresponding sender.
    state
        .ack_process_instances
        .insert(orphan.clone(), "process".into());
    let version = RecordVersion {
        topology_epoch: 0,
        owner_sequence: 1,
        owner_node_id: "node-1".into(),
    };
    insert_dedup_until(
        &mut state,
        "live".into(),
        Arc::from(&b"key"[..]),
        [0; 32],
        version,
        false,
        Instant::now() + std::time::Duration::from_secs(60),
    );
    state.dedup.get_mut("live").unwrap().required_acks = vec![(0, old.1.clone(), 1)];
    prune_ack_progress(&mut state);
    assert!(state.ack_progress.contains_key(&old));
    assert!(state.ack_process_instances.contains_key(&old));
    assert!(!state.ack_process_instances.contains_key(&orphan));
    assert!(state.ack_progress.contains_key(&current));

    state.dedup.get_mut("live").unwrap().required_acks.clear();
    prune_ack_progress(&mut state);
    assert!(!state.ack_progress.contains_key(&old));
    assert!(!state.ack_process_instances.contains_key(&old));
    let scanned = state.cleanup_timing.ack_prune_scanned_receipts;
    prune_ack_progress(&mut state);
    assert_eq!(state.cleanup_timing.ack_prune_scanned_receipts, scanned);
    assert!(state.ack_progress.contains_key(&current));
    assert!(state.ack_process_instances.contains_key(&current));
    state
        .ack_process_instances
        .insert(orphan.clone(), "late-instance".into());
    prune_ack_progress(&mut state);
    assert!(!state.ack_process_instances.contains_key(&orphan));
}
