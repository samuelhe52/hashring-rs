use super::*;

#[tokio::test]
async fn separate_processes_route_bytes_and_survive_coordinator_restart() {
    let _guard = process_test_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let state = directory.path().join("coordinator.redb");
    let ports = unused_ports(3);
    let coordinator_port = ports[0];
    let members = vec![
        ("node-1".to_owned(), ports[1]),
        ("node-2".to_owned(), ports[2]),
    ];
    let coordinator_endpoint = format!("http://127.0.0.1:{coordinator_port}");

    let mut coordinator = spawn_process(&coordinator_arguments(coordinator_port, &state, &members));
    let client = connect_eventually(&coordinator_endpoint).await;

    let mut nodes: Vec<_> = members
        .iter()
        .map(|(node_id, port)| {
            spawn_process(&[
                "node".into(),
                "--id".into(),
                node_id.clone(),
                "--listen".into(),
                format!("127.0.0.1:{port}"),
                "--coordinator".into(),
                coordinator_endpoint.clone(),
            ])
        })
        .collect();

    let topology = client.topology().await;
    let mut keys = Vec::new();
    for wanted in ["node-1", "node-2"] {
        let key = (0_u64..10_000)
            .map(|candidate| candidate.to_be_bytes().to_vec())
            .find(|key| topology.owner(key).unwrap().node_id == wanted)
            .unwrap();
        keys.push(key);
    }

    for (index, key) in keys.iter().enumerate() {
        let value = vec![0, 255, index as u8, 42];
        client.put(key.clone(), value.clone()).await.unwrap();
        assert_eq!(client.get(key.clone()).await.unwrap().value, value);
    }
    assert_eq!(
        client.delete(keys[1].clone()).await.unwrap().topology_epoch,
        1
    );
    assert_eq!(
        client.delete(keys[1].clone()).await.unwrap().topology_epoch,
        1
    );
    assert!(matches!(
        client.get(keys[1].clone()).await,
        Err(ClientError::Operation(ref failure)) if failure.code == ErrorCode::NotFound
    ));

    let large_key = b"large-value".to_vec();
    let large_value = vec![7; 5 * 1024 * 1024];
    client
        .put(large_key.clone(), large_value.clone())
        .await
        .unwrap();
    assert_eq!(client.get(large_key).await.unwrap().value, large_value);

    coordinator.stop();
    coordinator = spawn_process(&coordinator_arguments(coordinator_port, &state, &[]));
    let restarted_client = connect_eventually(&coordinator_endpoint).await;
    assert_eq!(
        restarted_client.get(keys[0].clone()).await.unwrap().value,
        vec![0, 255, 0, 42]
    );
    assert!(matches!(
        restarted_client.get(keys[1].clone()).await,
        Err(ClientError::Operation(ref failure)) if failure.code == ErrorCode::NotFound
    ));

    coordinator.stop();
    for node in &mut nodes {
        node.stop();
    }
}

#[tokio::test]
async fn committed_node_replacement_is_fenced_instead_of_serving_empty_data() {
    let _guard = process_test_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let state = directory.path().join("coordinator.redb");
    let ports = unused_ports(2);
    let coordinator_port = ports[0];
    let coordinator_endpoint = format!("http://127.0.0.1:{coordinator_port}");
    let members = vec![("node-1".to_owned(), ports[1])];
    let mut coordinator = spawn_process(&coordinator_arguments(coordinator_port, &state, &members));
    let client = connect_eventually(&coordinator_endpoint).await;
    let node_args = [
        "node".into(),
        "--id".into(),
        "node-1".into(),
        "--listen".into(),
        format!("127.0.0.1:{}", ports[1]),
        "--coordinator".into(),
        coordinator_endpoint.clone(),
    ];
    let mut node = spawn_process(&node_args);
    wait_for_listener(ports[1]);
    client
        .put(b"durable-owner".to_vec(), b"value".to_vec())
        .await
        .unwrap();
    node.stop();

    let mut replacement = spawn_process(&node_args);
    for _ in 0..100 {
        if replacement.has_exited() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        replacement.has_exited(),
        "replacement process was not fenced"
    );
    let error = client.get(b"durable-owner".to_vec()).await.unwrap_err();
    assert!(
        !matches!(error, ClientError::Operation(ref failure) if failure.code == ErrorCode::NotFound),
        "lost owner data was reported as an authoritative cache miss"
    );

    coordinator.stop();
}

#[tokio::test]
async fn permanent_grpc_status_is_returned_without_deadline_retry() {
    let _guard = process_test_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let state = directory.path().join("coordinator.redb");
    let coordinator_port = unused_ports(1)[0];
    let coordinator_endpoint = format!("http://127.0.0.1:{coordinator_port}");
    let members = vec![("node-1".to_owned(), coordinator_port)];
    let mut coordinator = spawn_process(&coordinator_arguments(coordinator_port, &state, &members));
    let client = connect_eventually(&coordinator_endpoint).await;

    let started = Instant::now();
    let error = client
        .put(b"key".to_vec(), b"value".to_vec())
        .await
        .unwrap_err();
    assert!(started.elapsed() < Duration::from_secs(1));
    assert!(matches!(
        error,
        ClientError::Rpc {
            code: Code::Unimplemented,
            unknown_write_outcome: false,
            ..
        }
    ));

    coordinator.stop();
}

#[tokio::test]
async fn moved_request_retries_a_transient_coordinator_outage_until_deadline() {
    let _guard = process_test_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let state = directory.path().join("coordinator.redb");
    let ports = unused_ports(3);
    let coordinator_port = ports[0];
    let coordinator_endpoint = format!("http://127.0.0.1:{coordinator_port}");
    let members = vec![("node-1".to_owned(), ports[1])];
    let mut coordinator = spawn_process(&coordinator_arguments(coordinator_port, &state, &members));
    let admin = connect_eventually(&coordinator_endpoint).await;
    let recovery_client =
        connect_without_polling(&coordinator_endpoint, Duration::from_secs(2)).await;
    let moved_recovery_client =
        connect_without_polling(&coordinator_endpoint, Duration::from_secs(2)).await;
    let deadline_client =
        connect_without_polling(&coordinator_endpoint, Duration::from_millis(250)).await;
    let mut polling_config = ClientConfig::new(Duration::from_secs(2));
    polling_config.topology_poll_interval = Some(Duration::from_millis(50));
    let polling_client =
        HashringClient::connect_with_config(coordinator_endpoint.clone(), polling_config)
            .await
            .unwrap();
    let mut node_1 = spawn_process(&[
        "node".into(),
        "--id".into(),
        "node-1".into(),
        "--listen".into(),
        format!("127.0.0.1:{}", ports[1]),
        "--coordinator".into(),
        coordinator_endpoint.clone(),
    ]);
    wait_for_listener(ports[1]);

    let target_members = vec![
        Member {
            node_id: "node-1".into(),
            endpoint: format!("http://127.0.0.1:{}", ports[1]),
        },
        Member {
            node_id: "node-2".into(),
            endpoint: format!("http://127.0.0.1:{}", ports[2]),
        },
    ];
    let plan = admin.begin_topology_change(target_members).await.unwrap();
    let old_topology = recovery_client.topology().await;
    let moved_key = (0_u64..10_000)
        .map(|candidate| candidate.to_be_bytes().to_vec())
        .find(|key| {
            let token = old_topology.key_token(key);
            plan.ranges.iter().any(|range| token_in_range(token, range))
        })
        .unwrap();
    let stationary_key = (0_u64..10_000)
        .map(|candidate| candidate.to_be_bytes().to_vec())
        .find(|key| {
            let token = old_topology.key_token(key);
            !plan.ranges.iter().any(|range| token_in_range(token, range))
        })
        .unwrap();
    recovery_client
        .put(moved_key.clone(), b"moved-value".to_vec())
        .await
        .unwrap();
    recovery_client
        .put(stationary_key.clone(), b"stationary-value".to_vec())
        .await
        .unwrap();
    let mut node_2 = spawn_process(&[
        "node".into(),
        "--id".into(),
        "node-2".into(),
        "--listen".into(),
        format!("127.0.0.1:{}", ports[2]),
        "--coordinator".into(),
        coordinator_endpoint.clone(),
    ]);
    wait_for_listener(ports[2]);
    let executor = HashringClient::connect(coordinator_endpoint.clone(), Duration::from_secs(30))
        .await
        .unwrap();
    let completed = executor
        .execute_topology_change(plan.change_id, plan.base_epoch, plan.target_topology.epoch)
        .await
        .unwrap();
    assert_eq!(completed.phase, MigrationPhase::Complete);
    assert_eq!(recovery_client.topology().await.epoch, 1);
    assert_eq!(moved_recovery_client.topology().await.epoch, 1);
    assert_eq!(deadline_client.topology().await.epoch, 1);
    for _ in 0..100 {
        if polling_client.topology().await.epoch == 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(polling_client.topology().await.epoch, 2);

    coordinator.stop();
    let started = Instant::now();
    assert_eq!(
        recovery_client
            .delete(stationary_key)
            .await
            .unwrap()
            .topology_epoch,
        2
    );
    assert!(
        started.elapsed() < Duration::from_millis(500),
        "a successful response waited for its background topology refresh"
    );
    coordinator = spawn_process(&coordinator_arguments(coordinator_port, &state, &[]));
    wait_for_listener(coordinator_port);
    for _ in 0..100 {
        if recovery_client.topology().await.epoch == 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(recovery_client.topology().await.epoch, 2);

    coordinator.stop();
    let request = tokio::spawn({
        let client = moved_recovery_client.clone();
        let key = moved_key.clone();
        async move { client.delete(key).await }
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    coordinator = spawn_process(&coordinator_arguments(coordinator_port, &state, &[]));
    wait_for_listener(coordinator_port);
    assert_eq!(request.await.unwrap().unwrap().topology_epoch, 2);
    assert_eq!(moved_recovery_client.topology().await.epoch, 2);
    assert!(matches!(
        moved_recovery_client.get(moved_key.clone()).await,
        Err(ClientError::Operation(ref failure)) if failure.code == ErrorCode::NotFound
    ));

    coordinator.stop();
    let started = Instant::now();
    assert!(matches!(
        deadline_client.get(moved_key).await,
        Err(ClientError::DeadlineExceeded {
            unknown_write_outcome: false
        })
    ));
    assert!(started.elapsed() >= Duration::from_millis(200));

    node_1.stop();
    node_2.stop();
}

#[tokio::test]
async fn topology_change_plan_is_exclusive_durable_and_not_published() {
    let _guard = process_test_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let state = directory.path().join("coordinator.redb");
    let ports = unused_ports(3);
    let coordinator_port = ports[0];
    let coordinator_endpoint = format!("http://127.0.0.1:{coordinator_port}");
    let initial_members = vec![("node-1".to_owned(), ports[1])];
    let mut coordinator = spawn_process(&coordinator_arguments(
        coordinator_port,
        &state,
        &initial_members,
    ));
    let client = connect_eventually(&coordinator_endpoint).await;
    let target_members = vec![
        Member {
            node_id: "node-1".into(),
            endpoint: format!("http://127.0.0.1:{}", ports[1]),
        },
        Member {
            node_id: "node-2".into(),
            endpoint: format!("http://127.0.0.1:{}", ports[2]),
        },
    ];

    let change = client
        .begin_topology_change(target_members.clone())
        .await
        .unwrap();
    assert_eq!(change.base_epoch, 1);
    assert_eq!(change.target_topology.epoch, 2);
    assert_eq!(change.phase, MigrationPhase::Planned);
    assert!(!change.ranges.is_empty());
    assert!(client.begin_topology_change(target_members).await.is_err());
    assert_eq!(
        connect_eventually(&coordinator_endpoint)
            .await
            .topology()
            .await
            .epoch,
        1
    );

    coordinator.stop();
    coordinator = spawn_process(&coordinator_arguments(coordinator_port, &state, &[]));
    let restarted = connect_eventually(&coordinator_endpoint).await;
    let recovered = restarted.topology_change().await.unwrap().unwrap();
    assert_eq!(recovered.change_id, change.change_id);
    assert_eq!(recovered.ranges, change.ranges);
    assert_eq!(restarted.topology().await.epoch, 1);

    coordinator.stop();
}

#[tokio::test]
async fn online_scale_out_and_scale_in_preserve_concurrent_writes() {
    let _guard = process_test_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let state = directory.path().join("coordinator.redb");
    let ports = unused_ports(3);
    let coordinator_port = ports[0];
    let coordinator_endpoint = format!("http://127.0.0.1:{coordinator_port}");
    let node_1_endpoint = format!("http://127.0.0.1:{}", ports[1]);
    let node_2_endpoint = format!("http://127.0.0.1:{}", ports[2]);
    let initial_members = vec![("node-1".to_owned(), ports[1])];
    let mut coordinator_args = coordinator_arguments(coordinator_port, &state, &initial_members);
    coordinator_args.extend(["--pre-publish-delay-ms".into(), "200".into()]);
    let mut coordinator = spawn_process(&coordinator_args);
    let client = connect_eventually(&coordinator_endpoint).await;
    let mut node_1 = spawn_process(&[
        "node".into(),
        "--id".into(),
        "node-1".into(),
        "--listen".into(),
        format!("127.0.0.1:{}", ports[1]),
        "--coordinator".into(),
        coordinator_endpoint.clone(),
    ]);

    let keys: Vec<_> = (0_u64..5_000)
        .map(|index| index.to_be_bytes().to_vec())
        .collect();
    let expected = std::sync::Arc::new(tokio::sync::Mutex::new(Vec::new()));
    for (index, key) in keys.iter().enumerate() {
        let mut value = vec![index as u8; 4 * 1024];
        value[..8].copy_from_slice(&(index as u64).to_be_bytes());
        client.put(key.clone(), value.clone()).await.unwrap();
        expected.lock().await.push(value);
    }

    let two_members = vec![
        Member {
            node_id: "node-1".into(),
            endpoint: node_1_endpoint.clone(),
        },
        Member {
            node_id: "node-2".into(),
            endpoint: node_2_endpoint.clone(),
        },
    ];
    let scale_out_plan = client
        .begin_topology_change(two_members.clone())
        .await
        .unwrap();
    let mut node_2 = spawn_process(&[
        "node".into(),
        "--id".into(),
        "node-2".into(),
        "--listen".into(),
        format!("127.0.0.1:{}", ports[2]),
        "--coordinator".into(),
        coordinator_endpoint.clone(),
    ]);
    wait_for_listener(ports[2]);

    let executor = HashringClient::connect(coordinator_endpoint.clone(), Duration::from_secs(60))
        .await
        .unwrap();
    let scale_out = execute_while_writing_moving_keys(
        &client,
        executor,
        scale_out_plan,
        &keys,
        &expected,
        "scale-out",
    )
    .await;
    assert_eq!(scale_out.phase, MigrationPhase::Complete);
    assert_eq!(scale_out.target_topology.epoch, 2);

    let fresh = HashringClient::connect(coordinator_endpoint.clone(), Duration::from_secs(2))
        .await
        .unwrap();
    let expected_values = expected.lock().await.clone();
    for (index, key) in keys.iter().enumerate() {
        assert_eq!(
            fresh.get(key.clone()).await.unwrap().value,
            expected_values[index]
        );
    }

    let scale_in_plan = fresh
        .begin_topology_change(vec![two_members[1].clone()])
        .await
        .unwrap();
    let stale_client = connect_without_polling(&coordinator_endpoint, Duration::from_secs(2)).await;
    let stale_key =
        moving_key_indexes(&stale_client.topology().await, &keys, &scale_in_plan.ranges)[0];
    let executor = HashringClient::connect(coordinator_endpoint.clone(), Duration::from_secs(60))
        .await
        .unwrap();
    let scale_in = execute_while_writing_moving_keys(
        &fresh,
        executor,
        scale_in_plan,
        &keys,
        &expected,
        "scale-in",
    )
    .await;
    assert_eq!(scale_in.phase, MigrationPhase::Complete);
    assert_eq!(scale_in.target_topology.epoch, 3);

    let final_client = HashringClient::connect(coordinator_endpoint, Duration::from_secs(2))
        .await
        .unwrap();
    let expected_values = expected.lock().await.clone();
    for (index, key) in keys.iter().enumerate() {
        assert_eq!(
            final_client.get(key.clone()).await.unwrap().value,
            expected_values[index]
        );
    }
    for _ in 0..50 {
        if node_1.has_exited() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(node_1.has_exited());
    assert_eq!(
        stale_client
            .delete(keys[stale_key].clone())
            .await
            .unwrap()
            .topology_epoch,
        3
    );
    assert!(matches!(
        stale_client.get(keys[stale_key].clone()).await,
        Err(ClientError::Operation(ref failure)) if failure.code == ErrorCode::NotFound
    ));
    assert_eq!(stale_client.topology().await.epoch, 3);

    node_2.stop();
    coordinator.stop();
}

#[tokio::test]
async fn scale_in_recovers_after_lost_stop_acknowledgement() {
    let _guard = process_test_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let state = directory.path().join("coordinator.redb");
    let ports = unused_ports(3);
    let coordinator_port = ports[0];
    let coordinator_endpoint = format!("http://127.0.0.1:{coordinator_port}");
    let members = vec![
        ("node-1".to_owned(), ports[1]),
        ("node-2".to_owned(), ports[2]),
    ];
    let mut coordinator = spawn_process(&coordinator_arguments(coordinator_port, &state, &members));
    let client = connect_eventually(&coordinator_endpoint).await;
    let mut node_1 = spawn_process(&[
        "node".into(),
        "--id".into(),
        "node-1".into(),
        "--listen".into(),
        format!("127.0.0.1:{}", ports[1]),
        "--coordinator".into(),
        coordinator_endpoint.clone(),
        "--stop-response-delay-ms".into(),
        "2000".into(),
    ]);
    let mut node_2 = spawn_process(&[
        "node".into(),
        "--id".into(),
        "node-2".into(),
        "--listen".into(),
        format!("127.0.0.1:{}", ports[2]),
        "--coordinator".into(),
        coordinator_endpoint.clone(),
    ]);
    wait_for_listener(ports[1]);
    wait_for_listener(ports[2]);

    for index in 0_u64..1_000 {
        client
            .put(index.to_be_bytes().to_vec(), vec![index as u8; 1024])
            .await
            .unwrap();
    }
    let plan = client
        .begin_topology_change(vec![Member {
            node_id: "node-2".into(),
            endpoint: format!("http://127.0.0.1:{}", ports[2]),
        }])
        .await
        .unwrap();
    let mut executor = spawn_process(&[
        "execute-change".into(),
        "--coordinator".into(),
        coordinator_endpoint.clone(),
        "--deadline-ms".into(),
        "60000".into(),
        "--change-id".into(),
        plan.change_id,
        "--base-epoch".into(),
        plan.base_epoch.to_string(),
        "--target-epoch".into(),
        plan.target_topology.epoch.to_string(),
    ]);

    let mut observed_stop_intent = false;
    for _ in 0..2_000 {
        if let Ok(Some(change)) = client.topology_change().await
            && change.phase == MigrationPhase::CleaningUp
            && change
                .stop_prepared_node_ids
                .iter()
                .any(|node| node == "node-1")
        {
            observed_stop_intent = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    assert!(
        observed_stop_intent,
        "coordinator never persisted stop intent"
    );
    coordinator.stop();
    executor.stop();
    for _ in 0..700 {
        if node_1.has_exited() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        node_1.has_exited(),
        "removed node did not execute fenced stop"
    );

    coordinator = spawn_process(&coordinator_arguments(coordinator_port, &state, &[]));
    let observer = connect_eventually(&coordinator_endpoint).await;
    let mut complete = false;
    for _ in 0..500 {
        if let Ok(Some(change)) = observer.topology_change().await
            && change.phase == MigrationPhase::Complete
        {
            complete = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(complete, "stop-intent recovery did not complete");
    observer.refresh_topology().await.unwrap();
    for index in (0_u64..1_000).step_by(31) {
        assert_eq!(
            observer
                .get(index.to_be_bytes().to_vec())
                .await
                .unwrap()
                .value,
            vec![index as u8; 1024]
        );
    }

    node_2.stop();
    coordinator.stop();
}

#[tokio::test]
async fn coordinator_restart_resumes_an_interrupted_copy() {
    let _guard = process_test_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let state = directory.path().join("coordinator.redb");
    let ports = unused_ports(3);
    let coordinator_port = ports[0];
    let coordinator_endpoint = format!("http://127.0.0.1:{coordinator_port}");
    let node_1_endpoint = format!("http://127.0.0.1:{}", ports[1]);
    let node_2_endpoint = format!("http://127.0.0.1:{}", ports[2]);
    let initial_members = vec![("node-1".to_owned(), ports[1])];
    let mut coordinator = spawn_process(&coordinator_arguments(
        coordinator_port,
        &state,
        &initial_members,
    ));
    let client = connect_eventually(&coordinator_endpoint).await;
    let mut node_1 = spawn_process(&[
        "node".into(),
        "--id".into(),
        "node-1".into(),
        "--listen".into(),
        format!("127.0.0.1:{}", ports[1]),
        "--coordinator".into(),
        coordinator_endpoint.clone(),
    ]);
    wait_for_listener(ports[1]);

    let value = vec![91; 4 * 1024];
    let mut writes = tokio::task::JoinSet::new();
    for index in 0_u64..5_000 {
        let client = client.clone();
        let value = value.clone();
        writes.spawn(async move {
            client
                .put(index.to_be_bytes().to_vec(), value)
                .await
                .unwrap();
        });
        if writes.len() >= 64 {
            writes.join_next().await.unwrap().unwrap();
        }
    }
    while let Some(result) = writes.join_next().await {
        result.unwrap();
    }

    let change = client
        .begin_topology_change(vec![
            Member {
                node_id: "node-1".into(),
                endpoint: node_1_endpoint,
            },
            Member {
                node_id: "node-2".into(),
                endpoint: node_2_endpoint,
            },
        ])
        .await
        .unwrap();
    let mut node_2 = spawn_process(&[
        "node".into(),
        "--id".into(),
        "node-2".into(),
        "--listen".into(),
        format!("127.0.0.1:{}", ports[2]),
        "--coordinator".into(),
        coordinator_endpoint.clone(),
    ]);
    wait_for_listener(ports[2]);
    let mut executor = spawn_process(&[
        "execute-change".into(),
        "--coordinator".into(),
        coordinator_endpoint.clone(),
        "--deadline-ms".into(),
        "60000".into(),
        "--change-id".into(),
        change.change_id,
        "--base-epoch".into(),
        change.base_epoch.to_string(),
        "--target-epoch".into(),
        change.target_topology.epoch.to_string(),
    ]);

    let mut observed_copy = false;
    for _ in 0..500 {
        if let Ok(Some(change)) = client.topology_change().await
            && change.phase == MigrationPhase::CopyingSnapshot
            && change
                .ranges
                .iter()
                .filter(|range| range.snapshot_records > 0)
                .count()
                > 0
            && change
                .ranges
                .iter()
                .filter(|range| range.snapshot_records > 0)
                .count()
                < change.ranges.len()
        {
            observed_copy = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    assert!(observed_copy, "migration never left the planned phase");
    coordinator.stop();
    executor.stop();

    coordinator = spawn_process(&coordinator_arguments(coordinator_port, &state, &[]));
    let observer = connect_eventually(&coordinator_endpoint).await;
    let mut completed = false;
    for _ in 0..1_000 {
        if let Ok(Some(change)) = observer.topology_change().await
            && change.phase == MigrationPhase::Complete
        {
            completed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(completed, "restarted coordinator did not finish migration");
    observer.refresh_topology().await.unwrap();
    assert_eq!(observer.topology().await.epoch, 2);
    for index in (0_u64..5_000).step_by(47) {
        assert_eq!(
            observer
                .get(index.to_be_bytes().to_vec())
                .await
                .unwrap()
                .value,
            value
        );
    }

    node_1.stop();
    node_2.stop();
    coordinator.stop();
}

#[tokio::test]
async fn interrupted_membership_retries_after_destination_returns() {
    let _guard = process_test_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let state = directory.path().join("retry-coordinator.redb");
    let ports = unused_ports(3);
    let endpoint = format!("http://127.0.0.1:{}", ports[0]);
    let members = vec![("node-1".to_owned(), ports[1])];
    let mut args = coordinator_arguments(ports[0], &state, &members);
    args.extend(["--pre-publish-delay-ms".into(), "5000".into()]);
    let mut coordinator = spawn_process(&args);
    wait_for_listener(ports[0]);
    let mut node_1 = spawn_process(&[
        "node".into(),
        "--id".into(),
        "node-1".into(),
        "--listen".into(),
        format!("127.0.0.1:{}", ports[1]),
        "--coordinator".into(),
        endpoint.clone(),
    ]);
    let client = HashringClient::connect(&endpoint, Duration::from_secs(30))
        .await
        .unwrap();
    for index in 0_u64..200 {
        client
            .put(index.to_be_bytes().to_vec(), index.to_be_bytes().to_vec())
            .await
            .unwrap();
    }
    let plan = client
        .begin_topology_change(vec![
            Member {
                node_id: "node-1".into(),
                endpoint: format!("http://127.0.0.1:{}", ports[1]),
            },
            Member {
                node_id: "node-2".into(),
                endpoint: format!("http://127.0.0.1:{}", ports[2]),
            },
        ])
        .await
        .unwrap();
    let mut node_2 = spawn_process(&[
        "node".into(),
        "--id".into(),
        "node-2".into(),
        "--listen".into(),
        format!("127.0.0.1:{}", ports[2]),
        "--coordinator".into(),
        endpoint.clone(),
    ]);
    wait_for_listener(ports[2]);
    let executor = client.clone();
    let execution = tokio::spawn(async move {
        executor
            .execute_topology_change(&plan.change_id, plan.base_epoch, plan.target_topology.epoch)
            .await
    });
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if client
                .topology_change()
                .await
                .unwrap()
                .is_some_and(|change| change.phase == MigrationPhase::ReadyToPublish)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("change did not reach the pre-publication barrier");
    coordinator.stop();
    execution.abort();
    node_2.stop();
    let mut recovery_args = coordinator_arguments(ports[0], &state, &[]);
    recovery_args.extend(["--migration-timeout-ms".into(), "500".into()]);
    coordinator = spawn_process(&recovery_args);
    wait_for_listener(ports[0]);
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if client
                .topology_change()
                .await
                .unwrap()
                .is_some_and(|change| change.phase == MigrationPhase::Resetting)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("transient destination outage did not retain a recoverable change");
    node_2 = spawn_process(&[
        "node".into(),
        "--id".into(),
        "node-2".into(),
        "--listen".into(),
        format!("127.0.0.1:{}", ports[2]),
        "--coordinator".into(),
        endpoint.clone(),
    ]);
    wait_for_listener(ports[2]);
    let completion = tokio::time::timeout(Duration::from_secs(25), async {
        loop {
            if client
                .topology_change()
                .await
                .unwrap()
                .is_some_and(|change| change.phase == MigrationPhase::Complete)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    if completion.is_err() {
        eprintln!("restarted change: {:?}", client.topology_change().await);
        eprintln!("destination exited: {}", node_2.has_exited());
    }
    completion.expect("restarted coordinator did not finish after the destination returned");
    client.refresh_topology().await.unwrap();
    for index in (0_u64..200).step_by(13) {
        assert_eq!(
            client
                .get(index.to_be_bytes().to_vec())
                .await
                .unwrap()
                .value,
            index.to_be_bytes()
        );
    }
    node_1.stop();
    node_2.stop();
    coordinator.stop();
}

#[tokio::test]
async fn permanently_lost_joiner_before_publication_aborts_after_grace() {
    let _guard = process_test_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let state = directory.path().join("prepublication-loss.redb");
    let ports = unused_ports(3);
    let endpoint = format!("http://127.0.0.1:{}", ports[0]);
    let members = vec![("node-1".to_owned(), ports[1])];
    let mut args = coordinator_arguments(ports[0], &state, &members);
    args.extend(["--pre-publish-delay-ms".into(), "5000".into()]);
    let mut coordinator = spawn_process(&args);
    wait_for_listener(ports[0]);
    let mut node_1 = spawn_process(&[
        "node".into(),
        "--id".into(),
        "node-1".into(),
        "--listen".into(),
        format!("127.0.0.1:{}", ports[1]),
        "--coordinator".into(),
        endpoint.clone(),
    ]);
    wait_for_listener(ports[1]);
    let client = HashringClient::connect(&endpoint, Duration::from_secs(10))
        .await
        .unwrap();
    for index in 0_u64..32 {
        client
            .put(index.to_be_bytes().to_vec(), index.to_be_bytes().to_vec())
            .await
            .unwrap();
    }
    let plan = client
        .begin_topology_change(vec![
            Member {
                node_id: "node-1".into(),
                endpoint: format!("http://127.0.0.1:{}", ports[1]),
            },
            Member {
                node_id: "node-2".into(),
                endpoint: format!("http://127.0.0.1:{}", ports[2]),
            },
        ])
        .await
        .unwrap();
    let mut node_2 = spawn_process(&[
        "node".into(),
        "--id".into(),
        "node-2".into(),
        "--listen".into(),
        format!("127.0.0.1:{}", ports[2]),
        "--coordinator".into(),
        endpoint.clone(),
    ]);
    wait_for_listener(ports[2]);
    let executor = client.clone();
    let execution = tokio::spawn(async move {
        executor
            .execute_topology_change(&plan.change_id, plan.base_epoch, plan.target_topology.epoch)
            .await
    });
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if client
                .topology_change()
                .await
                .unwrap()
                .is_some_and(|change| change.phase == MigrationPhase::ReadyToPublish)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("join did not reach the pre-publication barrier");
    coordinator.stop();
    execution.abort();
    node_2.stop();
    let mut restart_args = coordinator_arguments(ports[0], &state, &[]);
    restart_args.extend(["--migration-timeout-ms".into(), "500".into()]);
    coordinator = spawn_process(&restart_args);
    wait_for_listener(ports[0]);
    tokio::time::timeout(Duration::from_secs(45), async {
        loop {
            if client
                .topology_change()
                .await
                .unwrap()
                .is_some_and(|change| change.phase == MigrationPhase::Aborted)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    })
    .await
    .expect("permanently unavailable joiner was not removed from the pending change");
    client.refresh_topology().await.unwrap();
    assert_eq!(client.topology().await.epoch, 1);
    for index in (0_u64..32).step_by(7) {
        assert_eq!(
            client
                .get(index.to_be_bytes().to_vec())
                .await
                .unwrap()
                .value,
            index.to_be_bytes()
        );
    }
    node_1.stop();
    coordinator.stop();
}

#[tokio::test]
async fn destination_restart_before_publication_aborts_change() {
    let _guard = process_test_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let state = directory.path().join("coordinator.redb");
    let ports = unused_ports(3);
    let coordinator_port = ports[0];
    let coordinator_endpoint = format!("http://127.0.0.1:{coordinator_port}");
    let initial_members = vec![("node-1".to_owned(), ports[1])];
    let mut coordinator_args = coordinator_arguments(coordinator_port, &state, &initial_members);
    coordinator_args.extend(["--pre-publish-delay-ms".into(), "2000".into()]);
    let mut coordinator = spawn_process(&coordinator_args);
    let client = connect_eventually(&coordinator_endpoint).await;
    let mut node_1 = spawn_process(&[
        "node".into(),
        "--id".into(),
        "node-1".into(),
        "--listen".into(),
        format!("127.0.0.1:{}", ports[1]),
        "--coordinator".into(),
        coordinator_endpoint.clone(),
    ]);
    wait_for_listener(ports[1]);
    for index in 0_u64..1_000 {
        client
            .put(index.to_be_bytes().to_vec(), vec![index as u8; 1024])
            .await
            .unwrap();
    }
    let plan = client
        .begin_topology_change(vec![
            Member {
                node_id: "node-1".into(),
                endpoint: format!("http://127.0.0.1:{}", ports[1]),
            },
            Member {
                node_id: "node-2".into(),
                endpoint: format!("http://127.0.0.1:{}", ports[2]),
            },
        ])
        .await
        .unwrap();
    let mut node_2 = spawn_process(&[
        "node".into(),
        "--id".into(),
        "node-2".into(),
        "--listen".into(),
        format!("127.0.0.1:{}", ports[2]),
        "--coordinator".into(),
        coordinator_endpoint.clone(),
    ]);
    wait_for_listener(ports[2]);
    let mut executor = spawn_process(&[
        "execute-change".into(),
        "--coordinator".into(),
        coordinator_endpoint.clone(),
        "--deadline-ms".into(),
        "60000".into(),
        "--change-id".into(),
        plan.change_id,
        "--base-epoch".into(),
        plan.base_epoch.to_string(),
        "--target-epoch".into(),
        plan.target_topology.epoch.to_string(),
    ]);

    let mut ready = false;
    for _ in 0..1_000 {
        if let Ok(Some(change)) = client.topology_change().await
            && change.phase == MigrationPhase::ReadyToPublish
        {
            ready = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    assert!(ready, "migration never reached pre-publication fence");
    node_2.stop();
    node_2 = spawn_process(&[
        "node".into(),
        "--id".into(),
        "node-2".into(),
        "--listen".into(),
        format!("127.0.0.1:{}", ports[2]),
        "--coordinator".into(),
        coordinator_endpoint.clone(),
    ]);
    wait_for_listener(ports[2]);

    let mut aborted = false;
    for _ in 0..1_000 {
        if let Ok(Some(change)) = client.topology_change().await
            && change.phase == MigrationPhase::Aborted
        {
            aborted = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(aborted, "replacement destination was published");
    client.refresh_topology().await.unwrap();
    assert_eq!(client.topology().await.epoch, 1);
    for index in (0_u64..1_000).step_by(29) {
        assert_eq!(
            client
                .get(index.to_be_bytes().to_vec())
                .await
                .unwrap()
                .value,
            vec![index as u8; 1024]
        );
    }

    executor.stop();
    node_1.stop();
    node_2.stop();
    coordinator.stop();
}

#[tokio::test]
async fn unavailable_destination_aborts_without_publishing() {
    let _guard = process_test_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let state = directory.path().join("coordinator.redb");
    let ports = unused_ports(3);
    let coordinator_port = ports[0];
    let coordinator_endpoint = format!("http://127.0.0.1:{coordinator_port}");
    let initial_members = vec![("node-1".to_owned(), ports[1])];
    let mut coordinator = spawn_process(&coordinator_arguments(
        coordinator_port,
        &state,
        &initial_members,
    ));
    let client = connect_eventually(&coordinator_endpoint).await;
    let mut node_1 = spawn_process(&[
        "node".into(),
        "--id".into(),
        "node-1".into(),
        "--listen".into(),
        format!("127.0.0.1:{}", ports[1]),
        "--coordinator".into(),
        coordinator_endpoint.clone(),
    ]);
    wait_for_listener(ports[1]);
    client
        .put(b"survivor".to_vec(), b"authoritative".to_vec())
        .await
        .unwrap();
    let change = client
        .begin_topology_change(vec![
            Member {
                node_id: "node-1".into(),
                endpoint: format!("http://127.0.0.1:{}", ports[1]),
            },
            Member {
                node_id: "node-2".into(),
                endpoint: format!("http://127.0.0.1:{}", ports[2]),
            },
        ])
        .await
        .unwrap();
    let executor = HashringClient::connect(coordinator_endpoint.clone(), Duration::from_secs(5))
        .await
        .unwrap();
    let old_identity = (
        change.change_id.clone(),
        change.base_epoch,
        change.target_topology.epoch,
    );
    assert!(
        executor
            .execute_topology_change(old_identity.0.clone(), old_identity.1, old_identity.2,)
            .await
            .is_err()
    );
    let change = client.topology_change().await.unwrap().unwrap();
    assert_eq!(change.phase, MigrationPhase::Aborted);
    client.refresh_topology().await.unwrap();
    assert_eq!(client.topology().await.epoch, 1);
    assert_eq!(
        client.get(b"survivor".to_vec()).await.unwrap().value,
        b"authoritative"
    );

    let replacement = client
        .begin_topology_change(vec![
            Member {
                node_id: "node-1".into(),
                endpoint: format!("http://127.0.0.1:{}", ports[1]),
            },
            Member {
                node_id: "node-2".into(),
                endpoint: format!("http://127.0.0.1:{}", ports[2]),
            },
        ])
        .await
        .unwrap();
    assert_ne!(replacement.change_id, old_identity.0);
    assert!(
        executor
            .execute_topology_change(old_identity.0, old_identity.1, old_identity.2)
            .await
            .is_err(),
        "a retry for an older change must not execute its replacement"
    );
    assert_eq!(
        client.topology_change().await.unwrap().unwrap().change_id,
        replacement.change_id
    );

    node_1.stop();
    coordinator.stop();
}
