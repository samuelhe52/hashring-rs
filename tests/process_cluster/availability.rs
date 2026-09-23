use super::*;

#[tokio::test]
async fn rf_and_guard_transitions_publish_and_preserve_data() {
    let _guard = process_test_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let ports = unused_ports(4);
    let endpoint = format!("http://127.0.0.1:{}", ports[0]);
    let members: Vec<_> = (1..=3)
        .map(|index| (format!("node-{index}"), ports[index]))
        .collect();
    let mut args = coordinator_arguments(ports[0], &directory.path().join("config.redb"), &members);
    args.extend(["--desired-replication-factor".into(), "2".into()]);
    let _coordinator = spawn_process(&args);
    wait_for_listener(ports[0]);
    let client = HashringClient::connect(&endpoint, Duration::from_secs(30))
        .await
        .unwrap();
    let _nodes: Vec<_> = members
        .iter()
        .map(|(node_id, port)| {
            spawn_process(&[
                "node".into(),
                "--id".into(),
                node_id.clone(),
                "--listen".into(),
                format!("127.0.0.1:{port}"),
                "--coordinator".into(),
                endpoint.clone(),
            ])
        })
        .collect();
    wait_for_full_rf(&endpoint, 1, 2).await;
    client
        .put(b"config-key".to_vec(), b"value".to_vec())
        .await
        .unwrap();

    let mut config = client.topology().await.config();
    config.desired_replication_factor = 3;
    let increase = client
        .begin_topology_config_change(config.clone())
        .await
        .unwrap();
    assert!(increase.ranges.is_empty());
    assert!(!increase.replica_obligations.is_empty());
    let applied = client
        .execute_topology_change(
            &increase.change_id,
            increase.base_epoch,
            increase.target_topology.epoch,
        )
        .await
        .unwrap();
    assert_eq!(applied.phase, MigrationPhase::Complete);
    wait_for_full_rf(&endpoint, 2, 3).await;

    config.write_availability_guard.minimum_admitted_copies = 2;
    config.write_availability_guard.minimum_healthy_followers = 1;
    let guard_change = client
        .begin_topology_config_change(config.clone())
        .await
        .unwrap();
    assert!(guard_change.ranges.is_empty());
    let applied = client
        .execute_topology_change(
            &guard_change.change_id,
            guard_change.base_epoch,
            guard_change.target_topology.epoch,
        )
        .await
        .unwrap();
    assert_eq!(applied.target_topology.config(), config);
    assert_eq!(
        client.get(b"config-key".to_vec()).await.unwrap().value,
        b"value"
    );

    let mut invalid = config.clone();
    invalid.desired_replication_factor = 1;
    assert!(client.begin_topology_config_change(invalid).await.is_err());

    config.write_availability_guard.minimum_admitted_copies = 1;
    config.write_availability_guard.minimum_healthy_followers = 0;
    config.desired_replication_factor = 2;
    let decrease = client
        .begin_topology_config_change(config.clone())
        .await
        .unwrap();
    let applied = client
        .execute_topology_change(
            &decrease.change_id,
            decrease.base_epoch,
            decrease.target_topology.epoch,
        )
        .await
        .unwrap();
    assert_eq!(applied.target_topology.config(), config);
    assert_eq!(
        client.get(b"config-key".to_vec()).await.unwrap().value,
        b"value"
    );
}

#[tokio::test]
async fn policy_strengthening_waits_for_admitted_caught_up_followers() {
    let _guard = process_test_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let state = directory.path().join("policy-coordinator.redb");
    let ports = unused_ports(4);
    let coordinator_port = ports[0];
    let endpoint = format!("http://127.0.0.1:{coordinator_port}");
    let members: Vec<_> = (1..=3)
        .map(|index| (format!("node-{index}"), ports[index]))
        .collect();
    let mut args = coordinator_arguments(coordinator_port, &state, &members);
    let min_copies = args
        .iter()
        .position(|arg| arg == "--minimum-admitted-copies")
        .unwrap();
    args[min_copies + 1] = "2".into();
    let min_healthy = args
        .iter()
        .position(|arg| arg == "--minimum-healthy-followers")
        .unwrap();
    args[min_healthy + 1] = "1".into();
    let vnodes = args
        .iter()
        .position(|arg| arg == "--virtual-nodes")
        .unwrap();
    args[vnodes + 1] = "1".into();
    let _coordinator = spawn_process(&args);
    wait_for_listener(coordinator_port);
    let client = HashringClient::connect(&endpoint, Duration::from_secs(15))
        .await
        .unwrap();
    let _nodes: Vec<_> = members
        .iter()
        .map(|(node_id, port)| {
            spawn_process(&[
                "node".into(),
                "--id".into(),
                node_id.clone(),
                "--listen".into(),
                format!("127.0.0.1:{port}"),
                "--coordinator".into(),
                endpoint.clone(),
            ])
        })
        .collect();
    let ready = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let mut coordinator = CoordinatorClient::connect(endpoint.clone()).await.unwrap();
            let status = coordinator
                .get_replica_status(Empty {})
                .await
                .unwrap()
                .into_inner();
            if !status.ranges.is_empty()
                && status.ranges.iter().all(|range| {
                    range.followers.len() == 2
                        && range.followers.iter().all(|follower| {
                            follower.admitted
                                && follower.lag_known
                                && follower.stream_head == follower.stream_cursor
                        })
                })
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    assert!(ready.is_ok(), "replica admission did not become ready");
    let first = client
        .begin_write_policy_change(WriteAckPolicy::FirstSuccessor)
        .await
        .unwrap();
    let applied = client
        .execute_topology_change(
            &first.change_id,
            first.base_epoch,
            first.target_topology.epoch,
        )
        .await
        .unwrap();
    assert_eq!(applied.phase, MigrationPhase::Complete);
    let put = client
        .put(b"policy-key".to_vec(), b"first".to_vec())
        .await
        .unwrap();
    assert_eq!(put.topology_epoch, applied.target_topology.epoch);
    let all = client
        .begin_write_policy_change(WriteAckPolicy::AllReplicas)
        .await
        .unwrap();
    let applied = client
        .execute_topology_change(&all.change_id, all.base_epoch, all.target_topology.epoch)
        .await
        .unwrap();
    assert_eq!(applied.phase, MigrationPhase::Complete);
    let put = client
        .put(b"policy-key".to_vec(), b"all".to_vec())
        .await
        .unwrap();
    assert_eq!(put.topology_epoch, applied.target_topology.epoch);
}

#[tokio::test]
async fn first_successor_failover_promotes_admitted_copy_without_bulk_copy() {
    let _guard = process_test_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let state = directory.path().join("failover-coordinator.redb");
    let ports = unused_ports(4);
    let endpoint = format!("http://127.0.0.1:{}", ports[0]);
    let members: Vec<_> = (1..=3)
        .map(|index| (format!("node-{index}"), ports[index]))
        .collect();
    let mut args = coordinator_arguments(ports[0], &state, &members);
    let min_copies = args
        .iter()
        .position(|arg| arg == "--minimum-admitted-copies")
        .unwrap();
    args[min_copies + 1] = "2".into();
    let min_healthy = args
        .iter()
        .position(|arg| arg == "--minimum-healthy-followers")
        .unwrap();
    args[min_healthy + 1] = "1".into();
    let vnodes = args
        .iter()
        .position(|arg| arg == "--virtual-nodes")
        .unwrap();
    args[vnodes + 1] = "4".into();
    let _coordinator = spawn_process(&args);
    wait_for_listener(ports[0]);
    let client = HashringClient::connect(&endpoint, Duration::from_secs(15))
        .await
        .unwrap();
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
                endpoint.clone(),
            ])
        })
        .collect();
    let admitted = tokio::time::timeout(Duration::from_secs(25), async {
        loop {
            let mut admin = CoordinatorClient::connect(endpoint.clone()).await.unwrap();
            let status = admin
                .get_replica_status(Empty {})
                .await
                .unwrap()
                .into_inner();
            if !status.ranges.is_empty()
                && status.ranges.iter().all(|range| {
                    range.followers.iter().all(|follower| {
                        follower.admitted
                            && follower.lag_known
                            && follower.stream_head == follower.stream_cursor
                    })
                })
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    assert!(admitted.is_ok(), "replicas did not become admitted");
    let change = client
        .begin_write_policy_change(WriteAckPolicy::FirstSuccessor)
        .await
        .unwrap();
    let completed = client
        .execute_topology_change(
            &change.change_id,
            change.base_epoch,
            change.target_topology.epoch,
        )
        .await
        .unwrap();
    assert_eq!(completed.phase, MigrationPhase::Complete);
    let topology = completed.target_topology.clone();
    let keys: Vec<_> = (0_u64..100_000)
        .map(|candidate| candidate.to_be_bytes().to_vec())
        .filter(|key| topology.owner(key).unwrap().node_id == "node-1")
        .take(20)
        .collect();
    assert_eq!(keys.len(), 20);
    for (index, key) in keys.iter().enumerate() {
        client.put(key.clone(), vec![index as u8]).await.unwrap();
    }
    nodes[0].stop();
    let promoted = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let mut admin = CoordinatorClient::connect(endpoint.clone()).await.unwrap();
            let current: TopologySnapshot = admin
                .get_topology(Empty {})
                .await
                .unwrap()
                .into_inner()
                .try_into()
                .unwrap();
            if current.epoch == topology.epoch + 1 {
                assert!(
                    current
                        .members
                        .iter()
                        .all(|member| member.node_id != "node-1"),
                    "unexpected failover membership: {:?}",
                    current.members
                );
                break current;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("failed owner was not removed");
    let reader = HashringClient::connect(&endpoint, Duration::from_secs(5))
        .await
        .unwrap();
    for (index, key) in keys.iter().enumerate() {
        assert_eq!(
            reader.get(key.clone()).await.unwrap().value,
            vec![index as u8]
        );
        assert_ne!(promoted.owner(key).unwrap().node_id, "node-1");
    }
    let admin = HashringClient::connect(&endpoint, Duration::from_secs(30))
        .await
        .unwrap();
    let rejoin = admin
        .begin_topology_change(promoted_members(&members))
        .await
        .unwrap();
    nodes[0] = spawn_process(&[
        "node".into(),
        "--id".into(),
        "node-1".into(),
        "--listen".into(),
        format!("127.0.0.1:{}", ports[1]),
        "--coordinator".into(),
        endpoint.clone(),
    ]);
    wait_for_listener(ports[1]);
    let rejoined = admin
        .execute_topology_change(
            &rejoin.change_id,
            rejoin.base_epoch,
            rejoin.target_topology.epoch,
        )
        .await
        .unwrap();
    assert_eq!(rejoined.phase, MigrationPhase::Complete);
    for (index, key) in keys.iter().enumerate() {
        assert_eq!(
            admin.get(key.clone()).await.unwrap().value,
            vec![index as u8]
        );
    }
    let repaired = tokio::time::timeout(Duration::from_secs(25), async {
        loop {
            let mut coordinator = CoordinatorClient::connect(endpoint.clone()).await.unwrap();
            let status = coordinator
                .get_replica_status(Empty {})
                .await
                .unwrap()
                .into_inner();
            if status.topology_epoch == rejoined.target_topology.epoch
                && status.ranges.iter().all(|range| {
                    range.current_rf == 3 && range.followers.iter().all(|f| f.admitted)
                })
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    assert!(
        repaired.is_ok(),
        "returned node was not re-seeded and admitted"
    );
}

#[tokio::test]
async fn lost_joiner_after_publication_recovers_in_a_forward_epoch() {
    lost_joiner_after_publication_scenario(false, true).await;
}

#[tokio::test]
async fn lost_joiner_after_publication_recovers_after_coordinator_restart() {
    lost_joiner_after_publication_scenario(true, false).await;
}

async fn lost_joiner_after_publication_scenario(restart_coordinator: bool, first_successor: bool) {
    let _guard = process_test_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let state = directory.path().join("published-recovery.redb");
    let ports = unused_ports(5);
    let endpoint = format!("http://127.0.0.1:{}", ports[0]);
    let members: Vec<_> = (1..=4)
        .map(|index| (format!("node-{index}"), ports[index]))
        .collect();
    let mut args = coordinator_arguments(ports[0], &state, &members[..3]);
    for (flag, value) in [
        ("--virtual-nodes", "4"),
        ("--minimum-admitted-copies", "2"),
        ("--minimum-healthy-followers", "1"),
    ] {
        let index = args.iter().position(|arg| arg == flag).unwrap() + 1;
        args[index] = value.into();
    }
    args.extend([
        "--migration-timeout-ms".into(),
        "5000".into(),
        "--post-publish-delay-ms".into(),
        "8000".into(),
    ]);
    let mut coordinator = spawn_process(&args);
    wait_for_listener(ports[0]);
    let mut nodes: Vec<_> = members[..3]
        .iter()
        .map(|(node_id, port)| {
            spawn_process(&[
                "node".into(),
                "--id".into(),
                node_id.clone(),
                "--listen".into(),
                format!("127.0.0.1:{port}"),
                "--coordinator".into(),
                endpoint.clone(),
            ])
        })
        .collect();
    let client = HashringClient::connect(&endpoint, Duration::from_secs(90))
        .await
        .unwrap();
    wait_for_full_rf(&endpoint, 1, 3).await;
    if first_successor {
        let policy = client
            .begin_write_policy_change(WriteAckPolicy::FirstSuccessor)
            .await
            .unwrap();
        client
            .execute_topology_change(
                &policy.change_id,
                policy.base_epoch,
                policy.target_topology.epoch,
            )
            .await
            .unwrap();
        wait_for_full_rf(&endpoint, policy.target_topology.epoch, 3).await;
    }
    let keys: Vec<_> = (0_u64..80)
        .map(|index| index.to_be_bytes().to_vec())
        .collect();
    for (index, key) in keys.iter().enumerate() {
        client
            .put(key.clone(), vec![index as u8; 128])
            .await
            .unwrap();
    }
    let plan = client
        .begin_topology_change(promoted_members(&members))
        .await
        .unwrap();
    let old_topology = plan.base_topology.as_ref().unwrap();
    assert!(keys.iter().any(|key| {
        let token = old_topology.key_token(key);
        plan.ranges.iter().any(|range| token_in_range(token, range))
    }));
    let first_target_epoch = plan.target_topology.epoch;
    let original_change_id = plan.change_id.clone();
    nodes.push(spawn_process(&[
        "node".into(),
        "--id".into(),
        members[3].0.clone(),
        "--listen".into(),
        format!("127.0.0.1:{}", ports[4]),
        "--coordinator".into(),
        endpoint.clone(),
    ]));
    wait_for_listener(ports[4]);
    let executor = client.clone();
    let execution = tokio::spawn(async move {
        executor
            .execute_topology_change(&plan.change_id, plan.base_epoch, plan.target_topology.epoch)
            .await
    });
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if client
                .topology_change()
                .await
                .unwrap()
                .is_some_and(|change| change.phase == MigrationPhase::Published)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("join did not publish");
    nodes[3].stop();
    if restart_coordinator {
        coordinator.stop();
        let mut restart_args = coordinator_arguments(ports[0], &state, &[]);
        restart_args.extend(["--migration-timeout-ms".into(), "5000".into()]);
        coordinator = spawn_process(&restart_args);
        wait_for_listener(ports[0]);
    }
    let recovery = tokio::time::timeout(Duration::from_secs(45), async {
        loop {
            if let Some(change) = client.topology_change().await.unwrap()
                && change.phase == MigrationPhase::Complete
                && change.supersedes_change_id.as_deref() == Some(original_change_id.as_str())
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    })
    .await;
    if recovery.is_err() {
        eprintln!(
            "recovery change: {:?}",
            tokio::time::timeout(Duration::from_secs(2), client.topology_change()).await
        );
        let mut status_client = CoordinatorClient::connect(endpoint.clone()).await.unwrap();
        eprintln!(
            "recovery status: {:?}",
            tokio::time::timeout(
                Duration::from_secs(2),
                status_client.get_replica_status(Empty {})
            )
            .await
        );
        eprintln!("original execution finished: {}", execution.is_finished());
    }
    recovery.expect("published join was not superseded after the joiner died");
    execution.abort();
    client.refresh_topology().await.unwrap();
    assert_eq!(client.topology().await.epoch, first_target_epoch + 1);
    wait_for_full_rf(&endpoint, first_target_epoch + 1, 3).await;
    for (index, key) in keys.iter().enumerate() {
        assert_eq!(
            client.get(key.clone()).await.unwrap().value,
            vec![index as u8; 128]
        );
    }
    coordinator.stop();
}

#[tokio::test]
async fn lost_original_owner_during_join_stays_blocked_without_coverage_proof() {
    let _guard = process_test_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let state = directory.path().join("blocked-cutover.redb");
    let ports = unused_ports(5);
    let endpoint = format!("http://127.0.0.1:{}", ports[0]);
    let members: Vec<_> = (1..=4)
        .map(|index| (format!("node-{index}"), ports[index]))
        .collect();
    let mut args = coordinator_arguments(ports[0], &state, &members[..3]);
    let vnodes = args
        .iter()
        .position(|arg| arg == "--virtual-nodes")
        .unwrap()
        + 1;
    args[vnodes] = "4".into();
    args.extend([
        "--migration-timeout-ms".into(),
        "5000".into(),
        "--post-publish-delay-ms".into(),
        "8000".into(),
    ]);
    let _coordinator = spawn_process(&args);
    wait_for_listener(ports[0]);
    let mut nodes: Vec<_> = members[..3]
        .iter()
        .map(|(node_id, port)| {
            spawn_process(&[
                "node".into(),
                "--id".into(),
                node_id.clone(),
                "--listen".into(),
                format!("127.0.0.1:{port}"),
                "--coordinator".into(),
                endpoint.clone(),
            ])
        })
        .collect();
    let client = HashringClient::connect(&endpoint, Duration::from_secs(30))
        .await
        .unwrap();
    for index in 0_u64..32 {
        client
            .put(index.to_be_bytes().to_vec(), index.to_be_bytes().to_vec())
            .await
            .unwrap();
    }
    let plan = client
        .begin_topology_change(promoted_members(&members))
        .await
        .unwrap();
    nodes.push(spawn_process(&[
        "node".into(),
        "--id".into(),
        members[3].0.clone(),
        "--listen".into(),
        format!("127.0.0.1:{}", ports[4]),
        "--coordinator".into(),
        endpoint.clone(),
    ]));
    wait_for_listener(ports[4]);
    let executor = client.clone();
    let execution = tokio::spawn(async move {
        executor
            .execute_topology_change(&plan.change_id, plan.base_epoch, plan.target_topology.epoch)
            .await
    });
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if client
                .topology_change()
                .await
                .unwrap()
                .is_some_and(|change| change.phase == MigrationPhase::Published)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("join did not publish");
    nodes[0].stop();
    let mut coordinator = CoordinatorClient::connect(endpoint.clone()).await.unwrap();
    tokio::time::timeout(Duration::from_secs(25), async {
        loop {
            let status = coordinator
                .get_replica_status(Empty {})
                .await
                .unwrap()
                .into_inner();
            if status.ranges.iter().any(|range| {
                range
                    .write_block_reason
                    .contains("cannot prove complete frozen-range coverage")
            }) {
                assert_eq!(status.topology_epoch, 2);
                assert!(status.ranges.iter().all(|range| !range.writable));
                break;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    })
    .await
    .expect("cutover did not report the blocked recovery");
    execution.abort();
}

#[tokio::test]
async fn three_to_four_to_three_preserves_data_and_replica_readiness() {
    let _guard = process_test_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let state = directory.path().join("membership-coordinator.redb");
    let ports = unused_ports(5);
    let endpoint = format!("http://127.0.0.1:{}", ports[0]);
    let members: Vec<_> = (1..=4)
        .map(|index| (format!("node-{index}"), ports[index]))
        .collect();
    let mut args = coordinator_arguments(ports[0], &state, &members[..3]);
    for (flag, value) in [
        ("--minimum-admitted-copies", "2"),
        ("--minimum-healthy-followers", "1"),
        ("--virtual-nodes", "4"),
    ] {
        let index = args.iter().position(|arg| arg == flag).unwrap() + 1;
        args[index] = value.into();
    }
    let _coordinator = spawn_process(&args);
    wait_for_listener(ports[0]);
    let mut nodes: Vec<_> = members[..3]
        .iter()
        .map(|(node_id, port)| {
            spawn_process(&[
                "node".into(),
                "--id".into(),
                node_id.clone(),
                "--listen".into(),
                format!("127.0.0.1:{port}"),
                "--coordinator".into(),
                endpoint.clone(),
            ])
        })
        .collect();
    let client = HashringClient::connect(&endpoint, Duration::from_secs(45))
        .await
        .unwrap();
    wait_for_full_rf(&endpoint, 1, 3).await;
    let policy = client
        .begin_write_policy_change(WriteAckPolicy::FirstSuccessor)
        .await
        .unwrap();
    let applied = client
        .execute_topology_change(
            &policy.change_id,
            policy.base_epoch,
            policy.target_topology.epoch,
        )
        .await
        .unwrap();
    assert_eq!(applied.phase, MigrationPhase::Complete);
    wait_for_full_rf(&endpoint, applied.target_topology.epoch, 3).await;

    let keys: Vec<_> = (0_u64..24)
        .map(|index| index.to_be_bytes().to_vec())
        .collect();
    tokio::time::timeout(Duration::from_secs(30), async {
        for (index, key) in keys.iter().enumerate() {
            client
                .put(key.clone(), vec![index as u8; 256])
                .await
                .unwrap();
        }
    })
    .await
    .expect("initial acknowledged writes stalled");
    let plan = client
        .begin_topology_change(promoted_members(&members))
        .await
        .unwrap();
    let old_topology = client.topology().await;
    let moving_key = (24_u64..10_000)
        .map(|index| index.to_be_bytes().to_vec())
        .find(|key| {
            let token = old_topology.key_token(key);
            plan.ranges.iter().any(|range| token_in_range(token, range))
        })
        .expect("scale-out must move at least one test key");
    let stop = Arc::new(AtomicBool::new(false));
    let writer_stop = stop.clone();
    let writer_client = client.clone();
    let writer_key = moving_key.clone();
    let writer = tokio::spawn(async move {
        let mut count = 0_u64;
        while !writer_stop.load(Ordering::Relaxed) {
            writer_client
                .put(writer_key.clone(), count.to_be_bytes().to_vec())
                .await
                .unwrap();
            count += 1;
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        count
    });

    nodes.push(spawn_process(&[
        "node".into(),
        "--id".into(),
        members[3].0.clone(),
        "--listen".into(),
        format!("127.0.0.1:{}", ports[4]),
        "--coordinator".into(),
        endpoint.clone(),
    ]));
    wait_for_listener(ports[4]);
    let expanded = client
        .execute_topology_change(&plan.change_id, plan.base_epoch, plan.target_topology.epoch)
        .await
        .unwrap();
    assert_eq!(expanded.phase, MigrationPhase::Complete);
    wait_for_full_rf(&endpoint, expanded.target_topology.epoch, 3).await;
    for (index, key) in keys.iter().enumerate() {
        assert_eq!(
            client.get(key.clone()).await.unwrap().value,
            vec![index as u8; 256]
        );
    }

    let plan = client
        .begin_topology_change(promoted_members(&members[..3]))
        .await
        .unwrap();
    assert!(
        plan.ranges
            .iter()
            .any(|range| { token_in_range(plan.target_topology.key_token(&moving_key), range) })
    );
    let contracted = client
        .execute_topology_change(&plan.change_id, plan.base_epoch, plan.target_topology.epoch)
        .await
        .unwrap();
    assert_eq!(contracted.phase, MigrationPhase::Complete);
    wait_for_full_rf(&endpoint, contracted.target_topology.epoch, 3).await;
    stop.store(true, Ordering::Relaxed);
    let writes = writer.await.unwrap();
    assert!(writes > 0);
    assert_eq!(
        client.get(moving_key).await.unwrap().value,
        (writes - 1).to_be_bytes()
    );
    for (index, key) in keys.iter().enumerate() {
        assert_eq!(
            client.get(key.clone()).await.unwrap().value,
            vec![index as u8; 256]
        );
    }
}

#[tokio::test]
async fn node_self_fences_client_operations_when_coordinator_lease_expires() {
    let _guard = process_test_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let state = directory.path().join("lease-coordinator.redb");
    let ports = unused_ports(2);
    let endpoint = format!("http://127.0.0.1:{}", ports[0]);
    let members = vec![("node-1".to_owned(), ports[1])];
    let mut coordinator = spawn_process(&coordinator_arguments(ports[0], &state, &members));
    wait_for_listener(ports[0]);
    let client = connect_eventually(&endpoint).await;
    let _node = spawn_process(&[
        "node".into(),
        "--id".into(),
        "node-1".into(),
        "--listen".into(),
        format!("127.0.0.1:{}", ports[1]),
        "--coordinator".into(),
        endpoint.clone(),
    ]);
    wait_for_listener(ports[1]);
    client
        .put(b"lease-key".to_vec(), b"value".to_vec())
        .await
        .unwrap();
    coordinator.stop();
    tokio::time::sleep(Duration::from_secs(6)).await;
    let mut node = DataNodeClient::connect(format!("http://127.0.0.1:{}", ports[1]))
        .await
        .unwrap();
    let get = node
        .get(GetRequest {
            key: b"lease-key".to_vec(),
            topology_epoch: 1,
            request_id: "expired-get".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(get.error.unwrap().code, ErrorCode::LeaseExpired as i32);
    let put = node
        .put(hashring_rs::proto::PutRequest {
            key: b"lease-key".to_vec(),
            value: b"new".to_vec(),
            topology_epoch: 1,
            request_id: "expired-put".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(put.error.unwrap().code, ErrorCode::LeaseExpired as i32);
}

#[tokio::test]
async fn all_replicas_policy_cannot_publish_under_replicated_and_unfences_on_abort() {
    let _guard = process_test_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let state = directory.path().join("under-replicated-policy.redb");
    let ports = unused_ports(3);
    let endpoint = format!("http://127.0.0.1:{}", ports[0]);
    let members: Vec<_> = (1..=2)
        .map(|index| (format!("node-{index}"), ports[index]))
        .collect();
    let mut args = coordinator_arguments(ports[0], &state, &members);
    let min_copies = args
        .iter()
        .position(|arg| arg == "--minimum-admitted-copies")
        .unwrap();
    args[min_copies + 1] = "2".into();
    let min_healthy = args
        .iter()
        .position(|arg| arg == "--minimum-healthy-followers")
        .unwrap();
    args[min_healthy + 1] = "1".into();
    let vnodes = args
        .iter()
        .position(|arg| arg == "--virtual-nodes")
        .unwrap();
    args[vnodes + 1] = "1".into();
    args.extend(["--migration-timeout-ms".into(), "1500".into()]);
    let _coordinator = spawn_process(&args);
    wait_for_listener(ports[0]);
    let client = HashringClient::connect(&endpoint, Duration::from_secs(10))
        .await
        .unwrap();
    let _nodes: Vec<_> = members
        .iter()
        .map(|(node_id, port)| {
            spawn_process(&[
                "node".into(),
                "--id".into(),
                node_id.clone(),
                "--listen".into(),
                format!("127.0.0.1:{port}"),
                "--coordinator".into(),
                endpoint.clone(),
            ])
        })
        .collect();
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let mut coordinator = CoordinatorClient::connect(endpoint.clone()).await.unwrap();
            let status = coordinator
                .get_replica_status(Empty {})
                .await
                .unwrap()
                .into_inner();
            if !status.ranges.is_empty()
                && status.ranges.iter().all(|range| {
                    range
                        .followers
                        .iter()
                        .any(|follower| follower.admitted && follower.lag_known)
                })
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap();
    let original_epoch = client.topology().await.epoch;
    let change = client
        .begin_write_policy_change(WriteAckPolicy::AllReplicas)
        .await
        .unwrap();
    assert!(
        client
            .execute_topology_change(
                &change.change_id,
                change.base_epoch,
                change.target_topology.epoch
            )
            .await
            .is_err()
    );
    let current = client.topology_change().await.unwrap().unwrap();
    assert_eq!(current.phase, MigrationPhase::Aborted);
    let mut coordinator = CoordinatorClient::connect(endpoint.clone()).await.unwrap();
    assert_eq!(
        coordinator
            .get_topology(Empty {})
            .await
            .unwrap()
            .into_inner()
            .epoch,
        original_epoch
    );
    client
        .put(b"write-after-abort".to_vec(), b"value".to_vec())
        .await
        .unwrap();
}

#[tokio::test]
async fn follower_admissions_are_seeded_and_survive_coordinator_restart() {
    let _guard = process_test_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let state = directory.path().join("coordinator.redb");
    let ports = unused_ports(4);
    let coordinator_port = ports[0];
    let coordinator_endpoint = format!("http://127.0.0.1:{coordinator_port}");
    let members: Vec<_> = (1..=3)
        .map(|index| (format!("node-{index}"), ports[index]))
        .collect();
    let mut args = coordinator_arguments(coordinator_port, &state, &members);
    let vnode_position = args.iter().position(|argument| argument == "32").unwrap();
    args[vnode_position] = "1".into();
    let mut coordinator = spawn_process(&args);
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
    for index in 0..40_u64 {
        let key = index.to_be_bytes().to_vec();
        client
            .put(key.clone(), index.to_be_bytes().to_vec())
            .await
            .unwrap();
        if index % 3 == 0 {
            client.delete(key).await.unwrap();
        }
    }
    let stop_writes = Arc::new(AtomicBool::new(false));
    let writer_client = client.clone();
    let writer_stop = stop_writes.clone();
    let writer = tokio::spawn(async move {
        let mut writes = 0_u64;
        while !writer_stop.load(Ordering::Relaxed) {
            let key = b"concurrent-replica-seed".to_vec();
            writer_client
                .put(key.clone(), writes.to_be_bytes().to_vec())
                .await
                .unwrap();
            if writes.is_multiple_of(3) {
                writer_client.delete(key).await.unwrap();
            }
            writes += 1;
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        writes
    });
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut admin = CoordinatorClient::connect(coordinator_endpoint.clone())
        .await
        .unwrap();
    loop {
        let status = admin
            .get_replica_status(Empty {})
            .await
            .unwrap()
            .into_inner();
        if status.ranges.iter().all(|range| {
            range.current_rf == 3 && range.followers.iter().all(|follower| follower.admitted)
        }) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "replica admissions did not complete: {status:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    stop_writes.store(true, Ordering::Relaxed);
    assert!(writer.await.unwrap() > 0);
    loop {
        let status = admin
            .get_replica_status(Empty {})
            .await
            .unwrap()
            .into_inner();
        if status.ranges.iter().all(|range| {
            range.followers.iter().all(|follower| {
                follower.lag_known && follower.stream_head == follower.stream_cursor
            })
        }) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "replica streams remained behind: {status:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    coordinator.stop();
    coordinator = spawn_process(&coordinator_arguments(coordinator_port, &state, &[]));
    let _client = connect_eventually(&coordinator_endpoint).await;
    let mut admin = CoordinatorClient::connect(coordinator_endpoint.clone())
        .await
        .unwrap();
    let status = admin
        .get_replica_status(Empty {})
        .await
        .unwrap()
        .into_inner();
    assert!(status.ranges.iter().all(|range| range.current_rf == 3));
    coordinator.stop();
    for node in &mut nodes {
        node.stop();
    }
}
