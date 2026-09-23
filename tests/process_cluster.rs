use std::{
    net::TcpListener,
    path::Path,
    process::{Child, Command, Stdio},
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use hashring_rs::client::{ClientConfig, ClientError, HashringClient};
use hashring_rs::{
    migration::{MigrationPhase, RangeMigration, TopologyChange},
    proto::{
        Empty, ErrorCode, GetRequest, coordinator_client::CoordinatorClient,
        data_node_client::DataNodeClient,
    },
    topology::{Member, TopologySnapshot, WriteAckPolicy},
};
use tonic::Code;

struct Process(Child);

fn process_test_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

impl Process {
    fn stop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }

    fn has_exited(&mut self) -> bool {
        self.0.try_wait().unwrap().is_some()
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        self.stop();
    }
}

fn unused_ports(count: usize) -> Vec<u16> {
    let listeners: Vec<_> = (0..count)
        .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    listeners
        .iter()
        .map(|listener| listener.local_addr().unwrap().port())
        .collect()
}

fn spawn_process(arguments: &[String]) -> Process {
    Process(
        Command::new(env!("CARGO_BIN_EXE_hashring-rs"))
            .args(arguments)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    )
}

fn wait_for_listener(port: u16) {
    let address = format!("127.0.0.1:{port}").parse().unwrap();
    for _ in 0..300 {
        if std::net::TcpStream::connect_timeout(&address, Duration::from_millis(20)).is_ok() {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("listener did not become ready on port {port}");
}

fn coordinator_arguments(
    coordinator_port: u16,
    state: &Path,
    members: &[(String, u16)],
) -> Vec<String> {
    let mut arguments = vec![
        "coordinator".into(),
        "--listen".into(),
        format!("127.0.0.1:{coordinator_port}"),
        "--state".into(),
        state.display().to_string(),
        "--virtual-nodes".into(),
        "32".into(),
        "--minimum-admitted-copies".into(),
        "1".into(),
        "--minimum-healthy-followers".into(),
        "0".into(),
    ];
    for (node_id, port) in members {
        arguments.extend([
            "--member".into(),
            format!("{node_id}=http://127.0.0.1:{port}"),
        ]);
    }
    arguments
}

async fn connect_eventually(endpoint: &str) -> HashringClient {
    for _ in 0..50 {
        if let Ok(client) = HashringClient::connect(endpoint, Duration::from_secs(2)).await {
            return client;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("coordinator did not become ready at {endpoint}");
}

async fn connect_without_polling(endpoint: &str, timeout: Duration) -> HashringClient {
    let mut config = ClientConfig::new(timeout);
    config.topology_poll_interval = None;
    HashringClient::connect_with_config(endpoint, config)
        .await
        .unwrap()
}

async fn wait_for_full_rf(endpoint: &str, epoch: u64, desired_rf: u32) {
    let deadline = Instant::now() + Duration::from_secs(40);
    let mut coordinator = CoordinatorClient::connect(endpoint.to_owned())
        .await
        .unwrap();
    loop {
        let status = tokio::time::timeout(
            Duration::from_secs(3),
            coordinator.get_replica_status(Empty {}),
        )
        .await
        .expect("replica status RPC stalled")
        .unwrap()
        .into_inner();
        if status.topology_epoch == epoch
            && !status.ranges.is_empty()
            && status.ranges.iter().all(|range| {
                range.current_rf == desired_rf
                    && range.followers.iter().all(|follower| {
                        follower.admitted && follower.lag_known && follower.lag_millis <= 5_000
                    })
            })
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "epoch {epoch} replicas did not converge to RF {desired_rf}: {status:?}"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

fn promoted_members(members: &[(String, u16)]) -> Vec<Member> {
    members
        .iter()
        .map(|(node_id, port)| Member {
            node_id: node_id.clone(),
            endpoint: format!("http://127.0.0.1:{port}"),
        })
        .collect()
}

fn token_in_range(token: u64, range: &RangeMigration) -> bool {
    if range.start_exclusive < range.end_inclusive {
        token > range.start_exclusive && token <= range.end_inclusive
    } else if range.start_exclusive > range.end_inclusive {
        token > range.start_exclusive || token <= range.end_inclusive
    } else {
        true
    }
}

fn moving_key_indexes(
    topology: &TopologySnapshot,
    keys: &[Vec<u8>],
    ranges: &[RangeMigration],
) -> Vec<usize> {
    keys.iter()
        .enumerate()
        .filter_map(|(index, key)| {
            let token = topology.key_token(key);
            ranges
                .iter()
                .any(|range| token_in_range(token, range))
                .then_some(index)
        })
        .collect()
}

fn changing_owner_key_indexes(
    old: &TopologySnapshot,
    target: &TopologySnapshot,
    keys: &[Vec<u8>],
) -> Vec<usize> {
    keys.iter()
        .enumerate()
        .filter_map(|(index, key)| {
            (old.owner(key).unwrap().node_id != target.owner(key).unwrap().node_id).then_some(index)
        })
        .collect()
}

async fn execute_while_writing_moving_keys(
    observer: &HashringClient,
    executor: HashringClient,
    plan: TopologyChange,
    keys: &[Vec<u8>],
    expected: &std::sync::Arc<tokio::sync::Mutex<Vec<Vec<u8>>>>,
    label: &str,
) -> TopologyChange {
    let old_topology = observer.topology().await;
    let indexes = if plan.direct_merge {
        changing_owner_key_indexes(&old_topology, &plan.target_topology, keys)
    } else {
        moving_key_indexes(&old_topology, keys, &plan.ranges)
    };
    assert!(!indexes.is_empty(), "test has no keys in moving ranges");
    let read_index = indexes[0];
    let read_key = keys[read_index].clone();
    let read_token = old_topology.key_token(&read_key);
    let read_source_endpoint = if plan.direct_merge {
        old_topology.owner(&read_key).unwrap().endpoint.clone()
    } else {
        plan.ranges
            .iter()
            .find(|range| token_in_range(read_token, range))
            .unwrap()
            .source_endpoint
            .clone()
    };
    let read_epoch = plan.base_epoch;
    let identity = (
        plan.change_id.clone(),
        plan.base_epoch,
        plan.target_topology.epoch,
    );
    let execution = tokio::spawn(async move {
        executor
            .execute_topology_change(identity.0, identity.1, identity.2)
            .await
            .unwrap()
    });

    let mut observed_transition = false;
    for _ in 0..2_000 {
        if let Some(change) = observer.topology_change().await.unwrap()
            && (matches!(
                change.phase,
                MigrationPhase::CopyingSnapshot | MigrationPhase::ReplayingChangelog
            ) || (plan.direct_merge
                && matches!(
                    change.phase,
                    MigrationPhase::PausingWrites
                        | MigrationPhase::Verifying
                        | MigrationPhase::ReadyToPublish
                )))
        {
            observed_transition = true;
            break;
        }
        assert!(
            !execution.is_finished(),
            "{label} completed before transition overlap"
        );
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    assert!(
        observed_transition,
        "{label} never entered its transition phase"
    );

    let direct_reader = tokio::spawn(async move {
        let mut source = DataNodeClient::connect(read_source_endpoint).await.unwrap();
        let mut successful_reads = 0_u64;
        for attempt in 0_u64..10_000 {
            let response = source
                .get(GetRequest {
                    key: read_key.clone(),
                    topology_epoch: read_epoch,
                    request_id: format!("direct-migration-read-{attempt}"),
                })
                .await
                .unwrap()
                .into_inner();
            match response.error {
                None => successful_reads += 1,
                Some(error) if error.code == ErrorCode::Moved as i32 => {
                    return (successful_reads, true);
                }
                Some(error) if error.code == ErrorCode::RangeBusy as i32 => {
                    panic!("source GET was fenced with RangeBusy during migration");
                }
                Some(error) => panic!(
                    "source GET failed during migration: {:?}",
                    ErrorCode::try_from(error.code)
                ),
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        (successful_reads, false)
    });

    let mut writes_during_migration = 0_usize;
    while !execution.is_finished() {
        let index = indexes[writes_during_migration % indexes.len()];
        let value = format!("{label}-overlap-{writes_during_migration}").into_bytes();
        observer
            .put(keys[index].clone(), value.clone())
            .await
            .unwrap();
        expected.lock().await[index] = value;
        writes_during_migration += 1;
    }
    assert!(
        writes_during_migration > 0,
        "{label} had no write overlapping migration"
    );
    let completed = execution.await.unwrap();
    let (direct_reads, saw_moved) = tokio::time::timeout(Duration::from_secs(2), direct_reader)
        .await
        .expect("direct source reader did not observe the ownership handoff")
        .unwrap();
    assert!(direct_reads > 0, "{label} had no direct source reads");
    assert!(saw_moved, "{label} source never redirected after cutover");
    completed
}

#[path = "process_cluster/availability.rs"]
mod availability;
#[path = "process_cluster/experiment.rs"]
mod experiment;
#[path = "process_cluster/migration.rs"]
mod migration;
