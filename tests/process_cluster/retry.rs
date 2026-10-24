//! End-to-end retries after a mutation's response is lost.
//!
//! The driver applies a mutation with a caller-chosen mutation ID, throws the
//! successful response away, and then retries the same ID after a membership
//! change or an owner promotion. The retry must return the original version
//! and must not apply the mutation a second time.

use std::{
    fs,
    path::Path,
    time::{Duration, Instant},
};

use hashring_client::{ClientError, HashringClient};
use hashring_core::{
    migration::MigrationPhase,
    proto::{Empty, RecordVersion, coordinator_client::CoordinatorClient},
    topology::{Member, TopologySnapshot, WriteAckPolicy},
};

use super::{
    Process, coordinator_arguments, process_test_lock, spawn_process, unused_ports,
    wait_for_full_rf, wait_for_listener,
};

struct Cluster {
    endpoint: String,
    ports: Vec<u16>,
    members: Vec<(String, u16)>,
    _coordinator: Process,
    nodes: Vec<Process>,
}

impl Cluster {
    fn spare(&self, initial: usize) -> Member {
        let (node_id, _) = &self.members[initial];
        Member {
            node_id: node_id.clone(),
            endpoint: format!("http://127.0.0.1:{}", self.ports[initial + 1]),
        }
    }

    fn member_list(&self, prefix: usize) -> Vec<Member> {
        self.members[..prefix]
            .iter()
            .map(|(node_id, port)| Member {
                node_id: node_id.clone(),
                endpoint: format!("http://127.0.0.1:{port}"),
            })
            .collect()
    }
}

/// Everything the test learned about one logical request.
#[derive(Clone, Debug, serde::Serialize)]
struct RequestTrace {
    logical_id: String,
    key: u64,
    operation: &'static str,
    mutation_id: String,
    first_attempt_version: Option<String>,
    retry_epoch: u64,
    retry_version: Option<String>,
    outcome: String,
}

fn version_string(version: &RecordVersion) -> String {
    format!(
        "epoch={} sequence={} owner={}",
        version.topology_epoch, version.owner_sequence, version.owner_node_id
    )
}

fn artifact_dir(name: &str) -> std::path::PathBuf {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join("test-artifacts")
        .join(format!("{name}-{}", std::process::id()));
    fs::create_dir_all(&path).expect("creating the test artifact directory");
    path
}

fn write_trace(directory: &Path, traces: &[RequestTrace]) {
    let body = traces
        .iter()
        .map(|trace| serde_json::to_string(trace).expect("trace serializes"))
        .collect::<Vec<_>>()
        .join("\n");
    fs::write(directory.join("retry-trace.jsonl"), body + "\n").unwrap();
}

async fn start_cluster(directory: &Path, initial_nodes: usize, peak_nodes: usize) -> Cluster {
    let ports = unused_ports(peak_nodes + 1);
    let endpoint = format!("http://127.0.0.1:{}", ports[0]);
    let members: Vec<_> = (1..=peak_nodes)
        .map(|index| (format!("node-{index}"), ports[index]))
        .collect();
    let coordinator = spawn_process(&coordinator_arguments(
        ports[0],
        &directory.join("coordinator.redb"),
        &members[..initial_nodes],
    ));
    wait_for_listener(ports[0]);
    let mut nodes = Vec::new();
    for (node_id, port) in &members[..initial_nodes] {
        nodes.push(spawn_process(&[
            "node".into(),
            "--id".into(),
            node_id.clone(),
            "--listen".into(),
            format!("127.0.0.1:{port}"),
            "--coordinator".into(),
            endpoint.clone(),
        ]));
    }
    for (_, port) in &members[..initial_nodes] {
        wait_for_listener(*port);
    }
    wait_for_full_rf(&endpoint, 1, 3).await;
    Cluster {
        endpoint,
        ports,
        members,
        _coordinator: coordinator,
        nodes,
    }
}

async fn wait_for_caught_up_followers(endpoint: &str, epoch: u64) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let mut admin = CoordinatorClient::connect(endpoint.to_owned())
            .await
            .unwrap();
        let status = admin
            .get_replica_status(Empty {})
            .await
            .unwrap()
            .into_inner();
        if status.topology_epoch == epoch
            && !status.ranges.is_empty()
            && status.ranges.iter().all(|range| {
                range.followers.iter().all(|follower| {
                    follower.admitted
                        && follower.lag_known
                        && follower.stream_head == follower.stream_cursor
                })
            })
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "followers did not catch up at epoch {epoch}: {status:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn moving_key(base: &TopologySnapshot, target: &TopologySnapshot, start: u64) -> u64 {
    (start..start + 100_000)
        .find(|key| {
            let key = key.to_be_bytes();
            base.owner(&key).unwrap().node_id != target.owner(&key).unwrap().node_id
        })
        .expect("a scale-out must move at least one key")
}

#[tokio::test]
async fn response_loss_retry_after_migration_reuses_result_and_keeps_newer_value() {
    let _guard = process_test_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let mut cluster = start_cluster(directory.path(), 3, 4).await;
    let client = HashringClient::connect(&cluster.endpoint, Duration::from_secs(30))
        .await
        .unwrap();

    let base = client.refresh_topology().await.unwrap();
    let mut target = cluster.member_list(3);
    let spare = cluster.spare(3);
    target.push(spare.clone());
    let change = client.begin_topology_change(target).await.unwrap();
    let put_key = moving_key(&base, &change.target_topology, 0);
    let delete_key = moving_key(&base, &change.target_topology, put_key + 1);
    assert_ne!(put_key, delete_key);

    let put_id = "response-loss-migration-put";
    let put_value = b"lost-put-value".to_vec();
    // The response to this successful write is intentionally discarded. The
    // write is applied, but the caller only knows the mutation ID.
    let first_put = client
        .put_with_mutation_id(put_key.to_be_bytes().to_vec(), put_value.clone(), put_id)
        .await
        .unwrap();
    client
        .put(delete_key.to_be_bytes().to_vec(), b"before-delete".to_vec())
        .await
        .unwrap();
    let delete_id = "response-loss-migration-delete";
    let first_delete = client
        .delete_with_mutation_id(delete_key.to_be_bytes().to_vec(), delete_id)
        .await
        .unwrap();

    let spare_port = cluster.ports[4];
    cluster.nodes.push(spawn_process(&[
        "node".into(),
        "--id".into(),
        spare.node_id.clone(),
        "--listen".into(),
        format!("127.0.0.1:{spare_port}"),
        "--coordinator".into(),
        cluster.endpoint.clone(),
    ]));
    wait_for_listener(spare_port);
    let completed = client
        .execute_topology_change(
            &change.change_id,
            change.base_epoch,
            change.target_topology.epoch,
        )
        .await
        .unwrap();
    assert_eq!(completed.phase, MigrationPhase::Complete);
    wait_for_full_rf(&cluster.endpoint, completed.target_topology.epoch, 3).await;

    let moved = client.refresh_topology().await.unwrap();
    assert_ne!(
        moved.owner(&put_key.to_be_bytes()).unwrap().node_id,
        base.owner(&put_key.to_be_bytes()).unwrap().node_id,
        "the PUT key did not move"
    );
    assert_ne!(
        moved.owner(&delete_key.to_be_bytes()).unwrap().node_id,
        base.owner(&delete_key.to_be_bytes()).unwrap().node_id,
        "the DELETE key did not move"
    );

    // Retry the lost PUT on its new owner. The answer is the original receipt,
    // not a second application with a new version.
    let retried_put = client
        .put_with_mutation_id(put_key.to_be_bytes().to_vec(), put_value.clone(), put_id)
        .await
        .unwrap();
    assert_eq!(retried_put.version, first_put.version);
    let read = client.get(put_key.to_be_bytes().to_vec()).await.unwrap();
    assert_eq!(read.value, put_value);
    assert_eq!(read.version, first_put.version);

    // A later mutation of the same key wins. Retrying the old request must
    // answer from its receipt and must not overwrite the newer value.
    let newer = client
        .put(put_key.to_be_bytes().to_vec(), b"newer-value".to_vec())
        .await
        .unwrap();
    assert_ne!(newer.version, first_put.version);
    let old_retry = client
        .put_with_mutation_id(put_key.to_be_bytes().to_vec(), put_value.clone(), put_id)
        .await
        .unwrap();
    assert_eq!(old_retry.version, first_put.version);
    let read = client.get(put_key.to_be_bytes().to_vec()).await.unwrap();
    assert_eq!(read.value, b"newer-value");
    assert_eq!(read.version, newer.version);

    // Retrying the old mutation ID with a different payload is rejected, not
    // mistaken for the original write.
    let conflict = client
        .put_with_mutation_id(
            put_key.to_be_bytes().to_vec(),
            b"different-value".to_vec(),
            put_id,
        )
        .await
        .unwrap_err();
    match conflict {
        ClientError::Operation(failure) => {
            assert_eq!(
                failure.code,
                hashring_core::proto::ErrorCode::MutationIdConflict
            )
        }
        other => panic!("unexpected conflict result: {other:?}"),
    }

    // The delete's response is lost the same way. Its receipt still answers
    // with the original tombstone version and does not delete a newer value.
    let retried_delete = client
        .delete_with_mutation_id(delete_key.to_be_bytes().to_vec(), delete_id)
        .await
        .unwrap();
    assert_eq!(retried_delete.version, first_delete.version);
    match client.get(delete_key.to_be_bytes().to_vec()).await {
        Err(ClientError::Operation(failure)) => {
            assert_eq!(failure.code, hashring_core::proto::ErrorCode::NotFound)
        }
        Ok(output) => panic!("deleted key read back as {:?}", output.value),
        Err(other) => panic!("unexpected read result: {other:?}"),
    }
    let replacement = client
        .put(
            delete_key.to_be_bytes().to_vec(),
            b"after-old-delete".to_vec(),
        )
        .await
        .unwrap();
    let old_delete_retry = client
        .delete_with_mutation_id(delete_key.to_be_bytes().to_vec(), delete_id)
        .await
        .unwrap();
    assert_eq!(old_delete_retry.version, first_delete.version);
    let read = client.get(delete_key.to_be_bytes().to_vec()).await.unwrap();
    assert_eq!(read.value, b"after-old-delete");
    assert_eq!(read.version, replacement.version);

    let traces = vec![
        RequestTrace {
            logical_id: "migration-put".into(),
            key: put_key,
            operation: "put",
            mutation_id: put_id.into(),
            first_attempt_version: Some(version_string(&first_put.version)),
            retry_epoch: completed.target_topology.epoch,
            retry_version: Some(version_string(&retried_put.version)),
            outcome: "original result returned after migration".into(),
        },
        RequestTrace {
            logical_id: "migration-delete".into(),
            key: delete_key,
            operation: "delete",
            mutation_id: delete_id.into(),
            first_attempt_version: Some(version_string(&first_delete.version)),
            retry_epoch: completed.target_topology.epoch,
            retry_version: Some(version_string(&retried_delete.version)),
            outcome: "original result returned after migration".into(),
        },
    ];
    write_trace(&artifact_dir("retry-migration"), &traces);
}

#[tokio::test]
async fn response_loss_retry_after_owner_promotion_reuses_result_and_keeps_newer_value() {
    let _guard = process_test_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let mut cluster = start_cluster(directory.path(), 3, 3).await;
    let client = HashringClient::connect(&cluster.endpoint, Duration::from_secs(30))
        .await
        .unwrap();

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
    wait_for_caught_up_followers(&cluster.endpoint, applied.target_topology.epoch).await;

    let topology = client.refresh_topology().await.unwrap();
    let pick = |start: u64| {
        (start..start + 100_000)
            .map(|key| key.to_be_bytes().to_vec())
            .find(|key| {
                topology
                    .replica_node_ids_for_token(topology.key_token(key))
                    .unwrap()
                    .starts_with(&["node-1", "node-2"])
            })
            .expect("test needs a range owned by node-1 with node-2 as first successor")
    };
    let put_key = pick(0);
    let delete_key = pick(1);
    assert_ne!(put_key, delete_key);
    let put_key_number = u64::from_be_bytes(put_key.clone().try_into().unwrap());
    let delete_key_number = u64::from_be_bytes(delete_key.clone().try_into().unwrap());

    let put_id = "response-loss-promotion-put";
    let put_value = b"lost-before-promotion".to_vec();
    let first_put = client
        .put_with_mutation_id(put_key.clone(), put_value.clone(), put_id)
        .await
        .unwrap();
    client
        .put(delete_key.clone(), b"before-promotion-delete".to_vec())
        .await
        .unwrap();
    let delete_id = "response-loss-promotion-delete";
    let first_delete = client
        .delete_with_mutation_id(delete_key.clone(), delete_id)
        .await
        .unwrap();
    // Both mutations and their receipts must be on node-2 before it is
    // promoted, so this test checks deduplication across the promotion and
    // not a permitted FirstSuccessor tail loss.
    wait_for_caught_up_followers(&cluster.endpoint, applied.target_topology.epoch).await;

    cluster.nodes[0].stop();
    let promoted = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let current = client.refresh_topology().await.unwrap();
            if current.epoch > topology.epoch
                && current
                    .members
                    .iter()
                    .all(|member| member.node_id != "node-1")
            {
                break current;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("the failed owner was not removed");
    assert_eq!(promoted.owner(&put_key).unwrap().node_id, "node-2");
    assert_eq!(promoted.owner(&delete_key).unwrap().node_id, "node-2");

    let retried_put = client
        .put_with_mutation_id(put_key.clone(), put_value.clone(), put_id)
        .await
        .unwrap();
    assert_eq!(retried_put.version, first_put.version);
    let read = client.get(put_key.clone()).await.unwrap();
    assert_eq!(read.value, put_value);
    assert_eq!(read.version, first_put.version);

    let newer = client
        .put(put_key.clone(), b"newer-after-promotion".to_vec())
        .await
        .unwrap();
    let old_retry = client
        .put_with_mutation_id(put_key.clone(), put_value, put_id)
        .await
        .unwrap();
    assert_eq!(old_retry.version, first_put.version);
    let read = client.get(put_key.clone()).await.unwrap();
    assert_eq!(read.value, b"newer-after-promotion");
    assert_eq!(read.version, newer.version);

    let retried_delete = client
        .delete_with_mutation_id(delete_key.clone(), delete_id)
        .await
        .unwrap();
    assert_eq!(retried_delete.version, first_delete.version);
    match client.get(delete_key.clone()).await {
        Err(ClientError::Operation(failure)) => {
            assert_eq!(failure.code, hashring_core::proto::ErrorCode::NotFound)
        }
        Ok(output) => panic!("deleted key read back as {:?}", output.value),
        Err(other) => panic!("unexpected read result: {other:?}"),
    }
    let replacement = client
        .put(delete_key.clone(), b"after-promoted-delete".to_vec())
        .await
        .unwrap();
    let old_delete_retry = client
        .delete_with_mutation_id(delete_key.clone(), delete_id)
        .await
        .unwrap();
    assert_eq!(old_delete_retry.version, first_delete.version);
    let read = client.get(delete_key.clone()).await.unwrap();
    assert_eq!(read.value, b"after-promoted-delete");
    assert_eq!(read.version, replacement.version);

    let traces = vec![
        RequestTrace {
            logical_id: "promotion-put".into(),
            key: put_key_number,
            operation: "put",
            mutation_id: put_id.into(),
            first_attempt_version: Some(version_string(&first_put.version)),
            retry_epoch: promoted.epoch,
            retry_version: Some(version_string(&retried_put.version)),
            outcome: "original result returned after promotion".into(),
        },
        RequestTrace {
            logical_id: "promotion-delete".into(),
            key: delete_key_number,
            operation: "delete",
            mutation_id: delete_id.into(),
            first_attempt_version: Some(version_string(&first_delete.version)),
            retry_epoch: promoted.epoch,
            retry_version: Some(version_string(&retried_delete.version)),
            outcome: "original result returned after promotion".into(),
        },
    ];
    write_trace(&artifact_dir("retry-promotion"), &traces);
}
