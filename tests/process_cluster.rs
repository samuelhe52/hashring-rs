use std::{
    net::TcpListener,
    path::Path,
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

use hashring_rs::client::{ClientError, HashringClient};
use tonic::Code;

struct Process(Child);

impl Process {
    fn stop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
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

#[tokio::test]
async fn separate_processes_route_bytes_and_survive_coordinator_restart() {
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

    coordinator.stop();
    for node in &mut nodes {
        node.stop();
    }
}

#[tokio::test]
async fn permanent_grpc_status_is_returned_without_deadline_retry() {
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
