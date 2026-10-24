//! Concurrent same-key histories.
//!
//! Several clients issue PUT, GET, and DELETE against the same keys. Every
//! operation records its invocation and completion time, arguments, result,
//! returned version, and topology epoch. The checker below then asks whether
//! the mutations, ordered by the versions the owner assigned to them, satisfy
//! every real-time constraint and explain every read. A history that does not
//! is not linearizable.

use std::{
    collections::BTreeMap,
    fs,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use hashring_client::{ClientError, HashringClient};
use hashring_core::{
    migration::MigrationPhase,
    proto::{ErrorCode, RecordVersion},
    topology::Member,
};
use serde_json::json;
use tokio::sync::{Barrier, Mutex};

use super::{
    Process, connect_eventually, coordinator_arguments, process_test_lock, spawn_process,
    unused_ports, wait_for_full_rf, wait_for_listener,
};

const CLIENTS: usize = 4;
const OPERATIONS_PER_CLIENT: u64 = 40;

#[derive(Clone, Debug)]
enum MutationKind {
    Put(Vec<u8>),
    Delete,
}

#[derive(Clone, Debug)]
struct Mutation {
    client: usize,
    sequence: u64,
    key: u64,
    kind: MutationKind,
    version: RecordVersion,
    invoked: Instant,
    completed: Instant,
    invoked_unix_ms: u128,
    completed_unix_ms: u128,
    topology_epoch: u64,
}

#[derive(Clone, Debug)]
enum ReadObservation {
    Present {
        value: Vec<u8>,
        version: RecordVersion,
    },
    Absent,
}

#[derive(Clone, Debug)]
struct Read {
    client: usize,
    sequence: u64,
    key: u64,
    observation: ReadObservation,
    invoked: Instant,
    completed: Instant,
    invoked_unix_ms: u128,
    completed_unix_ms: u128,
    topology_epoch: u64,
}

#[derive(Clone, Debug)]
enum Entry {
    Mutation(Mutation),
    Read(Read),
}

fn version_key(version: &RecordVersion) -> (u64, u64) {
    (version.topology_epoch, version.owner_sequence)
}

fn describe_version(version: &RecordVersion) -> String {
    format!(
        "epoch={} sequence={} owner={}",
        version.topology_epoch, version.owner_sequence, version.owner_node_id
    )
}

fn unix_ms_now() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("the system clock is after the Unix epoch")
        .as_millis()
}

/// A state of the single-key register.
#[derive(Clone, Debug, PartialEq)]
enum State {
    Absent,
    Present {
        value: Vec<u8>,
        version: RecordVersion,
    },
}

fn state_after(mutations: &[&Mutation], prefix: usize) -> State {
    match prefix.checked_sub(1).and_then(|index| mutations.get(index)) {
        None => State::Absent,
        Some(mutation) => match &mutation.kind {
            MutationKind::Delete => State::Absent,
            MutationKind::Put(value) => State::Present {
                value: value.clone(),
                version: mutation.version.clone(),
            },
        },
    }
}

fn observation_matches(state: &State, observation: &ReadObservation) -> bool {
    match (state, observation) {
        (State::Absent, ReadObservation::Absent) => true,
        (
            State::Present { value, version },
            ReadObservation::Present {
                value: observed_value,
                version: observed_version,
            },
        ) => value == observed_value && version == observed_version,
        _ => false,
    }
}

/// Checks one key's history against a single-register model.
///
/// Mutations are ordered by the version their owner assigned. That order is
/// the owner's application order, so a mutation that completed before another
/// one started must also have a smaller version. A read must fit between two
/// mutations: every mutation before its serialization point had started before
/// the read completed, and every mutation after it completed after the read
/// started.
fn check_key_history(key: u64, entries: &[Entry], phase: &str) {
    let mut mutations: Vec<&Mutation> = entries
        .iter()
        .filter_map(|entry| match entry {
            Entry::Mutation(mutation) if mutation.key == key => Some(mutation),
            _ => None,
        })
        .collect();
    let reads: Vec<&Read> = entries
        .iter()
        .filter_map(|entry| match entry {
            Entry::Read(read) if read.key == key => Some(read),
            _ => None,
        })
        .collect();
    mutations.sort_by_key(|mutation| version_key(&mutation.version));
    assert!(!mutations.is_empty(), "{phase}: key {key} has no mutations");

    let positions: BTreeMap<(u64, u64), usize> = mutations
        .iter()
        .enumerate()
        .map(|(index, mutation)| (version_key(&mutation.version), index))
        .collect();
    assert_eq!(
        positions.len(),
        mutations.len(),
        "{phase}: two mutations of key {key} share a version"
    );
    for (index, mutation) in mutations.iter().enumerate() {
        assert_eq!(
            positions[&version_key(&mutation.version)],
            index,
            "{phase}: version order of key {key} is inconsistent"
        );
    }

    // The owner's version order must respect real time. A mutation that
    // completed before another was invoked cannot be ordered after it.
    for later in &mutations {
        for earlier in &mutations {
            if earlier.completed < later.invoked {
                let earlier_index = positions[&version_key(&earlier.version)];
                let later_index = positions[&version_key(&later.version)];
                assert!(
                    earlier_index < later_index,
                    "{phase}: key {key} mutation client {} sequence {} ({}) at epoch {} completed before client {} sequence {} ({}) at epoch {} was invoked, but has the later version",
                    earlier.client,
                    earlier.sequence,
                    describe_version(&earlier.version),
                    earlier.topology_epoch,
                    later.client,
                    later.sequence,
                    describe_version(&later.version),
                    later.topology_epoch
                );
            }
        }
    }

    for read in reads {
        let matching_prefixes: Vec<usize> = (0..=mutations.len())
            .filter(|prefix| {
                observation_matches(&state_after(&mutations, *prefix), &read.observation)
                    && mutations[..*prefix]
                        .iter()
                        .all(|mutation| mutation.invoked < read.completed)
                    && mutations[*prefix..]
                        .iter()
                        .all(|mutation| mutation.completed > read.invoked)
            })
            .collect();
        assert!(
            !matching_prefixes.is_empty(),
            "{phase}: key {key} read by client {} sequence {} ({:?}) at epoch {} has no legal serialization point among {} mutations",
            read.client,
            read.sequence,
            read.observation,
            read.topology_epoch,
            mutations.len()
        );
    }
}

struct Cluster {
    endpoint: String,
    ports: Vec<u16>,
    members: Vec<(String, u16)>,
    _coordinator: Process,
    nodes: Vec<Process>,
    client: HashringClient,
}

impl Cluster {
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

async fn start_cluster(directory: &std::path::Path, initial: usize, peak: usize) -> Cluster {
    let ports = unused_ports(peak + 1);
    let endpoint = format!("http://127.0.0.1:{}", ports[0]);
    let members: Vec<_> = (1..=peak)
        .map(|index| (format!("node-{index}"), ports[index]))
        .collect();
    let coordinator = spawn_process(&coordinator_arguments(
        ports[0],
        &directory.join("coordinator.redb"),
        &members[..initial],
    ));
    wait_for_listener(ports[0]);
    let client = connect_eventually(&endpoint).await;
    let mut nodes = Vec::new();
    for (node_id, port) in &members[..initial] {
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
    for (_, port) in &members[..initial] {
        wait_for_listener(*port);
    }
    wait_for_full_rf(&endpoint, 1, 3).await;
    Cluster {
        endpoint,
        ports,
        members,
        _coordinator: coordinator,
        nodes,
        client,
    }
}

async fn preload(client: &HashringClient, key: u64, value: &[u8], entries: &Mutex<Vec<Entry>>) {
    let invoked = Instant::now();
    let invoked_unix_ms = unix_ms_now();
    let epoch = client.epoch().await;
    let output = client
        .put(key.to_be_bytes().to_vec(), value.to_vec())
        .await
        .unwrap();
    let completed = Instant::now();
    entries.lock().await.push(Entry::Mutation(Mutation {
        client: 0,
        sequence: 0,
        key,
        kind: MutationKind::Put(value.to_vec()),
        version: output.version,
        invoked,
        completed,
        invoked_unix_ms,
        completed_unix_ms: unix_ms_now(),
        topology_epoch: epoch,
    }));
}

async fn run_phase(
    endpoint: &str,
    keys: &[u64],
    entries: Arc<Mutex<Vec<Entry>>>,
    id: Arc<AtomicU64>,
    label: &'static str,
) {
    let barrier = Arc::new(Barrier::new(CLIENTS));
    let mut handles = Vec::new();
    for client_id in 0..CLIENTS {
        let endpoint = endpoint.to_owned();
        let keys = keys.to_vec();
        let entries = Arc::clone(&entries);
        let barrier = Arc::clone(&barrier);
        let id = Arc::clone(&id);
        handles.push(tokio::spawn(async move {
            let client = HashringClient::connect(endpoint, Duration::from_secs(30))
                .await
                .unwrap();
            barrier.wait().await;
            for step in 0..OPERATIONS_PER_CLIENT {
                let key = keys[(step as usize + client_id) % keys.len()];
                let operation = (step * 7 + client_id as u64 * 3) % 10;
                let invoked = Instant::now();
                let invoked_unix_ms = unix_ms_now();
                let epoch = client.epoch().await;
                match operation {
                    0..=3 => {
                        let value = format!("{label}-client{client_id}-step{step}").into_bytes();
                        let output = client
                            .put(key.to_be_bytes().to_vec(), value.clone())
                            .await
                            .unwrap();
                        let completed = Instant::now();
                        entries.lock().await.push(Entry::Mutation(Mutation {
                            client: client_id,
                            sequence: id.fetch_add(1, Ordering::Relaxed),
                            key,
                            kind: MutationKind::Put(value),
                            version: output.version,
                            invoked,
                            completed,
                            invoked_unix_ms,
                            completed_unix_ms: unix_ms_now(),
                            topology_epoch: epoch,
                        }));
                    }
                    4..=6 => {
                        let observation = match client.get(key.to_be_bytes().to_vec()).await {
                            Ok(output) => ReadObservation::Present {
                                value: output.value,
                                version: output.version,
                            },
                            Err(ClientError::Operation(failure))
                                if failure.code == ErrorCode::NotFound =>
                            {
                                ReadObservation::Absent
                            }
                            Err(error) => {
                                panic!(
                                    "{label}: GET failed during a healthy history run: {error:?}"
                                )
                            }
                        };
                        let completed = Instant::now();
                        entries.lock().await.push(Entry::Read(Read {
                            client: client_id,
                            sequence: id.fetch_add(1, Ordering::Relaxed),
                            key,
                            observation,
                            invoked,
                            completed,
                            invoked_unix_ms,
                            completed_unix_ms: unix_ms_now(),
                            topology_epoch: epoch,
                        }));
                    }
                    _ => {
                        let output = client.delete(key.to_be_bytes().to_vec()).await.unwrap();
                        let completed = Instant::now();
                        entries.lock().await.push(Entry::Mutation(Mutation {
                            client: client_id,
                            sequence: id.fetch_add(1, Ordering::Relaxed),
                            key,
                            kind: MutationKind::Delete,
                            version: output.version,
                            invoked,
                            completed,
                            invoked_unix_ms,
                            completed_unix_ms: unix_ms_now(),
                            topology_epoch: epoch,
                        }));
                    }
                }
            }
        }));
    }
    for handle in handles {
        handle.await.unwrap();
    }
}

fn check_all(entries: &[Entry], keys: &[u64], phase: &str) {
    for &key in keys {
        check_key_history(key, entries, phase);
    }
}

fn artifact_dir(name: &str) -> PathBuf {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join("test-artifacts")
        .join(format!("{name}-{}", std::process::id()));
    fs::create_dir_all(&path).expect("creating the test artifact directory");
    path
}

/// Writes the recorded history next to the test binary, where a failed or
/// successful run can be inspected after the temporary cluster is gone.
fn write_history(name: &str, entries: &[Entry]) {
    let version = |version: &RecordVersion| {
        json!({
            "topology_epoch": version.topology_epoch,
            "owner_sequence": version.owner_sequence,
            "owner_node_id": version.owner_node_id,
        })
    };
    let lines: Vec<String> = entries
        .iter()
        .map(|entry| match entry {
            Entry::Mutation(mutation) => {
                let (kind, value) = match &mutation.kind {
                    MutationKind::Put(value) => ("put", Some(hex::encode(value))),
                    MutationKind::Delete => ("delete", None),
                };
                json!({
                    "kind": kind,
                    "key": mutation.key,
                    "value_hex": value,
                    "client": mutation.client,
                    "sequence": mutation.sequence,
                    "version": version(&mutation.version),
                    "invoked_unix_ms": mutation.invoked_unix_ms,
                    "completed_unix_ms": mutation.completed_unix_ms,
                    "topology_epoch": mutation.topology_epoch,
                })
                .to_string()
            }
            Entry::Read(read) => {
                let observation = match &read.observation {
                    ReadObservation::Present {
                        value,
                        version: observed,
                    } => json!({
                        "state": "present",
                        "value_hex": hex::encode(value),
                        "version": version(observed),
                    }),
                    ReadObservation::Absent => json!({ "state": "absent" }),
                };
                json!({
                    "kind": "get",
                    "key": read.key,
                    "client": read.client,
                    "sequence": read.sequence,
                    "observation": observation,
                    "invoked_unix_ms": read.invoked_unix_ms,
                    "completed_unix_ms": read.completed_unix_ms,
                    "topology_epoch": read.topology_epoch,
                })
                .to_string()
            }
        })
        .collect();
    let path = artifact_dir(name).join("history.jsonl");
    fs::write(&path, lines.join("\n") + "\n").expect("writing the recorded history");
    eprintln!("same-key history written to {}", path.display());
}

#[tokio::test]
async fn concurrent_same_key_histories_are_linearizable_within_one_owner_epoch() {
    let _guard = process_test_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let cluster = start_cluster(directory.path(), 3, 3).await;
    let keys = [7_u64, 8_u64];
    let entries = Arc::new(Mutex::new(Vec::new()));
    for &key in &keys {
        preload(
            &cluster.client,
            key,
            format!("preload-{key}").as_bytes(),
            &entries,
        )
        .await;
    }
    let id = Arc::new(AtomicU64::new(1));
    run_phase(
        &cluster.endpoint,
        &keys,
        Arc::clone(&entries),
        Arc::clone(&id),
        "epoch-one",
    )
    .await;
    let entries = entries.lock().await.clone();
    write_history("same-key-one-epoch", &entries);
    check_all(&entries, &keys, "one owner epoch");
    assert!(
        entries.iter().any(|entry| matches!(
            entry,
            Entry::Read(Read {
                observation: ReadObservation::Absent,
                ..
            })
        )),
        "the history has no read that observed an intermediate delete"
    );
}

#[tokio::test]
async fn concurrent_same_key_histories_are_linearizable_across_a_membership_handoff() {
    let _guard = process_test_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let mut cluster = start_cluster(directory.path(), 3, 4).await;
    let client = cluster.client.clone();
    let base = client.refresh_topology().await.unwrap();
    let mut target = cluster.member_list(3);
    let spare = Member {
        node_id: "node-4".into(),
        endpoint: format!("http://127.0.0.1:{}", cluster.ports[4]),
    };
    target.push(spare.clone());
    let change = client.begin_topology_change(target).await.unwrap();
    let keys: Vec<u64> = (0_u64..100_000)
        .filter(|key| {
            let key = key.to_be_bytes();
            base.owner(&key).unwrap().node_id != change.target_topology.owner(&key).unwrap().node_id
        })
        .take(2)
        .collect();
    assert_eq!(keys.len(), 2, "the handoff must move keys");

    let entries = Arc::new(Mutex::new(Vec::new()));
    for &key in &keys {
        preload(&client, key, format!("preload-{key}").as_bytes(), &entries).await;
    }
    let id = Arc::new(AtomicU64::new(1));
    run_phase(
        &cluster.endpoint,
        &keys,
        Arc::clone(&entries),
        Arc::clone(&id),
        "before-handoff",
    )
    .await;

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
    let handed_off = client.refresh_topology().await.unwrap();
    for &key in &keys {
        assert_ne!(
            handed_off.owner(&key.to_be_bytes()).unwrap().node_id,
            base.owner(&key.to_be_bytes()).unwrap().node_id,
            "key {key} did not change owner"
        );
    }

    run_phase(
        &cluster.endpoint,
        &keys,
        Arc::clone(&entries),
        Arc::clone(&id),
        "after-handoff",
    )
    .await;
    let entries = entries.lock().await.clone();
    write_history("same-key-handoff", &entries);
    check_all(&entries, &keys, "membership handoff");
}
