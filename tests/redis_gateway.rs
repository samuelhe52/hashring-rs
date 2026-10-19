//! The Redis gateway against a real cluster and a real Redis client.
//!
//! Everything here goes through a TCP socket and the `redis` crate, so what it
//! proves is that an unmodified client library can drive the gateway: the
//! handshake it sends on connect, the commands it reaches for, and RESP3
//! negotiation all have to work for `redis-cli`, Jedis, Lettuce and go-redis to
//! work at all.

use std::{
    collections::HashMap,
    path::Path,
    process::{Child, Command, Stdio},
    sync::OnceLock,
    time::Duration,
};

use hashring_client::HashringClient;
use hashring_core::topology::Member;

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

/// One test at a time: every case starts a coordinator, three nodes and a
/// gateway, and they are all reached over the loopback interface.
fn test_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

fn unused_ports(count: usize) -> Vec<u16> {
    let listeners: Vec<_> = (0..count)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    listeners
        .iter()
        .map(|listener| listener.local_addr().unwrap().port())
        .collect()
}

fn spawn(arguments: &[String]) -> Process {
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
    for _ in 0..600 {
        if std::net::TcpStream::connect_timeout(&address, Duration::from_millis(20)).is_ok() {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("nothing came up on port {port}");
}

/// A coordinator, three data nodes and a gateway, kept alive as long as the
/// value is held.
struct Cluster {
    processes: Vec<Process>,
    coordinator: String,
    port: u16,
    password: Option<String>,
}

impl Cluster {
    fn start(state: &Path, password: Option<&str>) -> Self {
        let ports = unused_ports(5);
        let (coordinator_port, gateway_port) = (ports[0], ports[4]);
        let members = vec![
            ("node-1".to_owned(), ports[1]),
            ("node-2".to_owned(), ports[2]),
            ("node-3".to_owned(), ports[3]),
        ];
        let coordinator = format!("http://127.0.0.1:{coordinator_port}");
        let mut bootstrap = vec![
            "coordinator".to_owned(),
            "--listen".to_owned(),
            format!("127.0.0.1:{coordinator_port}"),
            "--state".to_owned(),
            state.display().to_string(),
            "--virtual-nodes".to_owned(),
            "32".to_owned(),
            "--minimum-admitted-copies".to_owned(),
            "1".to_owned(),
            "--minimum-healthy-followers".to_owned(),
            "0".to_owned(),
        ];
        for (node_id, port) in &members {
            bootstrap.extend([
                "--member".to_owned(),
                format!("{node_id}=http://127.0.0.1:{port}"),
            ]);
        }
        let mut processes = vec![spawn(&bootstrap)];
        wait_for_listener(coordinator_port);
        for (node_id, port) in &members {
            processes.push(spawn(&[
                "node".to_owned(),
                "--id".to_owned(),
                node_id.clone(),
                "--listen".to_owned(),
                format!("127.0.0.1:{port}"),
                "--coordinator".to_owned(),
                coordinator.clone(),
            ]));
            wait_for_listener(*port);
        }
        let mut gateway = vec![
            "redis-gateway".to_owned(),
            "--listen".to_owned(),
            format!("127.0.0.1:{gateway_port}"),
            "--coordinator".to_owned(),
            coordinator.clone(),
            // The first command may arrive before the gateway has a topology.
            "--deadline-ms".to_owned(),
            "10000".to_owned(),
        ];
        if let Some(password) = password {
            gateway.extend(["--requirepass".to_owned(), password.to_owned()]);
        }
        processes.push(spawn(&gateway));
        wait_for_listener(gateway_port);
        Self {
            processes,
            coordinator,
            port: gateway_port,
            password: password.map(str::to_owned),
        }
    }

    fn url(&self, password: Option<&str>) -> String {
        match password {
            Some(password) => format!("redis://:{password}@127.0.0.1:{}", self.port),
            None => format!("redis://127.0.0.1:{}", self.port),
        }
    }

    /// Another gateway in front of the same cluster, as a second process
    /// behind a load balancer would be. Its URL, and the process to keep alive.
    fn second_gateway(&self) -> (Process, String) {
        let port = unused_ports(1)[0];
        let mut arguments = vec![
            "redis-gateway".to_owned(),
            "--listen".to_owned(),
            format!("127.0.0.1:{port}"),
            "--coordinator".to_owned(),
            self.coordinator.clone(),
            "--deadline-ms".to_owned(),
            "10000".to_owned(),
        ];
        if let Some(password) = &self.password {
            arguments.extend(["--requirepass".to_owned(), password.clone()]);
        }
        let process = spawn(&arguments);
        wait_for_listener(port);
        (process, format!("redis://127.0.0.1:{port}"))
    }

    fn authenticated_url(&self) -> String {
        self.url(self.password.as_deref())
    }

    async fn try_connect(
        &self,
        url: &str,
    ) -> redis::RedisResult<redis::aio::MultiplexedConnection> {
        redis::Client::open(url.to_owned())?
            .get_multiplexed_async_connection()
            .await
    }

    async fn connect(&self, url: &str) -> redis::aio::MultiplexedConnection {
        self.try_connect(url)
            .await
            .expect("a redis client could not connect to the gateway")
    }

    async fn client(&self) -> redis::aio::MultiplexedConnection {
        self.connect(&self.authenticated_url()).await
    }

    async fn cluster_client(&self) -> HashringClient {
        for _ in 0..100 {
            if let Ok(client) =
                HashringClient::connect(&self.coordinator, Duration::from_secs(2)).await
            {
                return client;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("the coordinator never became ready");
    }
}

/// Every key of a cluster that is spread over more than one owner, so a
/// multi-key command has to reach more than one node.
fn keys_owned_by_different_nodes(
    topology: &hashring_core::topology::TopologySnapshot,
) -> Vec<String> {
    let mut per_node = std::collections::HashMap::new();
    for index in 0..10_000 {
        let key = format!("fan-out-{index}");
        let owner = topology.owner(key.as_bytes()).unwrap().node_id.clone();
        per_node.entry(owner).or_insert_with(Vec::new).push(key);
        if per_node.values().all(|keys| keys.len() > 4) {
            break;
        }
    }
    per_node.into_values().take(2).flatten().collect::<Vec<_>>()
}

/// The remaining TTL of a key, as a client sees it: -1 for a key that never
/// expires and -2 for one that is not there.
async fn ttl(connection: &mut redis::aio::MultiplexedConnection, key: &str) -> i64 {
    redis::cmd("TTL")
        .arg(key)
        .query_async(connection)
        .await
        .unwrap()
}

#[tokio::test]
async fn a_redis_client_drives_the_cluster_without_knowing_it_is_a_cluster() {
    let _guard = test_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let cluster = Cluster::start(&directory.path().join("coordinator.redb"), None);
    let mut connection = cluster.client().await;

    // Getting here is itself the handshake result: the client connected,
    // selected its database and identified itself, and the gateway answered.
    let pong: String = redis::cmd("PING")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(pong, "PONG");

    let _: () = redis::cmd("SET")
        .arg("greeting")
        .arg("hello")
        .query_async(&mut connection)
        .await
        .unwrap();
    let value: String = redis::cmd("GET")
        .arg("greeting")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(value, "hello");

    // A miss is a nil rather than an error, which is what every client expects.
    let missing: Option<String> = redis::cmd("GET")
        .arg("absent")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(missing, None);
    let absent: i64 = redis::cmd("EXISTS")
        .arg("absent")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(absent, 0);

    // Values are byte strings, including ones that are not valid UTF-8 and ones
    // with line endings in them.
    let binary = vec![0_u8, 255, 10, 13, 0];
    let _: () = redis::cmd("SET")
        .arg("binary")
        .arg(&binary)
        .query_async(&mut connection)
        .await
        .unwrap();
    let read: Vec<u8> = redis::cmd("GET")
        .arg("binary")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(read, binary);

    let removed: i64 = redis::cmd("DEL")
        .arg("greeting")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(removed, 1);
    // DELETE is idempotent in the cluster, and Redis reports 0 the second time.
    let removed_again: i64 = redis::cmd("DEL")
        .arg("greeting")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(removed_again, 0);

    let kind: String = redis::cmd("TYPE")
        .arg("binary")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(kind, "string");
    let kind: String = redis::cmd("TYPE")
        .arg("absent")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(kind, "none");
}

#[tokio::test]
async fn the_string_commands_behave_the_way_a_redis_client_expects() {
    let _guard = test_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let cluster = Cluster::start(&directory.path().join("coordinator.redb"), None);
    let mut connection = cluster.client().await;

    let _: () = redis::cmd("SET")
        .arg("counter")
        .arg(1)
        .query_async(&mut connection)
        .await
        .unwrap();
    let value: i64 = redis::cmd("INCR")
        .arg("counter")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(value, 2);
    let value: i64 = redis::cmd("INCRBY")
        .arg("counter")
        .arg(10)
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(value, 12);
    let value: i64 = redis::cmd("DECRBY")
        .arg("counter")
        .arg(5)
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(value, 7);
    // An absent key counts as zero, which is what INCR documents.
    let value: i64 = redis::cmd("INCR")
        .arg("fresh-counter")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(value, 1);

    let _: () = redis::cmd("SET")
        .arg("float")
        .arg(10.5)
        .query_async(&mut connection)
        .await
        .unwrap();
    let updated: String = redis::cmd("INCRBYFLOAT")
        .arg("float")
        .arg(0.25)
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(updated, "10.75");
    let stored: String = redis::cmd("GET")
        .arg("float")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(stored, "10.75");

    let _: () = redis::cmd("SET")
        .arg("text")
        .arg("Hello")
        .query_async(&mut connection)
        .await
        .unwrap();
    let range: String = redis::cmd("GETRANGE")
        .arg("text")
        .arg(1)
        .arg(3)
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(range, "ell");
    let range: String = redis::cmd("GETRANGE")
        .arg("text")
        .arg(-3)
        .arg(-1)
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(range, "llo");
    let length: i64 = redis::cmd("STRLEN")
        .arg("text")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(length, 5);

    let length: i64 = redis::cmd("APPEND")
        .arg("text")
        .arg("!")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(length, 6);
    let length: i64 = redis::cmd("SETRANGE")
        .arg("text")
        .arg(0)
        .arg("J")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(length, 6);
    let whole: String = redis::cmd("GET")
        .arg("text")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(whole, "Jello!");

    // SETRANGE pads with zero bytes.
    let length: i64 = redis::cmd("SETRANGE")
        .arg("padded")
        .arg(3)
        .arg("x")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(length, 4);
    let padded: Vec<u8> = redis::cmd("GET")
        .arg("padded")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(padded, b"\0\0\0x".to_vec());

    // NX and XX, with and without the old value.
    let set: Option<String> = redis::cmd("SET")
        .arg("counter")
        .arg(99)
        .arg("NX")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(set, None, "NX on an existing key writes nothing");
    let set: Option<String> = redis::cmd("SET")
        .arg("brand-new")
        .arg(1)
        .arg("NX")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(set.as_deref(), Some("OK"));
    let refused: Option<String> = redis::cmd("SET")
        .arg("absent-key")
        .arg(1)
        .arg("XX")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(refused, None, "XX on a missing key writes nothing");
    let previous: Option<String> = redis::cmd("SET")
        .arg("counter")
        .arg(42)
        .arg("GET")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(previous.as_deref(), Some("7"), "GET returns what was there");

    // GETSET returns what was there, GETDEL removes it.
    let previous: Option<String> = redis::cmd("GETSET")
        .arg("brand-new")
        .arg(5)
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(previous.as_deref(), Some("1"));
    let removed: Option<String> = redis::cmd("GETDEL")
        .arg("brand-new")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(removed.as_deref(), Some("5"));

    // A value that is not an integer is an error, and the value survives it.
    let _: () = redis::cmd("SET")
        .arg("not-a-number")
        .arg("abc")
        .query_async(&mut connection)
        .await
        .unwrap();
    let error = redis::cmd("INCR")
        .arg("not-a-number")
        .query_async::<i64>(&mut connection)
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("not an integer"),
        "unexpected error: {error}"
    );
    let unchanged: String = redis::cmd("GET")
        .arg("not-a-number")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(unchanged, "abc");
}

#[tokio::test]
async fn multi_key_commands_reach_every_owner() {
    let _guard = test_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let cluster = Cluster::start(&directory.path().join("coordinator.redb"), None);
    let mut connection = cluster.client().await;
    let topology = cluster.cluster_client().await.topology().await;
    let keys = keys_owned_by_different_nodes(&topology);
    assert!(keys.len() > 8, "not enough keys to span owners");

    let pairs: Vec<(String, String)> = keys
        .iter()
        .enumerate()
        .map(|(index, key)| (key.clone(), format!("value-{index}")))
        .collect();
    let _: () = redis::cmd("MSET")
        .arg(&pairs)
        .query_async(&mut connection)
        .await
        .unwrap();

    let values: Vec<Option<String>> = redis::cmd("MGET")
        .arg(&keys)
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(values.len(), keys.len());
    for (index, value) in values.iter().enumerate() {
        assert_eq!(value.as_deref(), Some(&*format!("value-{index}")));
    }

    // A missing key inside a multi-key command is a nil in place, not a failure.
    let mut with_missing = keys.clone();
    with_missing.push("multi-missing".to_owned());
    let values: Vec<Option<String>> = redis::cmd("MGET")
        .arg(&with_missing)
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(values.last().unwrap(), &None);
    assert_eq!(values.len(), with_missing.len());

    let found: i64 = redis::cmd("EXISTS")
        .arg(&keys)
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(found as usize, keys.len());
    let removed: i64 = redis::cmd("DEL")
        .arg(&keys)
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(removed as usize, keys.len());
    let found: i64 = redis::cmd("EXISTS")
        .arg(&keys)
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(found, 0);

    // MSETNX cannot set several keys atomically, so it is declined for more
    // than one key and is SETNX for one. A key named twice is one key.
    let refused = redis::cmd("MSETNX")
        .arg(&[("nx-1", "1"), ("nx-2", "2")])
        .query_async::<i64>(&mut connection)
        .await
        .unwrap_err();
    assert!(refused.to_string().contains("cannot be set atomically"));
    let present: i64 = redis::cmd("EXISTS")
        .arg(&["nx-1", "nx-2"])
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(present, 0);
    let written: i64 = redis::cmd("MSETNX")
        .arg(&[("nx-0", "stale"), ("nx-0", "0")])
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(written, 1);
    let written: i64 = redis::cmd("MSETNX")
        .arg(&[("nx-0", "other")])
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(written, 0);

    let _: () = redis::cmd("RENAME")
        .arg("nx-0")
        .arg("nx-renamed")
        .query_async(&mut connection)
        .await
        .unwrap();
    let missing: Option<String> = redis::cmd("GET")
        .arg("nx-0")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(missing, None);
    let renamed: String = redis::cmd("GET")
        .arg("nx-renamed")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(renamed, "0");
    let error = redis::cmd("RENAME")
        .arg("nx-absent")
        .arg("nx-wherever")
        .query_async::<()>(&mut connection)
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("no such key"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn expiry_is_the_clusters_own_and_survives_a_membership_change() {
    let _guard = test_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let cluster = Cluster::start(&directory.path().join("coordinator.redb"), None);
    let mut connection = cluster.client().await;

    let _: () = redis::cmd("SET")
        .arg("eternal")
        .arg("value")
        .query_async(&mut connection)
        .await
        .unwrap();
    // -1 is a key that never expires and -2 is no key at all, which is how a
    // client tells a dead key from a live one.
    assert_eq!(ttl(&mut connection, "eternal").await, -1);
    assert_eq!(ttl(&mut connection, "absent").await, -2);

    // A short deadline really hides the value, and does so without a delete
    // travelling through the cluster: the value is simply past its deadline.
    let _: () = redis::cmd("SETEX")
        .arg("fleeting")
        .arg(1)
        .arg("here")
        .query_async(&mut connection)
        .await
        .unwrap();
    let value: Option<String> = redis::cmd("GET")
        .arg("fleeting")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(value.as_deref(), Some("here"));
    assert_eq!(ttl(&mut connection, "fleeting").await, 1);
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    let value: Option<String> = redis::cmd("GET")
        .arg("fleeting")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(value, None);
    assert_eq!(ttl(&mut connection, "fleeting").await, -2);

    // EXPIRE sets a deadline on an existing key and PERSIST removes it.
    let applied: i64 = redis::cmd("EXPIRE")
        .arg("eternal")
        .arg(100)
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(applied, 1);
    assert!((99..=100).contains(&ttl(&mut connection, "eternal").await));
    // NX does not touch a key that already has a deadline.
    let applied: i64 = redis::cmd("EXPIRE")
        .arg("eternal")
        .arg(10)
        .arg("NX")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(applied, 0);
    // XX extends one, GT and LT compare deadlines.
    let applied: i64 = redis::cmd("EXPIRE")
        .arg("eternal")
        .arg(200)
        .arg("GT")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(applied, 1);
    assert!((199..=200).contains(&ttl(&mut connection, "eternal").await));
    let removed: i64 = redis::cmd("PERSIST")
        .arg("eternal")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(removed, 1);
    assert_eq!(ttl(&mut connection, "eternal").await, -1);
    let removed: i64 = redis::cmd("PERSIST")
        .arg("eternal")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(removed, 0);
    let applied: i64 = redis::cmd("EXPIRE")
        .arg("absent")
        .arg(10)
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(applied, 0);

    // A plain SET drops a deadline, and KEEPTTL keeps it, as in Redis.
    let _: () = redis::cmd("SET")
        .arg("eternal")
        .arg("v")
        .arg("EX")
        .arg(60)
        .query_async(&mut connection)
        .await
        .unwrap();
    assert!((59..=60).contains(&ttl(&mut connection, "eternal").await));
    let _: () = redis::cmd("SET")
        .arg("eternal")
        .arg("v2")
        .arg("KEEPTTL")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert!((55..=60).contains(&ttl(&mut connection, "eternal").await));
    let _: () = redis::cmd("SET")
        .arg("eternal")
        .arg("v3")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(ttl(&mut connection, "eternal").await, -1);

    // A deadline travels with the data when a range moves to a new owner, which
    // only holds because the node stores an absolute one.
    let client = cluster.cluster_client().await;
    let topology = client.topology().await;
    let key = format!("migrating-{}", 7);
    let _: () = redis::cmd("SET")
        .arg(&key)
        .arg("travelling")
        .arg("PX")
        .arg(600_000)
        .query_async(&mut connection)
        .await
        .unwrap();

    let port = unused_ports(1)[0];
    let mut members: Vec<Member> = topology.members.clone();
    members.push(Member {
        node_id: "node-4".to_owned(),
        endpoint: format!("http://127.0.0.1:{port}"),
    });
    let change = client.begin_topology_change(members).await.unwrap();
    let _node = spawn(&[
        "node".to_owned(),
        "--id".to_owned(),
        "node-4".to_owned(),
        "--listen".to_owned(),
        format!("127.0.0.1:{port}"),
        "--coordinator".to_owned(),
        cluster.coordinator.clone(),
    ]);
    wait_for_listener(port);
    let executor = client.clone();
    let execution = tokio::spawn(async move {
        executor
            .execute_topology_change(
                change.change_id.clone(),
                change.base_epoch,
                change.target_topology.epoch,
            )
            .await
    });
    let completed = tokio::time::timeout(Duration::from_secs(90), execution)
        .await
        .expect("the membership change did not finish in time")
        .unwrap()
        .expect("the membership change was not published");
    assert!(completed.target_topology.epoch > 1);

    let value: Option<String> = redis::cmd("GET")
        .arg(&key)
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(value.as_deref(), Some("travelling"));
    let remaining: i64 = redis::cmd("PTTL")
        .arg(&key)
        .query_async(&mut connection)
        .await
        .unwrap();
    assert!(
        remaining > 0,
        "the deadline was lost with the range: {remaining}"
    );
}

#[tokio::test]
async fn a_connection_may_be_required_to_authenticate() {
    let _guard = test_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let cluster = Cluster::start(&directory.path().join("coordinator.redb"), Some("s3cret"));

    // The port answers before AUTH, and says why the command did not run.
    let mut anonymous = cluster.connect(&cluster.url(None)).await;
    let error = redis::cmd("GET")
        .arg("anything")
        .query_async::<Option<String>>(&mut anonymous)
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("Authentication required"),
        "unexpected error: {error}"
    );
    // The connection survives, so the client can authenticate on it.
    let _: () = redis::cmd("AUTH")
        .arg("s3cret")
        .query_async(&mut anonymous)
        .await
        .unwrap();
    let _: () = redis::cmd("SET")
        .arg("private")
        .arg("value")
        .query_async(&mut anonymous)
        .await
        .unwrap();
    let value: String = redis::cmd("GET")
        .arg("private")
        .query_async(&mut anonymous)
        .await
        .unwrap();
    assert_eq!(value, "value");

    // The wrong password does not open it, and the client says why.
    let error = cluster
        .try_connect(&cluster.url(Some("wrong")))
        .await
        .expect_err("the wrong password was accepted");
    assert!(
        error.kind() == redis::ErrorKind::AuthenticationFailed,
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn a_resp3_client_negotiates_and_uses_the_same_commands() {
    let _guard = test_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let cluster = Cluster::start(&directory.path().join("coordinator.redb"), None);
    let url = format!("redis://127.0.0.1:{}/?protocol=resp3", cluster.port);
    let mut connection = cluster.connect(&url).await;

    // The `HELLO 3` the client sent on connect was answered, so this works.
    let pong: String = redis::cmd("PING")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(pong, "PONG");
    let _: () = redis::cmd("SET")
        .arg("resp3")
        .arg("value")
        .query_async(&mut connection)
        .await
        .unwrap();
    let value: Option<String> = redis::cmd("GET")
        .arg("resp3")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(value.as_deref(), Some("value"));
    // RESP3 spells a missing value differently, and the client has to see that.
    let missing: Option<String> = redis::cmd("GET")
        .arg("resp3-missing")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(missing, None);
    let values: Vec<Option<String>> = redis::cmd("MGET")
        .arg(&["resp3", "resp3-missing"])
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(
        values,
        vec![Some("value".to_owned()), None],
        "a nil inside an array has to keep its position"
    );

    // A map is a flat array under RESP2 and a map under RESP3; either way the
    // client decodes it into pairs.
    let config: HashMap<String, String> = redis::cmd("CONFIG")
        .arg("GET")
        .arg("maxmemory*")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(config.get("maxmemory").map(String::as_str), Some("0"));
    let info: String = redis::cmd("INFO")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert!(
        info.contains("redis_mode:standalone"),
        "unexpected INFO: {info}"
    );
}

#[tokio::test]
async fn a_declined_command_says_so_instead_of_failing_obscurely() {
    let _guard = test_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let cluster = Cluster::start(&directory.path().join("coordinator.redb"), None);
    let mut connection = cluster.client().await;

    for (command, expected) in [
        ("SCAN", "SCAN is not supported"),
        ("KEYS", "KEYS is not supported"),
        ("DBSIZE", "DBSIZE is not supported"),
        ("FLUSHALL", "FLUSHALL is not supported"),
    ] {
        let error = redis::cmd(command)
            .arg("*")
            .query_async::<()>(&mut connection)
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains(expected),
            "{command} reported {error}"
        );
        // The connection survives a declined command.
        let pong: String = redis::cmd("PING")
            .query_async(&mut connection)
            .await
            .unwrap();
        assert_eq!(pong, "PONG");
    }

    // A data structure this gateway does not hold is unknown to it.
    let error = redis::cmd("HSET")
        .arg("a-hash")
        .arg("field")
        .arg("value")
        .query_async::<i64>(&mut connection)
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("unknown command"),
        "unexpected error: {error}"
    );
    let error = redis::cmd("EVAL")
        .arg("return 1")
        .arg(0)
        .query_async::<i64>(&mut connection)
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("unknown command"),
        "unexpected error: {error}"
    );

    // The command table describes the gateway, including what it declined.
    let count: Vec<usize> = redis::cmd("COMMAND")
        .arg("COUNT")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert!(count[0] > 40, "command table looks empty: {count:?}");
    let info: Vec<redis::Value> = redis::cmd("COMMAND")
        .arg("INFO")
        .arg("get")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(info.len(), 1);
    // A command the gateway does not know is a nil in place, not a gap in the
    // reply, which is what keeps a client's argument order usable.
    let unknown: Vec<redis::Value> = redis::cmd("COMMAND")
        .arg("INFO")
        .arg("nosuchcommand")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(unknown, vec![redis::Value::Nil]);
}

#[tokio::test]
async fn concurrent_clients_on_one_gateway_do_not_lose_increments() {
    let _guard = test_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let cluster = Cluster::start(&directory.path().join("coordinator.redb"), None);
    let url = cluster.url(None);
    let _: () = redis::cmd("SET")
        .arg("shared-counter")
        .arg(0)
        .query_async(&mut cluster.client().await)
        .await
        .unwrap();

    // Eight connections at once, as a pool of application connections would.
    // The gateway serializes the read and the write of an INCR, so every one
    // of these has to land.
    let mut tasks = Vec::new();
    for _ in 0..8 {
        let url = url.clone();
        tasks.push(tokio::spawn(async move {
            let mut connection = redis::Client::open(url)
                .unwrap()
                .get_multiplexed_async_connection()
                .await
                .unwrap();
            for _ in 0..25 {
                redis::cmd("INCR")
                    .arg("shared-counter")
                    .query_async::<i64>(&mut connection)
                    .await
                    .unwrap();
            }
        }));
    }
    for task in tasks {
        tokio::time::timeout(Duration::from_secs(30), task)
            .await
            .expect("an increment task stalled")
            .unwrap();
    }
    let total: String = redis::cmd("GET")
        .arg("shared-counter")
        .query_async(&mut cluster.client().await)
        .await
        .unwrap();
    assert_eq!(total, "200", "an increment was lost");
}

#[tokio::test]
async fn a_gateway_outlives_its_coordinator() {
    let _guard = test_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let mut cluster = Cluster::start(&directory.path().join("coordinator.redb"), None);
    let mut connection = cluster.client().await;
    let _: () = redis::cmd("SET")
        .arg("before-restart")
        .arg("value")
        .query_async(&mut connection)
        .await
        .unwrap();

    // The coordinator goes away while the gateway keeps running. The gateway
    // shares one cluster client, so a topology it already has keeps answering
    // reads and writes for as long as the nodes it names are up. That is the
    // cluster's contract, not a gateway leniency: nothing about the value
    // changes while the coordinator is gone.
    cluster.processes[0].stop();
    let value: Option<String> = redis::cmd("GET")
        .arg("before-restart")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(value.as_deref(), Some("value"));

    // A gateway that starts with no coordinator at all still answers, because
    // it connects on the first command rather than at startup.
    let ports = unused_ports(1);
    let gateway = spawn(&[
        "redis-gateway".to_owned(),
        "--listen".to_owned(),
        format!("127.0.0.1:{}", ports[0]),
        "--coordinator".to_owned(),
        "http://127.0.0.1:1".to_owned(),
        "--deadline-ms".to_owned(),
        "200".to_owned(),
    ]);
    wait_for_listener(ports[0]);
    let mut orphan = cluster
        .connect(&format!("redis://127.0.0.1:{}", ports[0]))
        .await;
    let pong: String = redis::cmd("PING").query_async(&mut orphan).await.unwrap();
    assert_eq!(
        pong, "PONG",
        "a gateway with no cluster must still handshake"
    );
    let error = redis::cmd("GET")
        .arg("anything")
        .query_async::<Option<String>>(&mut orphan)
        .await
        .unwrap_err();
    assert_eq!(
        error.kind(),
        redis::ErrorKind::Server(redis::ServerErrorKind::TryAgain),
        "a gateway with no cluster must ask the client to retry, not guess"
    );
    drop(gateway);
}

/// Sends one inline command over a raw socket and reads whatever comes back
/// before the gateway stops writing, for checks the client library would hide.
async fn raw_exchange(port: u16, request: &[u8]) -> Vec<u8> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut socket = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .unwrap();
    socket.write_all(request).await.unwrap();
    let mut answer = Vec::new();
    let mut chunk = [0_u8; 4096];
    loop {
        match tokio::time::timeout(Duration::from_millis(500), socket.read(&mut chunk)).await {
            Ok(Ok(0)) | Err(_) => return answer,
            Ok(Ok(read)) => answer.extend_from_slice(&chunk[..read]),
            Ok(Err(error)) => panic!("{error}"),
        }
    }
}

#[tokio::test]
async fn increments_through_two_gateways_are_not_lost() {
    let _guard = test_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let cluster = Cluster::start(&directory.path().join("coordinator.redb"), None);
    let (_second, second_url) = cluster.second_gateway();
    let _: () = redis::cmd("SET")
        .arg("two-gateway-counter")
        .arg(0)
        .query_async(&mut cluster.client().await)
        .await
        .unwrap();

    // The gateways share no lock. What keeps every increment is the owner
    // applying each write only if the key is still the version that was read.
    let mut tasks = Vec::new();
    for url in [cluster.url(None), second_url].into_iter().cycle().take(8) {
        tasks.push(tokio::spawn(async move {
            let mut connection = redis::Client::open(url)
                .unwrap()
                .get_multiplexed_async_connection()
                .await
                .unwrap();
            for _ in 0..25 {
                redis::cmd("INCR")
                    .arg("two-gateway-counter")
                    .query_async::<i64>(&mut connection)
                    .await
                    .unwrap();
            }
        }));
    }
    for task in tasks {
        tokio::time::timeout(Duration::from_secs(60), task)
            .await
            .expect("an increment task stalled")
            .unwrap();
    }
    let total: String = redis::cmd("GET")
        .arg("two-gateway-counter")
        .query_async(&mut cluster.client().await)
        .await
        .unwrap();
    assert_eq!(total, "200", "an increment was lost between two gateways");
}

#[tokio::test]
async fn options_and_edge_cases_answer_the_way_redis_does() {
    let _guard = test_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let cluster = Cluster::start(&directory.path().join("coordinator.redb"), None);
    let mut connection = cluster.client().await;
    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    // Each expiry option in its own unit.
    let _: () = redis::cmd("SET")
        .arg("px")
        .arg("v")
        .arg("PX")
        .arg(5_000)
        .query_async(&mut connection)
        .await
        .unwrap();
    let pttl: i64 = redis::cmd("PTTL")
        .arg("px")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert!((1..=5_000).contains(&pttl), "PX 5000 left a PTTL of {pttl}");
    let _: () = redis::cmd("SET")
        .arg("exat")
        .arg("v")
        .arg("EXAT")
        .arg(now_secs + 100)
        .query_async(&mut connection)
        .await
        .unwrap();
    assert!((98..=101).contains(&ttl(&mut connection, "exat").await));
    let value: Option<String> = redis::cmd("GETEX")
        .arg("exat")
        .arg("PX")
        .arg(2_000)
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(value.as_deref(), Some("v"));
    assert!((1..=2).contains(&ttl(&mut connection, "exat").await));

    // An option with its operand missing is a syntax error on a connection
    // that keeps working.
    let error = redis::cmd("SET")
        .arg("k")
        .arg("v")
        .arg("EX")
        .query_async::<()>(&mut connection)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("syntax error"), "{error}");
    let pong: String = redis::cmd("PING")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(pong, "PONG");

    // `SET ... NX GET` answers with the value it did not replace.
    let _: () = redis::cmd("SET")
        .arg("nxget")
        .arg("old")
        .query_async(&mut connection)
        .await
        .unwrap();
    let old: Option<String> = redis::cmd("SET")
        .arg("nxget")
        .arg("new")
        .arg("NX")
        .arg("GET")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(old.as_deref(), Some("old"));

    // An empty `SETRANGE` creates nothing.
    let length: i64 = redis::cmd("SETRANGE")
        .arg("never")
        .arg(100)
        .arg("")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(length, 0);
    let exists: i64 = redis::cmd("EXISTS")
        .arg("never")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(exists, 0);

    // A deadline in the past deletes; a key without one accepts `LT`.
    let _: () = redis::cmd("SET")
        .arg("gone")
        .arg("v")
        .query_async(&mut connection)
        .await
        .unwrap();
    let expired: i64 = redis::cmd("EXPIRE")
        .arg("gone")
        .arg(-1)
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(expired, 1);
    assert_eq!(ttl(&mut connection, "gone").await, -2);
    let _: () = redis::cmd("SET")
        .arg("lt")
        .arg("v")
        .query_async(&mut connection)
        .await
        .unwrap();
    let applied: i64 = redis::cmd("EXPIRE")
        .arg("lt")
        .arg(10)
        .arg("LT")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(applied, 1);
    assert!((9..=10).contains(&ttl(&mut connection, "lt").await));
    let persisted: i64 = redis::cmd("PERSIST")
        .arg("lt")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(persisted, 1);
    assert_eq!(ttl(&mut connection, "lt").await, -1);

    // The smallest integer is a result, not an overflow.
    let _: () = redis::cmd("SET")
        .arg("min")
        .arg(i64::MIN + 1)
        .query_async(&mut connection)
        .await
        .unwrap();
    let min: i64 = redis::cmd("DECR")
        .arg("min")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(min, i64::MIN);

    // A key named twice in `MSET` takes its later value.
    let _: () = redis::cmd("MSET")
        .arg("twice")
        .arg(1)
        .arg("twice")
        .arg(2)
        .query_async(&mut connection)
        .await
        .unwrap();
    let twice: String = redis::cmd("GET")
        .arg("twice")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(twice, "2");

    // A key the cluster refuses fails the whole multi-key command instead of
    // counting as absent.
    let oversized = "k".repeat(70 * 1024);
    for command in ["DEL", "EXISTS"] {
        let error = redis::cmd(command)
            .arg("twice")
            .arg(&oversized)
            .query_async::<i64>(&mut connection)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("exceeds"), "{command}: {error}");
    }
}

#[tokio::test]
async fn the_combined_handshake_authenticates_and_quit_is_answered() {
    let _guard = test_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let cluster = Cluster::start(&directory.path().join("coordinator.redb"), Some("s3cret"));

    let answer = raw_exchange(
        cluster.port,
        b"HELLO 3 AUTH default s3cret\r\nPING\r\nQUIT\r\n",
    )
    .await;
    let text = String::from_utf8_lossy(&answer);
    assert!(
        text.starts_with('%'),
        "HELLO 3 did not answer with a RESP3 map: {text}"
    );
    assert!(
        text.ends_with("+PONG\r\n+OK\r\n"),
        "unexpected tail: {text}"
    );

    // Replies switched off stay off until switched back on, which answers.
    let answer = raw_exchange(
        cluster.port,
        b"AUTH s3cret\r\nCLIENT REPLY OFF\r\nPING\r\nCLIENT REPLY ON\r\nPING\r\n",
    )
    .await;
    assert_eq!(answer, b"+OK\r\n+OK\r\n+PONG\r\n");
}
