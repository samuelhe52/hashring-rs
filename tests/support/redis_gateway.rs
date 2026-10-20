use hashring_client::HashringClient;

use std::{
    path::Path,
    process::{Child, Command, Stdio},
    sync::OnceLock,
    time::Duration,
};

pub(crate) struct Process(Child);

impl Process {
    pub(crate) fn stop(&mut self) {
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
pub(crate) fn test_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

pub(crate) fn unused_ports(count: usize) -> Vec<u16> {
    let listeners: Vec<_> = (0..count)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    listeners
        .iter()
        .map(|listener| listener.local_addr().unwrap().port())
        .collect()
}

pub(crate) fn spawn(arguments: &[String]) -> Process {
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

pub(crate) fn wait_for_listener(port: u16) {
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
pub(crate) struct Cluster {
    pub(crate) processes: Vec<Process>,
    pub(crate) coordinator: String,
    pub(crate) port: u16,
    password: Option<String>,
    gateway_command: &'static str,
}

impl Cluster {
    pub(crate) fn start(state: &Path, password: Option<&str>) -> Self {
        Self::start_with_gateway(state, password, "redis-gateway")
    }

    pub(crate) fn start_with_gateway(
        state: &Path,
        password: Option<&str>,
        gateway_command: &'static str,
    ) -> Self {
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
            gateway_command.to_owned(),
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
            gateway_command,
        }
    }

    pub(crate) fn url(&self, password: Option<&str>) -> String {
        match password {
            Some(password) => format!("redis://:{password}@127.0.0.1:{}", self.port),
            None => format!("redis://127.0.0.1:{}", self.port),
        }
    }

    /// Another gateway in front of the same cluster, as a second process
    /// behind a load balancer would be. Its URL, and the process to keep alive.
    pub(crate) fn second_gateway(&self) -> (Process, String) {
        let port = unused_ports(1)[0];
        let mut arguments = vec![
            self.gateway_command.to_owned(),
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

    pub(crate) fn authenticated_url(&self) -> String {
        self.url(self.password.as_deref())
    }

    pub(crate) async fn try_connect(
        &self,
        url: &str,
    ) -> redis::RedisResult<redis::aio::MultiplexedConnection> {
        redis::Client::open(url.to_owned())?
            .get_multiplexed_async_connection()
            .await
    }

    pub(crate) async fn connect(&self, url: &str) -> redis::aio::MultiplexedConnection {
        self.try_connect(url)
            .await
            .expect("a redis client could not connect to the gateway")
    }

    pub(crate) async fn client(&self) -> redis::aio::MultiplexedConnection {
        self.connect(&self.authenticated_url()).await
    }

    pub(crate) async fn cluster_client(&self) -> HashringClient {
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
