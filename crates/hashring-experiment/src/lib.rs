use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{BufWriter, Read, Write},
    net::{TcpListener, TcpStream},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail, ensure};
use futures::{StreamExt, TryStreamExt, stream};
use hashring_client::{ClientError, HashringClient};
use hashring_core::{
    migration::{MigrationPhase, TopologyChange},
    proto::{ErrorCode, PutRequest, data_node_client::DataNodeClient},
    topology::{Member, TopologySnapshot, WriteAckPolicy},
    transport::configure_data_node_client,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tonic::transport::Channel;

mod latency;
mod scaling;

/// The cluster a mode runs against and the outputs it fills in.
pub(crate) struct RunContext<'a> {
    pub config: &'a ExperimentConfig,
    pub members: &'a [Member],
    pub initial_count: usize,
    pub ports: &'a [u16],
    pub coordinator_endpoint: &'a str,
    pub admin_client: &'a HashringClient,
    pub workload_client: &'a HashringClient,
    pub processes: &'a mut ProcessGroup,
    pub events: &'a EventLog,
    pub measurements: &'a mut BTreeMap<String, Measurement>,
    pub observations: &'a mut BTreeMap<String, Value>,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ExperimentMode {
    Correctness,
    Performance,
    Availability,
    Latency,
    Scaling,
}

/// The membership change a `scaling` run performs.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum ScalingScenario {
    /// Grow by one node, then shrink back, in one cluster and one load.
    RoundTrip,
    Grow,
    Shrink,
    /// Replace one node with another at the same member count.
    Replace,
    /// Grow by several nodes at once, then shrink back.
    Churn,
    /// Keep the topology unchanged and measure the load alone.
    Control,
}

/// How a `scaling` run offers its load.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ScalingLoadMode {
    /// A fixed number of workers, each sending its next request when the
    /// previous one returns.
    Closed,
    /// Requests due at fixed offsets from the start of the run. A request that
    /// cannot be sent within `max_in_flight` is dropped and counted as late.
    Open,
}

impl ScalingLoadMode {
    pub fn name(self) -> &'static str {
        match self {
            Self::Closed => "closed",
            Self::Open => "open",
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ExperimentConfig {
    pub mode: ExperimentMode,
    pub output_dir: PathBuf,
    pub node_count: usize,
    pub key_count: u64,
    pub value_bytes: usize,
    pub concurrency: usize,
    pub hash_seed: u64,
    pub virtual_nodes: u32,
    pub operation_timeout_ms: u64,
    pub migration_timeout_ms: u64,
    pub range_move_concurrency: usize,
    pub replica_repair_concurrency: usize,
    pub prepublication_failure_grace_ms: u64,
    pub pre_publish_delay_ms: u64,
    pub require_clean_source: bool,
    pub verbose: bool,
    /// Tokio worker threads for the driver and every data node.
    pub worker_threads: usize,
    /// `None` keeps the coordinator's default of one worker per CPU; it is not
    /// on the data path.
    pub coordinator_worker_threads: Option<usize>,
    pub desired_replication_factor: u32,
    pub minimum_admitted_copies: u32,
    pub minimum_healthy_followers: u32,
    pub max_replica_lag_ms: u64,
    pub receipt_retention_ms: u64,
    pub lease_duration_ms: u64,
    pub write_ack_policy: WriteAckPolicy,
    /// The keys the `latency` mode writes before its load starts, and the key
    /// space its load reads and writes. `None` in the other modes.
    pub preload_keys: Option<u64>,
    /// The target rates of the `latency` mode, one stage each.
    pub target_rates: Vec<u64>,
    /// The seconds at the start of every stage that are not counted.
    pub warmup_seconds: u64,
    /// The seconds of every stage that are counted.
    pub duration_seconds: u64,
    /// The `GET:PUT:DELETE` share of the `latency` mode's requests.
    pub mix: String,
    /// How the `latency` mode picks the key of a request.
    pub key_distribution: String,
    /// How many requests the `latency` mode may have in flight at once.
    pub max_in_flight: usize,
    /// How many consecutive owner failures the `availability` mode injects.
    /// Every round kills the current owner of the probe key and then replaces
    /// it, so the run needs one spare node per round.
    pub availability_rounds: u32,
    /// The membership change a `scaling` run performs.
    pub scaling_scenario: ScalingScenario,
    /// How a `scaling` run offers its load.
    pub scaling_load_mode: ScalingLoadMode,
    /// The total operation rate of an open-loop `scaling` run. Zero in closed
    /// loop mode.
    pub scaling_open_rate: u64,
    /// The nodes a `scaling` run starts with. `None` derives it from the
    /// scenario: one fewer than the peak for a grow or a round trip, the peak
    /// for a shrink, and three fewer than the peak for a churn.
    pub initial_nodes: Option<usize>,
    /// The one-shot write and delete operations a `scaling` run offers per
    /// second. Reads are not paced.
    pub scaling_write_rate: u64,
    /// How long the one-shot pools of one `scaling` transition must last.
    pub scaling_pool_seconds: u64,
    /// How long a `scaling` run keeps the load running after a change.
    pub scaling_post_seconds: u64,
    /// Forwarded to every data node.
    pub node: NodeOptions,
}

impl ExperimentConfig {
    /// The members an `availability` run starts with, before it kills any.
    /// `validate_config` guarantees the result is at least one.
    pub fn availability_initial_members(&self) -> usize {
        self.node_count
            .saturating_sub(self.availability_rounds as usize)
    }
}

/// The `node` command's tuning flags, in their flag units.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct NodeOptions {
    pub max_dedup_bytes: usize,
    pub max_value_bytes: usize,
    pub max_pending_replication_bytes: usize,
    pub max_migration_journal_bytes: usize,
    pub replication_stream_queue_capacity: usize,
    pub required_ack_timeout_ms: u64,
    pub replication_rpc_timeout_ms: u64,
    pub replication_batch_linger_ms: u64,
}

impl NodeOptions {
    fn arguments(&self) -> Vec<String> {
        [
            ("--max-dedup-bytes", self.max_dedup_bytes.to_string()),
            ("--max-value-bytes", self.max_value_bytes.to_string()),
            (
                "--max-pending-replication-bytes",
                self.max_pending_replication_bytes.to_string(),
            ),
            (
                "--max-migration-journal-bytes",
                self.max_migration_journal_bytes.to_string(),
            ),
            (
                "--replication-stream-queue-capacity",
                self.replication_stream_queue_capacity.to_string(),
            ),
            (
                "--required-ack-timeout-ms",
                self.required_ack_timeout_ms.to_string(),
            ),
            (
                "--replication-rpc-timeout-ms",
                self.replication_rpc_timeout_ms.to_string(),
            ),
            (
                "--replication-batch-linger-ms",
                self.replication_batch_linger_ms.to_string(),
            ),
        ]
        .into_iter()
        .flat_map(|(flag, value)| [flag.to_owned(), value])
        .collect()
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ExperimentSummary {
    pub success: bool,
    pub error: Option<String>,
    pub elapsed_seconds: f64,
    pub final_epoch: Option<u64>,
    pub measurements: BTreeMap<String, Measurement>,
    pub observations: BTreeMap<String, Value>,
}

/// One entry of a run's `measurements`.
///
/// A throughput stage reports the operations it ran and the rate they ran at.
/// A `latency` stage reports one target rate, so it carries that rate's
/// latency distribution, its errors, and its load balance.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum Measurement {
    Throughput(ThroughputMeasurement),
    LatencyStage(LatencyStageMeasurement),
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ThroughputMeasurement {
    pub operations: u64,
    pub seconds: f64,
    pub operations_per_second: f64,
}

/// The percentiles of one operation's latency, in microseconds.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct LatencySummary {
    pub p50: u64,
    pub p95: u64,
    pub p99: u64,
    pub p999: u64,
    /// The upper bound of the bucket the largest sample fell in, which is a
    /// little above the largest sample itself.
    pub max: u64,
    /// The largest sample, in microseconds, whether or not it was inside the
    /// histogram's range.
    #[serde(default)]
    pub max_observed: u64,
    /// The samples that were longer than the histogram's range, which are
    /// recorded in its top bucket and are not measured by it.
    #[serde(default)]
    pub over_range: u64,
    pub samples: u64,
}

/// What one node did during a stage.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct LoadBalanceNode {
    /// The keys the node owns.
    pub owned_keys: u64,
    /// The operations the node served.
    pub ops_handled: u64,
    /// The share of one core the node used, or None when CPU sampling is unavailable.
    pub cpu_percent: Option<f64>,
}

/// How evenly the keys and the requests were spread over the nodes.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct LoadBalanceMeasurement {
    pub per_node: BTreeMap<String, LoadBalanceNode>,
    pub owned_keys_max_over_mean: f64,
    pub owned_keys_cv: f64,
    pub ops_handled_max_over_mean: f64,
    pub ops_handled_cv: f64,
}

/// One target rate of the `latency` mode.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct LatencyStageMeasurement {
    pub target_rate: u64,
    pub achieved_rate: f64,
    pub seconds: f64,
    /// Whether the driver kept up with its own schedule. A stage the driver
    /// could not keep up with does not describe the cluster.
    pub sender_saturated: bool,
    /// The requests the driver sent.
    pub issued: u64,
    /// The requests the driver did not send because too many were in flight.
    pub late: u64,
    /// The operations that succeeded, by kind.
    pub ops: BTreeMap<String, u64>,
    /// The operations that failed, by reason.
    pub errors: BTreeMap<String, u64>,
    /// The internal retries the client made during the stage.
    pub retries: u64,
    pub latency_us: BTreeMap<String, LatencySummary>,
    pub load_balance: LoadBalanceMeasurement,
}

#[derive(Serialize)]
struct ExperimentManifest<'a> {
    schema_version: u32,
    started_unix_ms: u128,
    executable_path: String,
    executable_blake3: String,
    config: &'a ExperimentConfig,
    build: BuildIdentity,
    runtime_source: RuntimeSource,
    source_reproducible: bool,
    system: Option<String>,
}

#[derive(Serialize)]
struct BuildIdentity {
    git_commit: &'static str,
    git_dirty: &'static str,
    profile: &'static str,
    target: &'static str,
    rustc_version: &'static str,
    source_tree_blake3: &'static str,
}

#[derive(Serialize)]
struct RuntimeSource {
    root: &'static str,
    git_commit: Option<String>,
    git_dirty: Option<bool>,
    git_status_porcelain: Option<String>,
    git_diff: Option<String>,
    source_tree_blake3: Option<String>,
}

#[derive(Clone, Copy)]
struct Workload {
    key_count: u64,
    value_bytes: usize,
    concurrency: usize,
}

struct MigrationWorkload {
    dataset: Workload,
    put_keys: Vec<u64>,
    delete_keys: Vec<u64>,
    sentinel_count: usize,
    round: u64,
    label: &'static str,
}

struct MovingKeyCohorts {
    all: Vec<u64>,
    rewrite: Vec<u64>,
    delete: Vec<u64>,
    sentinels: Vec<u64>,
}

#[derive(Clone, Copy)]
struct MutationCounts {
    writes: u64,
    deletes: u64,
}

#[derive(Clone)]
struct EventLog(Arc<Mutex<File>>);

impl EventLog {
    fn open(path: &Path) -> Result<Self> {
        Ok(Self(Arc::new(Mutex::new(
            OpenOptions::new().create_new(true).write(true).open(path)?,
        ))))
    }

    fn record(&self, event: &str, fields: Value) -> Result<()> {
        let value = json!({
            "unix_ms": unix_ms(),
            "event": event,
            "fields": fields,
        });
        let mut file = self
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("event log lock was poisoned"))?;
        serde_json::to_writer(&mut *file, &value)?;
        file.write_all(b"\n")?;
        file.flush()?;
        Ok(())
    }
}

struct ManagedProcess {
    name: String,
    child: Child,
}

struct ProcessGroup {
    executable: PathBuf,
    log_dir: PathBuf,
    verbose: bool,
    /// Passed to every data node: its share of the host's worker threads and
    /// the run's node tuning.
    node_arguments: Vec<String>,
    processes: Vec<ManagedProcess>,
}

impl ProcessGroup {
    fn new(
        executable: PathBuf,
        log_dir: PathBuf,
        verbose: bool,
        node_arguments: Vec<String>,
    ) -> Self {
        Self {
            executable,
            log_dir,
            verbose,
            node_arguments,
            processes: Vec::new(),
        }
    }

    fn spawn(&mut self, name: impl Into<String>, arguments: &[String]) -> Result<()> {
        let name = name.into();
        let stdout = File::create(self.log_dir.join(format!("{name}.stdout.log")))?;
        let stderr = File::create(self.log_dir.join(format!("{name}.stderr.log")))?;
        let child = Command::new(&self.executable)
            .args(arguments)
            .stdin(Stdio::null())
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr))
            .env(
                "RUST_LOG",
                if self.verbose {
                    "hashring_rs=info,hashring_coordinator=debug,hashring_node=info"
                } else {
                    "hashring_rs=info"
                },
            )
            .spawn()
            .with_context(|| format!("spawning {name}"))?;
        self.processes.push(ManagedProcess { name, child });
        Ok(())
    }

    fn ensure_running(&mut self) -> Result<()> {
        for process in &mut self.processes {
            if let Some(status) = process.child.try_wait()? {
                bail!("{} exited unexpectedly with {status}", process.name);
            }
        }
        Ok(())
    }

    async fn wait_for_successful_exit(&mut self, name: &str, timeout: Duration) -> Result<String> {
        let deadline = Instant::now() + timeout;
        loop {
            let index = self
                .processes
                .iter()
                .position(|process| process.name == name)
                .with_context(|| format!("unknown managed process {name}"))?;
            if let Some(status) = self.processes[index].child.try_wait()? {
                self.processes.remove(index);
                ensure!(
                    status.success(),
                    "{name} exited unsuccessfully with {status}"
                );
                return Ok(status.to_string());
            }
            if Instant::now() >= deadline {
                bail!("{name} did not exit within {timeout:?}");
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    /// The process ids of the started processes, by name.
    fn pids(&self) -> BTreeMap<String, u32> {
        self.processes
            .iter()
            .map(|process| (process.name.clone(), process.child.id()))
            .collect()
    }

    fn kill(&mut self, name: &str) -> Result<(String, Instant)> {
        let index = self
            .processes
            .iter()
            .position(|process| process.name == name)
            .with_context(|| format!("unknown managed process {name}"))?;
        let mut process = self.processes.remove(index);
        process
            .child
            .kill()
            .with_context(|| format!("killing {name}"))?;
        let signalled_at = Instant::now();
        Ok((process.child.wait()?.to_string(), signalled_at))
    }
}

impl Drop for ProcessGroup {
    fn drop(&mut self) {
        for process in &mut self.processes {
            if process.child.try_wait().ok().flatten().is_none() {
                let _ = process.child.kill();
            }
            let _ = process.child.wait();
        }
    }
}

pub async fn run_experiment(
    config: ExperimentConfig,
    executable: PathBuf,
) -> Result<ExperimentSummary> {
    validate_config(&config)?;
    prepare_output_directory(&config.output_dir)?;
    let log_dir = config.output_dir.join("process-logs");
    fs::create_dir(&log_dir)?;
    let events = EventLog::open(&config.output_dir.join("events.jsonl"))?;
    let build_source_root = Path::new(env!("HASHRING_BUILD_SOURCE_ROOT"));
    let runtime_git_commit = command_output(Some(build_source_root), "git", &["rev-parse", "HEAD"]);
    let runtime_git_status =
        command_output(Some(build_source_root), "git", &["status", "--porcelain"]);
    let runtime_tree_hash = source_tree_digest(build_source_root).ok();
    let source_reproducible = source_is_reproducible(
        env!("HASHRING_BUILD_GIT_DIRTY"),
        env!("HASHRING_BUILD_GIT_COMMIT"),
        env!("HASHRING_BUILD_SOURCE_TREE_BLAKE3"),
        runtime_git_status.as_deref(),
        runtime_git_commit.as_deref(),
        runtime_tree_hash.as_deref(),
    );
    let manifest = ExperimentManifest {
        schema_version: 9,
        started_unix_ms: unix_ms(),
        executable_path: executable.display().to_string(),
        executable_blake3: file_digest(&executable)?,
        config: &config,
        build: BuildIdentity {
            git_commit: env!("HASHRING_BUILD_GIT_COMMIT"),
            git_dirty: env!("HASHRING_BUILD_GIT_DIRTY"),
            profile: env!("HASHRING_BUILD_PROFILE"),
            target: env!("HASHRING_BUILD_TARGET"),
            rustc_version: env!("HASHRING_BUILD_RUSTC"),
            source_tree_blake3: env!("HASHRING_BUILD_SOURCE_TREE_BLAKE3"),
        },
        runtime_source: RuntimeSource {
            root: env!("HASHRING_BUILD_SOURCE_ROOT"),
            git_commit: runtime_git_commit,
            git_dirty: runtime_git_status.as_ref().map(|output| !output.is_empty()),
            git_status_porcelain: runtime_git_status,
            git_diff: command_output(
                Some(build_source_root),
                "git",
                &["diff", "--binary", "HEAD"],
            ),
            source_tree_blake3: runtime_tree_hash,
        },
        source_reproducible,
        system: command_output(None, "uname", &["-a"]),
    };
    write_json(&config.output_dir.join("manifest.json"), &manifest)?;
    events.record("experiment_started", json!({ "config": config }))?;
    if config.require_clean_source && !source_reproducible {
        let summary = summarize(
            Duration::ZERO,
            Err(anyhow::anyhow!(
                "formal run requires a clean build and matching clean runtime source tree"
            )),
            BTreeMap::new(),
            BTreeMap::new(),
        );
        write_json(&config.output_dir.join("summary.json"), &summary)?;
        events.record(
            "experiment_finished",
            json!({ "success": false, "elapsed_seconds": 0.0 }),
        )?;
        bail!("{}", summary.error.unwrap_or_default());
    }

    let started = Instant::now();
    let mut measurements = BTreeMap::new();
    let mut observations = BTreeMap::new();
    let result = run_cluster(
        &config,
        executable,
        log_dir,
        &events,
        &mut measurements,
        &mut observations,
    )
    .await;
    let summary = summarize(started.elapsed(), result, measurements, observations);
    write_json(&config.output_dir.join("summary.json"), &summary)?;
    events.record(
        "experiment_finished",
        json!({ "success": summary.success, "elapsed_seconds": summary.elapsed_seconds }),
    )?;
    if let Some(error) = &summary.error {
        bail!("experiment failed: {error}");
    }
    Ok(summary)
}

fn summarize(
    elapsed: Duration,
    result: Result<u64>,
    measurements: BTreeMap<String, Measurement>,
    observations: BTreeMap<String, Value>,
) -> ExperimentSummary {
    match result {
        Ok(final_epoch) => ExperimentSummary {
            success: true,
            error: None,
            elapsed_seconds: elapsed.as_secs_f64(),
            final_epoch: Some(final_epoch),
            measurements,
            observations,
        },
        Err(error) => ExperimentSummary {
            success: false,
            error: Some(format!("{error:#}")),
            elapsed_seconds: elapsed.as_secs_f64(),
            final_epoch: None,
            measurements,
            observations,
        },
    }
}

async fn run_cluster(
    config: &ExperimentConfig,
    executable: PathBuf,
    log_dir: PathBuf,
    events: &EventLog,
    measurements: &mut BTreeMap<String, Measurement>,
    observations: &mut BTreeMap<String, Value>,
) -> Result<u64> {
    let ports = reserve_ports(config.node_count + 1)?;
    let coordinator_port = ports[0];
    let coordinator_endpoint = format!("http://127.0.0.1:{coordinator_port}");
    let members: Vec<_> = ports[1..]
        .iter()
        .enumerate()
        .map(|(index, port)| Member {
            node_id: format!("node-{}", index + 1),
            endpoint: format!("http://127.0.0.1:{port}"),
        })
        .collect();
    let initial_count = match config.mode {
        ExperimentMode::Correctness => config.node_count - 1,
        ExperimentMode::Performance | ExperimentMode::Latency => config.node_count,
        // The availability mode starts with every node but the spares it
        // needs to replace the nodes it kills.
        ExperimentMode::Availability => config.availability_initial_members(),
        ExperimentMode::Scaling => {
            config
                .initial_nodes
                .unwrap_or_else(|| match config.scaling_scenario {
                    ScalingScenario::Shrink | ScalingScenario::Control => config.node_count,
                    ScalingScenario::Grow | ScalingScenario::RoundTrip => config.node_count - 1,
                    ScalingScenario::Replace => config.node_count - 1,
                    ScalingScenario::Churn => config.node_count.saturating_sub(3),
                })
        }
    };
    let initial_members = &members[..initial_count];
    let mut processes =
        ProcessGroup::new(executable, log_dir, config.verbose, node_arguments(config));
    let state_path = config.output_dir.join("coordinator.redb");
    let mut coordinator_args = vec![
        "coordinator".into(),
        "--listen".into(),
        format!("127.0.0.1:{coordinator_port}"),
        "--state".into(),
        state_path.display().to_string(),
        "--seed".into(),
        config.hash_seed.to_string(),
        "--virtual-nodes".into(),
        config.virtual_nodes.to_string(),
        "--minimum-admitted-copies".into(),
        config.minimum_admitted_copies.to_string(),
        "--minimum-healthy-followers".into(),
        config.minimum_healthy_followers.to_string(),
        "--desired-replication-factor".into(),
        config.desired_replication_factor.to_string(),
        "--migration-timeout-ms".into(),
        config.migration_timeout_ms.to_string(),
        "--range-move-concurrency".into(),
        config.range_move_concurrency.to_string(),
        "--replica-repair-concurrency".into(),
        config.replica_repair_concurrency.to_string(),
        "--prepublication-failure-grace-ms".into(),
        config.prepublication_failure_grace_ms.to_string(),
        "--max-replica-lag-ms".into(),
        config.max_replica_lag_ms.to_string(),
        "--receipt-retention-ms".into(),
        config.receipt_retention_ms.to_string(),
        "--lease-duration-ms".into(),
        config.lease_duration_ms.to_string(),
    ];
    if let Some(workers) = config.coordinator_worker_threads {
        coordinator_args.extend(["--worker-threads".into(), workers.to_string()]);
    }
    coordinator_args.extend([
        "--pre-publish-delay-ms".into(),
        config.pre_publish_delay_ms.to_string(),
    ]);
    for member in initial_members {
        coordinator_args.extend([
            "--member".into(),
            format!("{}={}", member.node_id, member.endpoint),
        ]);
    }
    processes.spawn("coordinator", &coordinator_args)?;
    wait_for_listener(coordinator_port, &mut processes).await?;
    for (member, port) in initial_members.iter().zip(&ports[1..=initial_count]) {
        spawn_node(
            &mut processes,
            member.node_id.clone(),
            *port,
            &coordinator_endpoint,
        )?;
    }
    for port in &ports[1..=initial_count] {
        wait_for_listener(*port, &mut processes).await?;
    }
    let workload_client = connect_eventually(
        &coordinator_endpoint,
        Duration::from_millis(config.operation_timeout_ms),
        &mut processes,
    )
    .await?;
    let admin_client = connect_eventually(
        &coordinator_endpoint,
        Duration::from_millis(config.migration_timeout_ms),
        &mut processes,
    )
    .await?;
    events.record(
        "cluster_ready",
        json!({ "initial_nodes": initial_count, "peak_nodes": config.node_count }),
    )?;
    let rf_started = Instant::now();
    wait_for_full_rf(
        &admin_client,
        config.desired_replication_factor,
        Duration::from_millis(config.migration_timeout_ms),
        &mut processes,
    )
    .await?;
    observations.insert(
        "initial_full_rf_seconds".into(),
        json!(rf_started.elapsed().as_secs_f64()),
    );
    record_topology(events, &admin_client).await?;
    if config.write_ack_policy != WriteAckPolicy::OwnerOnly {
        let change = admin_client
            .begin_write_policy_change(config.write_ack_policy)
            .await?;
        admin_client
            .execute_topology_change(
                change.change_id,
                change.base_epoch,
                change.target_topology.epoch,
            )
            .await?;
        events.record(
            "ack_policy_activated",
            json!({ "policy": config.write_ack_policy }),
        )?;
        record_topology(events, &admin_client).await?;
    }

    let workload = Workload {
        key_count: config.key_count,
        value_bytes: config.value_bytes,
        concurrency: config.concurrency,
    };
    let mut expected_rounds = vec![Some(0_u64); config.key_count as usize];
    let put_started = Instant::now();
    if let Err(error) = write_dataset(&workload_client, workload, 0).await {
        let _ = record_node_pressure(initial_members, events, observations).await;
        return Err(error);
    }
    measurements.insert(
        "initial_put".into(),
        measurement(config.key_count, put_started.elapsed()),
    );
    events.record(
        "initial_put_complete",
        json!({ "keys": config.key_count, "seconds": put_started.elapsed().as_secs_f64() }),
    )?;
    record_node_pressure(initial_members, events, observations).await?;

    let get_started = Instant::now();
    verify_dataset(&workload_client, workload, &expected_rounds).await?;
    measurements.insert(
        "initial_get_verify".into(),
        measurement(config.key_count, get_started.elapsed()),
    );
    events.record(
        "initial_get_verify_complete",
        json!({ "keys": config.key_count, "seconds": get_started.elapsed().as_secs_f64() }),
    )?;
    let sample_count = config.key_count.min(128);
    let mut latencies_us = Vec::with_capacity(sample_count as usize);
    for key in 0..sample_count {
        let started = Instant::now();
        workload_client
            .put(
                key.to_be_bytes().to_vec(),
                deterministic_value(key, 0, config.value_bytes),
            )
            .await?;
        latencies_us.push(started.elapsed().as_micros() as u64);
    }
    // The workload is over and the cluster is idle, so these figures are the
    // latency of an idle cluster and not the latency under load. The name
    // says so, and the `latency` mode measures the other one.
    observations.insert(
        "idle_client_put_latency_us".into(),
        latency_summary(&mut latencies_us),
    );
    events.record(
        "client_put_latency_sampled",
        json!({
            "samples": sample_count, "policy": config.write_ack_policy,
            "latency_us": observations["idle_client_put_latency_us"]
        }),
    )?;
    let sample_topology = admin_client.refresh_topology().await?;
    let operation_timeout = Duration::from_millis(config.operation_timeout_ms);
    let mut owner_clients: BTreeMap<String, DataNodeClient<Channel>> = BTreeMap::new();
    for member in &sample_topology.members {
        let connected = tokio::time::timeout(
            operation_timeout,
            DataNodeClient::connect(member.endpoint.clone()),
        )
        .await
        .with_context(|| {
            format!(
                "connecting direct owner RPC to {} timed out",
                member.node_id
            )
        })??;
        owner_clients.insert(
            member.node_id.clone(),
            configure_data_node_client(connected),
        );
    }
    let mut owner_rpc_latencies_us = Vec::with_capacity(sample_count as usize);
    for key in 0..sample_count {
        let encoded = key.to_be_bytes().to_vec();
        let owner = sample_topology.owner(&encoded)?;
        let client = owner_clients
            .get_mut(&owner.node_id)
            .expect("topology owner has a client");
        let started = Instant::now();
        let response = tokio::time::timeout(
            operation_timeout,
            client.put(PutRequest {
                key: encoded,
                value: deterministic_value(key, 0, config.value_bytes),
                topology_epoch: sample_topology.epoch,
                request_id: format!("experiment-owner-rpc-{}-{key}", std::process::id()),
                expires_at_unix_millis: None,
                preserve_value: false,
                ..Default::default()
            }),
        )
        .await
        .with_context(|| {
            format!(
                "direct owner PUT for key {key} on {} timed out",
                owner.node_id
            )
        })??
        .into_inner();
        ensure!(
            response.error.is_none() && response.version.is_some(),
            "direct owner PUT failed for key {key}: {:?}",
            response.error
        );
        owner_rpc_latencies_us.push(started.elapsed().as_micros() as u64);
    }
    observations.insert(
        "idle_owner_rpc_put_latency_us".into(),
        latency_summary(&mut owner_rpc_latencies_us),
    );
    events.record(
        "owner_rpc_put_latency_sampled",
        json!({
            "samples": sample_count, "policy": config.write_ack_policy,
            "latency_us": observations["idle_owner_rpc_put_latency_us"]
        }),
    )?;

    {
        let context = RunContext {
            config,
            members: &members,
            initial_count,
            ports: &ports,
            coordinator_endpoint: &coordinator_endpoint,
            admin_client: &admin_client,
            workload_client: &workload_client,
            processes: &mut processes,
            events,
            measurements,
            observations,
        };
        if matches!(context.config.mode, ExperimentMode::Availability) {
            return run_availability(context).await;
        }
        if matches!(context.config.mode, ExperimentMode::Latency) {
            return run_latency(context).await;
        }
        if matches!(context.config.mode, ExperimentMode::Scaling) {
            return scaling::run(context).await;
        }
    }

    if matches!(config.mode, ExperimentMode::Correctness) {
        let added = &members[config.node_count - 1];
        let scale_out = admin_client.begin_topology_change(members.clone()).await?;
        let scale_out_cohorts = moving_key_cohorts(&scale_out, config.key_count)?;
        spawn_node(
            &mut processes,
            added.node_id.clone(),
            ports[config.node_count],
            &coordinator_endpoint,
        )?;
        wait_for_listener(ports[config.node_count], &mut processes).await?;
        let scale_out_mutations =
            (scale_out_cohorts.rewrite.len() + scale_out_cohorts.delete.len()) as u64;
        let rf_started = Instant::now();
        let duration = migrate_while_mutating(
            &workload_client,
            &admin_client,
            scale_out,
            MigrationWorkload {
                dataset: workload,
                put_keys: scale_out_cohorts.rewrite.clone(),
                delete_keys: scale_out_cohorts.delete.clone(),
                sentinel_count: scale_out_cohorts.sentinels.len(),
                round: 1,
                label: "scale_out",
            },
            events,
        )
        .await?;
        for &key in &scale_out_cohorts.rewrite {
            expected_rounds[key as usize] = Some(1);
        }
        for &key in &scale_out_cohorts.delete {
            expected_rounds[key as usize] = None;
        }
        measurements.insert(
            "scale_out_with_mutations".into(),
            measurement(scale_out_mutations, duration),
        );
        wait_for_full_rf(
            &admin_client,
            config.desired_replication_factor,
            Duration::from_millis(config.migration_timeout_ms),
            &mut processes,
        )
        .await?;
        observations.insert(
            "scale_out_transition_to_full_rf_seconds".into(),
            json!(rf_started.elapsed().as_secs_f64()),
        );
        let verify_started = Instant::now();
        verify_dataset(&workload_client, workload, &expected_rounds).await?;
        let verify_duration = verify_started.elapsed();
        measurements.insert(
            "scale_out_verify".into(),
            measurement(config.key_count, verify_duration),
        );
        events.record(
            "migration_verification_complete",
            json!({
                "label": "scale_out",
                "keys": config.key_count,
                "rewritten_moving_keys": scale_out_cohorts.rewrite.len(),
                "deleted_moving_keys": scale_out_cohorts.delete.len(),
                "untouched_moving_sentinels": scale_out_cohorts.sentinels.len(),
                "seconds": verify_duration.as_secs_f64(),
            }),
        )?;
        let scale_in = admin_client
            .begin_topology_change(initial_members.to_vec())
            .await?;
        ensure!(
            moving_keys(&scale_in, config.key_count) == scale_out_cohorts.all,
            "reverse migration changed the moving-key cohort"
        );
        let scale_in_mutations =
            (scale_out_cohorts.delete.len() + scale_out_cohorts.rewrite.len()) as u64;
        let rf_started = Instant::now();
        let duration = migrate_while_mutating(
            &workload_client,
            &admin_client,
            scale_in,
            MigrationWorkload {
                dataset: workload,
                put_keys: scale_out_cohorts.delete.clone(),
                delete_keys: scale_out_cohorts.rewrite.clone(),
                sentinel_count: scale_out_cohorts.sentinels.len(),
                round: 2,
                label: "scale_in",
            },
            events,
        )
        .await?;
        for &key in &scale_out_cohorts.delete {
            expected_rounds[key as usize] = Some(2);
        }
        for &key in &scale_out_cohorts.rewrite {
            expected_rounds[key as usize] = None;
        }
        measurements.insert(
            "scale_in_with_mutations".into(),
            measurement(scale_in_mutations, duration),
        );
        let status = processes
            .wait_for_successful_exit(&added.node_id, Duration::from_secs(10))
            .await?;
        events.record(
            "removed_node_exited",
            json!({ "node_id": added.node_id, "status": status }),
        )?;
        wait_for_full_rf(
            &admin_client,
            config.desired_replication_factor,
            Duration::from_millis(config.migration_timeout_ms),
            &mut processes,
        )
        .await?;
        observations.insert(
            "scale_in_transition_to_full_rf_seconds".into(),
            json!(rf_started.elapsed().as_secs_f64()),
        );
        let verify_started = Instant::now();
        verify_dataset(&workload_client, workload, &expected_rounds).await?;
        let verify_duration = verify_started.elapsed();
        measurements.insert(
            "scale_in_verify".into(),
            measurement(config.key_count, verify_duration),
        );
        events.record(
            "migration_verification_complete",
            json!({
                "label": "scale_in",
                "keys": config.key_count,
                "restored_moving_keys": scale_out_cohorts.delete.len(),
                "deleted_moving_keys": scale_out_cohorts.rewrite.len(),
                "untouched_moving_sentinels": scale_out_cohorts.sentinels.len(),
                "seconds": verify_duration.as_secs_f64(),
            }),
        )?;
    }

    processes.ensure_running()?;
    let topology = admin_client.refresh_topology().await?;
    events.record(
        "verification_complete",
        json!({ "epoch": topology.epoch, "members": topology.members.len() }),
    )?;
    record_topology(events, &admin_client).await?;
    Ok(topology.epoch)
}

fn state_write_timing_json(timing: Option<hashring_core::proto::StateWriteTiming>) -> Value {
    timing.map_or(Value::Null, |t| {
        json!({
            "acquisitions": t.acquisitions, "wait_nanos": t.wait_nanos, "hold_nanos": t.hold_nanos,
            "max_wait_nanos": t.max_wait_nanos, "max_hold_nanos": t.max_hold_nanos,
        })
    })
}

async fn record_node_pressure(
    members: &[Member],
    events: &EventLog,
    observations: &mut BTreeMap<String, Value>,
) -> Result<()> {
    let mut nodes = BTreeMap::new();
    for member in members {
        let value = match query_node_info(&member.endpoint).await {
            Ok(info) => json!({
                "owner_write_timing": state_write_timing_json(info.owner_write_timing),
                "follower_write_timing": state_write_timing_json(info.follower_write_timing),
                "ack_write_timing": state_write_timing_json(info.ack_write_timing),
                "ack_completion_write_timing": state_write_timing_json(info.ack_completion_write_timing),
                "receipt_cleanup_timing": info.receipt_cleanup_timing.map(|t| json!({
                    "purge_calls": t.purge_calls, "purge_nanos": t.purge_nanos,
                    "max_purge_nanos": t.max_purge_nanos, "expired_receipts": t.expired_receipts,
                    "ack_prune_calls": t.ack_prune_calls, "ack_prune_nanos": t.ack_prune_nanos,
                    "ack_prune_scanned_receipts": t.ack_prune_scanned_receipts,
                })),
                "dedup_bytes": info.dedup_bytes,
                "dedup_peak_bytes": info.dedup_peak_bytes,
                "dedup_capacity_bytes": info.dedup_capacity_bytes,
                "owner_dedup_rejections": info.owner_dedup_rejections,
                "follower_dedup_rejections": info.follower_dedup_rejections,
                "replication_reservation_rejections": info.replication_reservation_rejections,
                "replication_rpc_retries": info.replication_rpc_retries,
                "replication_stream_peak_pending": info.replication_stream_peak_pending,
            }),
            Err(error) => json!({ "error": error.to_string() }),
        };
        nodes.insert(member.node_id.clone(), value);
    }
    events.record("node_pressure_sampled", json!({ "nodes": nodes }))?;
    observations.insert("node_pressure_after_initial_put".into(), json!(nodes));
    Ok(())
}

fn latency_summary(samples: &mut [u64]) -> Value {
    samples.sort_unstable();
    let percentile = |numerator: usize| -> u64 { samples[(samples.len() - 1) * numerator / 100] };
    json!({
        "samples": samples.len(),
        "p50": percentile(50),
        "p95": percentile(95),
        "p99": percentile(99),
        "max": samples[samples.len() - 1],
    })
}

async fn wait_for_full_rf(
    client: &HashringClient,
    desired_rf: u32,
    timeout: Duration,
    processes: &mut ProcessGroup,
) -> Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        processes.ensure_running()?;
        if let Ok(Ok(status)) =
            tokio::time::timeout(Duration::from_secs(3), client.replica_status()).await
            && !status.ranges.is_empty()
            && status.ranges.iter().all(|range| {
                range.live_rf == desired_rf
                    && !range.repairing
                    && range.followers.iter().all(|follower| follower.healthy)
            })
        {
            return Ok(());
        }
        ensure!(
            Instant::now() < deadline,
            "full RF did not converge within {timeout:?}"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// One injected failure and everything the run measured about it.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct AvailabilityRound {
    /// The round's number, starting at 1.
    pub round: u32,
    /// The node the round killed.
    pub victim: String,
    /// The committed epoch the victim owned when it was killed.
    pub victim_epoch: u64,
    /// The seconds from the kill to the failover's publication.
    pub kill_to_publish_seconds: f64,
    /// The seconds from the kill to the first successful read.
    pub read_recovery_seconds: f64,
    /// The seconds the first write after the failover took, or None when the
    /// write did not succeed.
    pub write_recovery_seconds: Option<f64>,
    /// The longest run of consecutive failed read probes.
    pub observed_unavailable_max_streak_ms: u128,
    /// The read probes that failed.
    pub unavailable_probe_count: u64,
    /// The milliseconds between the tail write's acknowledgement and the kill.
    pub tail_ack_to_kill_ms: u128,
    /// The key whose just-acknowledged value the round watches.
    pub tail_key: u64,
    /// The size of that value in bytes.
    pub tail_bytes: usize,
    /// The seconds the tail write took.
    pub tail_put_seconds: f64,
    /// The sampled keys that survived the failure, and the ones that did not.
    pub acknowledged_key_survival: Value,
    /// Whether the previous round's tail value is still readable.
    pub previous_tail_survived: Option<bool>,
    /// The seconds from the replacement's start to the full replication factor.
    pub replacement_to_full_rf_seconds: f64,
    /// The seconds from the kill to the full replication factor.
    pub failure_to_full_rf_seconds: f64,
    /// The live members at the end of the round.
    pub live_members: Vec<String>,
    /// The keys each live member owns.
    pub owned_keys: BTreeMap<String, u64>,
    /// How evenly the keys were spread over the live members.
    pub balance: KeyBalance,
    /// The members that never failed, when there are any.
    pub survivors: Vec<String>,
    /// The keys each surviving member owns.
    pub survivor_owned_keys: BTreeMap<String, u64>,
    /// How evenly the keys were spread over the surviving members.
    pub survivor_balance: Option<KeyBalance>,
}

/// How evenly a set of keys was spread over a set of nodes.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize)]
pub struct KeyBalance {
    /// The number of nodes the keys were spread over.
    pub nodes: usize,
    /// The largest share divided by the smallest one. When a node holds no
    /// keys the ratio is unbounded, and this is `f64::INFINITY`; JSON has no
    /// infinity, so it is written as `null` there.
    pub max_over_min: f64,
    /// The largest share minus the smallest one, divided by the mean.
    pub range_over_mean: f64,
    /// The largest share divided by the mean, minus one.
    pub max_over_mean_minus_one: f64,
    /// The standard deviation of the shares divided by the mean.
    pub cv: f64,
}

impl KeyBalance {
    /// The spread of the given shares. An empty set has no spread.
    fn of(shares: &[f64]) -> Self {
        if shares.is_empty() {
            return Self::default();
        }
        let mean = shares.iter().sum::<f64>() / shares.len() as f64;
        let max = shares.iter().copied().fold(f64::MIN, f64::max);
        let min = shares.iter().copied().fold(f64::MAX, f64::min);
        // A node with no keys makes the ratio unbounded. Reporting 0 would
        // read as perfect balance.
        let max_over_min = if min > 0.0 {
            max / min
        } else if max > 0.0 {
            f64::INFINITY
        } else {
            1.0
        };
        Self {
            nodes: shares.len(),
            max_over_min,
            range_over_mean: if mean > 0.0 { (max - min) / mean } else { 0.0 },
            max_over_mean_minus_one: if mean > 0.0 { max / mean - 1.0 } else { 0.0 },
            cv: coefficient_of_variation(shares),
        }
    }
}

/// The keys a set of members owns, and how evenly they hold them.
fn owned_key_balance(
    topology: &TopologySnapshot,
    key_space: u64,
    only: Option<&[String]>,
) -> (BTreeMap<String, u64>, KeyBalance) {
    let counts = owned_key_counts(topology, key_space);
    let counts: BTreeMap<String, u64> = match only {
        Some(members) => counts
            .into_iter()
            .filter(|(node, _)| members.contains(node))
            .collect(),
        None => counts,
    };
    let shares: Vec<f64> = counts.values().map(|count| *count as f64).collect();
    let balance = KeyBalance::of(&shares);
    (counts, balance)
}

async fn run_availability(context: RunContext<'_>) -> Result<u64> {
    let RunContext {
        config,
        members,
        ports,
        coordinator_endpoint,
        admin_client,
        processes,
        events,
        measurements,
        observations,
        ..
    } = context;
    let rounds = config.availability_rounds.max(1) as usize;
    let initial_count = members.len().saturating_sub(rounds);
    ensure!(
        initial_count >= 1,
        "the availability mode needs at least one initial member"
    );
    let topology = admin_client.refresh_topology().await?;
    let owner_keys: Vec<_> = (0..config.key_count)
        .filter(|key| {
            topology
                .owner(&key.to_be_bytes())
                .is_ok_and(|owner| owner.node_id == members[0].node_id)
        })
        .take(128)
        .collect();
    ensure!(
        !owner_keys.is_empty(),
        "availability dataset has no keys owned by the failed node"
    );
    events.record(
        "failover_sample_selected",
        json!({ "owner": members[0].node_id, "keys": owner_keys }),
    )?;
    // The probe key decides which node each round kills: its owner changes
    // with every failover, so every round kills a node that has not failed
    // yet.
    let probe_key = owner_keys[0];

    // The value each sampled key is expected to hold, as the run last saw it.
    // `None` means the run observed the key absent. The oracle follows what a
    // failure left behind, so a loss the policy allows is not reported again
    // by a later round that samples the same key.
    let mut expected_values: BTreeMap<u64, Option<Vec<u8>>> = BTreeMap::new();
    // The previous round's tail key and the value the run expects it to hold
    // now, which is the observed value when the policy allowed the write to
    // be lost.
    let mut previous_tail: Option<(u64, Option<Vec<u8>>)> = None;
    let mut killed: Vec<String> = Vec::new();
    let mut round_records: Vec<AvailabilityRound> = Vec::new();
    let mut lost_total = 0u64;
    let mut unavailable_total = 0u64;
    let mut longest_unavailable_total = 0u128;
    let mut slowest_read_recovery = 0.0_f64;
    let mut slowest_failover = 0.0_f64;
    let mut final_epoch = 0u64;
    let started = Instant::now();

    for round in 1..=rounds {
        let round = round as u32;
        let topology = admin_client.refresh_topology().await?;
        let victim = topology.owner(&probe_key.to_be_bytes())?.node_id.clone();
        let victim_epoch = topology.epoch;
        let sampled: Vec<u64> = (0..config.key_count)
            .filter(|key| {
                topology
                    .owner(&key.to_be_bytes())
                    .is_ok_and(|owner| owner.node_id == victim)
            })
            .take(128)
            .collect();
        ensure!(
            !sampled.is_empty(),
            "availability dataset has no keys owned by {victim}"
        );
        events.record(
            "failover_sample_selected",
            json!({ "round": round, "owner": victim, "keys": sampled }),
        )?;

        // An identifiable, just-acknowledged tail makes OwnerOnly loss
        // observable when it occurs; zero loss in one run is not a durability
        // guarantee. The key must differ from the previous round's, or this
        // round's own write would overwrite the value that the previous-tail
        // check is meant to look for.
        let tail_key = sampled
            .iter()
            .rev()
            .copied()
            .find(|key| {
                previous_tail
                    .as_ref()
                    .is_none_or(|(previous, _)| previous != key)
            })
            .unwrap_or_else(|| *sampled.last().expect("nonempty sample was checked"));
        // Whether this round had to reuse the previous round's tail key, which
        // makes the previous-tail check meaningless for it.
        let tail_reuses_previous = previous_tail
            .as_ref()
            .is_some_and(|(previous, _)| *previous == tail_key);
        // The tail value is made large enough to be a meaningful write, but
        // never larger than the data nodes accept.
        let tail_bytes = tail_value_bytes(config.node.max_value_bytes, config.value_bytes);
        let tail_value = deterministic_value(tail_key, u64::from(round), tail_bytes);
        // The value the key held before this round's write. A key that was
        // already observed keeps what the run saw then, including absence.
        let prior_tail_value = expected_values
            .get(&tail_key)
            .cloned()
            .unwrap_or_else(|| Some(deterministic_value(tail_key, 0, config.value_bytes)));
        let tail_put_started = Instant::now();
        let writer =
            HashringClient::connect(coordinator_endpoint.to_owned(), Duration::from_secs(10))
                .await?;
        writer
            .put(tail_key.to_be_bytes().to_vec(), tail_value.clone())
            .await?;
        let tail_put_seconds = tail_put_started.elapsed().as_secs_f64();
        expected_values.insert(tail_key, Some(tail_value.clone()));
        let tail_ack_at = Instant::now();
        let failed_at = Instant::now();
        let (exit, kill_signalled_at) = processes.kill(&victim)?;
        let tail_ack_to_kill_ms = kill_signalled_at.duration_since(tail_ack_at).as_millis();
        events.record(
            "owner_killed",
            json!({ "round": round, "node_id": victim, "exit": exit,
            "tail_key": tail_key, "tail_bytes": tail_bytes,
            "tail_put_seconds": tail_put_seconds,
            "tail_ack_to_kill_ms": tail_ack_to_kill_ms }),
        )?;
        let probe =
            HashringClient::connect(coordinator_endpoint.to_owned(), Duration::from_millis(250))
                .await?;
        let probe_bytes = probe_key.to_be_bytes().to_vec();
        let failover_deadline = Instant::now() + Duration::from_secs(30);
        let mut first_unavailable: Option<Instant> = None;
        let mut longest_unavailable_ms = 0u128;
        let mut unavailable_probes = 0u64;
        let mut published_seconds = None;
        loop {
            processes.ensure_running()?;
            let available = matches!(
                probe.get(probe_bytes.clone()).await,
                Ok(_)
                    | Err(ClientError::Operation(hashring_client::OperationFailure {
                        code: ErrorCode::NotFound,
                        ..
                    }))
            );
            if available {
                if let Some(started) = first_unavailable.take() {
                    longest_unavailable_ms =
                        longest_unavailable_ms.max(started.elapsed().as_millis());
                }
            } else {
                unavailable_probes += 1;
                first_unavailable.get_or_insert_with(Instant::now);
            }
            let current = admin_client.refresh_topology().await?;
            if published_seconds.is_none()
                && current.epoch > topology.epoch
                && !current
                    .members
                    .iter()
                    .any(|member| member.node_id == victim)
            {
                let seconds = failed_at.elapsed().as_secs_f64();
                published_seconds = Some(seconds);
                events.record(
                    "failover_published",
                    json!({ "round": round, "seconds": seconds, "epoch": current.epoch }),
                )?;
            }
            if published_seconds.is_some() && available {
                break;
            }
            ensure!(
                Instant::now() < failover_deadline,
                "automatic failover did not publish and resolve the read probe within 30 seconds"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        if let Some(started) = first_unavailable {
            longest_unavailable_ms = longest_unavailable_ms.max(started.elapsed().as_millis());
        }
        let failover_seconds = published_seconds.expect("loop exits only after publication");
        let read_recovery_seconds = failed_at.elapsed().as_secs_f64();
        events.record(
            "failover_read_recovered",
            json!({
                "round": round,
                "seconds": failover_seconds,
                "read_recovery_seconds": read_recovery_seconds,
                "observed_unavailable_max_streak_ms": longest_unavailable_ms,
                "unavailable_probe_count": unavailable_probes,
            }),
        )?;

        let reader =
            HashringClient::connect(coordinator_endpoint.to_owned(), Duration::from_secs(5))
                .await?;
        let mut lost = 0u64;
        let mut tail_survived = true;
        for key in &sampled {
            let expected = expected_values
                .get(key)
                .cloned()
                .unwrap_or_else(|| Some(deterministic_value(*key, 0, config.value_bytes)));
            match reader.get(key.to_be_bytes().to_vec()).await {
                Ok(output) if *key == tail_key => {
                    if output.value != tail_value {
                        // The policy may allow losing the just-acknowledged
                        // write. The key must then hold what it held before
                        // it, and the oracle follows the observed value.
                        ensure!(
                            Some(&output.value) == prior_tail_value.as_ref(),
                            "tail key {key} has a value other than the acknowledged or prior value"
                        );
                        expected_values.insert(*key, Some(output.value));
                        lost += 1;
                        tail_survived = false;
                    }
                }
                Ok(output) => {
                    ensure!(
                        expected.as_deref() == Some(output.value.as_slice()),
                        "surviving key {key} has an unexpected value"
                    );
                    expected_values.insert(*key, Some(output.value));
                }
                Err(ClientError::Operation(failure)) if failure.code == ErrorCode::NotFound => {
                    // A key the oracle already knew absent is not a new loss.
                    if expected.is_some() {
                        lost += 1;
                    }
                    expected_values.insert(*key, None);
                    if *key == tail_key {
                        tail_survived = false;
                    }
                }
                Err(error) => {
                    return Err(error)
                        .with_context(|| format!("reading acknowledged key {key} after failover"));
                }
            }
        }
        // The value the previous round left is checked here as well: a
        // failure that follows another one must not lose it either. A round
        // that had to reuse the previous tail key has already overwritten it,
        // so it reports no result rather than a loss.
        let mut previous_tail_survived = None;
        if !tail_reuses_previous && let Some((key, expected)) = previous_tail.as_ref() {
            previous_tail_survived = Some(match reader.get(key.to_be_bytes().to_vec()).await {
                Ok(output) => Some(&output.value) == expected.as_ref(),
                Err(ClientError::Operation(failure)) if failure.code == ErrorCode::NotFound => {
                    expected.is_none()
                }
                Err(error) => {
                    eprintln!("the previous round's tail key could not be read: {error}");
                    false
                }
            });
        }
        ensure!(
            round > 1 || config.write_ack_policy != WriteAckPolicy::FirstSuccessor || lost == 0,
            "FirstSuccessor lost {lost} acknowledged sampled keys"
        );
        // The cluster must serve writes again, not only reads. The key is
        // outside the sampled key space, so it never changes what the run
        // counts.
        let write_probe_key = config.key_count;
        let write_probe_value =
            deterministic_value(write_probe_key, u64::from(round), config.value_bytes);
        // The write after a failover waits for the new first successor to be
        // admitted, so it gets a longer deadline than the tail write.
        let probe_writer =
            HashringClient::connect(coordinator_endpoint.to_owned(), Duration::from_secs(30))
                .await?;
        let write_started = Instant::now();
        let write_result = probe_writer
            .put(
                write_probe_key.to_be_bytes().to_vec(),
                write_probe_value.clone(),
            )
            .await;
        let write_recovery_seconds = match write_result {
            Ok(_) => Some(write_started.elapsed().as_secs_f64()),
            Err(error) => {
                return Err(anyhow::Error::from(error)).with_context(|| {
                    format!(
                        "the cluster did not accept a write after the failover of round {round}"
                    )
                });
            }
        };
        let write_read_back = reader.get(write_probe_key.to_be_bytes().to_vec()).await?;
        ensure!(
            write_read_back.value == write_probe_value,
            "the write after the failover of round {round} did not read back"
        );
        events.record(
            "write_after_failover",
            json!({ "round": round, "key": write_probe_key,
            "seconds": write_recovery_seconds }),
        )?;
        lost_total += lost;
        unavailable_total += unavailable_probes;
        longest_unavailable_total = longest_unavailable_total.max(longest_unavailable_ms);
        slowest_read_recovery = slowest_read_recovery.max(read_recovery_seconds);
        slowest_failover = slowest_failover.max(failover_seconds);
        if round == 1 {
            // The run's own figures describe its first failure; every round
            // has its own record below.
            observations.insert(
                "acknowledged_key_survival".into(),
                json!({
                    "sampled": sampled.len(), "preserved": sampled.len() as u64 - lost,
                    "lost": lost, "policy": config.write_ack_policy,
                    "tail_key": tail_key, "tail_survived": tail_survived,
                }),
            );
        }
        events.record(
            "acknowledged_key_survival_checked",
            json!({
                "round": round,
                "sampled": sampled.len(), "preserved": sampled.len() as u64 - lost,
                "lost": lost, "policy": config.write_ack_policy,
                "tail_key": tail_key, "tail_survived": tail_survived,
                "previous_tail_survived": previous_tail_survived,
            }),
        )?;

        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let change = admin_client.topology_change().await?;
            if change.is_none_or(|change| change.phase.is_terminal()) {
                break;
            }
            ensure!(
                Instant::now() < deadline,
                "failover change did not complete"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // The replacement joins like a scale-out node: it is seeded from the
        // admitted copies and the membership returns to its size.
        let live = admin_client.refresh_topology().await?.members;
        let spare = &members[initial_count + round as usize - 1];
        let spare_port = ports[initial_count + round as usize];
        let mut target = live.clone();
        target.push(spare.clone());
        let change = admin_client.begin_topology_change(target).await?;
        let restore_started = Instant::now();
        spawn_node(
            processes,
            spare.node_id.clone(),
            spare_port,
            coordinator_endpoint,
        )?;
        wait_for_listener(spare_port, processes).await?;
        let completed = admin_client
            .execute_topology_change(
                change.change_id,
                change.base_epoch,
                change.target_topology.epoch,
            )
            .await?;
        wait_for_full_rf(
            admin_client,
            config.desired_replication_factor,
            Duration::from_millis(config.migration_timeout_ms),
            processes,
        )
        .await?;
        let restored_seconds = restore_started.elapsed().as_secs_f64();
        // Sample the recovery time once. The balance analysis below hashes the
        // whole key space twice, and sampling again after it would make the
        // per-round record disagree with the run-level figure.
        let failure_to_full_rf_seconds = failed_at.elapsed().as_secs_f64();
        if round == 1 {
            observations.insert(
                "replacement_to_full_rf_seconds".into(),
                json!(restored_seconds),
            );
            observations.insert(
                "failure_to_full_rf_seconds".into(),
                json!(failure_to_full_rf_seconds),
            );
        }
        measurements.insert(
            format!("restore_full_rf_round_{round}"),
            measurement(1, restore_started.elapsed()),
        );
        if round == 1 {
            // The run's own figures describe its first round; every round has
            // its own entry above.
            measurements.insert(
                "restore_full_rf".into(),
                measurement(1, restore_started.elapsed()),
            );
        }
        events.record(
            "full_rf_restored",
            json!({ "round": round, "epoch": completed.target_topology.epoch,
        "replacement_to_full_rf_seconds": restored_seconds }),
        )?;
        final_epoch = completed.target_topology.epoch;

        // The balance of the live members, and of the ones that never failed.
        let topology = admin_client.refresh_topology().await?;
        let live_members: Vec<String> = topology
            .members
            .iter()
            .map(|member| member.node_id.clone())
            .collect();
        let (owned_keys, balance) = owned_key_balance(&topology, config.key_count, None);
        killed.push(victim.clone());
        let survivors: Vec<String> = members[..initial_count]
            .iter()
            .map(|member| member.node_id.clone())
            .filter(|node_id| !killed.contains(node_id))
            .collect();
        let (survivor_owned_keys, survivor_balance) = if survivors.is_empty() {
            (BTreeMap::new(), None)
        } else {
            let (counts, balance) =
                owned_key_balance(&topology, config.key_count, Some(&survivors));
            (counts, Some(balance))
        };
        events.record(
            "availability_round_balanced",
            json!({ "round": round, "owned_keys": owned_keys, "balance": balance,
        "survivor_owned_keys": survivor_owned_keys, "survivor_balance": survivor_balance }),
        )?;
        round_records.push(AvailabilityRound {
            round,
            victim: victim.clone(),
            victim_epoch,
            kill_to_publish_seconds: failover_seconds,
            read_recovery_seconds,
            write_recovery_seconds,
            observed_unavailable_max_streak_ms: longest_unavailable_ms,
            unavailable_probe_count: unavailable_probes,
            tail_ack_to_kill_ms,
            tail_key,
            tail_bytes,
            tail_put_seconds,
            acknowledged_key_survival: json!({
                "sampled": sampled.len(), "preserved": sampled.len() as u64 - lost,
                "lost": lost, "policy": config.write_ack_policy,
                "tail_key": tail_key, "tail_survived": tail_survived,
            }),
            previous_tail_survived,
            replacement_to_full_rf_seconds: restored_seconds,
            failure_to_full_rf_seconds,
            live_members,
            owned_keys,
            balance,
            survivors,
            survivor_owned_keys,
            survivor_balance,
        });
        previous_tail = Some((
            tail_key,
            expected_values
                .get(&tail_key)
                .cloned()
                .unwrap_or_else(|| Some(tail_value.clone())),
        ));
    }

    observations.insert("availability_round_count".into(), json!(rounds));
    observations.insert(
        "availability_rounds".into(),
        serde_json::to_value(&round_records)?,
    );
    // The run's own figures describe its worst round, so a single failure
    // and a sequence of them are read the same way.
    observations.insert("failover_seconds".into(), json!(slowest_failover));
    observations.insert("read_recovery_seconds".into(), json!(slowest_read_recovery));
    observations.insert(
        "observed_unavailable_max_streak_ms".into(),
        json!(longest_unavailable_total),
    );
    observations.insert("unavailable_probe_count".into(), json!(unavailable_total));
    observations.insert(
        "availability_max_read_recovery_seconds".into(),
        json!(slowest_read_recovery),
    );
    observations.insert(
        "availability_observed_unavailable_max_streak_ms".into(),
        json!(longest_unavailable_total),
    );
    observations.insert(
        "availability_unavailable_probe_count".into(),
        json!(unavailable_total),
    );
    observations.insert(
        "availability_acknowledged_lost_total".into(),
        json!(lost_total),
    );
    observations.insert(
        "availability_total_seconds".into(),
        json!(started.elapsed().as_secs_f64()),
    );
    events.record(
        "availability_rounds_complete",
        json!({ "rounds": rounds, "lost": lost_total,
        "seconds": started.elapsed().as_secs_f64() }),
    )?;
    Ok(final_epoch)
}

/// Records the ring the cluster runs right now.
///
/// The snapshot is the output of `hashring-rs topology`, so a run that has
/// ended can still be read: a reader takes the snapshots from the event log
/// instead of asking a coordinator that is gone.
async fn record_topology(events: &EventLog, client: &HashringClient) -> Result<TopologySnapshot> {
    let topology = match client.refresh_topology().await {
        Ok(topology) => topology,
        Err(error) => {
            eprintln!("the topology could not be refreshed for the snapshot: {error}");
            client.topology().await
        }
    };
    events.record("topology_published", serde_json::to_value(&topology)?)?;
    Ok(topology)
}

/// The counters one node reports, and the CPU it used.
#[derive(Clone, Copy, Debug, Default)]
struct NodeCounters {
    ops_handled: u64,
    cpu_seconds: Option<f64>,
}

/// Runs the `latency` mode: one stage per target rate, and a check of the key
/// space the load left behind.
async fn run_latency(context: RunContext<'_>) -> Result<u64> {
    let RunContext {
        config,
        members,
        admin_client,
        workload_client,
        processes,
        events,
        measurements,
        observations,
        ..
    } = context;
    ensure!(
        !config.target_rates.is_empty(),
        "the latency mode needs at least one target rate"
    );
    let key_space = config.key_count;
    let mix = latency::OperationMix::parse(&config.mix)?;
    let delete_pool = delete_pool_size(config, &mix, key_space);
    // The distribution covers the keys the load reads and writes. The keys it
    // deletes are a pool of their own, so a delete never races another
    // operation on the same key.
    let distribution =
        latency::KeyDistribution::parse(&config.key_distribution, key_space - delete_pool)?;
    let plan = latency::LoadPlan {
        key_space,
        value_bytes: config.value_bytes,
        mix,
        distribution: distribution.clone(),
        max_in_flight: config.max_in_flight,
        delete_pool_start: key_space - delete_pool,
        deleted: Arc::new(
            (0..delete_pool as usize)
                .map(|_| AtomicBool::new(false))
                .collect(),
        ),
        highest_latency_micros: highest_latency_micros(config),
        drain_timeout: Duration::from_millis(config.operation_timeout_ms) + Duration::from_secs(5),
    };
    observations.insert("latency_key_space".into(), json!(key_space));
    observations.insert("latency_delete_pool_keys".into(), json!(delete_pool));
    observations.insert(
        "latency_key_distribution".into(),
        json!(distribution.name()),
    );
    observations.insert("latency_mix".into(), json!(config.mix));
    // The `measurements` of a latency run hold one entry per rate stage, so
    // the preload and the check that follows it move to the observations.
    for name in ["initial_put", "initial_get_verify"] {
        if let Some(measurement) = measurements.remove(name) {
            observations.insert(name.into(), serde_json::to_value(measurement)?);
        }
    }

    let topology = admin_client.refresh_topology().await?;
    let owned_keys = owned_key_counts(&topology, key_space);
    let runner = latency::LoadRunner::new(workload_client.clone(), plan.clone())?;
    let pids = processes.pids();
    let mut saturated_windows = serde_json::Map::new();
    for (index, rate) in config.target_rates.iter().enumerate() {
        let stage = latency::StageSettings {
            name: format!("rate_{index}"),
            target_rate: *rate,
            warmup_seconds: config.warmup_seconds,
            duration_seconds: config.duration_seconds,
        };
        runner.run_warmup(&stage, events).await?;
        let before = sample_node_counters(members, &pids).await?;
        let (mut measurement, saturated) = runner.run_measured(&stage, events).await?;
        let after = sample_node_counters(members, &pids).await?;
        measurement.load_balance = load_balance(&owned_keys, &before, &after, measurement.seconds)?;
        saturated_windows.insert(stage.name.clone(), json!(saturated));
        measurements.insert(stage.name, Measurement::LatencyStage(measurement));
    }
    observations.insert(
        "latency_saturated_windows".into(),
        Value::from(saturated_windows),
    );
    let expected = (0..key_space)
        .map(|key| (!plan.is_deleted(key)).then_some(0))
        .collect::<Vec<_>>();
    verify_dataset(
        workload_client,
        Workload {
            key_count: key_space,
            value_bytes: config.value_bytes,
            concurrency: config.concurrency,
        },
        &expected,
    )
    .await?;
    observations.insert("latency_verified_keys".into(), json!(key_space));
    processes.ensure_running()?;
    let topology = admin_client.refresh_topology().await?;
    events.record(
        "verification_complete",
        json!({ "epoch": topology.epoch, "members": topology.members.len() }),
    )?;
    record_topology(events, admin_client).await?;
    Ok(topology.epoch)
}

/// The keys the load may delete.
///
/// The load deletes from a pool of its own, so a delete never races another
/// operation on the same key and the key space can be checked afterwards. The
/// pool is as large as the run's deletes need, and never more than half of the
/// key space and never less than a twentieth of it.
fn delete_pool_size(config: &ExperimentConfig, mix: &latency::OperationMix, key_space: u64) -> u64 {
    if mix.delete_fraction() <= 0.0 {
        return 0;
    }
    let expected: f64 = config
        .target_rates
        .iter()
        .map(|rate| {
            *rate as f64
                * (config.duration_seconds + config.warmup_seconds) as f64
                * mix.delete_fraction()
        })
        .sum();
    let wanted = expected.ceil() as u64;
    let minimum = (key_space as f64 * 0.05).floor() as u64;
    let maximum = (key_space as f64 * 0.5).floor() as u64;
    wanted
        .clamp(minimum, maximum)
        .min(key_space.saturating_sub(1))
}

/// The largest latency the histogram keeps.
///
/// A request cannot take longer than the operation deadline, and the driver
/// measures from the moment the request was due, so the deadline plus a
/// second covers every request it can issue.
fn highest_latency_micros(config: &ExperimentConfig) -> u64 {
    config
        .operation_timeout_ms
        .saturating_mul(1_000)
        .saturating_add(1_000_000)
        .min(600_000_000)
}

/// The number of keys each node owns.
fn owned_key_counts(topology: &TopologySnapshot, key_space: u64) -> BTreeMap<String, u64> {
    let mut counts: BTreeMap<&str, u64> = topology
        .members
        .iter()
        .map(|member| (member.node_id.as_str(), 0))
        .collect();
    for key in 0..key_space {
        if let Ok(owner) = topology.owner(&key.to_be_bytes())
            && let Some(count) = counts.get_mut(owner.node_id.as_str())
        {
            *count += 1;
        }
    }
    counts
        .into_iter()
        .map(|(node, count)| (node.to_owned(), count))
        .collect()
}

/// The operations a node served and the CPU it used.
async fn sample_node_counters(
    members: &[Member],
    pids: &BTreeMap<String, u32>,
) -> Result<BTreeMap<String, NodeCounters>> {
    let mut counters = BTreeMap::new();
    for member in members {
        let ops_handled = query_node_info(&member.endpoint)
            .await
            .with_context(|| format!("sampling operations of {}", member.node_id))?
            .ops_handled;
        let cpu_seconds = pids
            .get(&member.node_id)
            .and_then(|pid| process_cpu_seconds(*pid));
        counters.insert(
            member.node_id.clone(),
            NodeCounters {
                ops_handled,
                cpu_seconds,
            },
        );
    }
    Ok(counters)
}

async fn query_node_info(endpoint: &str) -> Result<hashring_core::proto::NodeInfoResponse> {
    tokio::time::timeout(Duration::from_secs(2), async {
        let mut client =
            configure_data_node_client(DataNodeClient::connect(endpoint.to_owned()).await?);
        Ok(client
            .get_process_info(hashring_core::proto::Empty {})
            .await?
            .into_inner())
    })
    .await
    .context("node information request timed out")?
}

/// How the keys and the requests were spread over the nodes.
fn load_balance(
    owned_keys: &BTreeMap<String, u64>,
    before: &BTreeMap<String, NodeCounters>,
    after: &BTreeMap<String, NodeCounters>,
    seconds: f64,
) -> Result<LoadBalanceMeasurement> {
    let mut per_node = BTreeMap::new();
    let mut owned = Vec::new();
    let mut handled = Vec::new();
    for (node, keys) in owned_keys {
        let start = before.get(node).context("missing initial node counters")?;
        let end = after.get(node).context("missing final node counters")?;
        let ops_handled = end
            .ops_handled
            .checked_sub(start.ops_handled)
            .context("node operations counter decreased during the stage")?;
        let cpu_percent = start
            .cpu_seconds
            .zip(end.cpu_seconds)
            .and_then(|(a, b)| (seconds > 0.0 && b >= a).then_some((b - a) / seconds * 100.0));
        owned.push(*keys as f64);
        handled.push(ops_handled as f64);
        per_node.insert(
            node.clone(),
            LoadBalanceNode {
                owned_keys: *keys,
                ops_handled,
                cpu_percent,
            },
        );
    }
    Ok(LoadBalanceMeasurement {
        per_node,
        owned_keys_max_over_mean: max_over_mean(&owned),
        owned_keys_cv: coefficient_of_variation(&owned),
        ops_handled_max_over_mean: max_over_mean(&handled),
        ops_handled_cv: coefficient_of_variation(&handled),
    })
}

/// The largest value divided by the mean of the values.
fn max_over_mean(values: &[f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let mean = values.iter().sum::<f64>() / values.len() as f64;
    if mean <= 0.0 {
        return 0.0;
    }
    values.iter().copied().fold(0.0_f64, f64::max) / mean
}

/// The standard deviation of the values divided by their mean.
fn coefficient_of_variation(values: &[f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let mean = values.iter().sum::<f64>() / values.len() as f64;
    if mean <= 0.0 {
        return 0.0;
    }
    let variance = values
        .iter()
        .map(|value| (value - mean).powi(2))
        .sum::<f64>()
        / values.len() as f64;
    variance.sqrt() / mean
}

/// The CPU time a process has used, in seconds.
///
/// The counter is the kernel's, so it covers the whole life of the process,
/// not only the time it spent in this run.
fn process_cpu_seconds(pid: u32) -> Option<f64> {
    let stat = fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    // The command name is in parentheses and may hold spaces, so the fields
    // after it are found from the last closing parenthesis.
    let rest = stat.rsplit_once(") ")?.1;
    let fields: Vec<&str> = rest.split_whitespace().collect();
    let utime: u64 = fields.get(11)?.parse().ok()?;
    let stime: u64 = fields.get(12)?.parse().ok()?;
    Some((utime + stime) as f64 / clock_ticks_per_second() as f64)
}

/// The clock ticks the kernel counts CPU time in.
fn clock_ticks_per_second() -> u64 {
    static TICKS: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *TICKS.get_or_init(|| {
        command_output(None, "getconf", &["CLK_TCK"])
            .and_then(|value| value.trim().parse().ok())
            .unwrap_or(100)
    })
}

async fn migrate_while_mutating(
    workload_client: &HashringClient,
    admin_client: &HashringClient,
    change: TopologyChange,
    workload: MigrationWorkload,
    events: &EventLog,
) -> Result<Duration> {
    let label = workload.label;
    let direct_merge = change.direct_merge;
    events.record(
        "migration_started",
        json!({ "label": label, "change_id": change.change_id }),
    )?;
    let started = Instant::now();
    let execution_client = admin_client.clone();
    let change_id = change.change_id.clone();
    let execution = tokio::spawn(async move {
        execution_client
            .execute_topology_change(change_id, change.base_epoch, change.target_topology.epoch)
            .await
    });
    let active_phase = if direct_merge {
        None
    } else {
        Some(wait_for_migration_activity(admin_client).await?)
    };
    let completed_mutations = Arc::new(AtomicU64::new(0));
    let writer_client = workload_client.clone();
    let writer_progress = completed_mutations.clone();
    let writer = tokio::spawn(async move {
        mutate_keys(
            &writer_client,
            workload.put_keys,
            workload.delete_keys,
            workload.round,
            workload.dataset,
            writer_progress,
        )
        .await
    });
    if !direct_merge {
        let (overlap_phase, overlap_mutations) =
            match wait_for_mutation_overlap(admin_client, &completed_mutations, &writer).await {
                Ok(overlap) => overlap,
                Err(error) => {
                    execution.abort();
                    if writer.is_finished() {
                        if let Ok(Err(writer_error)) = writer.await {
                            return Err(writer_error.context(error));
                        }
                    } else {
                        writer.abort();
                    }
                    return Err(error);
                }
            };
        events.record(
            "migration_mutation_overlap_observed",
            json!({
                "label": label,
                "initial_phase": format!("{:?}", active_phase.expect("copy migration has an active phase")),
                "observed_phase": format!("{overlap_phase:?}"),
                "completed_mutations_while_active": overlap_mutations,
                "untouched_moving_sentinels": workload.sentinel_count,
            }),
        )?;
    }
    let writer_result = writer.await.context("joining concurrent mutator")?;
    if let Err(error) = &writer_result {
        events.record(
            "migration_writer_failed",
            json!({
                "label": label,
                "error": format!("{error:#}"),
                "completed_mutations": completed_mutations.load(AtomicOrdering::Acquire),
                "change_phase": admin_client.topology_change().await.ok().flatten().map(|change| format!("{:?}", change.phase)),
            }),
        )?;
    }
    let execution_result = execution.await.context("joining migration execution")?;
    events.record(
        "migration_execution_finished",
        json!({
            "label": label,
            "result": match &execution_result {
                Ok(change) => format!("{:?}", change.phase),
                Err(error) => format!("error: {error}"),
            },
        }),
    )?;
    let mutations = writer_result?;
    let completed = execution_result?;
    ensure!(
        completed.phase == MigrationPhase::Complete,
        "migration ended in {:?}",
        completed.phase
    );
    if direct_merge {
        events.record(
            "direct_merge_cutover_observed",
            json!({
                "label": label,
                "completed_mutations": completed_mutations.load(AtomicOrdering::Acquire),
                "untouched_moving_sentinels": workload.sentinel_count,
            }),
        )?;
    }
    let duration = started.elapsed();
    events.record(
        "migration_complete",
        json!({
            "label": label,
            "seconds": duration.as_secs_f64(),
            "writes": mutations.writes,
            "deletes": mutations.deletes,
        }),
    )?;
    Ok(duration)
}

async fn wait_for_migration_activity(client: &HashringClient) -> Result<MigrationPhase> {
    for _ in 0..2_000 {
        if let Some(change) = client.topology_change().await? {
            match change.phase {
                MigrationPhase::CopyingSnapshot
                | MigrationPhase::ReplayingChangelog
                | MigrationPhase::PausingWrites
                | MigrationPhase::Verifying
                | MigrationPhase::ReadyToPublish => return Ok(change.phase),
                MigrationPhase::Published
                | MigrationPhase::CleaningUp
                | MigrationPhase::Complete
                | MigrationPhase::Aborting
                | MigrationPhase::Aborted => {
                    bail!(
                        "migration reached {:?} before concurrent writes began",
                        change.phase
                    )
                }
                MigrationPhase::Planned | MigrationPhase::Resetting => {}
            }
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    bail!("migration did not leave its planned phase")
}

async fn wait_for_mutation_overlap(
    client: &HashringClient,
    completed_mutations: &AtomicU64,
    writer: &tokio::task::JoinHandle<Result<MutationCounts>>,
) -> Result<(MigrationPhase, u64)> {
    for _ in 0..2_000 {
        let mutations = completed_mutations.load(AtomicOrdering::Acquire);
        if let Some(change) = client.topology_change().await? {
            match change.phase {
                MigrationPhase::CopyingSnapshot
                | MigrationPhase::ReplayingChangelog
                | MigrationPhase::PausingWrites
                | MigrationPhase::Verifying
                | MigrationPhase::ReadyToPublish
                    if mutations > 0 =>
                {
                    return Ok((change.phase, mutations));
                }
                MigrationPhase::Published
                | MigrationPhase::CleaningUp
                | MigrationPhase::Complete
                | MigrationPhase::Aborting
                | MigrationPhase::Aborted => {
                    bail!(
                        "migration reached {:?} before any concurrent mutation completed",
                        change.phase
                    )
                }
                _ => {}
            }
        }
        // Reload the count: the writer may have completed every mutation
        // during the topology poll, after `mutations` was read.
        if writer.is_finished() && completed_mutations.load(AtomicOrdering::Acquire) == 0 {
            bail!("concurrent mutator finished without a successful mutation");
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    bail!("no successful mutation overlapped the active migration")
}

async fn mutate_keys(
    client: &HashringClient,
    put_keys: Vec<u64>,
    keys_to_delete: Vec<u64>,
    round: u64,
    workload: Workload,
    progress: Arc<AtomicU64>,
) -> Result<MutationCounts> {
    let (writes, deletes) = tokio::try_join!(
        write_keys(client, put_keys, round, workload, Some(progress.clone())),
        delete_keys(client, keys_to_delete, workload.concurrency, progress),
    )?;
    Ok(MutationCounts { writes, deletes })
}

async fn write_dataset(client: &HashringClient, workload: Workload, round: u64) -> Result<()> {
    write_keys(
        client,
        (0..workload.key_count).collect(),
        round,
        workload,
        None,
    )
    .await?;
    Ok(())
}

async fn write_keys(
    client: &HashringClient,
    keys: Vec<u64>,
    round: u64,
    workload: Workload,
    progress: Option<Arc<AtomicU64>>,
) -> Result<u64> {
    let operation_count = keys.len() as u64;
    let client = client.clone();
    stream::iter(keys)
        .map(Ok::<_, anyhow::Error>)
        .try_for_each_concurrent(Some(workload.concurrency), move |key| {
            let client = client.clone();
            let progress = progress.clone();
            async move {
                client
                    .put(
                        key.to_be_bytes().to_vec(),
                        deterministic_value(key, round, workload.value_bytes),
                    )
                    .await
                    .with_context(|| format!("put key {key} in round {round}"))?;
                if let Some(progress) = progress {
                    progress.fetch_add(1, AtomicOrdering::Release);
                }
                Ok(())
            }
        })
        .await?;
    Ok(operation_count)
}

async fn delete_keys(
    client: &HashringClient,
    keys: Vec<u64>,
    concurrency: usize,
    progress: Arc<AtomicU64>,
) -> Result<u64> {
    let operation_count = keys.len() as u64;
    let client = client.clone();
    stream::iter(keys)
        .map(Ok::<_, anyhow::Error>)
        .try_for_each_concurrent(Some(concurrency), move |key| {
            let client = client.clone();
            let progress = progress.clone();
            async move {
                client
                    .delete(key.to_be_bytes().to_vec())
                    .await
                    .with_context(|| format!("delete key {key}"))?;
                progress.fetch_add(1, AtomicOrdering::Release);
                Ok(())
            }
        })
        .await?;
    Ok(operation_count)
}

async fn verify_dataset(
    client: &HashringClient,
    workload: Workload,
    expected_rounds: &[Option<u64>],
) -> Result<()> {
    ensure!(
        expected_rounds.len() == workload.key_count as usize,
        "expected-round vector does not match key count"
    );
    let client = client.clone();
    stream::iter(0..workload.key_count)
        .map(Ok::<_, anyhow::Error>)
        .try_for_each_concurrent(Some(workload.concurrency), move |key| {
            let client = client.clone();
            async move {
                match expected_rounds[key as usize] {
                    Some(round) => {
                        let output = client
                            .get(key.to_be_bytes().to_vec())
                            .await
                            .with_context(|| format!("get key {key} in round {round}"))?;
                        ensure!(
                            output.value == deterministic_value(key, round, workload.value_bytes),
                            "value mismatch for key {key} in round {round}"
                        );
                        Ok(())
                    }
                    None => match client.get(key.to_be_bytes().to_vec()).await {
                        Err(ClientError::Operation(failure))
                            if failure.code == ErrorCode::NotFound =>
                        {
                            Ok(())
                        }
                        Ok(_) => bail!("deleted key {key} unexpectedly has a value"),
                        Err(error) => Err(error)
                            .with_context(|| format!("expected key {key} to remain absent")),
                    },
                }
            }
        })
        .await
}

fn moving_key_cohorts(change: &TopologyChange, key_count: u64) -> Result<MovingKeyCohorts> {
    let all = moving_keys(change, key_count);
    let mut rewrite = Vec::new();
    let mut delete = Vec::new();
    let mut sentinels = Vec::new();
    for (moving_index, key) in all.iter().copied().enumerate() {
        match moving_index % 3 {
            0 => rewrite.push(key),
            1 => delete.push(key),
            _ => sentinels.push(key),
        }
    }
    ensure!(
        !rewrite.is_empty(),
        "migration has no moving rewrite cohort"
    );
    ensure!(!delete.is_empty(), "migration has no moving delete cohort");
    ensure!(
        !sentinels.is_empty(),
        "migration has no untouched moving sentinel cohort"
    );
    Ok(MovingKeyCohorts {
        all,
        rewrite,
        delete,
        sentinels,
    })
}

fn moving_keys(change: &TopologyChange, key_count: u64) -> Vec<u64> {
    let base = change
        .base_topology
        .as_ref()
        .expect("planned change includes its base topology");
    let mut moving = Vec::new();
    for key in 0..key_count {
        let encoded = key.to_be_bytes();
        if base
            .owner(&encoded)
            .expect("base topology is valid")
            .node_id
            != change
                .target_topology
                .owner(&encoded)
                .expect("target topology is valid")
                .node_id
        {
            moving.push(key);
        }
    }
    moving
}

/// The size of the tail value: large enough to be a meaningful write, but
/// never larger than the data nodes accept.
///
/// The value size the run writes is the floor, and the run's validation keeps
/// it within the node's limit, so the tail never exceeds what the node takes.
fn tail_value_bytes(max_value_bytes: usize, value_bytes: usize) -> usize {
    (1024 * 1024).min(max_value_bytes).max(value_bytes)
}

fn deterministic_value(key: u64, round: u64, value_bytes: usize) -> Vec<u8> {
    let mut state = key ^ round.rotate_left(17) ^ 0x9e37_79b9_7f4a_7c15;
    let mut value = Vec::with_capacity(value_bytes);
    while value.len() < value_bytes {
        state ^= state >> 12;
        state ^= state << 25;
        state ^= state >> 27;
        let block = state.wrapping_mul(0x2545_f491_4f6c_dd1d).to_be_bytes();
        let remaining = value_bytes - value.len();
        value.extend_from_slice(&block[..remaining.min(block.len())]);
    }
    value
}

fn measurement(operations: u64, duration: Duration) -> Measurement {
    let seconds = duration.as_secs_f64();
    Measurement::Throughput(ThroughputMeasurement {
        operations,
        seconds,
        operations_per_second: operations as f64 / seconds.max(f64::EPSILON),
    })
}

fn spawn_node(
    processes: &mut ProcessGroup,
    node_id: String,
    port: u16,
    coordinator_endpoint: &str,
) -> Result<()> {
    let mut arguments = vec![
        "node".into(),
        "--id".into(),
        node_id.clone(),
        "--listen".into(),
        format!("127.0.0.1:{port}"),
        "--coordinator".into(),
        coordinator_endpoint.into(),
    ];
    arguments.extend(processes.node_arguments.iter().cloned());
    processes.spawn(node_id, &arguments)
}

fn node_arguments(config: &ExperimentConfig) -> Vec<String> {
    let mut arguments = vec!["--worker-threads".into(), config.worker_threads.to_string()];
    arguments.extend(config.node.arguments());
    arguments
}

async fn connect_eventually(
    endpoint: &str,
    timeout: Duration,
    processes: &mut ProcessGroup,
) -> Result<HashringClient> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match HashringClient::connect(endpoint.to_owned(), timeout).await {
            Ok(client) => return Ok(client),
            Err(error) if Instant::now() < deadline => {
                processes.ensure_running()?;
                let _ = error;
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            Err(error) => return Err(error.into()),
        }
    }
}

async fn wait_for_listener(port: u16, processes: &mut ProcessGroup) -> Result<()> {
    let address = format!("127.0.0.1:{port}").parse()?;
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if TcpStream::connect_timeout(&address, Duration::from_millis(20)).is_ok() {
            return Ok(());
        }
        processes.ensure_running()?;
        if Instant::now() >= deadline {
            bail!("listener did not become ready on port {port}");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn reserve_ports(count: usize) -> Result<Vec<u16>> {
    let listeners: Vec<_> = (0..count)
        .map(|_| TcpListener::bind("127.0.0.1:0"))
        .collect::<std::io::Result<_>>()?;
    listeners
        .iter()
        .map(|listener| Ok(listener.local_addr()?.port()))
        .collect()
}

fn validate_config(config: &ExperimentConfig) -> Result<()> {
    ensure!(config.node_count >= 2, "node_count must be at least 2");
    if matches!(config.mode, ExperimentMode::Availability) {
        ensure!(
            config.availability_rounds >= 1,
            "availability_rounds must be at least 1"
        );
        ensure!(
            config.node_count >= 4,
            "availability mode requires at least 4 nodes (3 initial and 1 replacement)"
        );
        // Compare before subtracting: `--nodes 4 --availability-rounds 5`
        // must be rejected, not overflow.
        ensure!(
            config.node_count > config.availability_rounds as usize,
            "availability mode needs at least one initial member besides its replacements"
        );
        ensure!(
            config.desired_replication_factor == 3,
            "availability mode requires desired_replication_factor=3"
        );
        ensure!(
            config.write_ack_policy != WriteAckPolicy::AllReplicas,
            "availability mode cannot keep AllReplicas writable after one node fails"
        );
    }
    // Only the scaling mode reads this; the other modes derive their initial
    // membership from their own rules, so an explicit value would be silently
    // ignored.
    ensure!(
        config.initial_nodes.is_none() || matches!(config.mode, ExperimentMode::Scaling),
        "initial_nodes only applies to the scaling mode"
    );
    let initial_count = match config.mode {
        ExperimentMode::Correctness => config.node_count - 1,
        ExperimentMode::Performance | ExperimentMode::Latency => config.node_count,
        ExperimentMode::Availability => config.availability_initial_members(),
        ExperimentMode::Scaling => {
            config
                .initial_nodes
                .unwrap_or_else(|| match config.scaling_scenario {
                    ScalingScenario::Shrink | ScalingScenario::Control => config.node_count,
                    ScalingScenario::Grow | ScalingScenario::RoundTrip => config.node_count - 1,
                    ScalingScenario::Replace => config.node_count - 1,
                    ScalingScenario::Churn => config.node_count.saturating_sub(3),
                })
        }
    };
    ensure!(
        initial_count >= config.desired_replication_factor as usize,
        "initial membership must be at least the desired replication factor"
    );
    ensure!(
        config.minimum_admitted_copies > 0
            && config.minimum_admitted_copies <= config.desired_replication_factor,
        "minimum admitted copies must be between 1 and desired RF"
    );
    ensure!(
        config.minimum_healthy_followers < config.desired_replication_factor,
        "minimum healthy followers must be below desired RF"
    );
    ensure!(config.key_count > 0, "key_count must be positive");
    ensure!(
        usize::try_from(config.key_count).is_ok(),
        "key_count exceeds the platform address space"
    );
    if matches!(config.mode, ExperimentMode::Latency) {
        ensure!(
            !config.target_rates.is_empty(),
            "the latency mode needs at least one target rate"
        );
        ensure!(
            config.target_rates.iter().all(|rate| *rate > 0),
            "every target rate must be positive"
        );
        ensure!(
            config.duration_seconds > 0,
            "duration_seconds must be positive"
        );
        ensure!(config.max_in_flight > 0, "max_in_flight must be positive");
    }
    ensure!(config.concurrency > 0, "concurrency must be positive");
    ensure!(
        config.range_move_concurrency > 0 && config.replica_repair_concurrency > 0,
        "range_move_concurrency and replica_repair_concurrency must be positive"
    );
    ensure!(config.value_bytes > 0, "value_bytes must be positive");
    ensure!(
        config.value_bytes <= config.node.max_value_bytes,
        "value_bytes exceeds the data-node limit"
    );
    ensure!(config.virtual_nodes > 0, "virtual_nodes must be positive");
    ensure!(
        config.operation_timeout_ms > 0 && config.migration_timeout_ms > 0,
        "timeouts must be positive"
    );
    ensure!(
        config.operation_timeout_ms <= config.receipt_retention_ms,
        "operation_timeout_ms exceeds the {} ms receipt retention period, which caps every \
         mutation's retries",
        config.receipt_retention_ms
    );
    if matches!(config.mode, ExperimentMode::Scaling) {
        ensure!(
            initial_count <= config.node_count,
            "initial_nodes exceeds the peak node count"
        );
        match config.scaling_scenario {
            ScalingScenario::Grow => ensure!(
                initial_count < config.node_count,
                "a grow run needs fewer initial nodes than the peak"
            ),
            ScalingScenario::Shrink => {
                ensure!(
                    initial_count == config.node_count,
                    "a shrink run starts with every peak node"
                );
                ensure!(
                    config.node_count > config.desired_replication_factor as usize,
                    "a shrink run needs more nodes than the desired replication factor"
                );
            }
            ScalingScenario::RoundTrip => ensure!(
                initial_count + 1 == config.node_count,
                "a round trip starts with one fewer node than the peak"
            ),
            ScalingScenario::Replace => ensure!(
                initial_count + 1 == config.node_count,
                "a replace run starts with one fewer node than the peak so one spare member can \
                 replace an initial member"
            ),
            ScalingScenario::Churn => ensure!(
                initial_count + 2 <= config.node_count,
                "a churn run needs at least two more peak nodes than its initial membership"
            ),
            ScalingScenario::Control => ensure!(
                initial_count == config.node_count,
                "a control run starts with every peak node and changes no membership"
            ),
        }
        if config.scaling_load_mode == ScalingLoadMode::Open {
            ensure!(
                config.scaling_open_rate > 0,
                "an open-loop scaling run needs a positive scaling_open_rate"
            );
            ensure!(
                config.scaling_open_rate * 3 / 100 > 0 && config.scaling_open_rate * 12 / 100 > 0,
                "an open-loop scaling run needs a rate high enough to offer every write cohort"
            );
            ensure!(
                config.max_in_flight > 0,
                "an open-loop scaling run needs a positive max_in_flight"
            );
        } else {
            ensure!(
                config.scaling_open_rate == 0,
                "scaling_open_rate only applies to an open-loop scaling run"
            );
            // Only the closed loop paces writes with this rate; an open-loop
            // run takes its write rates from scaling_open_rate.
            ensure!(
                config.scaling_write_rate > 0,
                "scaling_write_rate must be positive"
            );
        }
        ensure!(
            config.scaling_pool_seconds > 0,
            "scaling_pool_seconds must be positive"
        );
        ensure!(
            config.scaling_post_seconds > 0,
            "scaling_post_seconds must be positive"
        );
    }
    Ok(())
}

fn prepare_output_directory(path: &Path) -> Result<()> {
    if path.exists() {
        ensure!(path.is_dir(), "output path is not a directory");
        ensure!(
            fs::read_dir(path)?.next().is_none(),
            "output directory must be empty"
        );
    } else {
        fs::create_dir_all(path)?;
    }
    Ok(())
}

fn write_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let file = File::create(path)?;
    // The manifest inlines a multi-megabyte `git diff`. Serializing pretty JSON
    // emits a few bytes at a time, so an unbuffered handle turns a single write
    // into tens of thousands of `write` syscalls plus the ext4 metadata churn
    // that follows them.
    let mut writer = BufWriter::new(file);
    serde_json::to_writer_pretty(&mut writer, value)?;
    // Surface a deferred write error instead of losing it in `Drop`.
    writer.flush()?;
    Ok(())
}

fn command_output(cwd: Option<&Path>, program: &str, arguments: &[&str]) -> Option<String> {
    let mut command = Command::new(program);
    if let Some(cwd) = cwd {
        command.current_dir(cwd);
    }
    let output = command.args(arguments).output().ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

fn source_is_reproducible(
    build_git_dirty: &str,
    build_git_commit: &str,
    build_source_tree_blake3: &str,
    runtime_git_status: Option<&str>,
    runtime_git_commit: Option<&str>,
    runtime_source_tree_blake3: Option<&str>,
) -> bool {
    build_git_dirty == "false"
        && runtime_git_status == Some("")
        && runtime_git_commit == Some(build_git_commit)
        && runtime_source_tree_blake3 == Some(build_source_tree_blake3)
}

fn file_digest(path: &Path) -> Result<String> {
    let mut file = File::open(path)?;
    let mut hasher = blake3::Hasher::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hasher.finalize().to_hex().to_string())
}

fn source_tree_digest(workspace: &Path) -> Result<String> {
    let mut files = Vec::new();
    for path in [
        "Cargo.toml",
        "Cargo.lock",
        "PLAN.md",
        "README.md",
        "src",
        "crates",
        "tests",
    ] {
        collect_files(&workspace.join(path), &mut files)?;
    }
    files.sort();
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"hashring-rs:source-tree:v1\0");
    for path in files {
        let relative = path.strip_prefix(workspace).unwrap_or(&path);
        let encoded = relative.to_string_lossy();
        let contents = fs::read(&path)?;
        hasher.update(&(encoded.len() as u64).to_be_bytes());
        hasher.update(encoded.as_bytes());
        hasher.update(&(contents.len() as u64).to_be_bytes());
        hasher.update(&contents);
    }
    Ok(hasher.finalize().to_hex().to_string())
}

fn collect_files(path: &Path, files: &mut Vec<PathBuf>) -> Result<()> {
    if path.is_file() {
        files.push(path.to_owned());
    } else if path.is_dir() {
        for entry in fs::read_dir(path)? {
            collect_files(&entry?.path(), files)?;
        }
    }
    Ok(())
}

fn unix_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A scaling configuration that passes validation, so a test can change
    /// one setting and see what the validation says about it.
    fn scaling_config() -> ExperimentConfig {
        ExperimentConfig {
            mode: ExperimentMode::Scaling,
            output_dir: PathBuf::from("out"),
            node_count: 4,
            key_count: 8_000,
            value_bytes: 128,
            concurrency: 8,
            hash_seed: 1,
            virtual_nodes: 16,
            operation_timeout_ms: 30_000,
            migration_timeout_ms: 120_000,
            range_move_concurrency: 16,
            replica_repair_concurrency: 4,
            prepublication_failure_grace_ms: 30_000,
            pre_publish_delay_ms: 0,
            require_clean_source: false,
            verbose: false,
            worker_threads: 2,
            coordinator_worker_threads: None,
            desired_replication_factor: 2,
            minimum_admitted_copies: 1,
            minimum_healthy_followers: 0,
            max_replica_lag_ms: 1_000,
            receipt_retention_ms: 30_000,
            lease_duration_ms: 3_000,
            write_ack_policy: WriteAckPolicy::OwnerOnly,
            preload_keys: None,
            target_rates: Vec::new(),
            warmup_seconds: 0,
            duration_seconds: 0,
            mix: String::new(),
            key_distribution: "zipf:0.99".to_owned(),
            max_in_flight: 256,
            availability_rounds: 1,
            scaling_scenario: ScalingScenario::Grow,
            scaling_load_mode: ScalingLoadMode::Closed,
            scaling_open_rate: 0,
            initial_nodes: Some(3),
            scaling_write_rate: 200,
            scaling_pool_seconds: 8,
            scaling_post_seconds: 2,
            node: NodeOptions {
                max_dedup_bytes: 32 * 1024 * 1024,
                max_value_bytes: 1024 * 1024,
                max_pending_replication_bytes: 64 * 1024 * 1024,
                max_migration_journal_bytes: 16 * 1024 * 1024,
                replication_stream_queue_capacity: 1_024,
                required_ack_timeout_ms: 2_000,
                replication_rpc_timeout_ms: 5_000,
                replication_batch_linger_ms: 20,
            },
        }
    }

    #[test]
    fn an_open_loop_run_does_not_need_the_closed_loop_write_rate() {
        let mut config = scaling_config();
        config.scaling_load_mode = ScalingLoadMode::Open;
        config.scaling_open_rate = 1_000;
        // The open-loop run takes its write rates from its open rate, so the
        // closed-loop rate is not used and may be zero.
        config.scaling_write_rate = 0;
        validate_config(&config).unwrap();

        // The closed-loop run paces its writes with that rate, so it cannot be
        // zero there.
        let mut config = scaling_config();
        config.scaling_write_rate = 0;
        let error = validate_config(&config).unwrap_err().to_string();
        assert!(error.contains("scaling_write_rate"), "{error}");
    }

    #[test]
    fn the_tail_value_never_exceeds_the_node_limit() {
        assert_eq!(tail_value_bytes(1024 * 1024, 128), 1024 * 1024);
        // A limit below the tail's usual size is honoured, and the limit
        // itself is not a floor that panics.
        assert_eq!(tail_value_bytes(512 * 1024, 128), 512 * 1024);
        assert_eq!(tail_value_bytes(4_096, 128), 4_096);
        // The value size the run writes is the floor.
        assert_eq!(tail_value_bytes(4_096, 512), 4_096);
        for max_value_bytes in [1, 64, 4_096, 512 * 1024, 1024 * 1024] {
            for value_bytes in [1, 64, 128] {
                if value_bytes > max_value_bytes {
                    continue;
                }
                assert!(
                    tail_value_bytes(max_value_bytes, value_bytes) <= max_value_bytes,
                    "{max_value_bytes} {value_bytes}"
                );
            }
        }
    }

    #[test]
    fn load_balance_preserves_missing_cpu_and_rejects_missing_or_reset_counters() {
        let owned = BTreeMap::from([("node-1".to_owned(), 10)]);
        let before = BTreeMap::from([(
            "node-1".to_owned(),
            NodeCounters {
                ops_handled: 20,
                cpu_seconds: Some(1.0),
            },
        )]);
        let mut after = BTreeMap::from([(
            "node-1".to_owned(),
            NodeCounters {
                ops_handled: 40,
                cpu_seconds: None,
            },
        )]);
        let result = load_balance(&owned, &before, &after, 2.0).unwrap();
        assert_eq!(result.per_node["node-1"].ops_handled, 20);
        assert!(
            serde_json::to_value(result).unwrap()["per_node"]["node-1"]["cpu_percent"].is_null()
        );
        after.get_mut("node-1").unwrap().cpu_seconds = Some(2.0);
        assert_eq!(
            load_balance(&owned, &before, &after, 2.0).unwrap().per_node["node-1"].cpu_percent,
            Some(50.0)
        );
        after.get_mut("node-1").unwrap().ops_handled = 10;
        assert!(load_balance(&owned, &before, &after, 2.0).is_err());
        assert!(load_balance(&owned, &BTreeMap::new(), &after, 2.0).is_err());
    }

    #[test]
    fn key_balance_reports_the_spread_of_the_shares() {
        let balance = KeyBalance::of(&[10.0, 10.0, 10.0]);
        assert_eq!(balance.nodes, 3);
        assert_eq!(balance.max_over_min, 1.0);
        assert_eq!(balance.range_over_mean, 0.0);
        assert_eq!(balance.max_over_mean_minus_one, 0.0);
        assert_eq!(balance.cv, 0.0);

        let balance = KeyBalance::of(&[10.0, 20.0, 30.0]);
        assert_eq!(balance.max_over_min, 3.0);
        assert_eq!(balance.range_over_mean, 1.0);
        assert_eq!(balance.max_over_mean_minus_one, 0.5);
        assert!((balance.cv - 0.408248).abs() < 1e-6);

        // A set with no shares has no spread.
        let balance = KeyBalance::of(&[]);
        assert_eq!(balance.nodes, 0);
        assert_eq!(balance.max_over_min, 0.0);

        // A node with no keys makes the ratio unbounded, not balanced.
        let balance = KeyBalance::of(&[0.0, 10.0, 20.0]);
        assert!(balance.max_over_min.is_infinite());
        assert_eq!(
            serde_json::to_value(balance).unwrap()["max_over_min"],
            Value::Null
        );
    }

    #[tokio::test]
    async fn node_operations_sampling_propagates_connection_failure() {
        assert!(query_node_info("http://127.0.0.1:0").await.is_err());
    }

    #[test]
    fn deterministic_values_include_key_round_and_requested_length() {
        assert_eq!(deterministic_value(7, 3, 13).len(), 13);
        assert_eq!(deterministic_value(7, 3, 13), deterministic_value(7, 3, 13));
        assert_ne!(deterministic_value(7, 3, 13), deterministic_value(8, 3, 13));
        assert_ne!(deterministic_value(7, 3, 13), deterministic_value(7, 4, 13));
    }

    #[test]
    fn failure_summary_preserves_completed_measurements_on_disk() {
        let mut measurements = BTreeMap::new();
        measurements.insert(
            "initial_put".into(),
            measurement(10, Duration::from_secs(2)),
        );
        let summary = summarize(
            Duration::from_secs(3),
            Err(anyhow::anyhow!("injected failure")),
            measurements,
            BTreeMap::new(),
        );
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("summary.json");
        write_json(&path, &summary).unwrap();
        let restored: ExperimentSummary = serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
        assert!(!restored.success);
        assert!(restored.error.unwrap().contains("injected failure"));
        match &restored.measurements["initial_put"] {
            Measurement::Throughput(measurement) => assert_eq!(measurement.operations, 10),
            other => panic!("expected a throughput measurement, got {other:?}"),
        }
    }

    #[test]
    fn reproducibility_requires_every_clean_matching_source_condition() {
        let reproducible = |build_dirty, runtime_status, runtime_commit, runtime_tree| {
            source_is_reproducible(
                build_dirty,
                "commit-a",
                "tree-a",
                runtime_status,
                runtime_commit,
                runtime_tree,
            )
        };

        assert!(reproducible(
            "false",
            Some(""),
            Some("commit-a"),
            Some("tree-a")
        ));
        assert!(!reproducible(
            "true",
            Some(""),
            Some("commit-a"),
            Some("tree-a")
        ));
        assert!(!reproducible(
            "false",
            Some(" M src/lib.rs"),
            Some("commit-a"),
            Some("tree-a")
        ));
        assert!(!reproducible(
            "false",
            None,
            Some("commit-a"),
            Some("tree-a")
        ));
        assert!(!reproducible(
            "false",
            Some(""),
            Some("commit-b"),
            Some("tree-a")
        ));
        assert!(!reproducible("false", Some(""), None, Some("tree-a")));
        assert!(!reproducible(
            "false",
            Some(""),
            Some("commit-a"),
            Some("tree-b")
        ));
        assert!(!reproducible("false", Some(""), Some("commit-a"), None));
    }
}
