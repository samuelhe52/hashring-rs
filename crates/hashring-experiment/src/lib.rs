use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering as AtomicOrdering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail, ensure};
use futures::{StreamExt, TryStreamExt, stream};
use hashring_client::{ClientError, HashringClient};
use hashring_core::{
    limits::DEFAULT_MAX_VALUE_BYTES,
    migration::{MigrationPhase, TopologyChange},
    proto::{ErrorCode, PutRequest, data_node_client::DataNodeClient},
    topology::{Member, WriteAckPolicy},
    transport::configure_data_node_client,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tonic::transport::Channel;

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ExperimentMode {
    Correctness,
    Performance,
    Availability,
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
    pub pre_publish_delay_ms: u64,
    pub require_clean_source: bool,
    pub verbose: bool,
    pub desired_replication_factor: u32,
    pub minimum_admitted_copies: u32,
    pub minimum_healthy_followers: u32,
    pub write_ack_policy: WriteAckPolicy,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ExperimentSummary {
    pub success: bool,
    pub error: Option<String>,
    pub elapsed_seconds: f64,
    pub final_epoch: Option<u64>,
    pub measurements: BTreeMap<String, Measurement>,
    #[serde(default)]
    pub observations: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Measurement {
    pub operations: u64,
    pub seconds: f64,
    pub operations_per_second: f64,
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
    processes: Vec<ManagedProcess>,
}

impl ProcessGroup {
    fn new(executable: PathBuf, log_dir: PathBuf, verbose: bool) -> Self {
        Self {
            executable,
            log_dir,
            verbose,
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
        schema_version: 4,
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
        ExperimentMode::Performance => config.node_count,
        ExperimentMode::Availability => 3,
    };
    let initial_members = &members[..initial_count];
    let mut processes = ProcessGroup::new(executable, log_dir, config.verbose);
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
    ];
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
    }

    let workload = Workload {
        key_count: config.key_count,
        value_bytes: config.value_bytes,
        concurrency: config.concurrency,
    };
    let mut expected_rounds = vec![Some(0_u64); config.key_count as usize];
    let put_started = Instant::now();
    write_dataset(&workload_client, workload, 0).await?;
    measurements.insert(
        "initial_put".into(),
        measurement(config.key_count, put_started.elapsed()),
    );
    events.record(
        "initial_put_complete",
        json!({ "keys": config.key_count, "seconds": put_started.elapsed().as_secs_f64() }),
    )?;

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
    observations.insert(
        "client_put_latency_us".into(),
        latency_summary(&mut latencies_us),
    );
    events.record(
        "client_put_latency_sampled",
        json!({
            "samples": sample_count, "policy": config.write_ack_policy,
            "latency_us": observations["client_put_latency_us"]
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
        "owner_rpc_put_latency_us".into(),
        latency_summary(&mut owner_rpc_latencies_us),
    );
    events.record(
        "owner_rpc_put_latency_sampled",
        json!({
            "samples": sample_count, "policy": config.write_ack_policy,
            "latency_us": observations["owner_rpc_put_latency_us"]
        }),
    )?;

    if matches!(config.mode, ExperimentMode::Availability) {
        return run_availability(
            config,
            &members,
            &ports,
            &coordinator_endpoint,
            &admin_client,
            &mut processes,
            events,
            measurements,
            observations,
        )
        .await;
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
    Ok(topology.epoch)
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

#[allow(clippy::too_many_arguments)]
async fn run_availability(
    config: &ExperimentConfig,
    members: &[Member],
    ports: &[u16],
    coordinator_endpoint: &str,
    admin_client: &HashringClient,
    processes: &mut ProcessGroup,
    events: &EventLog,
    measurements: &mut BTreeMap<String, Measurement>,
    observations: &mut BTreeMap<String, Value>,
) -> Result<u64> {
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

    // An identifiable, just-acknowledged tail makes OwnerOnly loss observable
    // when it occurs; zero loss in one run is not a durability guarantee.
    let tail_key = *owner_keys.last().expect("nonempty sample was checked");
    let tail_bytes = config
        .value_bytes
        .clamp(1024 * 1024, DEFAULT_MAX_VALUE_BYTES);
    let tail_value = deterministic_value(tail_key, 1, tail_bytes);
    let tail_put_started = Instant::now();
    let writer =
        HashringClient::connect(coordinator_endpoint.to_owned(), Duration::from_secs(10)).await?;
    writer
        .put(tail_key.to_be_bytes().to_vec(), tail_value.clone())
        .await?;
    let tail_put_seconds = tail_put_started.elapsed().as_secs_f64();
    let tail_ack_at = Instant::now();
    let failed_at = Instant::now();
    let (exit, kill_signalled_at) = processes.kill(&members[0].node_id)?;
    let tail_ack_to_kill_ms = kill_signalled_at.duration_since(tail_ack_at).as_millis();
    events.record(
        "owner_killed",
        json!({ "node_id": members[0].node_id, "exit": exit,
            "tail_key": tail_key, "tail_bytes": tail_bytes,
            "tail_put_seconds": tail_put_seconds,
            "tail_ack_to_kill_ms": tail_ack_to_kill_ms }),
    )?;
    let probe =
        HashringClient::connect(coordinator_endpoint.to_owned(), Duration::from_millis(250))
            .await?;
    let probe_key = owner_keys[0].to_be_bytes().to_vec();
    let failover_deadline = Instant::now() + Duration::from_secs(30);
    let mut first_unavailable: Option<Instant> = None;
    let mut longest_unavailable_ms = 0u128;
    let mut unavailable_probes = 0u64;
    let mut published_seconds = None;
    loop {
        processes.ensure_running()?;
        let available = matches!(
            probe.get(probe_key.clone()).await,
            Ok(_)
                | Err(ClientError::Operation(hashring_client::OperationFailure {
                    code: ErrorCode::NotFound,
                    ..
                }))
        );
        if available {
            if let Some(started) = first_unavailable.take() {
                longest_unavailable_ms = longest_unavailable_ms.max(started.elapsed().as_millis());
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
                .any(|member| member.node_id == members[0].node_id)
        {
            let seconds = failed_at.elapsed().as_secs_f64();
            published_seconds = Some(seconds);
            events.record(
                "failover_published",
                json!({ "seconds": seconds, "epoch": current.epoch }),
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
    observations.insert("failover_seconds".into(), json!(failover_seconds));
    observations.insert("read_recovery_seconds".into(), json!(read_recovery_seconds));
    observations.insert(
        "observed_unavailable_max_streak_ms".into(),
        json!(longest_unavailable_ms),
    );
    observations.insert("unavailable_probe_count".into(), json!(unavailable_probes));
    events.record(
        "failover_read_recovered",
        json!({
            "seconds": failover_seconds,
            "read_recovery_seconds": read_recovery_seconds,
            "observed_unavailable_max_streak_ms": longest_unavailable_ms,
            "unavailable_probe_count": unavailable_probes,
        }),
    )?;

    let reader =
        HashringClient::connect(coordinator_endpoint.to_owned(), Duration::from_secs(5)).await?;
    let mut lost = 0u64;
    let mut tail_survived = true;
    for key in &owner_keys {
        match reader.get(key.to_be_bytes().to_vec()).await {
            Ok(output) if *key == tail_key => {
                if output.value != tail_value {
                    ensure!(
                        output.value == deterministic_value(*key, 0, config.value_bytes),
                        "tail key {key} has a value other than the acknowledged or prior value"
                    );
                    lost += 1;
                    tail_survived = false;
                }
            }
            Ok(output) => ensure!(
                output.value == deterministic_value(*key, 0, config.value_bytes),
                "surviving key {key} has an unexpected value"
            ),
            Err(ClientError::Operation(failure)) if failure.code == ErrorCode::NotFound => {
                lost += 1;
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
    ensure!(
        config.write_ack_policy != WriteAckPolicy::FirstSuccessor || lost == 0,
        "FirstSuccessor lost {lost} acknowledged sampled keys"
    );
    observations.insert(
        "acknowledged_key_survival".into(),
        json!({
            "sampled": owner_keys.len(), "preserved": owner_keys.len() as u64 - lost,
            "lost": lost, "policy": config.write_ack_policy,
            "tail_key": tail_key, "tail_survived": tail_survived,
        }),
    );
    events.record(
        "acknowledged_key_survival_checked",
        observations["acknowledged_key_survival"].clone(),
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
    let replacement = &members[3];
    let target = vec![members[1].clone(), members[2].clone(), replacement.clone()];
    let change = admin_client.begin_topology_change(target).await?;
    let restore_started = Instant::now();
    spawn_node(
        processes,
        replacement.node_id.clone(),
        ports[4],
        coordinator_endpoint,
    )?;
    wait_for_listener(ports[4], processes).await?;
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
    observations.insert(
        "replacement_to_full_rf_seconds".into(),
        json!(restored_seconds),
    );
    observations.insert(
        "failure_to_full_rf_seconds".into(),
        json!(failed_at.elapsed().as_secs_f64()),
    );
    measurements.insert(
        "restore_full_rf".into(),
        measurement(1, restore_started.elapsed()),
    );
    events.record(
        "full_rf_restored",
        json!({ "epoch": completed.target_topology.epoch,
        "replacement_to_full_rf_seconds": restored_seconds }),
    )?;
    Ok(completed.target_topology.epoch)
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
                    writer.abort();
                    execution.abort();
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
        if writer.is_finished() && mutations == 0 {
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
    Measurement {
        operations,
        seconds,
        operations_per_second: operations as f64 / seconds.max(f64::EPSILON),
    }
}

fn spawn_node(
    processes: &mut ProcessGroup,
    node_id: String,
    port: u16,
    coordinator_endpoint: &str,
) -> Result<()> {
    processes.spawn(
        node_id.clone(),
        &[
            "node".into(),
            "--id".into(),
            node_id,
            "--listen".into(),
            format!("127.0.0.1:{port}"),
            "--coordinator".into(),
            coordinator_endpoint.into(),
        ],
    )
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
            config.node_count == 4,
            "availability mode requires exactly 4 nodes (3 initial and 1 replacement)"
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
    let initial_count = match config.mode {
        ExperimentMode::Correctness => config.node_count - 1,
        ExperimentMode::Performance => config.node_count,
        ExperimentMode::Availability => 3,
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
    ensure!(config.concurrency > 0, "concurrency must be positive");
    ensure!(
        config.range_move_concurrency > 0,
        "range_move_concurrency must be positive"
    );
    ensure!(config.value_bytes > 0, "value_bytes must be positive");
    ensure!(
        config.value_bytes <= DEFAULT_MAX_VALUE_BYTES,
        "value_bytes exceeds the data-node limit"
    );
    ensure!(config.virtual_nodes > 0, "virtual_nodes must be positive");
    ensure!(
        config.operation_timeout_ms > 0 && config.migration_timeout_ms > 0,
        "timeouts must be positive"
    );
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
    serde_json::to_writer_pretty(file, value)?;
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
        assert_eq!(restored.measurements["initial_put"].operations, 10);
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
