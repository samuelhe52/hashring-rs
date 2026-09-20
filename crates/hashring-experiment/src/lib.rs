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
    proto::ErrorCode,
    topology::Member,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ExperimentMode {
    Correctness,
    Performance,
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
    pub pre_publish_delay_ms: u64,
    pub require_clean_source: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ExperimentSummary {
    pub success: bool,
    pub error: Option<String>,
    pub elapsed_seconds: f64,
    pub final_epoch: Option<u64>,
    pub measurements: BTreeMap<String, Measurement>,
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
    processes: Vec<ManagedProcess>,
}

impl ProcessGroup {
    fn new(executable: PathBuf, log_dir: PathBuf) -> Self {
        Self {
            executable,
            log_dir,
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
            .env("RUST_LOG", "hashring_rs=info")
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
        schema_version: 3,
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
    let result = run_cluster(&config, executable, log_dir, &events, &mut measurements).await;
    let summary = summarize(started.elapsed(), result, measurements);
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
) -> ExperimentSummary {
    match result {
        Ok(final_epoch) => ExperimentSummary {
            success: true,
            error: None,
            elapsed_seconds: elapsed.as_secs_f64(),
            final_epoch: Some(final_epoch),
            measurements,
        },
        Err(error) => ExperimentSummary {
            success: false,
            error: Some(format!("{error:#}")),
            elapsed_seconds: elapsed.as_secs_f64(),
            final_epoch: None,
            measurements,
        },
    }
}

async fn run_cluster(
    config: &ExperimentConfig,
    executable: PathBuf,
    log_dir: PathBuf,
    events: &EventLog,
    measurements: &mut BTreeMap<String, Measurement>,
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
    };
    let initial_members = &members[..initial_count];
    let mut processes = ProcessGroup::new(executable, log_dir);
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
        "--migration-timeout-ms".into(),
        config.migration_timeout_ms.to_string(),
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
        let status = processes
            .wait_for_successful_exit(&added.node_id, Duration::from_secs(10))
            .await?;
        events.record(
            "removed_node_exited",
            json!({ "node_id": added.node_id, "status": status }),
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

async fn migrate_while_mutating(
    workload_client: &HashringClient,
    admin_client: &HashringClient,
    change: TopologyChange,
    workload: MigrationWorkload,
    events: &EventLog,
) -> Result<Duration> {
    let label = workload.label;
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
    let active_phase = wait_for_migration_activity(admin_client).await?;
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
            "initial_phase": format!("{active_phase:?}"),
            "observed_phase": format!("{overlap_phase:?}"),
            "completed_mutations_while_active": overlap_mutations,
            "untouched_moving_sentinels": workload.sentinel_count,
        }),
    )?;
    let mutations = writer.await.context("joining concurrent mutator")??;
    let completed = execution.await.context("joining migration execution")??;
    ensure!(
        completed.phase == MigrationPhase::Complete,
        "migration ended in {:?}",
        completed.phase
    );
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
    let mut moving = Vec::new();
    for key in 0..key_count {
        let encoded = key.to_be_bytes();
        let token = change.target_topology.key_token(&encoded);
        if change
            .ranges
            .iter()
            .any(|range| range_contains(range.start_exclusive, range.end_inclusive, token))
        {
            moving.push(key);
        }
    }
    moving
}

fn range_contains(start_exclusive: u64, end_inclusive: u64, token: u64) -> bool {
    match start_exclusive.cmp(&end_inclusive) {
        std::cmp::Ordering::Less => token > start_exclusive && token <= end_inclusive,
        std::cmp::Ordering::Greater => token > start_exclusive || token <= end_inclusive,
        std::cmp::Ordering::Equal => true,
    }
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
    ensure!(config.key_count > 0, "key_count must be positive");
    ensure!(
        usize::try_from(config.key_count).is_ok(),
        "key_count exceeds the platform address space"
    );
    ensure!(config.concurrency > 0, "concurrency must be positive");
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
