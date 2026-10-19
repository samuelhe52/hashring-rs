use std::{net::SocketAddr, num::NonZeroUsize, path::PathBuf, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand, ValueEnum};
use config::{
    coordinator::{CoordinatorSettings, MemberSource},
    node::NodeConfig,
};
use hashring_client::HashringClient;
use hashring_coordinator::{
    CoordinatorRepository, CoordinatorService, RedbTopologyRepository, load_or_initialize,
};
use hashring_core::{
    limits::MAX_CONTROL_MESSAGE_BYTES,
    proto::{coordinator_server::CoordinatorServer, data_node_server::DataNodeServer},
    topology::{
        ClusterTimings, Member, TopologyConfig, TopologySnapshot, WriteAckPolicy,
        WriteAvailabilityGuard,
    },
};
use hashring_experiment::{ExperimentConfig, ExperimentMode, run_experiment};
use hashring_node::DataNodeService;
use serde::Deserialize;
use tonic::transport::Server;
use tracing::info;
use tracing_subscriber::EnvFilter;

mod config;

/// How often the coordinator retries a topology change and drains follower
/// repairs. A registration interrupts the wait, so this is the ceiling between
/// repair passes rather than their period.
const RECOVERY_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Parser)]
#[command(version, about = "A process-distributed consistent-hash cache")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run the durable topology coordinator.
    Coordinator(CoordinatorArgs),
    /// Run an in-memory data node.
    Node(NodeArgs),
    /// Store one key/value pair.
    Put(PutArgs),
    /// Fetch one key.
    Get(GetArgs),
    /// Ensure one key is absent.
    Delete(DeleteArgs),
    /// Print the canonical topology as JSON.
    Topology(ClientArgs),
    /// Persist a pending target topology and its moving-range plan.
    BeginChange(ChangeArgs),
    /// Stage a committed-topology ACK policy transition.
    BeginPolicyChange(PolicyChangeArgs),
    /// Stage an RF or write-availability guard transition.
    BeginConfigChange(ConfigChangeArgs),
    /// Print the active topology change, if any.
    ChangeStatus(ClientArgs),
    /// Print per-range admission, liveness, write readiness, and repair state.
    ReplicaStatus(ClientArgs),
    /// Execute the active migration through publication and cleanup.
    ExecuteChange(ExecuteChangeArgs),
    /// Serve the Redis protocol on a port, over this cluster.
    RedisGateway(GatewayArgs),
    /// Run a reproducible separate-process correctness or performance experiment.
    Experiment(ExperimentArgs),
}

#[derive(Args)]
struct CoordinatorArgs {
    /// YAML file with coordinator settings. Keys use the flag names with `_`
    /// for `-`, except that `--member` becomes a `members` list of
    /// `{node_id, endpoint}`. A flag on the command line wins over the file.
    #[arg(long)]
    config: Option<PathBuf>,
    /// [default: 127.0.0.1:50050]
    #[arg(long)]
    listen: Option<SocketAddr>,
    /// [default: ./coordinator.redb]
    #[arg(long)]
    state: Option<PathBuf>,
    /// Used only when creating a new store. [default: 1]
    #[arg(long)]
    seed: Option<u64>,
    /// Used only when creating a new store. [default: 128]
    #[arg(long)]
    virtual_nodes: Option<u32>,
    /// Used only when creating a new store. [default: 3]
    #[arg(long)]
    desired_replication_factor: Option<u32>,
    /// Used only when creating a new store. [default: 1]
    #[arg(long)]
    minimum_admitted_copies: Option<u32>,
    /// Used only when creating a new store. [default: 0]
    #[arg(long)]
    minimum_healthy_followers: Option<u32>,
    /// Used only when creating a new store. [default: 5000]
    #[arg(long)]
    max_replica_lag_ms: Option<u64>,
    /// How long every replica keeps a deduplication receipt; mutations stop
    /// retrying within it. Used only when creating a new store, then fixed.
    /// [default: 60000]
    #[arg(long)]
    receipt_retention_ms: Option<u64>,
    /// Node lease duration; nodes renew at a fifth of it. Used only when
    /// creating a new store, then fixed. [default: 5000]
    #[arg(long)]
    lease_duration_ms: Option<u64>,
    /// [default: 120000]
    #[arg(long)]
    migration_timeout_ms: Option<u64>,
    /// Maximum number of disjoint ranges moved concurrently within one topology
    /// change. [default: 16]
    #[arg(long)]
    range_move_concurrency: Option<NonZeroUsize>,
    /// Follower groups seeded, merged, or repaired at once, and ranges copied
    /// at once within one group. [default: 4]
    #[arg(long)]
    replica_repair_concurrency: Option<NonZeroUsize>,
    /// How long a migration participant may stay unreachable before
    /// publication before the change is recovered without it. [default: 30000]
    #[arg(long)]
    prepublication_failure_grace_ms: Option<u64>,
    /// Tokio worker threads. [default: one per available CPU]
    #[arg(long)]
    worker_threads: Option<NonZeroUsize>,
    #[arg(long, default_value_t = 0, hide = true)]
    pre_publish_delay_ms: u64,
    #[arg(long, default_value_t = 0, hide = true)]
    post_publish_delay_ms: u64,
    /// Bootstrap member in NODE_ID=HTTP_ENDPOINT form. Used only when creating
    /// a new store, and must match the stored topology on restart. Replaces the
    /// `members` list of the config file.
    #[arg(long = "member")]
    members: Vec<String>,
}

#[derive(Args)]
struct NodeArgs {
    /// YAML file with node settings. Keys use the flag names with `_` for `-`.
    /// A flag on the command line wins over the file.
    #[arg(long)]
    config: Option<PathBuf>,
    /// Required here or as `id` in the config file.
    #[arg(long)]
    id: Option<String>,
    /// Required here or as `listen` in the config file.
    #[arg(long)]
    listen: Option<SocketAddr>,
    /// [default: http://127.0.0.1:50050]
    #[arg(long)]
    coordinator: Option<String>,
    /// Tokio worker threads. Defaults to one per CPU available to the process
    /// (affinity and cgroup limits included); set it lower when the node shares
    /// its host with other busy processes.
    #[arg(long)]
    worker_threads: Option<NonZeroUsize>,
    #[command(flatten)]
    tuning: NodeTuningArgs,
    #[arg(long, default_value_t = 0, hide = true)]
    stop_response_delay_ms: u64,
}

/// Per-node budgets and timeouts, shared by `node` and `experiment`. Nodes in
/// one cluster may use different values.
#[derive(Args, Clone, Debug, Default)]
struct NodeTuningArgs {
    /// Bytes of deduplication receipts a node retains, and separately the bytes
    /// each migration destination stages. A full store rejects writes before
    /// applying them, with a retryable error. [default: 134217728 (128 MiB)]
    #[arg(long)]
    max_dedup_bytes: Option<usize>,
    /// The largest value a node accepts. A larger one is refused before it is
    /// written. Every node in a cluster may use its own; the wire and a
    /// migration page are sized for the largest the cluster allows.
    /// Raising it also raises what a replication batch and a journaled write
    /// need, so raise `--max-pending-replication-bytes` and
    /// `--max-migration-journal-bytes` with it. [default: 8388608 (8 MiB)]
    #[arg(long)]
    max_value_bytes: Option<usize>,
    /// Bytes of mutations queued for followers, and separately the bytes held
    /// by in-flight replication RPCs. [default: 67108864 (64 MiB)]
    #[arg(long)]
    max_pending_replication_bytes: Option<usize>,
    /// Bytes of writes journaled across a node's active migrations. When it is
    /// full, writes to moving ranges get a retryable rejection.
    /// [default: 16777216 (16 MiB)]
    #[arg(long)]
    max_migration_journal_bytes: Option<usize>,
    /// Mutations queued per follower stream, whatever their size. [default: 32]
    #[arg(long)]
    replication_stream_queue_capacity: Option<NonZeroUsize>,
    /// How long a write waits for its required follower ACKs before reporting
    /// an unknown outcome. [default: 2000]
    #[arg(long)]
    required_ack_timeout_ms: Option<u64>,
    /// Bounds each replication connect and batch RPC. [default: 2000]
    #[arg(long)]
    replication_rpc_timeout_ms: Option<u64>,
    /// How long a follower stream waits to fill a replication batch when no
    /// write waits for its ACK. 0 sends at once. [default: 2]
    #[arg(long)]
    replication_batch_linger_ms: Option<u64>,
}

#[derive(Args, Clone)]
struct ClientArgs {
    #[arg(long, default_value = "http://127.0.0.1:50050")]
    coordinator: String,
    /// `put` and `delete` stop retrying after the cluster's receipt retention
    /// period (default 60 s) even when this is longer.
    #[arg(long, default_value_t = 2_000)]
    deadline_ms: u64,
}

#[derive(Args)]
struct PutArgs {
    #[command(flatten)]
    client: ClientArgs,
    #[arg(long, conflicts_with = "key_hex", required_unless_present = "key_hex")]
    key: Option<String>,
    #[arg(long, conflicts_with = "key", required_unless_present = "key")]
    key_hex: Option<String>,
    #[arg(
        long,
        conflicts_with = "value_hex",
        required_unless_present = "value_hex"
    )]
    value: Option<String>,
    #[arg(long, conflicts_with = "value", required_unless_present = "value")]
    value_hex: Option<String>,
}

#[derive(Args)]
struct GetArgs {
    #[command(flatten)]
    client: ClientArgs,
    #[arg(long, conflicts_with = "key_hex", required_unless_present = "key_hex")]
    key: Option<String>,
    #[arg(long, conflicts_with = "key", required_unless_present = "key")]
    key_hex: Option<String>,
    /// Render the value as hexadecimal bytes instead of UTF-8.
    #[arg(long)]
    hex: bool,
}

#[derive(Args)]
struct DeleteArgs {
    #[command(flatten)]
    client: ClientArgs,
    #[arg(long, conflicts_with = "key_hex", required_unless_present = "key_hex")]
    key: Option<String>,
    #[arg(long, conflicts_with = "key", required_unless_present = "key")]
    key_hex: Option<String>,
}

#[derive(Args)]
struct ChangeArgs {
    #[command(flatten)]
    client: ClientArgs,
    /// Complete target membership in NODE_ID=HTTP_ENDPOINT form.
    #[arg(long = "member", required = true)]
    members: Vec<String>,
}

#[derive(Clone, Copy, Debug, Deserialize, ValueEnum)]
#[serde(rename_all = "kebab-case")]
enum PolicyArg {
    OwnerOnly,
    FirstSuccessor,
    AllReplicas,
}

impl From<PolicyArg> for WriteAckPolicy {
    fn from(policy: PolicyArg) -> Self {
        match policy {
            PolicyArg::OwnerOnly => WriteAckPolicy::OwnerOnly,
            PolicyArg::FirstSuccessor => WriteAckPolicy::FirstSuccessor,
            PolicyArg::AllReplicas => WriteAckPolicy::AllReplicas,
        }
    }
}

#[derive(Args)]
struct PolicyChangeArgs {
    #[command(flatten)]
    client: ClientArgs,
    #[arg(long, value_enum)]
    policy: PolicyArg,
}

#[derive(Args)]
struct ConfigChangeArgs {
    #[command(flatten)]
    client: ClientArgs,
    #[arg(long)]
    desired_replication_factor: Option<u32>,
    #[arg(long)]
    minimum_admitted_copies: Option<u32>,
    #[arg(long)]
    minimum_healthy_followers: Option<u32>,
    #[arg(long)]
    max_replica_lag_ms: Option<u64>,
}

#[derive(Args)]
struct GatewayArgs {
    /// YAML file with gateway settings. Keys use the flag names with `_` for
    /// `-`. A flag on the command line wins over the file.
    #[arg(long)]
    config: Option<PathBuf>,
    /// [default: 127.0.0.1:6379]
    #[arg(long)]
    listen: Option<SocketAddr>,
    /// [default: http://127.0.0.1:50050]
    #[arg(long)]
    coordinator: Option<String>,
    /// How long one command may take against the cluster before it is reported
    /// as not finished. [default: 5000]
    #[arg(long)]
    deadline_ms: Option<u64>,
    /// Require this password in AUTH before a connection may read or write.
    /// Falls back to the HASHRING_GATEWAY_PASSWORD environment variable, so a
    /// secret need not appear in the process list.
    #[arg(long)]
    requirepass: Option<String>,
    /// Connections open at once before a new one is refused, as Redis's
    /// `maxclients`. [default: 10000]
    #[arg(long)]
    maxclients: Option<NonZeroUsize>,
    /// Largest bulk string a connection may send, which is the largest value
    /// the gateway reads from a client. Raise it alongside the cluster's
    /// `--max-value-bytes` when that is raised. [default: 16777216 (16 MiB)]
    #[arg(long)]
    max_bulk_bytes: Option<usize>,
    /// Tokio worker threads. Defaults to one per CPU available to the process
    /// (affinity and cgroup limits included).
    #[arg(long)]
    worker_threads: Option<NonZeroUsize>,
}

#[derive(Args, Clone)]
struct ExecuteChangeArgs {
    #[arg(long, default_value = "http://127.0.0.1:50050")]
    coordinator: String,
    #[arg(long, default_value_t = 120_000)]
    deadline_ms: u64,
    #[arg(long)]
    change_id: String,
    #[arg(long)]
    base_epoch: u64,
    #[arg(long)]
    target_epoch: u64,
}

#[derive(Clone, Copy, Debug, Deserialize, ValueEnum)]
#[serde(rename_all = "kebab-case")]
enum ExperimentModeArg {
    Correctness,
    Performance,
    Availability,
}

impl From<ExperimentModeArg> for ExperimentMode {
    fn from(mode: ExperimentModeArg) -> Self {
        match mode {
            ExperimentModeArg::Correctness => ExperimentMode::Correctness,
            ExperimentModeArg::Performance => ExperimentMode::Performance,
            ExperimentModeArg::Availability => ExperimentMode::Availability,
        }
    }
}

#[derive(Args)]
struct ExperimentArgs {
    /// YAML file with experiment settings. Keys use the flag names with `_`
    /// for `-`, and values match the flag values. A flag on the command line
    /// wins over the file; a boolean flag can only turn its setting on.
    #[arg(long)]
    config: Option<PathBuf>,
    /// [default: correctness]
    #[arg(long, value_enum)]
    mode: Option<ExperimentModeArg>,
    /// Empty or not-yet-created directory for the manifest, raw logs, and
    /// summary. Required here or as `output` in the config file, where a
    /// relative path is resolved against the file's directory.
    #[arg(long)]
    output: Option<PathBuf>,
    /// Peak node count. Correctness mode starts with one fewer node, then
    /// scales out and in. [default: 4 for availability, 10 otherwise]
    #[arg(long)]
    nodes: Option<usize>,
    /// Logical keys. [default: 20,000 for correctness, 1,000,000 for
    /// performance, 1,000 for availability]
    #[arg(long)]
    keys: Option<u64>,
    /// [default: 128]
    #[arg(long)]
    value_bytes: Option<usize>,
    /// [default: 64]
    #[arg(long)]
    concurrency: Option<usize>,
    /// [default: 1]
    #[arg(long)]
    seed: Option<u64>,
    /// [default: 128]
    #[arg(long)]
    virtual_nodes: Option<u32>,
    /// At most --receipt-retention-ms, which caps mutation retries.
    /// [default: 30000]
    #[arg(long)]
    operation_timeout_ms: Option<u64>,
    /// [default: 600000]
    #[arg(long)]
    migration_timeout_ms: Option<u64>,
    /// Maximum number of disjoint ranges moved concurrently within one
    /// topology change. [default: 16]
    #[arg(long)]
    range_move_concurrency: Option<usize>,
    /// Follower groups seeded, merged, or repaired at once. [default: 4]
    #[arg(long)]
    replica_repair_concurrency: Option<NonZeroUsize>,
    /// [default: 30000]
    #[arg(long)]
    prepublication_failure_grace_ms: Option<u64>,
    /// Deterministic pre-publication observation window for migration stress
    /// runs. [default: 250]
    #[arg(long)]
    pre_publish_delay_ms: Option<u64>,
    /// Refuse the run unless build and runtime source match the same clean commit.
    #[arg(long)]
    require_clean_source: bool,
    /// Log client retries and migration/repair progress for diagnosis.
    #[arg(long)]
    verbose: bool,
    /// [default: 3]
    #[arg(long)]
    desired_replication_factor: Option<u32>,
    /// [default: 1]
    #[arg(long)]
    minimum_admitted_copies: Option<u32>,
    /// [default: 0]
    #[arg(long)]
    minimum_healthy_followers: Option<u32>,
    /// [default: 5000]
    #[arg(long)]
    max_replica_lag_ms: Option<u64>,
    /// [default: 60000]
    #[arg(long)]
    receipt_retention_ms: Option<u64>,
    /// [default: 5000]
    #[arg(long)]
    lease_duration_ms: Option<u64>,
    /// [default: first-successor for availability, owner-only otherwise]
    #[arg(long, value_enum)]
    policy: Option<PolicyArg>,
    /// Tokio worker threads for the driver and for each data node it starts.
    /// [default: the CPU count divided by peak nodes + 2, at least 2]
    #[arg(long)]
    worker_threads: Option<NonZeroUsize>,
    /// Tokio worker threads for the run's coordinator. [default: one per CPU]
    #[arg(long)]
    coordinator_worker_threads: Option<NonZeroUsize>,
    /// Forwarded to every data node the run starts.
    #[command(flatten)]
    node_tuning: NodeTuningArgs,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let cpus = std::thread::available_parallelism().map_or(1, NonZeroUsize::get);
    // A long-running command's config file can set its runtime size, so its
    // settings are resolved before the runtime is built.
    match &cli.command {
        Command::Experiment(args) => {
            let file = args.config.as_deref().map(config::load).transpose()?;
            let config = config::experiment::resolve(args, file, cpus)?;
            init_tracing(config.verbose)?;
            hashring_core::rpc_profile::start_reporter("driver");
            return build_runtime(Some(config.worker_threads))?
                .block_on(run_local_experiment(config));
        }
        Command::Node(args) => {
            let file = args.config.as_deref().map(config::load).transpose()?;
            let config = config::node::resolve(args, file)?;
            init_tracing(false)?;
            hashring_core::rpc_profile::start_reporter("node");
            return build_runtime(Some(config.runtime_workers(cpus)))?
                .block_on(run_node(config, args.stop_response_delay_ms));
        }
        Command::Coordinator(args) => {
            let file = args.config.as_deref().map(config::load).transpose()?;
            let settings = config::coordinator::resolve(args, file)?;
            init_tracing(false)?;
            return build_runtime(Some(settings.runtime_workers(cpus)))?.block_on(run_coordinator(
                settings,
                args.pre_publish_delay_ms,
                args.post_publish_delay_ms,
            ));
        }
        Command::RedisGateway(args) => {
            let file = args.config.as_deref().map(config::load).transpose()?;
            let settings = config::gateway::resolve(args, file)?;
            init_tracing(false)?;
            return build_runtime(Some(settings.runtime_workers(cpus)))?
                .block_on(run_redis_gateway(settings));
        }
        _ => {}
    }
    init_tracing(false)?;
    // One-shot client and admin commands issue a few RPCs and need no worker pool.
    build_runtime(None)?.block_on(run(cli.command))
}

/// Built explicitly rather than via #[tokio::main] so each role can size its
/// own runtime; see docs/optimizations/worker-pool-sizing.md. Servers default
/// to one worker per available CPU because in a deployment they own their
/// machine. `None` builds a single-threaded runtime.
fn build_runtime(worker_threads: Option<usize>) -> Result<tokio::runtime::Runtime> {
    let mut builder = match worker_threads {
        Some(workers) => {
            let mut builder = tokio::runtime::Builder::new_multi_thread();
            builder.worker_threads(workers);
            builder
        }
        None => tokio::runtime::Builder::new_current_thread(),
    };
    Ok(builder.enable_all().build()?)
}

fn init_tracing(verbose_experiment: bool) -> Result<()> {
    let mut filter = EnvFilter::from_default_env().add_directive("hashring_rs=info".parse()?);
    if verbose_experiment {
        filter = filter.add_directive("hashring_client=debug".parse()?);
    }
    tracing_subscriber::fmt().with_env_filter(filter).init();
    Ok(())
}

async fn run(command: Command) -> Result<()> {
    match command {
        Command::Put(args) => run_put(args).await,
        Command::Get(args) => run_get(args).await,
        Command::Delete(args) => run_delete(args).await,
        Command::Topology(args) => run_topology(args).await,
        Command::BeginChange(args) => run_begin_change(args).await,
        Command::BeginPolicyChange(args) => run_begin_policy_change(args).await,
        Command::BeginConfigChange(args) => run_begin_config_change(args).await,
        Command::ChangeStatus(args) => run_change_status(args).await,
        Command::ReplicaStatus(args) => run_replica_status(args).await,
        Command::ExecuteChange(args) => run_execute_change(args).await,
        Command::Coordinator(_)
        | Command::Node(_)
        | Command::Experiment(_)
        | Command::RedisGateway(_) => {
            unreachable!("main runs the long-running commands")
        }
    }
}

/// Serves the Redis protocol until the process is asked to stop.
///
/// The coordinator is contacted when the first command arrives rather than
/// before the port opens, so the gateway may be started before, with, or
/// instead of the coordinator: clients get `-TRYAGAIN` until it answers.
async fn run_redis_gateway(settings: config::gateway::GatewaySettings) -> Result<()> {
    let (shutdown, mut stopped) = tokio::sync::watch::channel(false);
    tokio::spawn(async move {
        shutdown_signal().await;
        let _ = shutdown.send(true);
    });
    let config = hashring_redis::GatewayConfig {
        coordinator: settings.coordinator,
        listen: settings.listen,
        operation_timeout: settings.operation_timeout,
        password: settings.password,
        max_connections: settings.max_connections.get(),
        limits: hashring_redis::resp::Limits {
            max_bulk_bytes: settings.max_bulk_bytes,
            ..Default::default()
        },
    };
    hashring_redis::serve(config, stopped.clone()).await?;
    stopped.changed().await.ok();
    Ok(())
}

async fn run_local_experiment(config: ExperimentConfig) -> Result<()> {
    let summary = run_experiment(config, std::env::current_exe()?).await?;
    println!("{}", serde_json::to_string_pretty(&summary)?);
    Ok(())
}

async fn run_coordinator(
    settings: CoordinatorSettings,
    pre_publish_delay_ms: u64,
    post_publish_delay_ms: u64,
) -> Result<()> {
    let repository = Arc::new(
        RedbTopologyRepository::open(&settings.state)
            .with_context(|| format!("opening {}", settings.state.display()))?,
    );
    let bootstrap = coordinator_bootstrap(&settings, repository.as_ref())?;
    let state = load_or_initialize(repository.as_ref(), bootstrap)?;
    info!(
        listen = %settings.listen,
        epoch = state.committed.epoch,
        digest = %state.committed.digest,
        members = state.committed.members.len(),
        active_change = state.active_change.is_some(),
        range_move_concurrency = settings.range_move_concurrency.get(),
        replica_repair_concurrency = settings.replica_repair_concurrency.get(),
        "coordinator ready"
    );
    let service = CoordinatorService::new(
        state,
        repository,
        Duration::from_millis(settings.migration_timeout_ms),
    )
    .with_range_move_concurrency(settings.range_move_concurrency.get())
    .with_replica_repair_concurrency(settings.replica_repair_concurrency.get())
    .with_prepublication_failure_grace(Duration::from_millis(
        settings.prepublication_failure_grace_ms,
    ))
    .with_pre_publish_delay(Duration::from_millis(pre_publish_delay_ms))
    .with_post_publish_delay(Duration::from_millis(post_publish_delay_ms));
    let recovery_service = service.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(RECOVERY_INTERVAL);
        loop {
            // A registration can make a repair pass runnable that the last tick
            // could not start, so wake on whichever arrives first. Without this
            // a cluster booting with an empty ring converges only on a tick,
            // which is a whole interval of doing nothing after the last node
            // has already registered.
            tokio::select! {
                _ = interval.tick() => {}
                _ = recovery_service.wait_for_repair_wake() => {}
            }
            match recovery_service.resume_interrupted_change().await {
                Ok(Some(change)) => {
                    tracing::info!(change_id = %change.change_id, phase = ?change.phase, "resumed topology change");
                }
                Ok(None) => {}
                Err(error) => {
                    tracing::warn!(%error, "topology change recovery remains pending");
                }
            }
            if let Err(error) = recovery_service.resume_replica_repairs().await {
                tracing::warn!(%error, "replica repair pass failed");
            }
        }
    });
    let failure_service = service.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        loop {
            interval.tick().await;
            if let Err(error) = failure_service.run_failure_pass().await {
                tracing::warn!(%error, "automatic failure transition is pending");
            }
        }
    });
    Server::builder()
        .add_service(
            CoordinatorServer::new(service)
                .max_decoding_message_size(MAX_CONTROL_MESSAGE_BYTES)
                .max_encoding_message_size(MAX_CONTROL_MESSAGE_BYTES),
        )
        .serve_with_shutdown(settings.listen, shutdown_signal())
        .await?;
    Ok(())
}

/// Builds the epoch-1 topology for a new store. Members from a config file are
/// skipped when the store already exists, so one file serves both the first
/// start and every restart; `--member` flags still must match the store.
fn coordinator_bootstrap(
    settings: &CoordinatorSettings,
    repository: &impl CoordinatorRepository,
) -> Result<Option<TopologySnapshot>> {
    if settings.members.is_empty() {
        return Ok(None);
    }
    if let MemberSource::File(path) = &settings.member_source
        && repository.load_state()?.is_some()
    {
        info!(
            config = %path.display(),
            "state store exists; ignoring bootstrap keys from the config file"
        );
        return Ok(None);
    }
    Ok(Some(TopologySnapshot::new_with_timings(
        1,
        settings.seed,
        settings.virtual_nodes,
        settings.members.clone(),
        TopologyConfig {
            desired_replication_factor: settings.desired_replication_factor,
            write_availability_guard: WriteAvailabilityGuard {
                minimum_admitted_copies: settings.minimum_admitted_copies,
                minimum_healthy_followers: settings.minimum_healthy_followers,
                max_replica_lag_millis: settings.max_replica_lag_ms,
            },
            ..TopologyConfig::default()
        },
        ClusterTimings {
            receipt_retention_millis: settings.receipt_retention_ms,
            lease_duration_millis: settings.lease_duration_ms,
        },
    )?))
}

async fn run_node(config: NodeConfig, stop_response_delay_ms: u64) -> Result<()> {
    let service = DataNodeService::connect(config.id.clone(), config.coordinator, config.settings)
        .await?
        .with_stop_response_delay(Duration::from_millis(stop_response_delay_ms));
    let mut remote_shutdown = service.shutdown_receiver();
    info!(
        node_id = %config.id,
        process_instance_id = service.process_instance_id(),
        listen = %config.listen,
        "data node ready"
    );
    Server::builder()
        .add_service(
            DataNodeServer::new(service)
                .max_decoding_message_size(MAX_CONTROL_MESSAGE_BYTES)
                .max_encoding_message_size(MAX_CONTROL_MESSAGE_BYTES),
        )
        .serve_with_shutdown(config.listen, async move {
            tokio::select! {
                () = shutdown_signal() => {}
                result = remote_shutdown.changed() => {
                    if result.is_err() || *remote_shutdown.borrow() {
                        tracing::info!("data node received remote shutdown");
                    }
                }
            }
        })
        .await?;
    Ok(())
}

async fn run_put(args: PutArgs) -> Result<()> {
    let client = connect_client(&args.client).await?;
    let key = bytes_arg(args.key, args.key_hex, "key")?;
    let value = bytes_arg(args.value, args.value_hex, "value")?;
    let output = client.put(key, value).await?;
    println!(
        "epoch={} version=({}, {}, {})",
        output.topology_epoch,
        output.version.topology_epoch,
        output.version.owner_sequence,
        output.version.owner_node_id
    );
    Ok(())
}

async fn run_get(args: GetArgs) -> Result<()> {
    let client = connect_client(&args.client).await?;
    let key = bytes_arg(args.key, args.key_hex, "key")?;
    let output = client.get(key).await?;
    if args.hex {
        println!("{}", hex::encode(output.value));
    } else {
        println!(
            "{}",
            String::from_utf8(output.value).context("value is not valid UTF-8; use --hex")?
        );
    }
    Ok(())
}

async fn run_delete(args: DeleteArgs) -> Result<()> {
    let client = connect_client(&args.client).await?;
    let key = bytes_arg(args.key, args.key_hex, "key")?;
    let output = client.delete(key).await?;
    println!("epoch={} absent=true", output.topology_epoch);
    Ok(())
}

async fn run_topology(args: ClientArgs) -> Result<()> {
    let client = connect_client(&args).await?;
    println!(
        "{}",
        serde_json::to_string_pretty(&client.topology().await)?
    );
    Ok(())
}

async fn run_begin_change(args: ChangeArgs) -> Result<()> {
    let client = connect_client(&args.client).await?;
    let members = args
        .members
        .iter()
        .map(|member| parse_member(member))
        .collect::<Result<Vec<_>>>()?;
    let change = client.begin_topology_change(members).await?;
    println!("{}", serde_json::to_string_pretty(&change)?);
    Ok(())
}

async fn run_begin_policy_change(args: PolicyChangeArgs) -> Result<()> {
    let client = connect_client(&args.client).await?;
    let change = client.begin_write_policy_change(args.policy.into()).await?;
    println!("{}", serde_json::to_string_pretty(&change)?);
    Ok(())
}

async fn run_begin_config_change(args: ConfigChangeArgs) -> Result<()> {
    let client = connect_client(&args.client).await?;
    let mut config = client.topology().await.config();
    if let Some(rf) = args.desired_replication_factor {
        config.desired_replication_factor = rf;
    }
    if let Some(copies) = args.minimum_admitted_copies {
        config.write_availability_guard.minimum_admitted_copies = copies;
    }
    if let Some(followers) = args.minimum_healthy_followers {
        config.write_availability_guard.minimum_healthy_followers = followers;
    }
    if let Some(lag) = args.max_replica_lag_ms {
        config.write_availability_guard.max_replica_lag_millis = lag;
    }
    let change = client.begin_topology_config_change(config).await?;
    println!("{}", serde_json::to_string_pretty(&change)?);
    Ok(())
}

async fn run_change_status(args: ClientArgs) -> Result<()> {
    let client = connect_client(&args).await?;
    println!(
        "{}",
        serde_json::to_string_pretty(&client.topology_change().await?)?
    );
    Ok(())
}

async fn run_replica_status(args: ClientArgs) -> Result<()> {
    let status = connect_client(&args).await?.replica_status().await?;
    let ranges: Vec<_> = status
        .ranges
        .into_iter()
        .map(|range| {
            let followers: Vec<_> = range
                .followers
                .into_iter()
                .map(|follower| {
                    serde_json::json!({
                        "node_id": follower.node_id,
                        "admitted": follower.admitted,
                        "leased": follower.leased,
                        "healthy": follower.healthy,
                        "process_instance_id": follower.process_instance_id,
                        "verified_watermark": follower.verified_watermark,
                        "stream_cursor": follower.stream_cursor,
                        "stream_head": follower.stream_head,
                        "last_ack_sequence": follower.last_ack_known.then_some(follower.last_ack_sequence),
                        "lag_millis": follower.lag_known.then_some(follower.lag_millis),
                        "repair_state": follower.repair_state,
                        "repair_retry_count": follower.repair_retry_count,
                        "repair_next_attempt_unix_millis": follower.repair_next_attempt_unix_millis,
                        "repair_last_error": follower.repair_last_error,
                    })
                })
                .collect();
            serde_json::json!({
                "start_exclusive": range.start_exclusive,
                "end_inclusive": range.end_inclusive,
                "owner_node_id": range.owner_node_id,
                "desired_rf": range.desired_rf,
                "current_rf": range.current_rf,
                "live_rf": range.live_rf,
                "owner_leased": range.owner_leased,
                "writable": range.writable,
                "write_block_reason": range.write_block_reason,
                "under_replicated": range.under_replicated,
                "repairing": range.repairing,
                "followers": followers,
            })
        })
        .collect();
    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::json!({
            "topology_epoch": status.topology_epoch,
            "topology_digest": status.topology_digest,
            "desired_rf": status.desired_rf,
            "write_ack_policy": hashring_core::proto::WriteAckPolicy::try_from(status.write_ack_policy)
                .map(|policy| policy.as_str_name())
                .unwrap_or("WRITE_ACK_POLICY_UNSPECIFIED"),
            "active_change_id": status.active_change_id,
            "active_change_phase": hashring_core::proto::MigrationPhase::try_from(status.active_change_phase)
                .map(|phase| phase.as_str_name())
                .unwrap_or("MIGRATION_PHASE_UNSPECIFIED"),
            "recovery_block_reason": status.recovery_block_reason,
            "activation_pending": status.activation_pending,
            "nodes": status.nodes.into_iter().map(|node| serde_json::json!({
                "node_id": node.node_id,
                "process_instance_id": node.process_instance_id,
                "leased": node.leased,
                "suspected": node.suspected,
                "fenced": node.fenced,
                "joining": node.joining,
                "lease_expires_unix_millis": (node.lease_expires_unix_millis != 0)
                    .then_some(node.lease_expires_unix_millis),
                "last_renewal_unix_millis": (node.last_renewal_unix_millis != 0)
                    .then_some(node.last_renewal_unix_millis),
            })).collect::<Vec<_>>(),
            "ranges": ranges,
        }))?
    );
    Ok(())
}

async fn run_execute_change(args: ExecuteChangeArgs) -> Result<()> {
    let client =
        HashringClient::connect(args.coordinator, Duration::from_millis(args.deadline_ms)).await?;
    let change = client
        .execute_topology_change(args.change_id, args.base_epoch, args.target_epoch)
        .await?;
    println!("{}", serde_json::to_string_pretty(&change)?);
    Ok(())
}

async fn connect_client(args: &ClientArgs) -> Result<HashringClient> {
    Ok(HashringClient::connect(
        args.coordinator.clone(),
        Duration::from_millis(args.deadline_ms),
    )
    .await?)
}

fn parse_member(value: &str) -> Result<Member> {
    let (node_id, endpoint) = value
        .split_once('=')
        .with_context(|| format!("invalid member {value:?}; expected NODE_ID=HTTP_ENDPOINT"))?;
    Ok(Member {
        node_id: node_id.to_owned(),
        endpoint: endpoint.to_owned(),
    })
}

fn bytes_arg(text: Option<String>, encoded: Option<String>, name: &str) -> Result<Vec<u8>> {
    match (text, encoded) {
        (Some(text), None) => Ok(text.into_bytes()),
        (None, Some(encoded)) => {
            hex::decode(encoded).with_context(|| format!("invalid {name} hex"))
        }
        _ => anyhow::bail!("exactly one {name} representation is required"),
    }
}

async fn shutdown_signal() {
    if let Err(error) = tokio::signal::ctrl_c().await {
        tracing::error!(%error, "failed to install shutdown signal handler");
    }
}

#[cfg(test)]
mod worker_thread_tests {
    use super::*;

    #[test]
    fn worker_thread_overrides_must_be_positive() {
        let node = [
            "hashring-rs",
            "node",
            "--id",
            "n",
            "--listen",
            "127.0.0.1:1",
        ];
        assert!(Cli::try_parse_from(node.iter().chain(&["--worker-threads", "0"])).is_err());
        let coordinator = ["hashring-rs", "coordinator"];
        assert!(Cli::try_parse_from(coordinator.iter().chain(&["--worker-threads", "0"])).is_err());
        let experiment = ["hashring-rs", "experiment", "--output", "unused"];
        assert!(Cli::try_parse_from(experiment.iter().chain(&["--worker-threads", "0"])).is_err());
    }

    #[test]
    fn worker_threads_is_not_a_client_flag() {
        assert!(Cli::try_parse_from(["hashring-rs", "--worker-threads", "2", "topology"]).is_err());
        assert!(Cli::try_parse_from(["hashring-rs", "topology", "--worker-threads", "2"]).is_err());
    }
}

#[cfg(test)]
mod coordinator_bootstrap_tests {
    use super::*;

    fn settings(state: PathBuf, extra: &[&str], file: Option<PathBuf>) -> CoordinatorSettings {
        let state = state.display().to_string();
        let mut argv = vec!["hashring-rs", "coordinator", "--state", &state];
        argv.extend_from_slice(extra);
        let Command::Coordinator(args) = Cli::try_parse_from(argv).unwrap().command else {
            unreachable!()
        };
        let file = file.map(|path| config::load(&path).unwrap());
        config::coordinator::resolve(&args, file).unwrap()
    }

    #[test]
    fn file_members_restart_an_existing_store_but_member_flags_must_match() {
        let dir = tempfile::tempdir().unwrap();
        let state = dir.path().join("coordinator.redb");
        let config = dir.path().join("cluster.yaml");
        std::fs::write(
            &config,
            "members:\n  - { node_id: a, endpoint: http://a }\n  - { node_id: b, endpoint: http://b }\n",
        )
        .unwrap();
        let repository = RedbTopologyRepository::open(&state).unwrap();

        // The first start creates the store from the file.
        let first = settings(state.clone(), &[], Some(config.clone()));
        let bootstrap = coordinator_bootstrap(&first, &repository).unwrap();
        assert!(bootstrap.is_some());
        load_or_initialize(&repository, bootstrap).unwrap();

        // A changed file no longer matches, but its bootstrap keys are ignored.
        let restart = settings(state.clone(), &["--seed", "9"], Some(config));
        assert!(
            coordinator_bootstrap(&restart, &repository)
                .unwrap()
                .is_none()
        );

        // Explicit flags still get the strict BootstrapMismatch check.
        let flags = settings(state, &["--member", "c=http://c"], None);
        let bootstrap = coordinator_bootstrap(&flags, &repository).unwrap();
        assert!(load_or_initialize(&repository, bootstrap).is_err());
    }

    #[test]
    fn cluster_timings_are_stored_and_cannot_change_on_restart() {
        let dir = tempfile::tempdir().unwrap();
        let state = dir.path().join("coordinator.redb");
        let repository = RedbTopologyRepository::open(&state).unwrap();
        let member = ["--member", "a=http://a"];
        let timings = [
            "--receipt-retention-ms",
            "90000",
            "--lease-duration-ms",
            "3000",
        ];
        let first = settings(state.clone(), &[&member[..], &timings].concat(), None);
        let bootstrap = coordinator_bootstrap(&first, &repository).unwrap();
        let stored = load_or_initialize(&repository, bootstrap).unwrap();
        assert_eq!(stored.committed.timings.receipt_retention_millis, 90_000);
        assert_eq!(stored.committed.timings.lease_duration_millis, 3_000);

        // Restarting with the default lease would shorten the startup quarantine.
        let restart = settings(state, &member, None);
        let bootstrap = coordinator_bootstrap(&restart, &repository).unwrap();
        assert!(load_or_initialize(&repository, bootstrap).is_err());
    }
}
