use std::{net::SocketAddr, num::NonZeroUsize, path::PathBuf, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand, ValueEnum};
use hashring_experiment::{ExperimentConfig, ExperimentMode, run_experiment};
use hashring_rs::{
    client::HashringClient,
    coordinator::{
        CoordinatorRepository, CoordinatorService, DEFAULT_RANGE_MOVE_CONCURRENCY,
        RedbTopologyRepository, load_or_initialize,
    },
    limits::MAX_CONTROL_MESSAGE_BYTES,
    node::DataNodeService,
    proto::{coordinator_server::CoordinatorServer, data_node_server::DataNodeServer},
    topology::{Member, TopologyConfig, TopologySnapshot, WriteAckPolicy, WriteAvailabilityGuard},
};
use tonic::transport::Server;
use tracing::info;
use tracing_subscriber::EnvFilter;

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
    /// Run a reproducible separate-process correctness or performance experiment.
    Experiment(ExperimentArgs),
}

#[derive(Args)]
struct CoordinatorArgs {
    #[arg(long, default_value = "127.0.0.1:50050")]
    listen: SocketAddr,
    #[arg(long, default_value = "./coordinator.redb")]
    state: PathBuf,
    #[arg(long, default_value_t = 1)]
    seed: u64,
    #[arg(long, default_value_t = 128)]
    virtual_nodes: u32,
    #[arg(long, default_value_t = 3)]
    desired_replication_factor: u32,
    #[arg(long)]
    minimum_admitted_copies: Option<u32>,
    #[arg(long)]
    minimum_healthy_followers: Option<u32>,
    #[arg(long, default_value_t = 5_000)]
    max_replica_lag_ms: u64,
    #[arg(long, default_value_t = 120_000)]
    migration_timeout_ms: u64,
    /// Maximum number of disjoint ranges moved concurrently within one topology change.
    #[arg(long, default_value_t = NonZeroUsize::new(DEFAULT_RANGE_MOVE_CONCURRENCY).unwrap())]
    range_move_concurrency: NonZeroUsize,
    #[arg(long, default_value_t = 0, hide = true)]
    pre_publish_delay_ms: u64,
    #[arg(long, default_value_t = 0, hide = true)]
    post_publish_delay_ms: u64,
    /// Bootstrap member in NODE_ID=HTTP_ENDPOINT form. Required only for a new store.
    #[arg(long = "member")]
    members: Vec<String>,
}

#[derive(Args)]
struct NodeArgs {
    #[arg(long)]
    id: String,
    #[arg(long)]
    listen: SocketAddr,
    #[arg(long, default_value = "http://127.0.0.1:50050")]
    coordinator: String,
    #[arg(long, default_value_t = 0, hide = true)]
    stop_response_delay_ms: u64,
}

#[derive(Args, Clone)]
struct ClientArgs {
    #[arg(long, default_value = "http://127.0.0.1:50050")]
    coordinator: String,
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

#[derive(Clone, Copy, ValueEnum)]
enum PolicyArg {
    OwnerOnly,
    FirstSuccessor,
    AllReplicas,
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

#[derive(Clone, Copy, ValueEnum)]
enum ExperimentModeArg {
    Correctness,
    Performance,
    Availability,
}

#[derive(Args)]
struct ExperimentArgs {
    #[arg(long, value_enum, default_value_t = ExperimentModeArg::Correctness)]
    mode: ExperimentModeArg,
    /// Empty or not-yet-created directory for the manifest, raw logs, and summary.
    #[arg(long)]
    output: PathBuf,
    /// Peak node count. Correctness mode starts with one fewer node, then scales out and in.
    #[arg(long)]
    nodes: Option<usize>,
    /// Logical keys. Defaults to 20,000 for correctness and 1,000,000 for performance.
    #[arg(long)]
    keys: Option<u64>,
    #[arg(long, default_value_t = 128)]
    value_bytes: usize,
    #[arg(long, default_value_t = 64)]
    concurrency: usize,
    #[arg(long, default_value_t = 1)]
    seed: u64,
    #[arg(long, default_value_t = 128)]
    virtual_nodes: u32,
    #[arg(long, default_value_t = 30_000)]
    operation_timeout_ms: u64,
    #[arg(long, default_value_t = 600_000)]
    migration_timeout_ms: u64,
    /// Maximum number of disjoint ranges moved concurrently within one topology change.
    #[arg(long, default_value_t = DEFAULT_RANGE_MOVE_CONCURRENCY)]
    range_move_concurrency: usize,
    /// Deterministic pre-publication observation window for migration stress runs.
    #[arg(long, default_value_t = 250)]
    pre_publish_delay_ms: u64,
    /// Refuse the run unless build and runtime source match the same clean commit.
    #[arg(long)]
    require_clean_source: bool,
    /// Log client retries and migration/repair progress for diagnosis.
    #[arg(long)]
    verbose: bool,
    #[arg(long, default_value_t = 3)]
    desired_replication_factor: u32,
    #[arg(long, default_value_t = 1)]
    minimum_admitted_copies: u32,
    #[arg(long, default_value_t = 0)]
    minimum_healthy_followers: u32,
    /// Defaults to FirstSuccessor for availability and OwnerOnly otherwise.
    #[arg(long, value_enum)]
    policy: Option<PolicyArg>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let verbose_experiment = matches!(&cli.command, Command::Experiment(args) if args.verbose);
    let mut filter = EnvFilter::from_default_env().add_directive("hashring_rs=info".parse()?);
    if verbose_experiment {
        filter = filter.add_directive("hashring_client=debug".parse()?);
    }
    tracing_subscriber::fmt().with_env_filter(filter).init();

    match cli.command {
        Command::Coordinator(args) => run_coordinator(args).await,
        Command::Node(args) => run_node(args).await,
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
        Command::Experiment(args) => run_local_experiment(args).await,
    }
}

async fn run_local_experiment(args: ExperimentArgs) -> Result<()> {
    let mode = match args.mode {
        ExperimentModeArg::Correctness => ExperimentMode::Correctness,
        ExperimentModeArg::Performance => ExperimentMode::Performance,
        ExperimentModeArg::Availability => ExperimentMode::Availability,
    };
    let key_count = args.keys.unwrap_or(match mode {
        ExperimentMode::Correctness => 20_000,
        ExperimentMode::Performance => 1_000_000,
        ExperimentMode::Availability => 1_000,
    });
    let summary = run_experiment(
        ExperimentConfig {
            mode,
            output_dir: args.output,
            node_count: args
                .nodes
                .unwrap_or(if matches!(mode, ExperimentMode::Availability) {
                    4
                } else {
                    10
                }),
            key_count,
            value_bytes: args.value_bytes,
            concurrency: args.concurrency,
            hash_seed: args.seed,
            virtual_nodes: args.virtual_nodes,
            operation_timeout_ms: args.operation_timeout_ms,
            migration_timeout_ms: args.migration_timeout_ms,
            range_move_concurrency: args.range_move_concurrency,
            pre_publish_delay_ms: args.pre_publish_delay_ms,
            require_clean_source: args.require_clean_source,
            verbose: args.verbose,
            desired_replication_factor: args.desired_replication_factor,
            minimum_admitted_copies: args.minimum_admitted_copies,
            minimum_healthy_followers: args.minimum_healthy_followers,
            write_ack_policy: match args.policy.unwrap_or(
                if matches!(mode, ExperimentMode::Availability) {
                    PolicyArg::FirstSuccessor
                } else {
                    PolicyArg::OwnerOnly
                },
            ) {
                PolicyArg::OwnerOnly => WriteAckPolicy::OwnerOnly,
                PolicyArg::FirstSuccessor => WriteAckPolicy::FirstSuccessor,
                PolicyArg::AllReplicas => WriteAckPolicy::AllReplicas,
            },
        },
        std::env::current_exe()?,
    )
    .await?;
    println!("{}", serde_json::to_string_pretty(&summary)?);
    Ok(())
}

async fn run_coordinator(args: CoordinatorArgs) -> Result<()> {
    let repository = Arc::new(
        RedbTopologyRepository::open(&args.state)
            .with_context(|| format!("opening {}", args.state.display()))?,
    );
    let bootstrap = if args.members.is_empty() {
        None
    } else {
        Some(TopologySnapshot::new_with_config(
            1,
            args.seed,
            args.virtual_nodes,
            args.members
                .iter()
                .map(|member| parse_member(member))
                .collect::<Result<Vec<_>>>()?,
            TopologyConfig {
                desired_replication_factor: args.desired_replication_factor,
                write_availability_guard: coordinator_bootstrap_guard(&args, repository.as_ref())?,
                ..TopologyConfig::default()
            },
        )?)
    };
    let state = load_or_initialize(repository.as_ref(), bootstrap)?;
    info!(
        listen = %args.listen,
        epoch = state.committed.epoch,
        digest = %state.committed.digest,
        members = state.committed.members.len(),
        active_change = state.active_change.is_some(),
        range_move_concurrency = args.range_move_concurrency.get(),
        "coordinator ready"
    );
    let service = CoordinatorService::new(
        state,
        repository,
        Duration::from_millis(args.migration_timeout_ms),
    )
    .with_range_move_concurrency(args.range_move_concurrency.get())
    .with_pre_publish_delay(Duration::from_millis(args.pre_publish_delay_ms))
    .with_post_publish_delay(Duration::from_millis(args.post_publish_delay_ms));
    let recovery_service = service.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(5));
        loop {
            interval.tick().await;
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
        .serve_with_shutdown(args.listen, shutdown_signal())
        .await?;
    Ok(())
}

fn coordinator_bootstrap_guard(
    args: &CoordinatorArgs,
    repository: &impl CoordinatorRepository,
) -> Result<WriteAvailabilityGuard> {
    let persisted_guard =
        if args.minimum_admitted_copies.is_none() || args.minimum_healthy_followers.is_none() {
            repository
                .load_state()?
                .map(|state| state.committed.write_availability_guard)
        } else {
            None
        };
    let default_guard = WriteAvailabilityGuard::default();
    Ok(WriteAvailabilityGuard {
        minimum_admitted_copies: args.minimum_admitted_copies.unwrap_or_else(|| {
            persisted_guard
                .as_ref()
                .map_or(default_guard.minimum_admitted_copies, |guard| {
                    guard.minimum_admitted_copies
                })
        }),
        minimum_healthy_followers: args.minimum_healthy_followers.unwrap_or_else(|| {
            persisted_guard
                .as_ref()
                .map_or(default_guard.minimum_healthy_followers, |guard| {
                    guard.minimum_healthy_followers
                })
        }),
        max_replica_lag_millis: args.max_replica_lag_ms,
    })
}

async fn run_node(args: NodeArgs) -> Result<()> {
    let service = DataNodeService::connect(args.id.clone(), args.coordinator)
        .await?
        .with_stop_response_delay(Duration::from_millis(args.stop_response_delay_ms));
    let mut remote_shutdown = service.shutdown_receiver();
    info!(
        node_id = %args.id,
        process_instance_id = service.process_instance_id(),
        listen = %args.listen,
        "data node ready"
    );
    Server::builder()
        .add_service(
            DataNodeServer::new(service)
                .max_decoding_message_size(MAX_CONTROL_MESSAGE_BYTES)
                .max_encoding_message_size(MAX_CONTROL_MESSAGE_BYTES),
        )
        .serve_with_shutdown(args.listen, async move {
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
    let policy = match args.policy {
        PolicyArg::OwnerOnly => WriteAckPolicy::OwnerOnly,
        PolicyArg::FirstSuccessor => WriteAckPolicy::FirstSuccessor,
        PolicyArg::AllReplicas => WriteAckPolicy::AllReplicas,
    };
    let change = client.begin_write_policy_change(policy).await?;
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
mod tests {
    use super::*;

    #[test]
    fn omitted_bootstrap_guard_preserves_existing_topology() {
        let directory = tempfile::tempdir().unwrap();
        let repository =
            RedbTopologyRepository::open(directory.path().join("coordinator.redb")).unwrap();
        let command = [
            "hashring-rs",
            "coordinator",
            "--member",
            "n1=http://127.0.0.1:5001",
            "--desired-replication-factor",
            "1",
        ];
        let Cli {
            command: Command::Coordinator(args),
        } = Cli::try_parse_from(command).unwrap()
        else {
            unreachable!();
        };
        assert_eq!(
            coordinator_bootstrap_guard(&args, &repository).unwrap(),
            WriteAvailabilityGuard::default()
        );

        let legacy = WriteAvailabilityGuard {
            minimum_admitted_copies: 2,
            minimum_healthy_followers: 1,
            ..WriteAvailabilityGuard::default()
        };
        let members = vec![parse_member(&args.members[0]).unwrap()];
        let persisted = TopologySnapshot::new_with_config(
            1,
            args.seed,
            args.virtual_nodes,
            members.clone(),
            TopologyConfig {
                desired_replication_factor: 1,
                write_availability_guard: legacy.clone(),
                ..TopologyConfig::default()
            },
        )
        .unwrap();
        load_or_initialize(&repository, Some(persisted.clone())).unwrap();

        let resolved = coordinator_bootstrap_guard(&args, &repository).unwrap();
        assert_eq!(resolved, legacy);
        let bootstrap = TopologySnapshot::new_with_config(
            1,
            args.seed,
            args.virtual_nodes,
            members.clone(),
            TopologyConfig {
                desired_replication_factor: 1,
                write_availability_guard: resolved,
                ..TopologyConfig::default()
            },
        )
        .unwrap();
        assert_eq!(
            load_or_initialize(&repository, Some(bootstrap))
                .unwrap()
                .committed,
            persisted
        );

        let Cli {
            command: Command::Coordinator(explicit),
        } = Cli::try_parse_from([
            "hashring-rs",
            "coordinator",
            "--member",
            "n1=http://127.0.0.1:5001",
            "--desired-replication-factor",
            "1",
            "--minimum-admitted-copies",
            "1",
            "--minimum-healthy-followers",
            "0",
        ])
        .unwrap()
        else {
            unreachable!();
        };
        let conflicting = TopologySnapshot::new_with_config(
            1,
            explicit.seed,
            explicit.virtual_nodes,
            members,
            TopologyConfig {
                desired_replication_factor: 1,
                write_availability_guard: coordinator_bootstrap_guard(&explicit, &repository)
                    .unwrap(),
                ..TopologyConfig::default()
            },
        )
        .unwrap();
        assert!(load_or_initialize(&repository, Some(conflicting)).is_err());
    }
}
