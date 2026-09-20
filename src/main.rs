use std::{net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand};
use hashring_rs::{
    client::HashringClient,
    coordinator::{CoordinatorService, RedbTopologyRepository, load_or_initialize},
    node::{DataNodeService, MAX_DATA_MESSAGE_BYTES},
    proto::{coordinator_server::CoordinatorServer, data_node_server::DataNodeServer},
    topology::{Member, TopologySnapshot},
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
    /// Print the canonical topology as JSON.
    Topology(ClientArgs),
    /// Persist a pending target topology and its moving-range plan.
    BeginChange(ChangeArgs),
    /// Print the active topology change, if any.
    ChangeStatus(ClientArgs),
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
struct ChangeArgs {
    #[command(flatten)]
    client: ClientArgs,
    /// Complete target membership in NODE_ID=HTTP_ENDPOINT form.
    #[arg(long = "member", required = true)]
    members: Vec<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("hashring_rs=info".parse()?))
        .init();

    match Cli::parse().command {
        Command::Coordinator(args) => run_coordinator(args).await,
        Command::Node(args) => run_node(args).await,
        Command::Put(args) => run_put(args).await,
        Command::Get(args) => run_get(args).await,
        Command::Topology(args) => run_topology(args).await,
        Command::BeginChange(args) => run_begin_change(args).await,
        Command::ChangeStatus(args) => run_change_status(args).await,
    }
}

async fn run_coordinator(args: CoordinatorArgs) -> Result<()> {
    let repository = Arc::new(
        RedbTopologyRepository::open(&args.state)
            .with_context(|| format!("opening {}", args.state.display()))?,
    );
    let bootstrap = if args.members.is_empty() {
        None
    } else {
        Some(TopologySnapshot::new(
            1,
            args.seed,
            args.virtual_nodes,
            args.members
                .iter()
                .map(|member| parse_member(member))
                .collect::<Result<Vec<_>>>()?,
        )?)
    };
    let state = load_or_initialize(repository.as_ref(), bootstrap)?;
    info!(
        listen = %args.listen,
        epoch = state.committed.epoch,
        digest = %state.committed.digest,
        members = state.committed.members.len(),
        active_change = state.active_change.is_some(),
        "coordinator ready"
    );
    Server::builder()
        .add_service(CoordinatorServer::new(CoordinatorService::new(
            state, repository,
        )))
        .serve_with_shutdown(args.listen, shutdown_signal())
        .await?;
    Ok(())
}

async fn run_node(args: NodeArgs) -> Result<()> {
    let service = DataNodeService::connect(args.id.clone(), args.coordinator).await?;
    info!(node_id = %args.id, listen = %args.listen, "data node ready");
    Server::builder()
        .add_service(
            DataNodeServer::new(service)
                .max_decoding_message_size(MAX_DATA_MESSAGE_BYTES)
                .max_encoding_message_size(MAX_DATA_MESSAGE_BYTES),
        )
        .serve_with_shutdown(args.listen, shutdown_signal())
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

async fn run_change_status(args: ClientArgs) -> Result<()> {
    let client = connect_client(&args).await?;
    println!(
        "{}",
        serde_json::to_string_pretty(&client.topology_change().await?)?
    );
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
