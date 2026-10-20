//! A Redis-protocol front end for a hashring-rs cluster.
//!
//! The gateway speaks RESP on a TCP port and reaches the cluster through
//! [`hashring_client`], so a Redis client connects to it the way it would to a
//! Redis server: `redis-cli`, `redis-py`, Jedis, Lettuce and go-redis all work
//! against it in single-node mode, and none of them has to know that the data
//! lives on ten nodes behind a consistent hash ring.
//!
//! What it is not: Redis Cluster. There is no slot table, no `MOVED`, and no
//! gossip, because the cluster already has one owner per key and a coordinator
//! that hands out the whole topology. Routing stays in the client, where it
//! already was.
//!
//! The commands it answers, the ones it declines, and the places where its
//! semantics differ from a single-threaded Redis server are all in
//! `docs/redis-compatibility.md`.

use std::{net::SocketAddr, sync::Arc, time::Duration};

pub mod client;
pub mod commands;
pub mod error;
pub mod resp;
pub mod server;
pub mod table;

use client::SharedClient;
use commands::Dispatcher;
use hashring_client::ClientConfig;

/// The version this gateway reports in `HELLO` and `INFO`. It is the
/// hashring-rs version, not a Redis version, so a client that checks for Redis
/// 7 features does not find them claimed.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// How the gateway is set up. Every field has a default a deployment can
/// override, and none of them changes what a command means.
#[derive(Clone, Debug)]
pub struct GatewayConfig {
    /// The coordinator the cluster's topology comes from.
    pub coordinator: String,
    /// The port Redis clients connect to.
    pub listen: SocketAddr,
    /// How long one command may take against the cluster before it is reported
    /// as not finished. Redis has no such deadline; a proxy does, because the
    /// cluster can be mid-migration and slow to answer.
    pub operation_timeout: Duration,
    /// When set, a connection must send `AUTH` before it may read or write.
    pub password: Option<Arc<str>>,
    pub limits: resp::Limits,
    /// Connections open at once; one more is answered with Redis's
    /// `max number of clients reached` and closed. Redis's `maxclients`.
    pub max_connections: usize,
    /// How many HTTP/2 connections the gateway opens to each data node.
    ///
    /// One is enough for correctness and is the default. More than one lets
    /// concurrent requests to the same node spread over more than one
    /// connection instead of sharing the stream bookkeeping of a single one.
    pub client_connection_pool: usize,
}

impl Default for GatewayConfig {
    fn default() -> Self {
        Self {
            coordinator: "http://127.0.0.1:50050".into(),
            listen: SocketAddr::from(([127, 0, 0, 1], 6379)),
            operation_timeout: Duration::from_secs(5),
            password: None,
            limits: resp::Limits::default(),
            max_connections: 10_000,
            client_connection_pool: 1,
        }
    }
}

/// Binds the port and serves clients until `shutdown` is set.
///
/// The coordinator is contacted on the first command rather than before the
/// port is opened, so a gateway may start before, alongside, or instead of the
/// coordinator being up; clients get `-TRYAGAIN` until the cluster answers.
pub async fn serve(
    config: GatewayConfig,
    shutdown: tokio::sync::watch::Receiver<bool>,
) -> std::io::Result<()> {
    serve_inner(config, shutdown, None).await
}

/// Serve the same protocol and connection handling with additional commands.
pub async fn serve_with_extension(
    config: GatewayConfig,
    shutdown: tokio::sync::watch::Receiver<bool>,
    extension: Arc<dyn commands::CommandExtension>,
) -> std::io::Result<()> {
    serve_inner(config, shutdown, Some(extension)).await
}

async fn serve_inner(
    config: GatewayConfig,
    shutdown: tokio::sync::watch::Receiver<bool>,
    extension: Option<Arc<dyn commands::CommandExtension>>,
) -> std::io::Result<()> {
    let listener = tokio::net::TcpListener::bind(config.listen).await?;
    let local = listener.local_addr()?;
    let client = Arc::new(SharedClient::new(
        config.coordinator.clone(),
        ClientConfig::new(config.operation_timeout)
            .with_connection_pool_size(config.client_connection_pool),
    ));
    let mut dispatcher = Dispatcher::new(
        client,
        config.password.clone(),
        config.limits,
        config.operation_timeout,
    );
    if let Some(extension) = extension {
        dispatcher = dispatcher.with_extension(extension);
    }
    let dispatcher = Arc::new(dispatcher);
    tracing::info!(
        listen = %local,
        coordinator = %config.coordinator,
        operation_timeout_ms = config.operation_timeout.as_millis(),
        authentication = config.password.is_some(),
        max_connections = config.max_connections,
        client_connection_pool = config.client_connection_pool,
        "redis gateway ready"
    );
    server::Gateway::new(dispatcher, shutdown, config.max_connections)
        .serve(listener)
        .await
}
