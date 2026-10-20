//! Purely experimental Lua support for the shared Redis gateway.
//!
//! Ordinary commands, RESP framing and connection handling come from
//! `hashring-redis`. This crate adds scripting through its generic extension API.
//! Compatibility and resource isolation are intentionally incomplete.

use std::sync::Arc;

pub use hashring_redis::{GatewayConfig, client, error, resp, table};

mod commands;
pub mod script;

pub use commands::Scripts;

/// Run the shared gateway with the experimental scripting extension installed.
pub async fn serve(
    config: GatewayConfig,
    shutdown: tokio::sync::watch::Receiver<bool>,
) -> std::io::Result<()> {
    hashring_redis::serve_with_extension(config, shutdown, Arc::new(Scripts::default())).await
}
