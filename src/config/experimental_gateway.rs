//! Additional settings for the purely experimental Lua gateway.

use anyhow::Result;
use hashring_core::limits::MAX_VALUE_BYTES;
use hashring_redis::resp::Limits;
use serde::Deserialize;

use super::{LoadedFile, gateway};
use crate::ExperimentalGatewayArgs;

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct GatewayFile {
    pub listen: Option<std::net::SocketAddr>,
    pub coordinator: Option<String>,
    pub deadline_ms: Option<u64>,
    pub requirepass: Option<String>,
    pub maxclients: Option<std::num::NonZeroUsize>,
    pub worker_threads: Option<std::num::NonZeroUsize>,
    pub max_bulk_bytes: Option<usize>,
}

pub struct GatewaySettings {
    pub gateway: gateway::GatewaySettings,
    pub max_bulk_bytes: usize,
}

pub fn resolve(
    args: &ExperimentalGatewayArgs,
    file: Option<LoadedFile<GatewayFile>>,
) -> Result<GatewaySettings> {
    let max_bulk_bytes = args
        .max_bulk_bytes
        .or(file.as_ref().and_then(|loaded| loaded.body.max_bulk_bytes))
        .unwrap_or(Limits::default().max_bulk_bytes);
    anyhow::ensure!(
        max_bulk_bytes > 0 && max_bulk_bytes <= MAX_VALUE_BYTES,
        "max_bulk_bytes must be positive and at most {MAX_VALUE_BYTES}"
    );
    let file = file.map(|loaded| LoadedFile {
        path: loaded.path,
        body: gateway::GatewayFile {
            listen: loaded.body.listen,
            coordinator: loaded.body.coordinator,
            deadline_ms: loaded.body.deadline_ms,
            requirepass: loaded.body.requirepass,
            maxclients: loaded.body.maxclients,
            worker_threads: loaded.body.worker_threads,
        },
    });
    Ok(GatewaySettings {
        gateway: gateway::resolve(&args.gateway, file)?,
        max_bulk_bytes,
    })
}
