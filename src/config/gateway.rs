//! YAML settings for the `redis-gateway` command.

use std::{net::SocketAddr, num::NonZeroUsize, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use hashring_core::limits::MAX_VALUE_BYTES;
use hashring_redis::resp::Limits;
use serde::Deserialize;

use super::LoadedFile;
use crate::GatewayArgs;

pub const DEFAULT_LISTEN: &str = "127.0.0.1:6379";
pub const DEFAULT_COORDINATOR: &str = "http://127.0.0.1:50050";
pub const DEFAULT_DEADLINE_MS: u64 = 5_000;
pub const DEFAULT_MAXCLIENTS: usize = 10_000;

/// Read when neither the flag nor the file sets a password, so a deployment can
/// keep the secret out of both the process list and the config file.
pub const PASSWORD_ENV: &str = "HASHRING_GATEWAY_PASSWORD";

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct GatewayFile {
    pub listen: Option<SocketAddr>,
    pub coordinator: Option<String>,
    pub deadline_ms: Option<u64>,
    pub requirepass: Option<String>,
    pub maxclients: Option<NonZeroUsize>,
    pub max_bulk_bytes: Option<usize>,
    pub worker_threads: Option<NonZeroUsize>,
}

#[derive(Debug)]
pub struct GatewaySettings {
    pub listen: SocketAddr,
    pub coordinator: String,
    pub operation_timeout: Duration,
    pub password: Option<Arc<str>>,
    pub max_connections: NonZeroUsize,
    /// Largest bulk string a connection may send, so it is the largest value
    /// the gateway will read from a client. This is the gateway's own ceiling;
    /// what a node accepts is the cluster's `--max-value-bytes`.
    pub max_bulk_bytes: usize,
    /// `None` takes one worker per available CPU.
    pub worker_threads: Option<NonZeroUsize>,
}

impl GatewaySettings {
    pub fn runtime_workers(&self, cpus: usize) -> usize {
        self.worker_threads.map_or(cpus, NonZeroUsize::get)
    }
}

pub fn resolve(
    args: &GatewayArgs,
    file: Option<LoadedFile<GatewayFile>>,
) -> Result<GatewaySettings> {
    let file = file.map(|loaded| loaded.body).unwrap_or_default();
    let deadline_ms = args
        .deadline_ms
        .or(file.deadline_ms)
        .unwrap_or(DEFAULT_DEADLINE_MS);
    anyhow::ensure!(deadline_ms > 0, "deadline_ms must be positive");
    let password = args
        .requirepass
        .clone()
        .or(file.requirepass)
        .or_else(|| std::env::var(PASSWORD_ENV).ok())
        .filter(|password| !password.is_empty())
        .map(Arc::<str>::from);
    let max_bulk_bytes = args
        .max_bulk_bytes
        .or(file.max_bulk_bytes)
        .unwrap_or(Limits::default().max_bulk_bytes);
    anyhow::ensure!(max_bulk_bytes > 0, "max_bulk_bytes must be positive");
    anyhow::ensure!(
        max_bulk_bytes <= MAX_VALUE_BYTES,
        "max_bulk_bytes must be at most {MAX_VALUE_BYTES}"
    );
    Ok(GatewaySettings {
        listen: args.listen.or(file.listen).unwrap_or(
            DEFAULT_LISTEN
                .parse()
                .context("the default gateway listen address is not a socket address")?,
        ),
        coordinator: args
            .coordinator
            .clone()
            .or(file.coordinator)
            .unwrap_or_else(|| DEFAULT_COORDINATOR.to_owned()),
        operation_timeout: Duration::from_millis(deadline_ms),
        password,
        max_connections: args
            .maxclients
            .or(file.maxclients)
            .unwrap_or(NonZeroUsize::new(DEFAULT_MAXCLIENTS).expect("the default is positive")),
        max_bulk_bytes,
        worker_threads: args.worker_threads.or(file.worker_threads),
    })
}

#[cfg(test)]
mod tests {
    use std::{fs, path::Path, path::PathBuf};

    use clap::Parser;

    use super::*;
    use crate::{Cli, Command, config::load};

    fn gateway_args(extra: &[&str]) -> GatewayArgs {
        let mut argv = vec!["hashring-rs", "redis-gateway"];
        argv.extend_from_slice(extra);
        match Cli::try_parse_from(argv).unwrap().command {
            Command::RedisGateway(args) => args,
            _ => unreachable!(),
        }
    }

    fn write(dir: &Path, text: &str) -> PathBuf {
        let path = dir.join("redis-gateway.yaml");
        fs::write(&path, text).unwrap();
        path
    }

    #[test]
    fn the_defaults_serve_one_local_client() {
        let settings = resolve(&gateway_args(&[]), None).unwrap();
        assert_eq!(settings.listen.to_string(), DEFAULT_LISTEN);
        assert_eq!(settings.coordinator, DEFAULT_COORDINATOR);
        assert_eq!(settings.operation_timeout, Duration::from_secs(5));
        assert!(settings.password.is_none());
        assert_eq!(settings.max_connections.get(), DEFAULT_MAXCLIENTS);
        assert_eq!(settings.runtime_workers(24), 24);
    }

    #[test]
    fn flags_override_file_and_file_overrides_defaults() {
        let dir = tempfile::tempdir().unwrap();
        let path = write(
            dir.path(),
            "listen: 127.0.0.1:7000\ncoordinator: http://127.0.0.1:6000\n\
             deadline_ms: 1234\nrequirepass: from-file\nworker_threads: 3\n",
        );
        let args = gateway_args(&["--listen", "127.0.0.1:7100", "--requirepass", "from-flag"]);
        let settings = resolve(&args, Some(load(&path).unwrap())).unwrap();
        assert_eq!(settings.listen.to_string(), "127.0.0.1:7100");
        assert_eq!(settings.coordinator, "http://127.0.0.1:6000");
        assert_eq!(settings.operation_timeout, Duration::from_millis(1234));
        assert_eq!(settings.password.as_deref(), Some("from-flag"));
        assert_eq!(settings.runtime_workers(24), 3);
    }

    #[test]
    fn a_zero_deadline_is_rejected() {
        let args = gateway_args(&["--deadline-ms", "0"]);
        assert!(resolve(&args, None).is_err());
    }

    #[test]
    fn the_bulk_limit_is_the_gateway_s_to_set_and_stops_at_what_can_be_stored() {
        // Raised for a cluster that raises its own value limit.
        let settings = resolve(&gateway_args(&["--max-bulk-bytes", "33554432"]), None).unwrap();
        assert_eq!(settings.max_bulk_bytes, 32 * 1024 * 1024);
        // The default is still what a node at its default limit never refuses.
        let settings = resolve(&gateway_args(&[]), None).unwrap();
        assert_eq!(settings.max_bulk_bytes, 16 * 1024 * 1024);
        // Nothing can be stored past what the transport carries, so parsing
        // more than that is refused rather than accepted and then dropped.
        let error = resolve(&gateway_args(&["--max-bulk-bytes", "135267328"]), None).unwrap_err();
        assert!(error.to_string().contains("max_bulk_bytes"), "{error}");
    }

    #[test]
    fn an_unknown_key_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = write(dir.path(), "maxmemory: 1\n");
        let error = format!("{:#}", load::<GatewayFile>(&path).unwrap_err());
        assert!(error.contains("unknown field `maxmemory`"), "{error}");
    }
}
