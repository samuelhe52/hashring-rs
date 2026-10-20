//! YAML settings for the `node` command, and the node tuning that the
//! `experiment` command forwards to the nodes it starts.

use std::{net::SocketAddr, num::NonZeroUsize, time::Duration};

use anyhow::{Context, Result};
use hashring_experiment::NodeOptions;
use hashring_node::NodeSettings;
use serde::Deserialize;

use super::LoadedFile;
use crate::{NodeArgs, NodeTuningArgs};

pub const DEFAULT_COORDINATOR: &str = "http://127.0.0.1:50050";

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct NodeFile {
    pub id: Option<String>,
    pub listen: Option<SocketAddr>,
    pub coordinator: Option<String>,
    pub worker_threads: Option<NonZeroUsize>,
    pub max_dedup_bytes: Option<usize>,
    pub max_value_bytes: Option<usize>,
    pub max_pending_replication_bytes: Option<usize>,
    pub max_migration_journal_bytes: Option<usize>,
    pub replication_stream_queue_capacity: Option<NonZeroUsize>,
    pub required_ack_timeout_ms: Option<u64>,
    pub replication_rpc_timeout_ms: Option<u64>,
    pub replication_batch_linger_ms: Option<u64>,
    pub replication_batch_max_entries: Option<NonZeroUsize>,
}

impl NodeFile {
    fn tuning(&self) -> NodeTuningArgs {
        NodeTuningArgs {
            max_dedup_bytes: self.max_dedup_bytes,
            max_value_bytes: self.max_value_bytes,
            max_pending_replication_bytes: self.max_pending_replication_bytes,
            max_migration_journal_bytes: self.max_migration_journal_bytes,
            replication_stream_queue_capacity: self.replication_stream_queue_capacity,
            required_ack_timeout_ms: self.required_ack_timeout_ms,
            replication_rpc_timeout_ms: self.replication_rpc_timeout_ms,
            replication_batch_linger_ms: self.replication_batch_linger_ms,
            replication_batch_max_entries: self.replication_batch_max_entries,
        }
    }
}

#[derive(Debug)]
pub struct NodeConfig {
    pub id: String,
    pub listen: SocketAddr,
    pub coordinator: String,
    /// `None` takes one worker per available CPU.
    pub worker_threads: Option<NonZeroUsize>,
    pub settings: NodeSettings,
}

impl NodeConfig {
    pub fn runtime_workers(&self, cpus: usize) -> usize {
        self.worker_threads.map_or(cpus, NonZeroUsize::get)
    }
}

pub fn resolve(args: &NodeArgs, file: Option<LoadedFile<NodeFile>>) -> Result<NodeConfig> {
    let file = file.map(|loaded| loaded.body).unwrap_or_default();
    let settings = settings(&args.tuning, &file.tuning());
    settings.validate()?;
    Ok(NodeConfig {
        id: args
            .id
            .clone()
            .or(file.id)
            .context("a node id is required: pass --id or set `id` in the config file")?,
        listen: args.listen.or(file.listen).context(
            "a listen address is required: pass --listen or set `listen` in the config file",
        )?,
        coordinator: args
            .coordinator
            .clone()
            .or(file.coordinator)
            .unwrap_or_else(|| DEFAULT_COORDINATOR.to_owned()),
        worker_threads: args.worker_threads.or(file.worker_threads),
        settings,
    })
}

/// Flags win over the file, and the file over the node's defaults.
pub fn settings(flags: &NodeTuningArgs, file: &NodeTuningArgs) -> NodeSettings {
    let defaults = NodeSettings::default();
    NodeSettings {
        max_dedup_bytes: flags
            .max_dedup_bytes
            .or(file.max_dedup_bytes)
            .unwrap_or(defaults.max_dedup_bytes),
        max_value_bytes: flags
            .max_value_bytes
            .or(file.max_value_bytes)
            .unwrap_or(defaults.max_value_bytes),
        max_pending_replication_bytes: flags
            .max_pending_replication_bytes
            .or(file.max_pending_replication_bytes)
            .unwrap_or(defaults.max_pending_replication_bytes),
        max_migration_journal_bytes: flags
            .max_migration_journal_bytes
            .or(file.max_migration_journal_bytes)
            .unwrap_or(defaults.max_migration_journal_bytes),
        replication_stream_queue_capacity: flags
            .replication_stream_queue_capacity
            .or(file.replication_stream_queue_capacity)
            .map_or(
                defaults.replication_stream_queue_capacity,
                NonZeroUsize::get,
            ),
        required_ack_timeout: flags
            .required_ack_timeout_ms
            .or(file.required_ack_timeout_ms)
            .map_or(defaults.required_ack_timeout, Duration::from_millis),
        replication_rpc_timeout: flags
            .replication_rpc_timeout_ms
            .or(file.replication_rpc_timeout_ms)
            .map_or(defaults.replication_rpc_timeout, Duration::from_millis),
        replication_batch_linger: flags
            .replication_batch_linger_ms
            .or(file.replication_batch_linger_ms)
            .map_or(defaults.replication_batch_linger, Duration::from_millis),
        replication_batch_max_entries: flags
            .replication_batch_max_entries
            .or(file.replication_batch_max_entries)
            .map_or(
                defaults.replication_batch_max_entries,
                NonZeroUsize::get,
            ),
    }
}

/// The settings in the `node` command's flag units, for an experiment to
/// forward and record.
pub fn options(settings: &NodeSettings) -> NodeOptions {
    NodeOptions {
        max_dedup_bytes: settings.max_dedup_bytes,
        max_value_bytes: settings.max_value_bytes,
        max_pending_replication_bytes: settings.max_pending_replication_bytes,
        max_migration_journal_bytes: settings.max_migration_journal_bytes,
        replication_stream_queue_capacity: settings.replication_stream_queue_capacity,
        required_ack_timeout_ms: duration_ms(settings.required_ack_timeout),
        replication_rpc_timeout_ms: duration_ms(settings.replication_rpc_timeout),
        replication_batch_linger_ms: duration_ms(settings.replication_batch_linger),
        replication_batch_max_entries: settings.replication_batch_max_entries,
    }
}

fn duration_ms(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use std::{fs, path::Path, path::PathBuf};

    use clap::Parser;
    use hashring_node::{
        DEFAULT_MAX_DEDUP_BYTES, DEFAULT_REPLICATION_BATCH_MAX_ENTRIES,
        DEFAULT_REQUIRED_ACK_TIMEOUT, MAX_VALUE_BYTES,
    };

    use super::*;
    use crate::{Cli, Command, config::load};

    fn node_args(extra: &[&str]) -> NodeArgs {
        let mut argv = vec!["hashring-rs", "node"];
        argv.extend_from_slice(extra);
        match Cli::try_parse_from(argv).unwrap().command {
            Command::Node(args) => args,
            _ => unreachable!(),
        }
    }

    fn write(dir: &Path, text: &str) -> PathBuf {
        let path = dir.join("node.yaml");
        fs::write(&path, text).unwrap();
        path
    }

    #[test]
    fn flags_override_file_and_file_overrides_defaults() {
        let dir = tempfile::tempdir().unwrap();
        let path = write(
            dir.path(),
            "id: node-1\nlisten: 127.0.0.1:50051\nworker_threads: 3\n\
             max_dedup_bytes: 1024\nreplication_rpc_timeout_ms: 750\n",
        );
        let args = node_args(&["--id", "node-2", "--max-dedup-bytes", "2048"]);
        let config = resolve(&args, Some(load(&path).unwrap())).unwrap();
        assert_eq!(config.id, "node-2");
        assert_eq!(config.listen, "127.0.0.1:50051".parse().unwrap());
        assert_eq!(config.coordinator, DEFAULT_COORDINATOR);
        assert_eq!(config.runtime_workers(24), 3);
        assert_eq!(config.settings.max_dedup_bytes, 2048);
        assert_eq!(
            config.settings.replication_rpc_timeout,
            Duration::from_millis(750)
        );
        assert_eq!(
            config.settings.required_ack_timeout,
            DEFAULT_REQUIRED_ACK_TIMEOUT
        );
    }

    #[test]
    fn the_replication_batch_size_comes_from_the_flag_or_the_file() {
        let config = resolve(&node_args(&["--id", "a", "--listen", "127.0.0.1:1"]), None).unwrap();
        assert_eq!(
            config.settings.replication_batch_max_entries,
            DEFAULT_REPLICATION_BATCH_MAX_ENTRIES
        );

        let dir = tempfile::tempdir().unwrap();
        let path = write(
            dir.path(),
            "id: node-1\nlisten: 127.0.0.1:50051\nreplication_batch_max_entries: 8\n",
        );
        let config = resolve(&node_args(&[]), Some(load(&path).unwrap())).unwrap();
        assert_eq!(config.settings.replication_batch_max_entries, 8);

        let config = resolve(
            &node_args(&["--replication-batch-max-entries", "16"]),
            Some(load(&path).unwrap()),
        )
        .unwrap();
        assert_eq!(config.settings.replication_batch_max_entries, 16);
    }

    #[test]
    fn id_and_listen_are_required_somewhere() {
        let error = resolve(&node_args(&["--listen", "127.0.0.1:1"]), None).unwrap_err();
        assert!(error.to_string().contains("--id"), "{error}");
        let error = resolve(&node_args(&["--id", "a"]), None).unwrap_err();
        assert!(error.to_string().contains("--listen"), "{error}");
    }

    #[test]
    fn no_file_keeps_the_node_defaults() {
        let config = resolve(&node_args(&["--id", "a", "--listen", "127.0.0.1:1"]), None).unwrap();
        assert_eq!(config.runtime_workers(24), 24);
        assert_eq!(config.settings.max_dedup_bytes, DEFAULT_MAX_DEDUP_BYTES);
    }

    #[test]
    fn a_raised_value_limit_brings_the_budgets_that_have_to_carry_it() {
        let dir = tempfile::tempdir().unwrap();
        let path = write(
            dir.path(),
            "id: node-1\nlisten: 127.0.0.1:50051\nmax_value_bytes: 33554432\n\
             max_migration_journal_bytes: 33554432\n\
             max_pending_replication_bytes: 67108864\n",
        );
        let config = resolve(&node_args(&[]), Some(load(&path).unwrap())).unwrap();
        assert_eq!(config.settings.max_value_bytes, 32 * 1024 * 1024);

        // The same limit without the budgets that would have to carry it is
        // refused, and the refusal names the flag that is short.
        let path = write(
            dir.path(),
            "id: node-1\nlisten: 127.0.0.1:50051\nmax_value_bytes: 33554432\n\
             max_migration_journal_bytes: 16777216\n\
             max_pending_replication_bytes: 67108864\n",
        );
        let error = resolve(&node_args(&[]), Some(load(&path).unwrap())).unwrap_err();
        assert!(
            error.to_string().contains("max_migration_journal_bytes"),
            "{error}"
        );
    }

    #[test]
    fn the_value_limit_stops_at_what_the_transport_can_carry() {
        let error = resolve(
            &node_args(&[
                "--id",
                "a",
                "--listen",
                "127.0.0.1:1",
                "--max-value-bytes",
                "135267328",
                "--max-migration-journal-bytes",
                "135267328",
                "--max-pending-replication-bytes",
                "135267328",
            ]),
            None,
        )
        .unwrap_err();
        assert!(
            error.to_string().contains(&MAX_VALUE_BYTES.to_string()),
            "{error}"
        );

        // The ceiling itself is a configuration, not only a constant: it takes
        // the two budgets with it and the node starts.
        let config = resolve(
            &node_args(&[
                "--id",
                "a",
                "--listen",
                "127.0.0.1:1",
                "--max-value-bytes",
                &MAX_VALUE_BYTES.to_string(),
                "--max-migration-journal-bytes",
                &MAX_VALUE_BYTES.to_string(),
                "--max-pending-replication-bytes",
                &MAX_VALUE_BYTES.to_string(),
            ]),
            None,
        )
        .unwrap();
        assert_eq!(config.settings.max_value_bytes, MAX_VALUE_BYTES);
    }

    #[test]
    fn invalid_values_and_unknown_keys_are_rejected() {
        let args = node_args(&[
            "--id",
            "a",
            "--listen",
            "127.0.0.1:1",
            "--max-dedup-bytes",
            "0",
        ]);
        assert!(resolve(&args, None).is_err());

        let dir = tempfile::tempdir().unwrap();
        let key = write(dir.path(), "max_receipt_bytes: 1\n");
        let error = format!("{:#}", load::<NodeFile>(&key).unwrap_err());
        assert!(
            error.contains("unknown field `max_receipt_bytes`"),
            "{error}"
        );
    }
}
