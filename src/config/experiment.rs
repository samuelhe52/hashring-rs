//! YAML settings for the `experiment` command.

use std::{num::NonZeroUsize, path::PathBuf};

use anyhow::{Result, bail};
use hashring_coordinator::DEFAULT_RANGE_MOVE_CONCURRENCY;
use hashring_core::topology::{
    DEFAULT_DESIRED_REPLICATION_FACTOR, DEFAULT_MINIMUM_ADMITTED_COPIES,
    DEFAULT_MINIMUM_HEALTHY_FOLLOWERS,
};
use hashring_experiment::{ExperimentConfig, ExperimentMode};
use serde::Deserialize;

use super::LoadedFile;
use crate::{ExperimentArgs, ExperimentModeArg, PolicyArg};

pub const DEFAULT_VALUE_BYTES: usize = 128;
pub const DEFAULT_CONCURRENCY: usize = 64;
pub const DEFAULT_SEED: u64 = 1;
pub const DEFAULT_VIRTUAL_NODES: u32 = 128;
pub const DEFAULT_OPERATION_TIMEOUT_MS: u64 = 30_000;
pub const DEFAULT_MIGRATION_TIMEOUT_MS: u64 = 600_000;
pub const DEFAULT_PRE_PUBLISH_DELAY_MS: u64 = 250;

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ExperimentFile {
    pub mode: Option<ExperimentModeArg>,
    /// Relative paths are resolved against the directory holding the file.
    pub output: Option<PathBuf>,
    pub nodes: Option<usize>,
    pub keys: Option<u64>,
    pub value_bytes: Option<usize>,
    pub concurrency: Option<usize>,
    pub seed: Option<u64>,
    pub virtual_nodes: Option<u32>,
    pub operation_timeout_ms: Option<u64>,
    pub migration_timeout_ms: Option<u64>,
    pub range_move_concurrency: Option<usize>,
    pub pre_publish_delay_ms: Option<u64>,
    pub require_clean_source: Option<bool>,
    pub verbose: Option<bool>,
    pub desired_replication_factor: Option<u32>,
    pub minimum_admitted_copies: Option<u32>,
    pub minimum_healthy_followers: Option<u32>,
    pub policy: Option<PolicyArg>,
    pub worker_threads: Option<NonZeroUsize>,
}

/// Resolves the run's configuration. Boolean flags can only turn a setting on,
/// so `--verbose` wins over `verbose: false` but cannot undo `verbose: true`.
pub fn resolve(
    args: &ExperimentArgs,
    file: Option<LoadedFile<ExperimentFile>>,
    cpus: usize,
) -> Result<ExperimentConfig> {
    let file_dir = file
        .as_ref()
        .map(|loaded| loaded.dir().to_owned())
        .unwrap_or_default();
    let file = file.map(|loaded| loaded.body).unwrap_or_default();

    let Some(output_dir) = args
        .output
        .clone()
        .or_else(|| file.output.map(|output| file_dir.join(output)))
    else {
        bail!("an output directory is required: pass --output or set `output` in the config file");
    };
    let mode: ExperimentMode = args
        .mode
        .or(file.mode)
        .unwrap_or(ExperimentModeArg::Correctness)
        .into();
    let node_count = args.nodes.or(file.nodes).unwrap_or(match mode {
        ExperimentMode::Correctness | ExperimentMode::Performance => 10,
        ExperimentMode::Availability => 4,
    });
    let policy = args.policy.or(file.policy).unwrap_or(match mode {
        ExperimentMode::Availability => PolicyArg::FirstSuccessor,
        _ => PolicyArg::OwnerOnly,
    });

    Ok(ExperimentConfig {
        mode,
        output_dir,
        node_count,
        key_count: args.keys.or(file.keys).unwrap_or(match mode {
            ExperimentMode::Correctness => 20_000,
            ExperimentMode::Performance => 1_000_000,
            ExperimentMode::Availability => 1_000,
        }),
        value_bytes: args
            .value_bytes
            .or(file.value_bytes)
            .unwrap_or(DEFAULT_VALUE_BYTES),
        concurrency: args
            .concurrency
            .or(file.concurrency)
            .unwrap_or(DEFAULT_CONCURRENCY),
        hash_seed: args.seed.or(file.seed).unwrap_or(DEFAULT_SEED),
        virtual_nodes: args
            .virtual_nodes
            .or(file.virtual_nodes)
            .unwrap_or(DEFAULT_VIRTUAL_NODES),
        operation_timeout_ms: args
            .operation_timeout_ms
            .or(file.operation_timeout_ms)
            .unwrap_or(DEFAULT_OPERATION_TIMEOUT_MS),
        migration_timeout_ms: args
            .migration_timeout_ms
            .or(file.migration_timeout_ms)
            .unwrap_or(DEFAULT_MIGRATION_TIMEOUT_MS),
        range_move_concurrency: args
            .range_move_concurrency
            .or(file.range_move_concurrency)
            .unwrap_or(DEFAULT_RANGE_MOVE_CONCURRENCY),
        pre_publish_delay_ms: args
            .pre_publish_delay_ms
            .or(file.pre_publish_delay_ms)
            .unwrap_or(DEFAULT_PRE_PUBLISH_DELAY_MS),
        require_clean_source: args.require_clean_source
            || file.require_clean_source.unwrap_or(false),
        verbose: args.verbose || file.verbose.unwrap_or(false),
        worker_threads: worker_threads(
            args.worker_threads.or(file.worker_threads),
            node_count,
            cpus,
        ),
        desired_replication_factor: args
            .desired_replication_factor
            .or(file.desired_replication_factor)
            .unwrap_or(DEFAULT_DESIRED_REPLICATION_FACTOR),
        minimum_admitted_copies: args
            .minimum_admitted_copies
            .or(file.minimum_admitted_copies)
            .unwrap_or(DEFAULT_MINIMUM_ADMITTED_COPIES),
        minimum_healthy_followers: args
            .minimum_healthy_followers
            .or(file.minimum_healthy_followers)
            .unwrap_or(DEFAULT_MINIMUM_HEALTHY_FOLLOWERS),
        write_ack_policy: policy.into(),
    })
}

/// The `experiment` command starts the coordinator, every data node and its own
/// load driver on one host, so the driver and each node get the host budget
/// divided by the number of processes. A per-role A/B showed both the driver
/// and the nodes slow down when uncapped, while the coordinator does not, so the
/// coordinator keeps its own per-CPU default.
fn worker_threads(requested: Option<NonZeroUsize>, peak_nodes: usize, cpus: usize) -> usize {
    if let Some(workers) = requested {
        return workers.get();
    }
    // Driver, coordinator, and every data node share the host.
    let processes = peak_nodes + 2;
    (cpus / processes).clamp(2.min(cpus), cpus)
}

#[cfg(test)]
mod tests {
    use std::{fs, path::Path};

    use clap::Parser;
    use hashring_core::topology::WriteAckPolicy;

    use super::*;
    use crate::{Cli, Command, config::load};

    fn experiment_args(extra: &[&str]) -> ExperimentArgs {
        let mut argv = vec!["hashring-rs", "experiment"];
        argv.extend_from_slice(extra);
        match Cli::try_parse_from(argv).unwrap().command {
            Command::Experiment(args) => args,
            _ => unreachable!(),
        }
    }

    fn write(dir: &Path, text: &str) -> PathBuf {
        let path = dir.join("experiment.yaml");
        fs::write(&path, text).unwrap();
        path
    }

    #[test]
    fn flags_override_file_and_file_overrides_mode_defaults() {
        let dir = tempfile::tempdir().unwrap();
        let path = write(
            dir.path(),
            "mode: availability\noutput: results/a\nkeys: 500\nseed: 9\n\
             require_clean_source: true\n",
        );
        let args = experiment_args(&["--seed", "7", "--policy", "all-replicas"]);
        let config = resolve(&args, Some(load(&path).unwrap()), 24).unwrap();
        assert!(matches!(config.mode, ExperimentMode::Availability));
        assert_eq!(config.output_dir, dir.path().join("results/a"));
        assert_eq!(config.key_count, 500);
        assert_eq!(config.hash_seed, 7);
        // Mode-dependent defaults follow the mode chosen in the file.
        assert_eq!(config.node_count, 4);
        assert_eq!(config.worker_threads, 4);
        assert!(matches!(
            config.write_ack_policy,
            WriteAckPolicy::AllReplicas
        ));
        assert!(config.require_clean_source);
        assert!(!config.verbose);
    }

    #[test]
    fn output_flag_is_not_relative_to_the_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = write(dir.path(), "output: from-file\npolicy: first-successor\n");
        let args = experiment_args(&["--output", "from-flag"]);
        let config = resolve(&args, Some(load(&path).unwrap()), 1).unwrap();
        assert_eq!(config.output_dir, PathBuf::from("from-flag"));
        assert!(matches!(
            config.write_ack_policy,
            WriteAckPolicy::FirstSuccessor
        ));
    }

    #[test]
    fn output_is_required() {
        let error = resolve(&experiment_args(&[]), None, 1).unwrap_err();
        assert!(error.to_string().contains("--output"), "{error}");
    }

    #[test]
    fn no_file_keeps_the_flag_defaults() {
        let config = resolve(&experiment_args(&["--output", "out"]), None, 24).unwrap();
        assert!(matches!(config.mode, ExperimentMode::Correctness));
        assert_eq!(config.node_count, 10);
        assert_eq!(config.key_count, 20_000);
        assert_eq!(
            config.range_move_concurrency,
            DEFAULT_RANGE_MOVE_CONCURRENCY
        );
        assert!(matches!(config.write_ack_policy, WriteAckPolicy::OwnerOnly));
    }

    #[test]
    fn worker_threads_split_the_host_unless_overridden() {
        let args = experiment_args(&["--output", "out"]);
        assert_eq!(resolve(&args, None, 1).unwrap().worker_threads, 1);
        assert_eq!(resolve(&args, None, 24).unwrap().worker_threads, 2);

        let dir = tempfile::tempdir().unwrap();
        let path = write(dir.path(), "worker_threads: 6\n");
        let config = resolve(&args, Some(load(&path).unwrap()), 24).unwrap();
        assert_eq!(config.worker_threads, 6);
        let args = experiment_args(&["--output", "out", "--worker-threads", "5"]);
        let config = resolve(&args, Some(load(&path).unwrap()), 24).unwrap();
        assert_eq!(config.worker_threads, 5);

        let zero = write(dir.path(), "worker_threads: 0\n");
        assert!(load::<ExperimentFile>(&zero).is_err());
    }

    #[test]
    fn unknown_keys_and_values_are_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let key = write(dir.path(), "node_count: 3\n");
        let error = format!("{:#}", load::<ExperimentFile>(&key).unwrap_err());
        assert!(error.contains("unknown field `node_count`"), "{error}");

        let value = write(dir.path(), "policy: FirstSuccessor\n");
        let error = format!("{:#}", load::<ExperimentFile>(&value).unwrap_err());
        assert!(
            error.contains("unknown variant `FirstSuccessor`"),
            "{error}"
        );
    }
}
