//! YAML settings for the `experiment` command.

use std::{num::NonZeroUsize, path::PathBuf};

use anyhow::{Result, bail};
use hashring_coordinator::{
    DEFAULT_PREPUBLICATION_FAILURE_GRACE, DEFAULT_RANGE_MOVE_CONCURRENCY,
    DEFAULT_REPLICA_REPAIR_CONCURRENCY,
};
use hashring_core::topology::{
    DEFAULT_DESIRED_REPLICATION_FACTOR, DEFAULT_LEASE_DURATION_MILLIS,
    DEFAULT_MAX_REPLICA_LAG_MILLIS, DEFAULT_MINIMUM_ADMITTED_COPIES,
    DEFAULT_MINIMUM_HEALTHY_FOLLOWERS, DEFAULT_RECEIPT_RETENTION_MILLIS,
};
use hashring_experiment::{ExperimentConfig, ExperimentMode, ScalingLoadMode, ScalingScenario};
use serde::Deserialize;

use super::{LoadedFile, node};
use crate::{ExperimentArgs, ExperimentModeArg, NodeTuningArgs, PolicyArg};

pub const DEFAULT_VALUE_BYTES: usize = 128;
pub const DEFAULT_CONCURRENCY: usize = 64;
pub const DEFAULT_SEED: u64 = 1;
pub const DEFAULT_VIRTUAL_NODES: u32 = 128;
pub const DEFAULT_OPERATION_TIMEOUT_MS: u64 = 30_000;
pub const DEFAULT_MIGRATION_TIMEOUT_MS: u64 = 600_000;
pub const DEFAULT_PRE_PUBLISH_DELAY_MS: u64 = 250;
/// The keys the `latency` mode writes before its load starts.
pub const DEFAULT_PRELOAD_KEYS: u64 = 100_000;
/// The seconds at the start of every `latency` stage that are not counted.
pub const DEFAULT_WARMUP_SECONDS: u64 = 10;
/// The seconds of every `latency` stage that are counted.
pub const DEFAULT_DURATION_SECONDS: u64 = 60;
/// The `GET:PUT:DELETE` share of the `latency` mode's requests.
pub const DEFAULT_MIX: &str = "90:9:1";
/// How the `latency` mode picks the key of a request.
pub const DEFAULT_KEY_DISTRIBUTION: &str = "zipf:0.99";
/// How many requests the `latency` mode may have in flight at once.
pub const DEFAULT_MAX_IN_FLIGHT: usize = 4096;
/// The one-shot write and delete operations a `scaling` run offers per second.
pub const DEFAULT_SCALING_WRITE_RATE: u64 = 200;
/// How long the one-shot pools of one `scaling` transition must last.
pub const DEFAULT_SCALING_POOL_SECONDS: u64 = 120;
/// How long a `scaling` run keeps the load running after a change.
pub const DEFAULT_SCALING_POST_SECONDS: u64 = 10;
/// How many consecutive owner failures an `availability` run injects.
pub const DEFAULT_AVAILABILITY_ROUNDS: u32 = 1;
/// The members an `availability` run starts with, before it kills any.
pub const DEFAULT_AVAILABILITY_INITIAL_NODES: usize = 3;

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
    pub replica_repair_concurrency: Option<NonZeroUsize>,
    pub prepublication_failure_grace_ms: Option<u64>,
    pub pre_publish_delay_ms: Option<u64>,
    pub require_clean_source: Option<bool>,
    pub verbose: Option<bool>,
    pub desired_replication_factor: Option<u32>,
    pub minimum_admitted_copies: Option<u32>,
    pub minimum_healthy_followers: Option<u32>,
    pub max_replica_lag_ms: Option<u64>,
    pub receipt_retention_ms: Option<u64>,
    pub lease_duration_ms: Option<u64>,
    pub policy: Option<PolicyArg>,
    pub worker_threads: Option<NonZeroUsize>,
    pub coordinator_worker_threads: Option<NonZeroUsize>,
    pub preload_keys: Option<u64>,
    pub target_rates: Option<Vec<u64>>,
    pub warmup_seconds: Option<u64>,
    pub duration_seconds: Option<u64>,
    pub mix: Option<String>,
    pub key_distribution: Option<String>,
    pub max_in_flight: Option<usize>,
    pub availability_rounds: Option<u32>,
    pub scaling_scenario: Option<ScalingScenario>,
    pub scaling_load_mode: Option<ScalingLoadMode>,
    pub scaling_open_rate: Option<u64>,
    pub initial_nodes: Option<usize>,
    pub scaling_write_rate: Option<u64>,
    pub scaling_pool_seconds: Option<u64>,
    pub scaling_post_seconds: Option<u64>,
    pub max_dedup_bytes: Option<usize>,
    pub max_value_bytes: Option<usize>,
    pub max_pending_replication_bytes: Option<usize>,
    pub max_migration_journal_bytes: Option<usize>,
    pub replication_stream_queue_capacity: Option<NonZeroUsize>,
    pub required_ack_timeout_ms: Option<u64>,
    pub replication_rpc_timeout_ms: Option<u64>,
    pub replication_batch_linger_ms: Option<u64>,
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
    let node_settings = node::settings(
        &args.node_tuning,
        &NodeTuningArgs {
            max_dedup_bytes: file.max_dedup_bytes,
            max_value_bytes: file.max_value_bytes,
            max_pending_replication_bytes: file.max_pending_replication_bytes,
            max_migration_journal_bytes: file.max_migration_journal_bytes,
            replication_stream_queue_capacity: file.replication_stream_queue_capacity,
            required_ack_timeout_ms: file.required_ack_timeout_ms,
            replication_rpc_timeout_ms: file.replication_rpc_timeout_ms,
            replication_batch_linger_ms: file.replication_batch_linger_ms,
        },
    );
    node_settings.validate()?;

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
    let scaling_scenario = args
        .scaling_scenario
        .map(ScalingScenario::from)
        .or(file.scaling_scenario)
        .unwrap_or(ScalingScenario::RoundTrip);
    let availability_rounds = args
        .availability_rounds
        .or(file.availability_rounds)
        .unwrap_or(DEFAULT_AVAILABILITY_ROUNDS);
    let node_count = args.nodes.or(file.nodes).unwrap_or(match mode {
        ExperimentMode::Scaling if scaling_scenario == ScalingScenario::Replace => 11,
        ExperimentMode::Correctness
        | ExperimentMode::Performance
        | ExperimentMode::Latency
        | ExperimentMode::Scaling => 10,
        // Every availability round needs a spare node to replace the one it
        // kills, so the run's node count grows with the number of rounds.
        ExperimentMode::Availability => {
            DEFAULT_AVAILABILITY_INITIAL_NODES + availability_rounds as usize
        }
    });
    let policy = args.policy.or(file.policy).unwrap_or(match mode {
        ExperimentMode::Availability => PolicyArg::FirstSuccessor,
        _ => PolicyArg::OwnerOnly,
    });
    let initial_nodes = args.initial_nodes.or(file.initial_nodes).or({
        match mode {
            ExperimentMode::Scaling => Some(match scaling_scenario {
                ScalingScenario::Shrink | ScalingScenario::Control => node_count,
                ScalingScenario::Grow | ScalingScenario::RoundTrip => node_count.saturating_sub(1),
                ScalingScenario::Replace => node_count.saturating_sub(1),
                ScalingScenario::Churn => node_count.saturating_sub(3),
            }),
            ExperimentMode::Correctness => Some(node_count.saturating_sub(1)),
            _ => None,
        }
    });
    // The `latency` mode writes its keys before the load starts and the load
    // reads and writes the same keys, so the preload count is the key space
    // of the run. `--keys` names the same count there.
    let preload_keys = args.preload_keys.or(file.preload_keys);
    let key_count = match mode {
        ExperimentMode::Latency => preload_keys
            .or(args.keys)
            .or(file.keys)
            .unwrap_or(DEFAULT_PRELOAD_KEYS),
        _ => args.keys.or(file.keys).unwrap_or(match mode {
            ExperimentMode::Correctness => 20_000,
            ExperimentMode::Performance => 1_000_000,
            ExperimentMode::Availability => 1_000,
            ExperimentMode::Scaling => 100_000,
            ExperimentMode::Latency => DEFAULT_PRELOAD_KEYS,
        }),
    };
    let target_rates = if args.target_rates.is_empty() {
        file.target_rates.clone().unwrap_or_default()
    } else {
        args.target_rates.clone()
    };

    Ok(ExperimentConfig {
        mode,
        output_dir,
        node_count,
        key_count,
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
        replica_repair_concurrency: args
            .replica_repair_concurrency
            .or(file.replica_repair_concurrency)
            .map_or(DEFAULT_REPLICA_REPAIR_CONCURRENCY, NonZeroUsize::get),
        prepublication_failure_grace_ms: args
            .prepublication_failure_grace_ms
            .or(file.prepublication_failure_grace_ms)
            .unwrap_or(DEFAULT_PREPUBLICATION_FAILURE_GRACE.as_millis() as u64),
        pre_publish_delay_ms: args
            .pre_publish_delay_ms
            .or(file.pre_publish_delay_ms)
            .unwrap_or(match mode {
                // The delay runs after the writes have been paused, so a
                // non-zero default would be measured as migration latency.
                ExperimentMode::Scaling => 0,
                _ => DEFAULT_PRE_PUBLISH_DELAY_MS,
            }),
        require_clean_source: args.require_clean_source
            || file.require_clean_source.unwrap_or(false),
        verbose: args.verbose || file.verbose.unwrap_or(false),
        worker_threads: worker_threads(
            args.worker_threads.or(file.worker_threads),
            node_count,
            cpus,
        ),
        coordinator_worker_threads: args
            .coordinator_worker_threads
            .or(file.coordinator_worker_threads)
            .map(NonZeroUsize::get),
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
        max_replica_lag_ms: args
            .max_replica_lag_ms
            .or(file.max_replica_lag_ms)
            .unwrap_or(DEFAULT_MAX_REPLICA_LAG_MILLIS),
        receipt_retention_ms: args
            .receipt_retention_ms
            .or(file.receipt_retention_ms)
            .unwrap_or(DEFAULT_RECEIPT_RETENTION_MILLIS),
        lease_duration_ms: args
            .lease_duration_ms
            .or(file.lease_duration_ms)
            .unwrap_or(DEFAULT_LEASE_DURATION_MILLIS),
        write_ack_policy: policy.into(),
        preload_keys: match mode {
            ExperimentMode::Latency => Some(key_count),
            _ => None,
        },
        target_rates,
        warmup_seconds: args
            .warmup_seconds
            .or(file.warmup_seconds)
            .unwrap_or(DEFAULT_WARMUP_SECONDS),
        duration_seconds: args
            .duration_seconds
            .or(file.duration_seconds)
            .unwrap_or(DEFAULT_DURATION_SECONDS),
        mix: args
            .mix
            .clone()
            .or(file.mix)
            .unwrap_or(DEFAULT_MIX.to_owned()),
        key_distribution: args
            .key_distribution
            .clone()
            .or(file.key_distribution)
            .unwrap_or(DEFAULT_KEY_DISTRIBUTION.to_owned()),
        max_in_flight: args
            .max_in_flight
            .or(file.max_in_flight)
            .unwrap_or(DEFAULT_MAX_IN_FLIGHT),
        availability_rounds,
        scaling_scenario,
        scaling_load_mode: args
            .scaling_load_mode
            .map(ScalingLoadMode::from)
            .or(file.scaling_load_mode)
            .unwrap_or(ScalingLoadMode::Closed),
        scaling_open_rate: args
            .scaling_open_rate
            .or(file.scaling_open_rate)
            .unwrap_or(0),
        initial_nodes,
        scaling_write_rate: args
            .scaling_write_rate
            .or(file.scaling_write_rate)
            .unwrap_or(DEFAULT_SCALING_WRITE_RATE),
        scaling_pool_seconds: args
            .scaling_pool_seconds
            .or(file.scaling_pool_seconds)
            .unwrap_or(DEFAULT_SCALING_POOL_SECONDS),
        scaling_post_seconds: args
            .scaling_post_seconds
            .or(file.scaling_post_seconds)
            .unwrap_or(DEFAULT_SCALING_POST_SECONDS),
        node: node::options(&node_settings),
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
            Command::Experiment(args) => *args,
            _ => unreachable!(),
        }
    }

    fn write(dir: &Path, text: &str) -> PathBuf {
        let path = dir.join("experiment.yaml");
        fs::write(&path, text).unwrap();
        path
    }

    #[test]
    fn scaling_defaults_are_a_round_trip_without_a_publish_delay() {
        let args = experiment_args(&["--mode", "scaling", "--output", "out"]);
        let config = resolve(&args, None, 24).unwrap();
        assert!(matches!(config.mode, ExperimentMode::Scaling));
        assert!(matches!(
            config.scaling_scenario,
            ScalingScenario::RoundTrip
        ));
        assert_eq!(config.initial_nodes, Some(9));
        assert_eq!(config.key_count, 100_000);
        assert_eq!(config.pre_publish_delay_ms, 0);
        assert_eq!(config.scaling_write_rate, 200);
        assert_eq!(config.scaling_pool_seconds, 120);
        assert_eq!(config.scaling_post_seconds, 10);
        assert!(matches!(config.scaling_load_mode, ScalingLoadMode::Closed));
        assert_eq!(config.scaling_open_rate, 0);

        let args = experiment_args(&[
            "--mode",
            "scaling",
            "--scaling-scenario",
            "replace",
            "--output",
            "out",
        ]);
        let config = resolve(&args, None, 24).unwrap();
        assert_eq!(config.node_count, 11);
        assert_eq!(config.initial_nodes, Some(10));

        let args = experiment_args(&[
            "--mode",
            "scaling",
            "--scaling-scenario",
            "control",
            "--scaling-load-mode",
            "open",
            "--scaling-open-rate",
            "1000",
            "--output",
            "out",
        ]);
        let config = resolve(&args, None, 24).unwrap();
        assert_eq!(config.initial_nodes, Some(10));
        assert!(matches!(config.scaling_load_mode, ScalingLoadMode::Open));
        assert_eq!(config.scaling_open_rate, 1000);
    }

    #[test]
    fn availability_defaults_to_one_round_and_grows_with_the_rounds() {
        let args = experiment_args(&["--mode", "availability", "--output", "out"]);
        let config = resolve(&args, None, 24).unwrap();
        assert_eq!(config.availability_rounds, 1);
        assert_eq!(config.node_count, 4);

        // Every round needs a spare node to replace the one it kills, so the
        // run's node count grows with the number of rounds.
        let args = experiment_args(&[
            "--mode",
            "availability",
            "--availability-rounds",
            "3",
            "--output",
            "out",
        ]);
        let config = resolve(&args, None, 24).unwrap();
        assert_eq!(config.availability_rounds, 3);
        assert_eq!(config.node_count, 6);

        let args = experiment_args(&[
            "--mode",
            "availability",
            "--availability-rounds",
            "3",
            "--nodes",
            "9",
            "--output",
            "out",
        ]);
        let config = resolve(&args, None, 24).unwrap();
        assert_eq!(config.node_count, 9);
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
        assert_eq!(
            config.replica_repair_concurrency,
            DEFAULT_REPLICA_REPAIR_CONCURRENCY
        );
        assert_eq!(config.max_replica_lag_ms, DEFAULT_MAX_REPLICA_LAG_MILLIS);
        assert_eq!(config.coordinator_worker_threads, None);
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
    fn node_tuning_is_resolved_for_every_node() {
        let dir = tempfile::tempdir().unwrap();
        let path = write(
            dir.path(),
            "max_dedup_bytes: 4096\nreplication_stream_queue_capacity: 8\n",
        );
        let args = experiment_args(&["--output", "out", "--max-dedup-bytes", "8192"]);
        let config = resolve(&args, Some(load(&path).unwrap()), 24).unwrap();
        assert_eq!(config.node.max_dedup_bytes, 8192);
        assert_eq!(config.node.replication_stream_queue_capacity, 8);
        assert_eq!(
            config.node.max_pending_replication_bytes,
            hashring_node::DEFAULT_MAX_PENDING_REPLICATION_BYTES
        );
        assert_eq!(config.node.required_ack_timeout_ms, 2_000);

        let args = experiment_args(&["--output", "out", "--required-ack-timeout-ms", "0"]);
        assert!(resolve(&args, None, 24).is_err());
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
