//! YAML settings for the `coordinator` command.

use std::{net::SocketAddr, num::NonZeroUsize, path::PathBuf};

use anyhow::Result;
use hashring_coordinator::{
    DEFAULT_PREPUBLICATION_FAILURE_GRACE, DEFAULT_RANGE_MOVE_CONCURRENCY,
    DEFAULT_REPLICA_REPAIR_CONCURRENCY,
};
use hashring_core::topology::{
    DEFAULT_DESIRED_REPLICATION_FACTOR, DEFAULT_MAX_REPLICA_LAG_MILLIS,
    DEFAULT_MINIMUM_ADMITTED_COPIES, DEFAULT_MINIMUM_HEALTHY_FOLLOWERS, Member,
};
use serde::Deserialize;

use super::LoadedFile;
use crate::{CoordinatorArgs, parse_member};

pub const DEFAULT_LISTEN: &str = "127.0.0.1:50050";
pub const DEFAULT_STATE: &str = "./coordinator.redb";
pub const DEFAULT_SEED: u64 = 1;
pub const DEFAULT_VIRTUAL_NODES: u32 = 128;
pub const DEFAULT_MIGRATION_TIMEOUT_MS: u64 = 120_000;

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct CoordinatorFile {
    pub listen: Option<SocketAddr>,
    /// Relative paths are resolved against the directory holding the file.
    pub state: Option<PathBuf>,
    pub migration_timeout_ms: Option<u64>,
    pub range_move_concurrency: Option<NonZeroUsize>,
    pub replica_repair_concurrency: Option<NonZeroUsize>,
    pub prepublication_failure_grace_ms: Option<u64>,
    pub worker_threads: Option<NonZeroUsize>,
    pub seed: Option<u64>,
    pub virtual_nodes: Option<u32>,
    pub desired_replication_factor: Option<u32>,
    pub minimum_admitted_copies: Option<u32>,
    pub minimum_healthy_followers: Option<u32>,
    pub max_replica_lag_ms: Option<u64>,
    pub members: Vec<FileMember>,
}

/// Mirrors [`Member`] but rejects unknown keys, so a misspelled field is an error.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileMember {
    pub node_id: String,
    pub endpoint: String,
}

/// Where the bootstrap membership came from. File members are skipped for an
/// existing store; explicit `--member` flags keep the strict match check.
#[derive(Debug, PartialEq, Eq)]
pub enum MemberSource {
    None,
    Flags,
    File(PathBuf),
}

#[derive(Debug)]
pub struct CoordinatorSettings {
    pub listen: SocketAddr,
    pub state: PathBuf,
    pub migration_timeout_ms: u64,
    pub range_move_concurrency: NonZeroUsize,
    pub replica_repair_concurrency: NonZeroUsize,
    pub prepublication_failure_grace_ms: u64,
    /// `None` takes one worker per available CPU.
    pub worker_threads: Option<NonZeroUsize>,
    pub seed: u64,
    pub virtual_nodes: u32,
    pub desired_replication_factor: u32,
    pub minimum_admitted_copies: u32,
    pub minimum_healthy_followers: u32,
    pub max_replica_lag_ms: u64,
    pub members: Vec<Member>,
    pub member_source: MemberSource,
}

pub fn resolve(
    args: &CoordinatorArgs,
    file: Option<LoadedFile<CoordinatorFile>>,
) -> Result<CoordinatorSettings> {
    let file_dir = file
        .as_ref()
        .map(|loaded| loaded.dir().to_owned())
        .unwrap_or_default();
    let (path, file) = match file {
        Some(loaded) => (Some(loaded.path), loaded.body),
        None => (None, CoordinatorFile::default()),
    };

    let (members, member_source) = if !args.members.is_empty() {
        let members = args
            .members
            .iter()
            .map(|member| parse_member(member))
            .collect::<Result<_>>()?;
        (members, MemberSource::Flags)
    } else if let (Some(path), false) = (&path, file.members.is_empty()) {
        let members = file
            .members
            .into_iter()
            .map(|member| Member {
                node_id: member.node_id,
                endpoint: member.endpoint,
            })
            .collect();
        (members, MemberSource::File(path.clone()))
    } else {
        (Vec::new(), MemberSource::None)
    };

    Ok(CoordinatorSettings {
        listen: args
            .listen
            .or(file.listen)
            .unwrap_or_else(|| DEFAULT_LISTEN.parse().expect("default listen address")),
        state: args
            .state
            .clone()
            .or_else(|| file.state.map(|state| file_dir.join(state)))
            .unwrap_or_else(|| PathBuf::from(DEFAULT_STATE)),
        migration_timeout_ms: args
            .migration_timeout_ms
            .or(file.migration_timeout_ms)
            .unwrap_or(DEFAULT_MIGRATION_TIMEOUT_MS),
        range_move_concurrency: args
            .range_move_concurrency
            .or(file.range_move_concurrency)
            .unwrap_or(
                NonZeroUsize::new(DEFAULT_RANGE_MOVE_CONCURRENCY)
                    .expect("default range move concurrency is positive"),
            ),
        replica_repair_concurrency: args
            .replica_repair_concurrency
            .or(file.replica_repair_concurrency)
            .unwrap_or(
                NonZeroUsize::new(DEFAULT_REPLICA_REPAIR_CONCURRENCY)
                    .expect("default replica repair concurrency is positive"),
            ),
        prepublication_failure_grace_ms: args
            .prepublication_failure_grace_ms
            .or(file.prepublication_failure_grace_ms)
            .unwrap_or(DEFAULT_PREPUBLICATION_FAILURE_GRACE.as_millis() as u64),
        worker_threads: args.worker_threads.or(file.worker_threads),
        seed: args.seed.or(file.seed).unwrap_or(DEFAULT_SEED),
        virtual_nodes: args
            .virtual_nodes
            .or(file.virtual_nodes)
            .unwrap_or(DEFAULT_VIRTUAL_NODES),
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
        members,
        member_source,
    })
}

impl CoordinatorSettings {
    pub fn runtime_workers(&self, cpus: usize) -> usize {
        self.worker_threads.map_or(cpus, NonZeroUsize::get)
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, path::Path};

    use clap::Parser;

    use super::*;
    use crate::{Cli, Command, config::load};

    fn coordinator_args(extra: &[&str]) -> CoordinatorArgs {
        let mut argv = vec!["hashring-rs", "coordinator"];
        argv.extend_from_slice(extra);
        match Cli::try_parse_from(argv).unwrap().command {
            Command::Coordinator(args) => args,
            _ => unreachable!(),
        }
    }

    fn write(dir: &Path, text: &str) -> PathBuf {
        let path = dir.join("cluster.yaml");
        fs::write(&path, text).unwrap();
        path
    }

    #[test]
    fn flags_override_file_and_file_overrides_defaults() {
        let dir = tempfile::tempdir().unwrap();
        let path = write(
            dir.path(),
            "listen: 0.0.0.0:6000\nseed: 9\nvirtual_nodes: 64\nstate: data/c.redb\n\
             replica_repair_concurrency: 6\nworker_threads: 3\n\
             members:\n  - { node_id: node-1, endpoint: http://127.0.0.1:50051 }\n",
        );
        let args = coordinator_args(&["--seed", "7", "--replica-repair-concurrency", "8"]);
        let settings = resolve(&args, Some(load::<CoordinatorFile>(&path).unwrap())).unwrap();
        assert_eq!(settings.replica_repair_concurrency.get(), 8);
        assert_eq!(settings.runtime_workers(24), 3);
        assert_eq!(settings.listen, "0.0.0.0:6000".parse().unwrap());
        assert_eq!(settings.seed, 7);
        assert_eq!(settings.virtual_nodes, 64);
        assert_eq!(settings.state, dir.path().join("data/c.redb"));
        assert_eq!(
            settings.desired_replication_factor,
            DEFAULT_DESIRED_REPLICATION_FACTOR
        );
        assert_eq!(settings.member_source, MemberSource::File(path));
        assert_eq!(settings.members[0].node_id, "node-1");
    }

    #[test]
    fn member_flags_replace_the_file_list() {
        let dir = tempfile::tempdir().unwrap();
        let path = write(
            dir.path(),
            "members:\n  - { node_id: a, endpoint: http://a }\n  - { node_id: b, endpoint: http://b }\n",
        );
        let args = coordinator_args(&["--member", "c=http://c", "--state", "x.redb"]);
        let settings = resolve(&args, Some(load::<CoordinatorFile>(&path).unwrap())).unwrap();
        assert_eq!(settings.member_source, MemberSource::Flags);
        assert_eq!(settings.members.len(), 1);
        assert_eq!(settings.members[0].node_id, "c");
        assert_eq!(settings.state, PathBuf::from("x.redb"));
    }

    #[test]
    fn no_file_keeps_the_flag_defaults() {
        let settings = resolve(&coordinator_args(&[]), None).unwrap();
        assert_eq!(settings.listen, DEFAULT_LISTEN.parse().unwrap());
        assert_eq!(settings.state, PathBuf::from(DEFAULT_STATE));
        assert_eq!(
            settings.range_move_concurrency.get(),
            DEFAULT_RANGE_MOVE_CONCURRENCY
        );
        assert_eq!(settings.member_source, MemberSource::None);
        assert_eq!(
            settings.replica_repair_concurrency.get(),
            DEFAULT_REPLICA_REPAIR_CONCURRENCY
        );
        assert_eq!(settings.prepublication_failure_grace_ms, 30_000);
        assert_eq!(settings.runtime_workers(24), 24);
    }

    #[test]
    fn unknown_keys_are_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let top_level = write(dir.path(), "replication_factor: 3\n");
        let error = format!("{:#}", load::<CoordinatorFile>(&top_level).unwrap_err());
        assert!(
            error.contains("unknown field `replication_factor`"),
            "{error}"
        );

        let in_member = write(
            dir.path(),
            "members:\n  - { node_id: a, endpoint: http://a, port: 1 }\n",
        );
        let error = format!("{:#}", load::<CoordinatorFile>(&in_member).unwrap_err());
        assert!(error.contains("unknown field `port`"), "{error}");
    }
}
