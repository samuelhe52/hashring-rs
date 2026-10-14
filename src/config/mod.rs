//! YAML settings files for the long-running commands.
//!
//! Every key is optional and named after the flag it mirrors. A flag given on
//! the command line wins over the file, and the file wins over the built-in
//! default. Unknown keys are rejected.

use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use serde::de::DeserializeOwned;

pub mod coordinator;
pub mod experiment;
pub mod node;

#[derive(Debug)]
pub struct LoadedFile<T> {
    pub path: PathBuf,
    pub body: T,
}

impl<T> LoadedFile<T> {
    /// Relative paths in a file are resolved against the directory holding it.
    pub fn dir(&self) -> &Path {
        self.path.parent().unwrap_or(Path::new(""))
    }
}

pub fn load<T: DeserializeOwned>(path: &Path) -> Result<LoadedFile<T>> {
    let text = fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    let body =
        serde_yaml::from_str(&text).with_context(|| format!("parsing {}", path.display()))?;
    Ok(LoadedFile {
        path: path.to_owned(),
        body,
    })
}

#[cfg(test)]
mod tests {
    use clap::CommandFactory;

    use super::*;
    use crate::Cli;

    const CONFIGURATION_DOC: &str = include_str!("../../docs/configuration.md");

    /// The visible flags of a subcommand, without `--config` and `--help`.
    fn flags(subcommand: &str) -> Vec<String> {
        let cli = Cli::command();
        let command = cli.find_subcommand(subcommand).unwrap();
        command
            .get_arguments()
            .filter(|arg| !arg.is_hide_set())
            .filter_map(|arg| arg.get_long())
            .filter(|long| !matches!(*long, "config" | "help"))
            .map(str::to_owned)
            .collect()
    }

    fn example(name: &str) -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("examples")
            .join(name)
    }

    #[test]
    fn every_flag_is_documented() {
        for subcommand in ["coordinator", "node", "experiment"] {
            for flag in flags(subcommand) {
                assert!(
                    CONFIGURATION_DOC.contains(&format!("`--{flag}`")),
                    "docs/configuration.md does not document `{subcommand} --{flag}`"
                );
            }
        }
    }

    #[test]
    fn examples_load_and_list_every_key() {
        load::<coordinator::CoordinatorFile>(&example("coordinator.yaml")).unwrap();
        load::<node::NodeFile>(&example("node.yaml")).unwrap();
        load::<experiment::ExperimentFile>(&example("experiment.yaml")).unwrap();
        for (subcommand, file) in [
            ("coordinator", "coordinator.yaml"),
            ("node", "node.yaml"),
            ("experiment", "experiment.yaml"),
        ] {
            let text = fs::read_to_string(example(file)).unwrap();
            for flag in flags(subcommand) {
                let key = match flag.as_str() {
                    "member" => "members".to_owned(),
                    flag => flag.replace('-', "_"),
                };
                // Commented-out keys count: some have no fixed default.
                assert!(
                    text.lines().any(|line| line
                        .trim_start_matches(['#', ' '])
                        .starts_with(&format!("{key}:"))),
                    "examples/{file} does not list `{key}`"
                );
            }
        }
    }
}
