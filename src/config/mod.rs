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
