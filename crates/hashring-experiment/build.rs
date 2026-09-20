use std::{
    env, fs,
    path::{Path, PathBuf},
    process::Command,
};

const SOURCE_PATHS: &[&str] = &[
    "Cargo.toml",
    "Cargo.lock",
    "PLAN.md",
    "README.md",
    "src",
    "crates",
    "tests",
];

fn main() {
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR");
    let workspace = Path::new(&manifest_dir)
        .join("../..")
        .canonicalize()
        .expect("canonical workspace path");
    emit("HASHRING_BUILD_SOURCE_ROOT", workspace.to_string_lossy());
    emit(
        "HASHRING_BUILD_GIT_COMMIT",
        command_output(&workspace, "git", &["rev-parse", "HEAD"])
            .unwrap_or_else(|| "unknown".into()),
    );
    emit(
        "HASHRING_BUILD_GIT_DIRTY",
        command_output(&workspace, "git", &["status", "--porcelain"])
            .map(|output| (!output.is_empty()).to_string())
            .unwrap_or_else(|| "unknown".into()),
    );
    emit(
        "HASHRING_BUILD_RUSTC",
        command_output(&workspace, "rustc", &["--version"]).unwrap_or_else(|| "unknown".into()),
    );
    emit(
        "HASHRING_BUILD_PROFILE",
        env::var("PROFILE").unwrap_or_else(|_| "unknown".into()),
    );
    emit(
        "HASHRING_BUILD_TARGET",
        env::var("TARGET").unwrap_or_else(|_| "unknown".into()),
    );
    emit(
        "HASHRING_BUILD_SOURCE_TREE_BLAKE3",
        source_tree_digest(&workspace).expect("hashing workspace source inputs"),
    );

    println!(
        "cargo:rerun-if-changed={}",
        workspace.join(".git/HEAD").display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        workspace.join(".git/index").display()
    );
    for path in SOURCE_PATHS {
        println!("cargo:rerun-if-changed={}", workspace.join(path).display());
    }
}

fn emit(name: &str, value: impl AsRef<str>) {
    println!("cargo:rustc-env={name}={}", value.as_ref());
}

fn source_tree_digest(workspace: &Path) -> std::io::Result<String> {
    let mut files = Vec::new();
    for path in SOURCE_PATHS {
        collect_files(&workspace.join(path), &mut files)?;
    }
    files.sort();
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"hashring-rs:source-tree:v1\0");
    for path in files {
        let relative = path.strip_prefix(workspace).unwrap_or(&path);
        let encoded = relative.to_string_lossy();
        let contents = fs::read(&path)?;
        hasher.update(&(encoded.len() as u64).to_be_bytes());
        hasher.update(encoded.as_bytes());
        hasher.update(&(contents.len() as u64).to_be_bytes());
        hasher.update(&contents);
    }
    Ok(hasher.finalize().to_hex().to_string())
}

fn collect_files(path: &Path, files: &mut Vec<PathBuf>) -> std::io::Result<()> {
    if path.is_file() {
        files.push(path.to_owned());
    } else if path.is_dir() {
        for entry in fs::read_dir(path)? {
            collect_files(&entry?.path(), files)?;
        }
    }
    Ok(())
}

fn command_output(cwd: &Path, program: &str, arguments: &[&str]) -> Option<String> {
    let output = Command::new(program)
        .current_dir(cwd)
        .args(arguments)
        .output()
        .ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_owned())
}
