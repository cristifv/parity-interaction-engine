//! The healthy and drained files of ARCHITECTURE.md §7.3:
//! `/tmp/parity-interaction-engine.healthy` and `.drained`.

use std::io::ErrorKind;
use std::path::{Path, PathBuf};

/// The service name the files are named after.
pub const SERVICE: &str = "parity-interaction-engine";
/// The state directory of §7.3.
pub const DEFAULT_DIR: &str = "/tmp";

/// The two files in one directory.
#[derive(Debug, Clone)]
pub struct StateFiles {
    dir: PathBuf,
}

impl StateFiles {
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self { dir: dir.into() }
    }

    pub fn healthy(&self) -> PathBuf {
        self.dir.join(format!("{SERVICE}.healthy"))
    }

    pub fn drained(&self) -> PathBuf {
        self.dir.join(format!("{SERVICE}.drained"))
    }

    /// Both files removed ("files left by an earlier run never count"). "Not found" is fine.
    pub fn remove_stale(&self) -> std::io::Result<()> {
        for p in [self.healthy(), self.drained()] {
            remove_sync(&p)?;
        }
        Ok(())
    }

    pub async fn write_healthy(&self) -> std::io::Result<()> {
        tokio::fs::write(self.healthy(), b"").await
    }

    pub async fn remove_healthy(&self) -> std::io::Result<()> {
        match tokio::fs::remove_file(self.healthy()).await {
            Err(e) if e.kind() != ErrorKind::NotFound => Err(e),
            _ => Ok(()),
        }
    }

    pub async fn write_drained(&self) -> std::io::Result<()> {
        tokio::fs::write(self.drained(), b"").await
    }

    /// Synchronous removal of the healthy file, for paths that can't await (a dead gate).
    pub fn remove_healthy_sync(&self) -> std::io::Result<()> {
        remove_sync(&self.healthy())
    }
}

fn remove_sync(p: &Path) -> std::io::Result<()> {
    match std::fs::remove_file(p) {
        Err(e) if e.kind() != ErrorKind::NotFound => Err(e),
        _ => Ok(()),
    }
}
