//! Test support for every crate that logs (ADR 0001 §7.2, F6): the **production** subscriber
//! over an in-memory writer, and the canary check. Behind the `testing` feature.

use crate::logging::{Identity, layer};
use serde_json::Value;
use std::io::{self, Write};
use std::sync::{Arc, Mutex, PoisonError};
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::layer::SubscriberExt;

/// A fixed identity for tests.
pub fn identity() -> Identity {
    Identity {
        service: "parity-interaction-engine".into(),
        version: "0.0.0-test".into(),
        zone: "local".into(),
        bin: "bin-test".into(),
        instance: "test-1".into(),
    }
}

/// An in-memory writer shared by every line.
#[derive(Clone, Default)]
pub struct Buffer(Arc<Mutex<Vec<u8>>>);

impl Buffer {
    /// What was written, as lines.
    pub fn lines(&self) -> Vec<String> {
        let bytes = self
            .0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        String::from_utf8_lossy(&bytes)
            .lines()
            .map(str::to_owned)
            .collect()
    }
}

impl Write for Buffer {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for Buffer {
    type Writer = Buffer;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

/// Runs `f` under the production JSON layer (every level enabled) and returns the lines it
/// logged.
pub fn capture_logs(f: impl FnOnce()) -> Vec<String> {
    let buffer = Buffer::default();
    let subscriber = tracing_subscriber::registry().with(layer(&identity(), buffer.clone()));
    tracing::subscriber::with_default(subscriber, f);
    buffer.lines()
}

/// Fails if any line contains a canary (a longer run of a number's digits than masking
/// allows), or isn't a JSON object with the envelope fields.
pub fn assert_no_canary(lines: &[String], canaries: &[&str]) -> Result<(), String> {
    for line in lines {
        if let Some(c) = canaries.iter().find(|c| line.contains(*c)) {
            return Err(format!("canary {c} leaked in: {line}"));
        }
        let v: Value = serde_json::from_str(line).map_err(|e| format!("not JSON ({e}): {line}"))?;
        // log-record.v1's required fields.
        for key in [
            "ts", "level", "service", "version", "instance", "seq", "event", "msg",
        ] {
            if v.get(key).is_none() {
                return Err(format!("no {key} in: {line}"));
            }
        }
    }
    Ok(())
}

/// CI's log check (contract-check `--logs`): when `IE_TEST_LOG_DIR` is set, installs the
/// production subscriber (every level) as this test process's global default, writing to
/// `<dir>/<name>-<pid>.ndjson`. Idempotent; a no-op without the variable or when another global
/// subscriber is installed. Each process writes its own file (its `seq` starts at 1).
pub fn capture_to_file(name: &str) {
    static ONCE: std::sync::OnceLock<()> = std::sync::OnceLock::new();
    ONCE.get_or_init(|| {
        let Some(dir) = std::env::var_os("IE_TEST_LOG_DIR") else {
            return;
        };
        let path = std::path::Path::new(&dir).join(format!("{name}-{}.ndjson", std::process::id()));
        let Ok(file) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
        else {
            return;
        };
        let writer = std::sync::Mutex::new(file);
        // IE's own records down to debug; libraries (h2, tonic, hyper) at info.
        let filter = tracing_subscriber::EnvFilter::new(
            "info,ie_obs=debug,ie_core=debug,ie_routing_client=debug,ie_telephony_client=debug,interaction_engine=debug",
        );
        let subscriber = tracing_subscriber::registry()
            .with(filter)
            .with(layer(&identity(), writer));
        let _ = tracing::subscriber::set_global_default(subscriber);
    });
}

/// Writes the lines a child process logged to `<IE_TEST_LOG_DIR>/<name>-<child pid>.ndjson`
/// (no-op without the variable).
pub fn save_lines(name: &str, pid: u32, lines: &[String]) {
    let Some(dir) = std::env::var_os("IE_TEST_LOG_DIR") else {
        return;
    };
    let path = std::path::Path::new(&dir).join(format!("{name}-{pid}.ndjson"));
    let mut text = lines.join("\n");
    if !text.is_empty() {
        text.push('\n');
    }
    let _ = std::fs::write(path, text);
}
