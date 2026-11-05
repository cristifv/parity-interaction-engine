//! S0-AC3 and S0-AC4 against the real binary: the healthy and drained files of
//! ARCHITECTURE.md §7.3, the drain on SIGTERM, a second signal, configuration errors, JSON
//! logs and the metrics endpoint.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use serde_json::Value;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const BIN: &str = env!("CARGO_BIN_EXE_parity-interaction-engine");

struct Ie {
    child: Child,
    dir: tempfile::TempDir,
    port: u16,
    stdout: Arc<Mutex<Vec<String>>>,
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn config(port: u16, zone: &str, delay_ms: u64, extra: &str) -> String {
    let drain_ms = (delay_ms + 2_600).max(3_000);
    format!(
        "[service]\nzone = \"{zone}\"\nbin = \"bin-a\"\nreplica_id = \"ie-test\"\n\n[management]\nlisten = \"127.0.0.1:{port}\"\n\n\
         [lifecycle]\nderegistration_delay_ms = {delay_ms}\ndrain_timeout_ms = {drain_ms}\n{extra}"
    )
}

fn start_with(dir: tempfile::TempDir, port: u16, cfg: &str) -> Ie {
    let path = dir.path().join("config.toml");
    std::fs::write(&path, cfg).unwrap();
    let mut child = Command::new(BIN)
        .env("IE_STATE_DIR", dir.path())
        .env("IE_CONFIG", &path)
        .env_remove("IE_LOG")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let stdout = Arc::new(Mutex::new(Vec::new()));
    let sink = stdout.clone();
    let out = child.stdout.take().unwrap();
    std::thread::spawn(move || {
        for line in BufReader::new(out).lines().map_while(Result::ok) {
            sink.lock().unwrap().push(line);
        }
    });
    Ie {
        child,
        dir,
        port,
        stdout,
    }
}

fn start(delay_ms: u64) -> Ie {
    let port = free_port();
    start_with(
        tempfile::tempdir().unwrap(),
        port,
        &config(port, "local", delay_ms, ""),
    )
}

impl Ie {
    fn healthy(&self) -> PathBuf {
        self.dir.path().join("parity-interaction-engine.healthy")
    }

    fn drained(&self) -> PathBuf {
        self.dir.path().join("parity-interaction-engine.drained")
    }

    fn signal(&self, sig: &str) {
        let ok = Command::new("kill")
            .args([sig, &self.child.id().to_string()])
            .status()
            .unwrap();
        assert!(ok.success());
    }

    fn wait_exit(&mut self, within: Duration) -> ExitStatus {
        let end = Instant::now() + within;
        loop {
            if let Some(s) = self.child.try_wait().unwrap() {
                return s;
            }
            assert!(Instant::now() < end, "still running after {within:?}");
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn get(&self, path: &str) -> Option<(u16, String)> {
        get(self.port, path)
    }

    fn lines(&self) -> Vec<String> {
        self.stdout.lock().unwrap().clone()
    }
}

impl Drop for Ie {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        // CI's log check: what the binary wrote, one file per process.
        std::thread::sleep(Duration::from_millis(50));
        ie_obs::testing::save_lines("lifecycle", self.child.id(), &self.lines());
    }
}

fn get(port: u16, path: &str) -> Option<(u16, String)> {
    let mut s = TcpStream::connect(("127.0.0.1", port)).ok()?;
    s.set_read_timeout(Some(Duration::from_secs(2))).ok()?;
    write!(
        s,
        "GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
    )
    .ok()?;
    let mut text = String::new();
    s.read_to_string(&mut text).ok()?;
    let code = text.split_whitespace().nth(1)?.parse().ok()?;
    let body = text
        .split_once("\r\n\r\n")
        .map(|(_, b)| b.to_owned())
        .unwrap_or_default();
    Some((code, body))
}

fn wait_for(what: &str, within: Duration, mut f: impl FnMut() -> bool) {
    let end = Instant::now() + within;
    while !f() {
        assert!(Instant::now() < end, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn exists(p: &Path) -> bool {
    p.exists()
}

#[test]
fn stale_files_are_removed_at_start() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("parity-interaction-engine.healthy"), b"").unwrap();
    std::fs::write(dir.path().join("parity-interaction-engine.drained"), b"").unwrap();
    // Hold the port so the gate can't start: the stale healthy file must disappear and stay away.
    let holder = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = holder.local_addr().unwrap().port();
    let ie = start_with(dir, port, &config(port, "local", 100, ""));
    wait_for("stale files removed", Duration::from_secs(2), || {
        !exists(&ie.healthy()) && !exists(&ie.drained())
    });
    std::thread::sleep(Duration::from_millis(300));
    assert!(!exists(&ie.healthy()));
    drop(holder);
    wait_for("healthy file", Duration::from_secs(8), || {
        exists(&ie.healthy())
    });
}

#[test]
fn healthy_only_after_checks_pass() {
    let holder = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = holder.local_addr().unwrap().port();
    let ie = start_with(
        tempfile::tempdir().unwrap(),
        port,
        &config(port, "local", 100, ""),
    );
    let end = Instant::now() + Duration::from_millis(1500);
    while Instant::now() < end {
        assert!(
            !exists(&ie.healthy()),
            "healthy before the management check passed"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    drop(holder);
    wait_for(
        "healthy file after the port is free",
        Duration::from_secs(8),
        || exists(&ie.healthy()),
    );
    assert_eq!(ie.get("/readyz").map(|r| r.0), Some(200));
}

#[test]
fn sigterm_drains_and_exits_zero() {
    let mut ie = start(400);
    wait_for("healthy", Duration::from_secs(5), || exists(&ie.healthy()));
    let sent = Instant::now();
    ie.signal("-TERM");
    wait_for("healthy removed", Duration::from_millis(500), || {
        !exists(&ie.healthy())
    });
    let (ready, _) = ie.get("/readyz").unwrap();
    let (live, _) = ie.get("/livez").unwrap();
    assert_eq!((ready, live), (503, 200), "during the deregistration delay");
    assert!(!exists(&ie.drained()), "drained before the delay");
    let status = ie.wait_exit(Duration::from_secs(4));
    assert_eq!(status.code(), Some(0));
    assert!(exists(&ie.drained()));
    assert!(
        sent.elapsed() >= Duration::from_millis(400),
        "exited before the deregistration delay"
    );
    let lines = ie.lines();
    assert!(
        lines.iter().any(|l| l.contains("\"drained\"")),
        "{lines:#?}"
    );
}

#[test]
fn sigterm_before_ready_drains() {
    let holder = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = holder.local_addr().unwrap().port();
    let mut ie = start_with(
        tempfile::tempdir().unwrap(),
        port,
        &config(port, "local", 100, ""),
    );
    wait_for("startup", Duration::from_secs(5), || !ie.lines().is_empty());
    std::thread::sleep(Duration::from_millis(200));
    ie.signal("-TERM");
    assert_eq!(ie.wait_exit(Duration::from_secs(4)).code(), Some(0));
    assert!(exists(&ie.drained()));
    assert!(!exists(&ie.healthy()));
    drop(holder);
}

#[test]
fn a_second_signal_exits_at_once_with_128_plus_n() {
    for (second, code) in [("-INT", 130), ("-TERM", 143)] {
        let mut ie = start(10_000);
        wait_for("healthy", Duration::from_secs(5), || exists(&ie.healthy()));
        ie.signal("-TERM");
        wait_for("draining", Duration::from_secs(1), || {
            !exists(&ie.healthy())
        });
        let sent = Instant::now();
        ie.signal(second);
        assert_eq!(
            ie.wait_exit(Duration::from_secs(3)).code(),
            Some(code),
            "{second}"
        );
        assert!(sent.elapsed() < Duration::from_millis(1500));
        assert!(!exists(&ie.drained()), "no drained file after a forced end");
    }
}

#[test]
fn an_invalid_config_exits_2_and_still_removes_stale_files() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("parity-interaction-engine.healthy"), b"").unwrap();
    std::fs::write(dir.path().join("parity-interaction-engine.drained"), b"").unwrap();
    let port = free_port();
    let path = dir.path().join("config.toml");
    std::fs::write(&path, config(port, "local", 100, "typo = 1\n")).unwrap();
    let out = Command::new(BIN)
        .env("IE_STATE_DIR", dir.path())
        .env("IE_CONFIG", &path)
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
    let err = String::from_utf8_lossy(&out.stderr);
    let line: Value = serde_json::from_str(err.trim()).unwrap();
    assert_eq!(line["message"], "invalid configuration");
    assert!(
        !dir.path()
            .join("parity-interaction-engine.healthy")
            .exists()
    );
    assert!(
        !dir.path()
            .join("parity-interaction-engine.drained")
            .exists()
    );
}

#[test]
fn the_state_dir_override_is_refused_outside_zone_local() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config.toml");
    std::fs::write(&path, config(free_port(), "zone-a", 100, "")).unwrap();
    let out = Command::new(BIN)
        .env("IE_STATE_DIR", dir.path())
        .env("IE_CONFIG", &path)
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&out.stderr).contains("IE_STATE_DIR"));
}

#[test]
fn logs_are_json_and_metrics_are_served() {
    let mut ie = start(100);
    wait_for("healthy", Duration::from_secs(5), || exists(&ie.healthy()));
    let (code, body) = ie.get("/metrics").unwrap();
    assert_eq!(code, 200);
    assert!(body.contains("interaction_engine_ready{"), "{body}");
    assert!(body.contains("interaction_engine_started{"), "{body}");
    assert!(body.contains("process_resident_memory_bytes"), "{body}");
    for line in body
        .lines()
        .filter(|l| !l.starts_with('#') && !l.is_empty())
    {
        for label in [
            "service=\"parity-interaction-engine\"",
            "zone=\"local\"",
            "bin=\"bin-a\"",
            "instance=\"ie-test\"",
            "version=\"",
        ] {
            assert!(line.contains(label), "{label} missing: {line}");
        }
        assert!(!line.contains("tenant"), "{line}");
    }
    ie.signal("-TERM");
    assert_eq!(ie.wait_exit(Duration::from_secs(4)).code(), Some(0));
    std::thread::sleep(Duration::from_millis(100));
    let lines = ie.lines();
    assert!(lines.len() >= 5, "{lines:#?}");
    for line in &lines {
        let v: Value =
            serde_json::from_str(line).unwrap_or_else(|e| panic!("not JSON ({e}): {line}"));
        // log-record.v1 (parity-contracts 0.13.0): its required fields, a lowercase level.
        for key in [
            "ts",
            "level",
            "service",
            "version",
            "instance",
            "incarnation",
            "seq",
            "event",
            "msg",
            "bin",
            "zone",
        ] {
            assert!(v.get(key).is_some(), "{key} missing: {line}");
        }
        assert_ne!(
            v["event"], "log.unclassified",
            "every IE record names its event: {line}"
        );
        assert!(
            ["trace", "debug", "info", "warn", "error", "fatal"]
                .contains(&v["level"].as_str().unwrap()),
            "{line}"
        );
    }
}

#[test]
fn cli_calls_leave_a_running_process_files_alone() {
    // F-B: --version, --help and --check-config run inside a live container must not delete
    // the serving process's healthy file (Swarm would restart the task).
    let dir = tempfile::tempdir().unwrap();
    let healthy = dir.path().join("parity-interaction-engine.healthy");
    let drained = dir.path().join("parity-interaction-engine.drained");
    std::fs::write(&healthy, b"").unwrap();
    std::fs::write(&drained, b"").unwrap();
    let path = dir.path().join("config.toml");
    std::fs::write(&path, config(free_port(), "local", 100, "")).unwrap();
    for args in [vec!["--version"], vec!["--help"], vec!["--check-config"]] {
        let out = Command::new(BIN)
            .args(&args)
            .env("IE_STATE_DIR", dir.path())
            .env("IE_CONFIG", &path)
            .output()
            .unwrap();
        assert_eq!(
            out.status.code(),
            Some(0),
            "{args:?}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert!(healthy.exists(), "{args:?} removed the healthy file");
        assert!(drained.exists(), "{args:?} removed the drained file");
    }
}
