//! fake-telephony-controller for IE's tests (ADR 0003 §5): TC's jar with the `fake` profile,
//! spawned on free ports, driven through its control endpoint (`/_fake/v1/{reset,inject,advance,
//! invariants}`). The jar: `$PARITY_TC_JAR`, else `../parity-telephony-controller/target/`.
#![allow(dead_code)]

use serde_json::Value;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

pub struct Fake {
    child: Child,
    pub grpc_port: u16,
    pub control_port: u16,
    pub management_port: u16,
    _dir: tempfile::TempDir,
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .and_then(|l| l.local_addr())
        .map(|a| a.port())
        .unwrap_or(0)
}

pub fn jar() -> PathBuf {
    if let Some(p) = std::env::var_os("PARITY_TC_JAR") {
        return PathBuf::from(p);
    }
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../../parity-telephony-controller/target");
    let found = std::fs::read_dir(&dir).ok().and_then(|rd| {
        rd.filter_map(Result::ok).map(|e| e.path()).find(|p| {
            let n = p.file_name().and_then(|n| n.to_str()).unwrap_or("");
            n.starts_with("parity-telephony-controller-")
                && n.ends_with(".jar")
                && !n.ends_with("-plain.jar")
        })
    });
    found.unwrap_or_else(|| {
        panic!(
            "fake-telephony-controller's jar is missing in {}: build it with parity-telephony-controller/scripts/ci.sh (or mvn -q -DskipTests package), or set PARITY_TC_JAR",
            dir.display()
        )
    })
}

impl Fake {
    pub async fn start() -> Self {
        let dir = tempfile::tempdir().expect("a temp dir");
        let (grpc_port, control_port, management_port) = (free_port(), free_port(), free_port());
        let log = std::fs::File::create(dir.path().join("fake.log")).expect("a log file");
        let p = |k: &str, v: String| format!("--{k}={v}");
        let d = dir.path().display().to_string();
        let child = Command::new("java")
            .arg("-jar")
            .arg(jar())
            .arg("--spring.profiles.active=fake")
            .arg(p("server.port", management_port.to_string()))
            .arg(p(
                "parity.telephony-controller.grpc.port",
                grpc_port.to_string(),
            ))
            .arg(p(
                "parity.telephony-controller.fake.control-port",
                control_port.to_string(),
            ))
            .arg(p(
                "parity.telephony-controller.fake.control-host",
                "127.0.0.1".into(),
            ))
            .arg(p(
                "parity.telephony-controller.lock-dir",
                format!("{d}/lock"),
            ))
            .arg(p(
                "parity.telephony-controller.lifecycle.healthy-file",
                format!("{d}/healthy"),
            ))
            .arg(p(
                "parity.telephony-controller.lifecycle.drained-file",
                format!("{d}/drained"),
            ))
            .arg(p(
                "parity.telephony-controller.journal.dir",
                format!("{d}/journal"),
            ))
            .stdout(Stdio::from(log.try_clone().expect("a log handle")))
            .stderr(Stdio::from(log))
            .spawn()
            .expect("java -jar fake-telephony-controller");
        let mut fake = Self {
            child,
            grpc_port,
            control_port,
            management_port,
            _dir: dir,
        };
        let until = Instant::now() + Duration::from_secs(90);
        loop {
            if fake.get("invariants").await.is_ok()
                && TcpStream::connect(("127.0.0.1", grpc_port)).await.is_ok()
            {
                break;
            }
            if let Ok(Some(status)) = fake.child.try_wait() {
                let log =
                    std::fs::read_to_string(fake._dir.path().join("fake.log")).unwrap_or_default();
                panic!(
                    "the fake exited ({status}) before it was ready:\n{}",
                    log.lines().rev().take(40).collect::<Vec<_>>().join("\n")
                );
            }
            assert!(Instant::now() < until, "the fake didn't start within 90 s");
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        fake
    }

    pub fn grpc_uri(&self) -> String {
        format!("http://127.0.0.1:{}", self.grpc_port)
    }

    async fn http(&self, method: &str, path: &str, body: Option<&Value>) -> Result<Value, String> {
        let mut s = TcpStream::connect(("127.0.0.1", self.control_port))
            .await
            .map_err(|e| e.to_string())?;
        let b = body.map(Value::to_string).unwrap_or_default();
        let req = format!(
            "{method} /_fake/v1/{path} HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{b}",
            b.len()
        );
        s.write_all(req.as_bytes())
            .await
            .map_err(|e| e.to_string())?;
        let mut out = Vec::new();
        s.read_to_end(&mut out).await.map_err(|e| e.to_string())?;
        let text = String::from_utf8_lossy(&out).into_owned();
        let (head, body) = text.split_once("\r\n\r\n").ok_or("no HTTP response")?;
        if !head.starts_with("HTTP/1.1 200") {
            return Err(format!(
                "control {path}: {}",
                head.lines().next().unwrap_or("")
            ));
        }
        let body = if head
            .to_ascii_lowercase()
            .contains("transfer-encoding: chunked")
        {
            dechunk(body)
        } else {
            body.to_owned()
        };
        if body.trim().is_empty() {
            Ok(Value::Null)
        } else {
            serde_json::from_str(&body).map_err(|e| e.to_string())
        }
    }

    pub async fn get(&self, path: &str) -> Result<Value, String> {
        self.http("GET", path, None).await
    }

    pub async fn post(&self, path: &str, body: &Value) -> Result<Value, String> {
        self.http("POST", path, Some(body)).await
    }

    pub async fn reset(&self, given: &Value) -> Value {
        self.post("reset", given).await.expect("reset")
    }

    pub async fn inject(&self, what: &str, args: &Value) -> Value {
        self.post("inject", &serde_json::json!({"what": what, "args": args}))
            .await
            .unwrap_or_else(|e| panic!("inject {what}: {e}"))
    }

    pub async fn advance(&self, ms: u64) {
        self.post("advance", &serde_json::json!({"ms": ms.to_string()}))
            .await
            .expect("advance");
    }

    pub async fn invariants(&self) -> Vec<String> {
        self.get("invariants")
            .await
            .ok()
            .and_then(|v| v.get("violations").and_then(Value::as_array).cloned())
            .map(|a| {
                a.iter()
                    .map(|v| v.as_str().unwrap_or("?").to_owned())
                    .collect()
            })
            .unwrap_or_default()
    }
}

fn dechunk(body: &str) -> String {
    let mut out = String::new();
    let mut rest = body;
    while let Some((size, tail)) = rest.split_once("\r\n") {
        let n = usize::from_str_radix(size.trim(), 16).unwrap_or(0);
        if n == 0 || tail.len() < n {
            break;
        }
        out.push_str(&tail[..n]);
        rest = tail[n..].trim_start_matches("\r\n");
    }
    out
}

impl Drop for Fake {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
