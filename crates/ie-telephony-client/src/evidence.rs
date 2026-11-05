//! Positive evidence that a node is dead (ADR 0003 §4.6, D4). IE has no network path to
//! freeswitch or parity-ms, so three independent kinds of source answer, and all three must say
//! `Dead`, continuously for `dead_after`, before the client ends a node's sessions. Any `Unknown`
//! (a source down, a stale sample) blocks it: the calls stay unmanaged.

use crate::config::NodeConfig;
use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// One source's answer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Verdict {
    /// This source says the node is gone.
    Dead,
    /// This source sees it alive.
    Alive,
    /// No answer, stale data, disagreeing replicas.
    Unknown,
}

/// The three independent kinds of evidence.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum SourceKind {
    /// TC's own ports, probed from IE.
    Tc,
    /// opensips's dispatcher: the node's destination probe-failed (never admin-disabled or draining).
    Dispatcher,
    /// freeswitch's and parity-ms's scrape targets on the node.
    NodeServices,
}

/// A boxed future, so sources can be kept as trait objects.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// A source of evidence about a node.
pub trait NodeEvidence: Send + Sync {
    /// Which kind it is.
    fn kind(&self) -> SourceKind;
    /// Its current answer for one node.
    fn check<'a>(&'a self, node: &'a NodeConfig) -> BoxFuture<'a, Verdict>;
}

/// TC's ports from IE: `Dead` only when both the gRPC port refuses a TCP connection and the
/// management port's `/livez` fails; `Alive` if either answers.
#[derive(Debug, Clone)]
pub struct TcProbe {
    /// Per attempt.
    pub timeout: Duration,
}

impl Default for TcProbe {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(1),
        }
    }
}

fn host_port(uri: &str) -> Option<String> {
    let rest = uri.split("://").nth(1)?;
    Some(rest.split('/').next()?.to_owned())
}

async fn tcp_ok(addr: &str, t: Duration) -> bool {
    matches!(
        tokio::time::timeout(t, TcpStream::connect(addr)).await,
        Ok(Ok(_))
    )
}

async fn livez_ok(addr: &str, t: Duration) -> bool {
    let go = async {
        let mut s = TcpStream::connect(addr).await.ok()?;
        let req = format!("GET /livez HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n");
        s.write_all(req.as_bytes()).await.ok()?;
        let mut buf = vec![0u8; 64];
        let n = s.read(&mut buf).await.ok()?;
        let head = String::from_utf8_lossy(&buf[..n]).into_owned();
        Some(head.starts_with("HTTP/1.1 200") || head.starts_with("HTTP/1.0 200"))
    };
    matches!(tokio::time::timeout(t, go).await, Ok(Some(true)))
}

impl NodeEvidence for TcProbe {
    fn kind(&self) -> SourceKind {
        SourceKind::Tc
    }
    fn check<'a>(&'a self, node: &'a NodeConfig) -> BoxFuture<'a, Verdict> {
        Box::pin(async move {
            let Some(grpc) = host_port(&node.grpc) else {
                return Verdict::Unknown;
            };
            let Some(mgmt) = node.management.as_deref() else {
                return Verdict::Unknown;
            };
            if tcp_ok(&grpc, self.timeout).await || livez_ok(mgmt, self.timeout).await {
                Verdict::Alive
            } else {
                Verdict::Dead
            }
        })
    }
}

/// A source whose answer a test (or, later, an operator tool) sets.
#[derive(Debug)]
pub struct StaticEvidence {
    kind: SourceKind,
    verdict: Mutex<Verdict>,
}

impl StaticEvidence {
    /// Starts `Unknown`.
    pub fn new(kind: SourceKind) -> Self {
        Self {
            kind,
            verdict: Mutex::new(Verdict::Unknown),
        }
    }
    /// Sets the answer.
    pub fn set(&self, v: Verdict) {
        if let Ok(mut g) = self.verdict.lock() {
            *g = v;
        }
    }
}

impl NodeEvidence for StaticEvidence {
    fn kind(&self) -> SourceKind {
        self.kind
    }
    fn check<'a>(&'a self, _node: &'a NodeConfig) -> BoxFuture<'a, Verdict> {
        let v = self.verdict.lock().map(|g| *g).unwrap_or(Verdict::Unknown);
        Box::pin(async move { v })
    }
}

/// The window: every kind present and `Dead` on every sample, for `dead_after`.
#[derive(Debug, Default)]
pub(crate) struct Evaluator {
    since_ms: Option<u64>,
}

impl Evaluator {
    /// One round of samples at `now_ms`; true when the node is confirmed dead.
    pub fn observe(
        &mut self,
        now_ms: u64,
        dead_after_ms: u64,
        samples: &[(SourceKind, Verdict)],
    ) -> bool {
        let all_kinds = [
            SourceKind::Tc,
            SourceKind::Dispatcher,
            SourceKind::NodeServices,
        ]
        .iter()
        .all(|k| samples.iter().any(|(sk, _)| sk == k));
        let all_dead = all_kinds && samples.iter().all(|(_, v)| *v == Verdict::Dead);
        if !all_dead {
            self.since_ms = None;
            return false;
        }
        let since = *self.since_ms.get_or_insert(now_ms);
        now_ms.saturating_sub(since) >= dead_after_ms
    }

    pub fn reset(&mut self) {
        self.since_ms = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const D: Verdict = Verdict::Dead;

    fn all(v: Verdict) -> Vec<(SourceKind, Verdict)> {
        vec![
            (SourceKind::Tc, D),
            (SourceKind::Dispatcher, D),
            (SourceKind::NodeServices, v),
        ]
    }

    #[test]
    fn dead_needs_every_kind_dead_for_the_whole_window() {
        let mut e = Evaluator::default();
        assert!(!e.observe(0, 30_000, &all(D)));
        assert!(!e.observe(28_000, 30_000, &all(D)));
        assert!(
            !e.observe(29_000, 30_000, &all(Verdict::Unknown)),
            "an Unknown restarts the window"
        );
        assert!(!e.observe(31_000, 30_000, &all(D)));
        assert!(!e.observe(60_000, 30_000, &all(D)));
        assert!(e.observe(61_000, 30_000, &all(D)));
        let mut e = Evaluator::default();
        let two = vec![(SourceKind::Tc, D), (SourceKind::Dispatcher, D)];
        assert!(!e.observe(0, 0, &two), "a missing kind is never enough");
    }
}
