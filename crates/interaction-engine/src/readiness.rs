//! The readiness gate (ADR 0001 §6.1, F3 option B).
//!
//! - Checks are named watch channels; later crates expose their status the same way.
//! - One task owns the gate's state and the healthy file, so a check turning ready can't race
//!   a drain.
//! - `Starting` → `Started` once every check is ready **and** the file is written.
//!   After that a failing check never removes the file: it shows on `/readyz` and
//!   `interaction_engine_ready`. Only `Draining` removes it, for good.

use crate::backoff::Backoff;
use crate::files::StateFiles;
use opentelemetry::KeyValue;
use opentelemetry::metrics::{Counter, Gauge, Meter};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::{Notify, mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::Instant;

/// One check's state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckState {
    Pending(String),
    Ready,
    Failed(String),
}

/// Sets one check's state.
#[derive(Clone)]
pub struct CheckHandle {
    tx: Arc<watch::Sender<CheckState>>,
    notify: Arc<Notify>,
}

impl CheckHandle {
    pub fn set(&self, state: CheckState) {
        self.tx.send_replace(state);
        self.notify.notify_one();
    }

    pub fn ready(&self) {
        self.set(CheckState::Ready);
    }

    pub fn failed(&self, reason: impl Into<String>) {
        self.set(CheckState::Failed(reason.into()));
    }
}

/// The registered checks.
pub struct Readiness {
    checks: Vec<(String, watch::Receiver<CheckState>)>,
    notify: Arc<Notify>,
}

impl Default for Readiness {
    fn default() -> Self {
        Self::new()
    }
}

impl Readiness {
    pub fn new() -> Self {
        Self {
            checks: Vec::new(),
            notify: Arc::new(Notify::new()),
        }
    }

    /// Adds a check, `Pending` until its owner says otherwise.
    pub fn register(&mut self, name: &str) -> CheckHandle {
        let (tx, rx) = watch::channel(CheckState::Pending("starting".into()));
        self.checks.push((name.to_owned(), rx));
        CheckHandle {
            tx: Arc::new(tx),
            notify: self.notify.clone(),
        }
    }

    fn failing(&self) -> Vec<String> {
        self.checks
            .iter()
            .filter(|(_, rx)| *rx.borrow() != CheckState::Ready)
            .map(|(n, _)| n.clone())
            .collect()
    }
}

/// Where the gate is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GateState {
    Starting,
    Started,
    Draining,
}

impl GateState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::Started => "started",
            Self::Draining => "draining",
        }
    }
}

/// What `/readyz` reports.
#[derive(Debug, Clone)]
pub struct GateStatus {
    pub state: GateState,
    pub failing: Vec<String>,
}

impl GateStatus {
    /// Started, not draining, and every check passing now.
    pub fn ready(&self) -> bool {
        self.state == GateState::Started && self.failing.is_empty()
    }
}

/// The gate's instruments.
pub struct GateMetrics {
    ready: Gauge<u64>,
    started: Gauge<u64>,
    draining: Gauge<u64>,
    check: Gauge<u64>,
    file_errors: Counter<u64>,
}

impl GateMetrics {
    pub fn new(meter: &Meter) -> Self {
        Self {
            ready: meter
                .u64_gauge("interaction_engine_ready")
                .with_description("1 while every readiness check passes and the service has started and isn't draining")
                .build(),
            started: meter
                .u64_gauge("interaction_engine_started")
                .with_description("1 while the healthy file exists: started and not draining")
                .build(),
            draining: meter.u64_gauge("interaction_engine_draining").with_description("1 once draining started").build(),
            check: meter
                .u64_gauge("interaction_engine_readiness_check_ready")
                .with_description("1 while the check passes")
                .build(),
            file_errors: meter
                .u64_counter("interaction_engine_healthy_file_errors")
                .with_description("Healthy-file operations that failed")
                .build(),
        }
    }
}

/// The running gate.
pub struct Gate {
    status: watch::Receiver<GateStatus>,
    drain: mpsc::Sender<oneshot::Sender<()>>,
    join: JoinHandle<()>,
}

/// The gate task is gone.
#[derive(Debug, thiserror::Error)]
#[error("the readiness gate has stopped")]
pub struct GateGone;

impl Gate {
    pub fn spawn(readiness: Readiness, files: StateFiles, metrics: GateMetrics) -> Self {
        let (status_tx, status) = watch::channel(GateStatus {
            state: GateState::Starting,
            failing: readiness.failing(),
        });
        let (drain, drain_rx) = mpsc::channel(1);
        let join = tokio::spawn(run(readiness, files, metrics, status_tx, drain_rx));
        Self {
            status,
            drain,
            join,
        }
    }

    pub fn status(&self) -> watch::Receiver<GateStatus> {
        self.status.clone()
    }

    /// §7.3 step 1: enters `Draining` and removes the healthy file; returns once done.
    pub async fn begin_drain(&self) -> Result<(), GateGone> {
        let (ack, done) = oneshot::channel();
        self.drain.send(ack).await.map_err(|_| GateGone)?;
        done.await.map_err(|_| GateGone)
    }

    /// The task, for supervision: it must not end before a drain.
    pub fn join_mut(&mut self) -> &mut JoinHandle<()> {
        &mut self.join
    }
}

async fn run(
    readiness: Readiness,
    files: StateFiles,
    metrics: GateMetrics,
    status_tx: watch::Sender<GateStatus>,
    mut drain_rx: mpsc::Receiver<oneshot::Sender<()>>,
) {
    let mut state = GateState::Starting;
    let mut backoff = Backoff::new(Duration::from_millis(100), Duration::from_secs(5));
    let mut retry_at: Option<Instant> = None;
    let mut was_ready = false;
    loop {
        for (name, rx) in &readiness.checks {
            let ok = u64::from(*rx.borrow() == CheckState::Ready);
            metrics
                .check
                .record(ok, &[KeyValue::new("check", name.clone())]);
        }
        let failing = readiness.failing();
        if state == GateState::Starting
            && failing.is_empty()
            && retry_at.is_none_or(|t| Instant::now() >= t)
        {
            // F-D: a drain requested meanwhile wins: the file is never written after it.
            match drain_rx.try_recv() {
                Ok(ack) => {
                    return finish_drain(&files, &metrics, &status_tx, failing, Some(ack)).await;
                }
                Err(TryRecvError::Disconnected) => {
                    return finish_drain(&files, &metrics, &status_tx, failing, None).await;
                }
                Err(TryRecvError::Empty) => {}
            }
            match files.write_healthy().await {
                Ok(()) => {
                    state = GateState::Started;
                    retry_at = None;
                    tracing::info!(event = "service.ready",  healthy_file = %files.healthy().display(), "started: every readiness check passed");
                }
                Err(e) => {
                    metrics.file_errors.add(1, &[KeyValue::new("op", "write")]);
                    tracing::error!(event = "health_file.failed",  error = %e, healthy_file = %files.healthy().display(), "can't write the healthy file; retrying");
                    retry_at = Some(Instant::now() + backoff.next_delay());
                }
            }
        }
        let status = GateStatus { state, failing };
        let ready = status.ready();
        if state == GateState::Started && ready != was_ready {
            if ready {
                tracing::info!(event = "service.ready", "ready");
            } else {
                tracing::warn!(event = "service.not_ready",  failing = ?status.failing, "not ready; the healthy file stays until a drain (option B)");
            }
        }
        was_ready = ready;
        metrics.ready.record(u64::from(ready), &[]);
        metrics
            .started
            .record(u64::from(state == GateState::Started), &[]);
        metrics.draining.record(0, &[]);
        let failing = status.failing.clone();
        status_tx.send_replace(status);
        let retry = async {
            match retry_at {
                Some(t) => tokio::time::sleep_until(t).await,
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            biased;
            ack = drain_rx.recv() => return finish_drain(&files, &metrics, &status_tx, failing, ack).await,
            () = readiness.notify.notified() => {}
            () = retry => {}
        }
    }
}

/// §7.3 step 1: `Draining` for good; the healthy file removed; the requester told.
async fn finish_drain(
    files: &StateFiles,
    metrics: &GateMetrics,
    status_tx: &watch::Sender<GateStatus>,
    failing: Vec<String>,
    ack: Option<oneshot::Sender<()>>,
) {
    if let Err(e) = files.remove_healthy().await {
        metrics.file_errors.add(1, &[KeyValue::new("op", "remove")]);
        tracing::error!(event = "health_file.failed",  error = %e, "can't remove the healthy file; draining anyway");
    }
    metrics.ready.record(0, &[]);
    metrics.started.record(0, &[]);
    metrics.draining.record(1, &[]);
    status_tx.send_replace(GateStatus {
        state: GateState::Draining,
        failing,
    });
    if let Some(ack) = ack {
        let _ = ack.send(());
    }
}
