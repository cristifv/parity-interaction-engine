//! Start, signals and the drain of ARCHITECTURE.md §7.3 (ADR 0001 §6.2, §6.3).

use crate::config::Config;
use crate::files::StateFiles;
use crate::management;
use crate::readiness::{Gate, GateMetrics, Readiness};
use ie_obs::Metrics;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::signal::unix::{Signal, SignalKind, signal};
use tokio::sync::watch;
use tokio::time::Instant;

/// `SIGINT`'s number.
pub const SIGINT: i32 = 2;
/// `SIGTERM`'s number.
pub const SIGTERM: i32 = 15;

/// How a run ended, and so the exit code.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// Drained in time; the drained file exists. Exit 0.
    Drained,
    /// The drain ran out of budget or a drainer failed; no drained file. Exit 1.
    NotDrained(String),
    /// A fatal runtime error. Exit 1.
    Fatal(String),
    /// A startup error. Exit 2.
    Startup(String),
    /// A second signal during the drain: exit 128 + its number, no drained file.
    SecondSignal(i32),
}

impl Outcome {
    pub fn exit_code(&self) -> u8 {
        match self {
            Self::Drained => 0,
            Self::NotDrained(_) | Self::Fatal(_) => 1,
            Self::Startup(_) => 2,
            Self::SecondSignal(n) => u8::try_from(128 + n).unwrap_or(1),
        }
    }
}

/// §7.3's timings.
#[derive(Debug, Clone)]
pub struct Timing {
    pub deregistration_delay: Duration,
    pub drain_timeout: Duration,
}

impl From<&Config> for Timing {
    fn from(c: &Config) -> Self {
        Self {
            deregistration_delay: Duration::from_millis(c.lifecycle.deregistration_delay_ms),
            drain_timeout: Duration::from_millis(c.lifecycle.drain_timeout_ms),
        }
    }
}

/// The work a [`Drainer`] runs, given the remaining budget by the caller's timeout.
pub type DrainFuture = Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>>;

/// A piece of in-flight work to finish during step 3 (later: release calls, flush the
/// producer, close the RE stream, commit consumers, give up the hangup-listener elections).
pub struct Drainer {
    pub name: String,
    pub run: Box<dyn FnOnce() -> DrainFuture + Send>,
}

/// `SIGTERM` and `SIGINT`, installed before anything slow.
pub struct Signals {
    term: Signal,
    int: Signal,
}

impl Signals {
    pub fn install() -> std::io::Result<Self> {
        Ok(Self {
            term: signal(SignalKind::terminate())?,
            int: signal(SignalKind::interrupt())?,
        })
    }

    /// The next signal's number.
    pub async fn next(&mut self) -> i32 {
        tokio::select! {
            _ = self.term.recv() => SIGTERM,
            _ = self.int.recv() => SIGINT,
        }
    }
}

/// Steps 1–3: not ready, the deregistration delay, the drainers, then the drained file.
/// `second_signal` resolving at any point ends the drain at once.
pub async fn drain(
    gate: &Gate,
    files: &StateFiles,
    timing: &Timing,
    drainers: Vec<Drainer>,
    second_signal: impl Future<Output = i32>,
) -> Outcome {
    let deadline = Instant::now() + timing.drain_timeout;
    tokio::pin!(second_signal);
    if let Err(e) = gate.begin_drain().await {
        return Outcome::Fatal(e.to_string());
    }
    tracing::info!(
        event = "service.draining",
        delay_ms = timing.deregistration_delay.as_millis() as u64,
        "draining: healthy file removed"
    );
    tokio::select! {
        () = tokio::time::sleep(timing.deregistration_delay) => {}
        n = &mut second_signal => return second(n),
    }
    for d in drainers {
        let left = deadline.saturating_duration_since(Instant::now());
        tokio::select! {
            r = tokio::time::timeout(left, (d.run)()) => match r {
                Ok(Ok(())) => tracing::info!(event = "drain.completed",  drainer = %d.name, "drained"),
                Ok(Err(e)) => {
                    tracing::error!(event = "drain.failed",  drainer = %d.name, error = %e, "drain failed; no drained file");
                    return Outcome::NotDrained(format!("{}: {e}", d.name));
                }
                Err(_) => {
                    tracing::error!(event = "drain.failed",  drainer = %d.name, "the drain budget ran out; no drained file");
                    return Outcome::NotDrained(format!("{}: out of time", d.name));
                }
            },
            n = &mut second_signal => return second(n),
        }
    }
    if Instant::now() > deadline {
        return Outcome::NotDrained("out of time".into());
    }
    match files.write_drained().await {
        Ok(()) => {
            tracing::info!(event = "drain.completed",  drained_file = %files.drained().display(), "drained");
            Outcome::Drained
        }
        Err(e) => Outcome::Fatal(format!("can't write the drained file: {e}")),
    }
}

fn second(n: i32) -> Outcome {
    tracing::warn!(
        event = "signal.received",
        signal = n,
        "second signal: exiting at once, without a drained file"
    );
    Outcome::SecondSignal(n)
}

/// The whole run, from the registered checks to step 4.
pub async fn run(
    cfg: &Config,
    files: StateFiles,
    metrics: Arc<Metrics>,
    mut signals: Signals,
) -> Outcome {
    let mut readiness = Readiness::new();
    readiness.register("config").ready();
    let management_check = readiness.register("management");
    let calls_check = cfg
        .telephony
        .is_some()
        .then(|| readiness.register("telephony"));
    let mut gate = Gate::spawn(readiness, files.clone(), GateMetrics::new(metrics.meter()));
    let (stop_tx, stop_rx) = watch::channel(false);
    // Step 3: the call path. Calls in progress keep their audio when IE stops (their TC holds
    // them, unmanaged); handing them over on a drain is step 16's.
    let mut calls = calls_check.map(|check| {
        tokio::spawn(crate::calls::serve(
            cfg.clone(),
            metrics.clone(),
            check,
            stop_rx.clone(),
        ))
    });
    let mut server = tokio::spawn(management::serve(
        cfg.management.listen,
        gate.status(),
        metrics.clone(),
        management_check,
        stop_rx,
    ));
    let first = loop {
        tokio::select! {
            n = signals.next() => break n,
            r = &mut server => {
                let _ = files.remove_healthy_sync();
                return Outcome::Fatal(format!("the management server stopped: {r:?}"));
            }
            r = gate.join_mut() => {
                let _ = files.remove_healthy_sync();
                return Outcome::Fatal(format!("the readiness gate stopped: {r:?}"));
            }
            // The call path may ask for an exit (none does since IE-059 removed CR-080's). A call
            // path that ended otherwise (it never became ready) waits for a signal as before.
            r = async {
                match calls.as_mut() {
                    Some(c) => c.await,
                    None => std::future::pending().await,
                }
            }, if calls.is_some() => {
                calls = None;
                if let Ok(Some(reason)) = r {
                    let _ = files.remove_healthy_sync();
                    return Outcome::Fatal(reason);
                }
            }
        }
    };
    tracing::info!(
        event = "signal.received",
        signal = first,
        "signal received: draining"
    );
    let outcome = drain(
        &gate,
        &files,
        &Timing::from(cfg),
        Vec::new(),
        signals.next(),
    )
    .await;
    // Step 4: release (the etcd lease, from step 7) and close. /livez and /metrics stay up
    // until here.
    stop_tx.send_replace(true);
    if let Some(c) = calls
        && tokio::time::timeout(Duration::from_secs(1), c)
            .await
            .is_err()
    {
        tracing::warn!(
            event = "service.stop_slow",
            "the call path didn't stop within 1 s"
        );
    }
    if tokio::time::timeout(Duration::from_secs(1), server)
        .await
        .is_err()
    {
        tracing::warn!(
            event = "service.stop_slow",
            "the management server didn't stop within 1 s"
        );
    }
    outcome
}
