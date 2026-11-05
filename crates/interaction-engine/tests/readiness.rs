//! The readiness gate (F3, option B) and the drain, in process, with injected checks and
//! drainers.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use ie_obs::Metrics;
use ie_obs::testing::identity;
use interaction_engine::files::StateFiles;
use interaction_engine::lifecycle::{Drainer, Outcome, Timing, drain};
use interaction_engine::readiness::{Gate, GateMetrics, GateState, Readiness};
use std::future::pending;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::time::{Duration, Instant};

async fn eventually(what: &str, mut f: impl FnMut() -> bool) {
    let end = Instant::now() + Duration::from_secs(5);
    while !f() {
        assert!(Instant::now() < end, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

async fn stays(what: &str, for_ms: u64, mut f: impl FnMut() -> bool) {
    let end = Instant::now() + Duration::from_millis(for_ms);
    while Instant::now() < end {
        assert!(f(), "{what}");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

fn exists(p: &Path) -> bool {
    p.exists()
}

fn metric(metrics: &Metrics, name: &str) -> String {
    metrics
        .render()
        .unwrap()
        .lines()
        .filter(|l| l.starts_with(name))
        .collect::<Vec<_>>()
        .join("\n")
}

fn timing(delay_ms: u64, timeout_ms: u64) -> Timing {
    Timing {
        deregistration_delay: Duration::from_millis(delay_ms),
        drain_timeout: Duration::from_millis(timeout_ms),
    }
}

#[tokio::test]
async fn the_file_appears_only_after_the_last_check_and_stays_when_one_fails_after_start() {
    let dir = tempfile::tempdir().unwrap();
    let files = StateFiles::new(dir.path());
    let metrics = Metrics::new(&identity()).unwrap();
    let mut r = Readiness::new();
    let (a, b, c) = (r.register("a"), r.register("b"), r.register("c"));
    let gate = Gate::spawn(r, files.clone(), GateMetrics::new(metrics.meter()));
    let status = gate.status();
    a.ready();
    b.ready();
    stays("no file before the last check", 100, || {
        !exists(&files.healthy())
    })
    .await;
    c.ready();
    eventually("healthy file", || exists(&files.healthy())).await;
    eventually("ready", || status.borrow().ready()).await;
    // Option B: a failing check after start leaves the file; /readyz and the gauge follow.
    b.failed("etcd unreachable");
    eventually("not ready", || !status.borrow().ready()).await;
    assert_eq!(status.borrow().failing, vec!["b".to_string()]);
    assert!(exists(&files.healthy()), "option B: the file stays");
    assert!(metric(&metrics, "interaction_engine_ready{").ends_with(" 0"));
    assert!(metric(&metrics, "interaction_engine_started{").ends_with(" 1"));
    b.ready();
    eventually("ready again", || status.borrow().ready()).await;
    assert!(metric(&metrics, "interaction_engine_ready{").ends_with(" 1"));
}

#[tokio::test]
async fn a_check_turning_ready_after_the_drain_never_brings_the_file_back() {
    let dir = tempfile::tempdir().unwrap();
    let files = StateFiles::new(dir.path());
    let metrics = Metrics::new(&identity()).unwrap();
    let mut r = Readiness::new();
    let late = r.register("late");
    let gate = Gate::spawn(r, files.clone(), GateMetrics::new(metrics.meter()));
    gate.begin_drain().await.unwrap();
    late.ready();
    stays("no healthy file after the drain began", 200, || {
        !exists(&files.healthy())
    })
    .await;
    assert_eq!(gate.status().borrow().state, GateState::Draining);
}

#[tokio::test]
async fn a_failed_write_is_retried_and_counted() {
    let dir = tempfile::tempdir().unwrap();
    let state = dir.path().join("state");
    std::fs::create_dir(&state).unwrap();
    std::fs::set_permissions(&state, std::fs::Permissions::from_mode(0o500)).unwrap();
    let files = StateFiles::new(&state);
    let metrics = Metrics::new(&identity()).unwrap();
    let mut r = Readiness::new();
    r.register("x").ready();
    let gate = Gate::spawn(r, files.clone(), GateMetrics::new(metrics.meter()));
    eventually("a counted write failure", || {
        metric(&metrics, "interaction_engine_healthy_file_errors_total").contains("op=\"write\"")
    })
    .await;
    assert_eq!(gate.status().borrow().state, GateState::Starting);
    assert!(!gate.status().borrow().ready());
    std::fs::set_permissions(&state, std::fs::Permissions::from_mode(0o700)).unwrap();
    eventually("the retried write", || exists(&files.healthy())).await;
}

#[tokio::test]
async fn a_drain_writes_the_drained_file_after_the_delay_and_the_drainers() {
    let dir = tempfile::tempdir().unwrap();
    let files = StateFiles::new(dir.path());
    let metrics = Metrics::new(&identity()).unwrap();
    let mut r = Readiness::new();
    r.register("x").ready();
    let gate = Gate::spawn(r, files.clone(), GateMetrics::new(metrics.meter()));
    let status = gate.status();
    eventually("healthy", || exists(&files.healthy())).await;
    let order = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let drainers = ["first", "second"]
        .into_iter()
        .map(|name| {
            let order = order.clone();
            Drainer {
                name: name.into(),
                run: Box::new(move || {
                    Box::pin(async move {
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        order.lock().unwrap().push(name);
                        Ok(())
                    })
                }),
            }
        })
        .collect();
    let started = Instant::now();
    let outcome = drain(&gate, &files, &timing(100, 2_000), drainers, pending()).await;
    assert_eq!(outcome, Outcome::Drained);
    assert_eq!(outcome.exit_code(), 0);
    assert!(started.elapsed() >= Duration::from_millis(200));
    assert_eq!(*order.lock().unwrap(), vec!["first", "second"]);
    assert!(exists(&files.drained()));
    assert!(!exists(&files.healthy()));
    assert_eq!(status.borrow().state, GateState::Draining);
}

#[tokio::test]
async fn a_drain_that_runs_out_of_time_writes_no_drained_file_and_exits_1() {
    let dir = tempfile::tempdir().unwrap();
    let files = StateFiles::new(dir.path());
    let metrics = Metrics::new(&identity()).unwrap();
    let mut r = Readiness::new();
    r.register("x").ready();
    let gate = Gate::spawn(r, files.clone(), GateMetrics::new(metrics.meter()));
    eventually("healthy", || exists(&files.healthy())).await;
    let slow = Drainer {
        name: "slow".into(),
        run: Box::new(|| Box::pin(async { pending::<anyhow::Result<()>>().await })),
    };
    let outcome = drain(&gate, &files, &timing(50, 300), vec![slow], pending()).await;
    assert!(matches!(outcome, Outcome::NotDrained(_)), "{outcome:?}");
    assert_eq!(outcome.exit_code(), 1);
    assert!(!exists(&files.drained()));
}

#[tokio::test]
async fn a_second_signal_ends_the_drain_at_once() {
    let dir = tempfile::tempdir().unwrap();
    let files = StateFiles::new(dir.path());
    let metrics = Metrics::new(&identity()).unwrap();
    let mut r = Readiness::new();
    r.register("x").ready();
    let gate = Gate::spawn(r, files.clone(), GateMetrics::new(metrics.meter()));
    eventually("healthy", || exists(&files.healthy())).await;
    let second = async {
        tokio::time::sleep(Duration::from_millis(50)).await;
        2
    };
    let outcome = drain(&gate, &files, &timing(5_000, 10_000), vec![], second).await;
    assert_eq!(outcome, Outcome::SecondSignal(2));
    assert_eq!(outcome.exit_code(), 130);
    assert_eq!(Outcome::SecondSignal(15).exit_code(), 143);
    assert!(!exists(&files.drained()));
}

#[test]
fn a_drain_queued_before_the_gate_first_looks_means_the_file_is_never_written() {
    // F-D. On a current-thread runtime the gate task first runs only when the test awaits:
    // by then its check is ready and a drain is queued. The file must never be written (the
    // "started" line is the witness: a written-then-removed file leaves no other trace).
    let dir = tempfile::tempdir().unwrap();
    let files = StateFiles::new(dir.path());
    let lines = ie_obs::testing::capture_logs(|| {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let metrics = Metrics::new(&identity()).unwrap();
            let mut r = Readiness::new();
            let check = r.register("x");
            let gate = Gate::spawn(r, files.clone(), GateMetrics::new(metrics.meter()));
            check.ready();
            gate.begin_drain().await.unwrap();
            assert_eq!(gate.status().borrow().state, GateState::Draining);
        });
    });
    assert!(
        !lines
            .iter()
            .any(|l| l.contains("started: every readiness check passed")),
        "{lines:#?}"
    );
    assert!(!files.healthy().exists());
}
