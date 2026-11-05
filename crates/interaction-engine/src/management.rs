//! The management port (ADR 0001 §6.1, §7.3): `/livez`, `/readyz`, `/metrics`. Binds with
//! backoff and stays not ready until it can: the first instance of "never exit for a
//! dependency that may come back".

use crate::backoff::Backoff;
use crate::readiness::{CheckHandle, GateStatus};
use axum::Router;
use axum::extract::State;
use axum::http::{StatusCode, header};
use axum::response::IntoResponse;
use axum::routing::get;
use ie_obs::Metrics;
use opentelemetry::metrics::Counter;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::time::Instant;

#[derive(Clone)]
struct AppState {
    status: watch::Receiver<GateStatus>,
    metrics: Arc<Metrics>,
}

async fn livez() -> &'static str {
    "ok\n"
}

async fn readyz(State(s): State<AppState>) -> impl IntoResponse {
    let status = s.status.borrow().clone();
    let body =
        serde_json::json!({"state": status.state.as_str(), "failing": status.failing}).to_string();
    let code = if status.ready() {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (code, [(header::CONTENT_TYPE, "application/json")], body)
}

async fn render_metrics(State(s): State<AppState>) -> impl IntoResponse {
    match s.metrics.render() {
        Ok(text) => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, ie_obs::metrics::CONTENT_TYPE)],
            text,
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            [(header::CONTENT_TYPE, "text/plain")],
            e.to_string(),
        ),
    }
}

/// Binds `addr` (retrying), marks `check` ready, and serves until `shutdown` turns true.
pub async fn serve(
    addr: SocketAddr,
    status: watch::Receiver<GateStatus>,
    metrics: Arc<Metrics>,
    check: CheckHandle,
    mut shutdown: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let bind_failures: Counter<u64> = metrics
        .meter()
        .u64_counter("interaction_engine_management_bind_failures")
        .with_description("Failed attempts to bind the management port")
        .build();
    let mut backoff = Backoff::new(Duration::from_millis(100), Duration::from_secs(5));
    let mut last_warning: Option<Instant> = None;
    let listener = loop {
        match TcpListener::bind(addr).await {
            Ok(l) => break l,
            Err(e) => {
                bind_failures.add(1, &[]);
                check.failed(format!("can't bind {addr}: {e}"));
                if last_warning.is_none_or(|t| t.elapsed() >= Duration::from_secs(10)) {
                    tracing::warn!(event = "management.bind_failed",  %addr, error = %e, "can't bind the management port; retrying");
                    last_warning = Some(Instant::now());
                }
                tokio::select! {
                    () = tokio::time::sleep(backoff.next_delay()) => {}
                    _ = shutdown.wait_for(|stop| *stop) => return Ok(()),
                }
            }
        }
    };
    tracing::info!(event = "management.listening",  addr = %listener.local_addr()?, "management port listening");
    check.ready();
    let app = Router::new()
        .route("/livez", get(livez))
        .route("/readyz", get(readyz))
        .route("/metrics", get(render_metrics))
        .with_state(AppState { status, metrics });
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            let _ = shutdown.wait_for(|stop| *stop).await;
        })
        .await?;
    Ok(())
}
