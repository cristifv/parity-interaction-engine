//! The call path (PLAN steps 3 and 4): inbound calls through parity-telephony-controller, each
//! run by `ie-core`'s state machine and the flow its number is bound to. IE sends intents and
//! never sees an SDP (T3).
//!
//! - [`run`]: the dispatcher and the call tasks (one per interaction).
//! - [`routes`]: the number map (tenant maps file) joined with the flow file.
//! - [`owner`]: STEP-3 MINIMAL etcd ownership (the lease, the replica key, the owner claim),
//!   pulled forward from step 7; a lost lease is granted again (IE-023).
//! - routing-engine (step 5a): with `[routing] enabled` only (off by default, refused in
//!   production): `ie-routing-client`, its dispatcher in [`run`]. IE-059 (CONVENTIONS "A leader
//!   change"): a rise of routing-engine's `leader_epoch` is a leader change: every call keeps
//!   going, the client follows the leader, and each call RE may hold re-attaches (`run`). IE
//!   never exits or restarts on it (phase 1's CR-080 exit is gone).

pub mod owner;
pub mod routes;
pub mod run;

use crate::config::Config;
use crate::readiness::{CheckHandle, CheckState};
use ie_obs::Metrics;
use ie_telephony_client::{
    Link, MemoryEndedLog, NodeEvidence, NodeId, NotReady, SystemClock, TcProbe, TelephonyClient,
};
use owner::{Etcd, Lease, Registration};
use run::{CallMetrics, Calls};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;

/// The `telephony` readiness check from what the call path watches: a replica conflict or a
/// superseded routing stream fails it; no etcd lease (IE-023: new calls would be declined) or no
/// live media node leaves it pending; otherwise ready.
pub fn telephony_check(
    client: Result<(), NotReady>,
    routing_superseded: bool,
    lease_live: bool,
    node_live: bool,
) -> CheckState {
    match client {
        Err(e) => CheckState::Failed(e.to_string()),
        Ok(()) if routing_superseded => CheckState::Failed(
            "routing: stream_superseded (another process uses this replica id)".to_owned(),
        ),
        Ok(()) if !lease_live => CheckState::Pending(
            "etcd: no lease (lost; a new one is being granted): new calls are declined".to_owned(),
        ),
        Ok(()) if node_live => CheckState::Ready,
        Ok(()) => CheckState::Pending("no live media node".into()),
    }
}

/// Runs the call path until `stop` turns true: registers this replica in etcd (retrying until
/// etcd answers), starts the telephony client, and hands arrivals to call tasks. `check` turns
/// ready once a media node's stream is live, and back to pending while there is no etcd lease.
/// Returns `Some(reason)` when the process must exit (nothing does since IE-059).
pub async fn serve(
    cfg: Config,
    metrics: Arc<Metrics>,
    check: CheckHandle,
    stop: watch::Receiver<bool>,
) -> Option<String> {
    serve_calls(cfg, metrics, check, stop).await
}

async fn serve_calls(
    cfg: Config,
    metrics: Arc<Metrics>,
    check: CheckHandle,
    mut stop: watch::Receiver<bool>,
) -> Option<String> {
    let (Some(tel), Some(etcd_cfg)) = (cfg.telephony.clone(), cfg.etcd.clone()) else {
        return None;
    };
    let routes = match routes::Routes::load(&cfg) {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(event = "config.refused", file = "flows", error = %e, "the number map or the flow file is refused; never ready");
            check.failed(e);
            return None;
        }
    };
    tracing::info!(
        event = "config.loaded",
        file = "flows",
        numbers = routes.len(),
        test_data = routes.test_data(),
        "number map and flows loaded"
    );
    let cdr = match crate::cdr::target(&cfg) {
        Ok(t) => crate::cdr::spawn(
            t,
            cfg.cdr.as_ref().map_or(10_000, |c| c.queue),
            metrics.meter(),
        ),
        Err(e) => {
            tracing::error!(event = "config.refused", file = "cdr", error = %e, "the CDR database settings are unusable; never ready");
            check.failed(e);
            return None;
        }
    };
    let replica_id = cfg.replica_id().to_owned();
    let bin = cfg.service.bin.clone();
    let timeout = Duration::from_millis(etcd_cfg.timeout_ms);
    let etcd = match Etcd::connect(&etcd_cfg.endpoint, timeout) {
        Ok(e) => e,
        Err(e) => {
            check.failed(format!("etcd: {e}"));
            return None;
        }
    };
    // The lease and the replica key first: TC refuses a Watch from an unregistered replica.
    let reg = Registration {
        bin: bin.clone(),
        replica_id: replica_id.clone(),
        value: serde_json::json!({
            "replica_id": replica_id,
            "bin": bin,
            "nodes": tel.nodes.iter().map(|n| n.id.clone()).collect::<Vec<_>>(),
        })
        .to_string()
        .into_bytes(),
        ttl_s: etcd_cfg.lease_ttl_s,
    };
    let first = owner::register(
        &etcd,
        &reg,
        |e| {
            check.set(CheckState::Pending(format!("etcd: {e}")));
            tracing::warn!(event = "dependency.unavailable", dependency = "etcd", error = %e, "registering in etcd failed; retrying");
        },
        &mut stop,
    )
    .await?;
    tracing::info!(
        event = "etcd.registered",
        lease = first,
        "registered in etcd (STEP-3 MINIMAL lease: granted again if lost, no step-down)"
    );
    let lease = Lease::new(first);
    register_lease_metrics(&metrics, &lease);
    tokio::spawn(owner::maintain(
        etcd.clone(),
        reg,
        lease.clone(),
        stop.clone(),
    ));
    let ended_log = match MemoryEndedLog::new(tel.profile) {
        Ok(l) => l,
        Err(e) => {
            check.failed(e.to_string());
            return None;
        }
    };
    let client_cfg = tel.client_config(&replica_id, &bin);
    let evidence: Vec<Arc<dyn NodeEvidence>> = vec![Arc::new(TcProbe::default())];
    let started = match TelephonyClient::start(client_cfg, Arc::new(SystemClock::new()), evidence) {
        Ok(s) => s,
        Err(e) => {
            check.failed(format!("telephony: {e}"));
            return None;
        }
    };
    let client = started.client;
    register_gauges(&metrics, &client, &tel.nodes);
    tokio::spawn(follow_lease(lease.clone(), client.clone(), stop.clone()));
    // routing-engine (step 5a): only with [routing] enabled. RE being down never makes IE
    // unready (queued callers wait, then the queue's no-agent action); a superseded stream does.
    let routing = match cfg.routing.as_ref().filter(|r| r.enabled) {
        None => None,
        Some(r) => {
            match ie_routing_client::RoutingClient::start(r.client_config(&replica_id, &bin)) {
                Ok(s) => {
                    tracing::info!(
                        event = "routing.client_started",
                        endpoints = r.endpoints.len() + usize::from(!r.endpoint.is_empty()),
                        "routing-engine client started"
                    );
                    Some(s)
                }
                Err(e) => {
                    check.failed(e.to_string());
                    client.shutdown();
                    return None;
                }
            }
        }
    };
    let (routing_client, routing_events) = match routing {
        Some(s) => (Some(s.client), Some(s.events)),
        None => (None, None),
    };
    if let Some(rc) = routing_client.clone() {
        let rc2 = rc.clone();
        metrics
            .meter()
            .u64_observable_gauge("interaction_engine_routing_watch_live")
            .with_description("1 while the routing-engine Watch stream is live")
            .with_callback(move |o| {
                o.observe(u64::from(rc.link() == ie_routing_client::Link::Live), &[]);
            })
            .build();
        metrics
            .meter()
            .u64_observable_counter("interaction_engine_routing_leader_changes_total")
            .with_description(
                "routing-engine leader changes seen (IE-059: calls keep going and re-attach)",
            )
            .with_callback(move |o| o.observe(rc2.leader_changes(), &[]))
            .build();
    }
    let calls = Arc::new(Calls {
        client: client.clone(),
        etcd,
        lease: lease.clone(),
        replica_id,
        bin,
        routes,
        cdr,
        ended_log,
        metrics: CallMetrics::new(metrics.meter()),
        routing: routing_client.clone(),
        agents: cfg.agents.clone(),
        recording: cfg.recording.as_ref().map(|r| r.mode).unwrap_or_default(),
    });
    let routing_dispatcher =
        routing_events.map(|ev| tokio::spawn(Arc::clone(&calls).routing_dispatch(ev)));
    let dispatcher = tokio::spawn(calls.dispatch(started.events));
    // Ready once any node's stream is live (and while no replica conflict is reported).
    let nodes: Vec<NodeId> = tel.nodes.iter().map(|n| NodeId(n.id.clone())).collect();
    let mut tick = tokio::time::interval(Duration::from_millis(250));
    let fatal = None;
    loop {
        tokio::select! {
            _ = tick.tick() => {}
            _ = stop.changed() => break,
        }
        let superseded = routing_client
            .as_ref()
            .is_some_and(|rc| rc.link() == ie_routing_client::Link::Superseded);
        check.set(telephony_check(
            client.ready(),
            superseded,
            lease.current().is_some(),
            nodes.iter().any(|n| client.link(n) == Some(Link::Live)),
        ));
    }
    client.shutdown();
    if let Some(rc) = &routing_client {
        rc.shutdown();
    }
    if let Some(d) = routing_dispatcher {
        d.abort();
    }
    dispatcher.abort();
    fatal
}

/// IE-055 (the coordinator's ruling, 2026-10-31): while there is no etcd lease the client takes
/// no arrivals (no ack, no decline), so TC offers them to a healthy replica after its 1 s no-ack
/// timer; the caller gets TC's 503 at the 5 s deadline only when no replica takes it. An arrival
/// taken just before the loss still gets 503 at its claim (it was acked: TC won't re-offer it).
pub async fn follow_lease(lease: Lease, client: TelephonyClient, mut stop: watch::Receiver<bool>) {
    let mut rx = lease.subscribe();
    loop {
        let live = rx.borrow_and_update().is_some();
        client.accept_arrivals(live);
        tokio::select! {
            r = rx.changed() => if r.is_err() { return },
            _ = stop.changed() => return,
        }
    }
}

fn register_lease_metrics(metrics: &Metrics, lease: &Lease) {
    let l = lease.clone();
    metrics
        .meter()
        .u64_observable_gauge("interaction_engine_etcd_lease_live")
        .with_description(
            "1 while this replica holds a live etcd lease (0: new calls are declined)",
        )
        .with_callback(move |o| o.observe(u64::from(l.current().is_some()), &[]))
        .build();
    let l = lease.clone();
    metrics
        .meter()
        .u64_observable_counter("interaction_engine_etcd_lease_regrants_total")
        .with_description("Lost etcd leases replaced by a new one (IE-023)")
        .with_callback(move |o| o.observe(l.regrants(), &[]))
        .build();
}

fn register_gauges(
    metrics: &Metrics,
    client: &TelephonyClient,
    nodes: &[ie_telephony_client::NodeConfig],
) {
    let c = client.clone();
    metrics
        .meter()
        .u64_observable_gauge("interaction_engine_telephony_sessions_held")
        .with_description("What the telephony client holds: sessions with state, and recently forgotten keys (IE-022)")
        .with_callback(move |o| {
            let h = c.held();
            o.observe(h.sessions as u64, &[opentelemetry::KeyValue::new("kind", "session")]);
            o.observe(h.forgotten as u64, &[opentelemetry::KeyValue::new("kind", "forgotten")]);
        })
        .build();
    let ids: Vec<NodeId> = nodes.iter().map(|n| NodeId(n.id.clone())).collect();
    let (c, i) = (client.clone(), ids.clone());
    metrics
        .meter()
        .u64_observable_gauge("interaction_engine_unmanaged_calls")
        .with_description("Calls on a media node whose TC stream is lost (they keep their audio)")
        .with_callback(move |o| {
            for n in &i {
                o.observe(
                    c.unmanaged_calls(n) as u64,
                    &[opentelemetry::KeyValue::new("node", n.0.clone())],
                );
            }
        })
        .build();
    let c = client.clone();
    metrics
        .meter()
        .u64_observable_gauge("interaction_engine_telephony_node_live")
        .with_description("1 while the media node's TC stream is live")
        .with_callback(move |o| {
            for n in &ids {
                o.observe(
                    u64::from(c.link(n) == Some(Link::Live)),
                    &[opentelemetry::KeyValue::new("node", n.0.clone())],
                );
            }
        })
        .build();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_etcd_lease_is_not_ready_and_its_return_is() {
        let ready = |lease, node| telephony_check(Ok(()), false, lease, node);
        assert_eq!(ready(true, true), CheckState::Ready);
        assert!(
            matches!(ready(false, true), CheckState::Pending(m) if m.contains("no lease")),
            "IE-023: a live node without a lease is not ready"
        );
        assert!(matches!(ready(true, false), CheckState::Pending(_)));
        let conflict = NotReady::ReplicaConflict {
            replica_id: "ie-1".into(),
            node: NodeId("media-1".into()),
        };
        assert!(matches!(
            telephony_check(Err(conflict), false, true, true),
            CheckState::Failed(_)
        ));
        assert!(matches!(
            telephony_check(Ok(()), true, true, true),
            CheckState::Failed(_)
        ));
    }
}
