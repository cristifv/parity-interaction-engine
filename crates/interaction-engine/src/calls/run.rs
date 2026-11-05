//! The call tasks (PLAN "How one call runs inside IE"): one dispatcher for the client's events,
//! one task per arrival driving `ie-core`'s [`Call`]. The task runs the machine's effects one at a
//! time, in order (so one command is in flight per session, T16), feeds back each result, reads
//! TC's events between effects, and sleeps until the machine's next deadline.
//!
//! Digits go from TC's `DtmfReceived` straight into the machine: never logged here. An agent's
//! DTMF action codes (P5) are configured on the agent's leg and matched in parity-ms; only what
//! they did reaches IE (`DtmfAction`), never a digit.
//!
//! Step 6: the agent's leg is placed through TC (`AddLeg`, a passthrough call TC bridges on
//! answer), hold music and position announcements play while queued, recordings start per leg
//! (TC's null sink until step 9), the CDR opens at answer and closes at the end. After a restart
//! under the same replica id, the end of a call the previous process owned closes its CDR and
//! frees its agent in RE (`orphan_ended`), without any command to TC.

use super::owner::{Claim, Etcd, EtcdError, Lease, OwnerRecord};
use super::routes::Routes;
use crate::cdr::{CdrEnd, CdrOp, CdrRecord, CdrSink};
use crate::config::{AgentDtmfAction, AgentsConfig, RecordingMode};
use ie_core::{
    AgentLeg, AgentOutcome, Call, CallEvent, Cmd, CmdResult, ConsentBasis, Decision, DequeueReason,
    DtmfKind, DtmfResult, Effect, EndCause, EnqueueReason, Ids, Input, OverflowAction, Phase,
    RecState, Route, RouteCmd, RouteResult, RouteSync,
};
use ie_routing_client::{Delivery, RouteError, RoutingClient, RoutingEvent};
use ie_telephony_client::{
    AddLeg, Arrival, ClientEvent, CodecPolicy, Destination, DigitReporting, Digits, DtmfActionKind,
    DtmfActionResult, DtmfActions, EndReason, EndedAck, EndedBy, EndedLog, Endpoint, Facet, Intent,
    LegRole, LegState, MemoryEndedLog, MonitorMode, Number, Outcome, Owner, PlayEndReason,
    PlayFailReason, RecordingKey, RecordingState, SessionEndReason, SessionEvent, SessionEvents,
    SessionState, Telephony,
};
use opentelemetry::KeyValue;
use opentelemetry::metrics::{Counter, Meter};
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::{Instrument, error, info, info_span, warn};

/// The calls' counters.
#[derive(Clone)]
pub struct CallMetrics {
    arrivals: Counter<u64>,
    answered: Counter<u64>,
    declined: Counter<u64>,
    tenant_mismatch: Counter<u64>,
    ended: Counter<u64>,
    consent: Counter<u64>,
    flow: Counter<u64>,
    routing: Counter<u64>,
    /// Calls in `Ending` past `ENDING_STUCK_MS` right now (IE-056: waiting, never closed).
    stuck: Arc<std::sync::atomic::AtomicU64>,
}

impl CallMetrics {
    pub fn new(meter: &Meter) -> Self {
        let c = |name: &'static str, d: &'static str| {
            meter.u64_counter(name).with_description(d).build()
        };
        Self {
            arrivals: c(
                "interaction_engine_arrivals_total",
                "Arrivals offered to this replica",
            ),
            answered: c(
                "interaction_engine_calls_answered_total",
                "Calls answered (AnswerLeg done)",
            ),
            declined: c(
                "interaction_engine_arrivals_declined_total",
                "Arrivals declined, by SIP status",
            ),
            tenant_mismatch: c(
                "interaction_engine_tenant_mismatch_total",
                "Arrivals whose X-Tenant-ID disagreed with the number map (refused, alerted)",
            ),
            ended: c(
                "interaction_engine_calls_ended_total",
                "Calls whose ENDED record was written",
            ),
            consent: c(
                "interaction_engine_consent_decisions_total",
                "Consent decisions, by decision and basis",
            ),
            flow: c(
                "interaction_engine_flow_events_total",
                "Flow events: skipped verbs, missing prompts, dropped digits, loop guards, no-input actions",
            ),
            routing: c(
                "interaction_engine_routing_requests_total",
                "Requests to routing-engine, by rpc and result",
            ),
            stuck: {
                let n = Arc::new(std::sync::atomic::AtomicU64::new(0));
                let r = Arc::clone(&n);
                meter
                    .u64_observable_gauge("interaction_engine_calls_ending_stuck")
                    .with_description("Calls waiting in Ending for TC's end longer than 5 min (TC unreachable or the end refused); they keep their owner key")
                    .with_callback(move |o| {
                        o.observe(r.load(std::sync::atomic::Ordering::Relaxed), &[]);
                    })
                    .build();
                n
            },
        }
    }
}

/// What every call task shares.
pub struct Calls<T: Telephony> {
    pub client: T,
    pub etcd: Etcd,
    /// STEP-3 MINIMAL: the lease claims are written on; none while it is being granted again.
    pub lease: Lease,
    pub replica_id: String,
    pub bin: String,
    /// Number → tenant (the maps file) and flow (the flow file).
    pub routes: Routes,
    /// The CDR writer's queue.
    pub cdr: CdrSink,
    /// STEP-3 MINIMAL: not durable (IE-006, step 8).
    pub ended_log: MemoryEndedLog,
    pub metrics: CallMetrics,
    /// routing-engine's client, with `[routing] enabled` (step 5a).
    pub routing: Option<RoutingClient>,
    /// The agents' legs: their DTMF action codes (step 6).
    pub agents: AgentsConfig,
    /// Recordings (step 6): off, or TC's null sink.
    pub recording: RecordingMode,
}

/// The agent leg's ring time, as the machine's answer timer.
const AGENT_RING_S: u32 = (ie_core::machine::AGENT_LEG_ANSWER_MS / 1000) as u32;

/// The null sink's key (step 6, MS-012): a throwaway data key from the OS RNG, held in memory
/// for the one request, never logged; TC drops it at once and writes nothing.
fn null_sink_key() -> Option<RecordingKey> {
    let mut dek = vec![0u8; 32];
    getrandom::fill(&mut dek).ok()?;
    Some(RecordingKey {
        dek,
        wrapped_dek: Vec::new(),
        kek_id: "local-null-sink".to_owned(),
    })
}

/// A routing request in flight: its answer as the machine's input.
type RouteFut = Pin<Box<dyn Future<Output = Input> + Send>>;

/// What woke the call task.
enum Next {
    Tc(Option<SessionEvent>),
    Routing(Option<Delivery>),
    Routed(Input),
    /// IE-059: routing-engine's leader changed (or the watch closed: `false`).
    Leader(bool),
    Resynced(Input),
    Tick,
}

/// IE-059: the next leader change (pending forever without routing, or once it closed).
async fn leader_changed(rx: &mut Option<tokio::sync::watch::Receiver<u64>>) -> bool {
    match rx {
        Some(r) => r.changed().await.is_ok(),
        None => std::future::pending().await,
    }
}

async fn recv_routing(rx: &mut Option<mpsc::Receiver<Delivery>>) -> Option<Delivery> {
    match rx {
        Some(r) => r.recv().await,
        None => std::future::pending().await,
    }
}

async fn routed(f: &mut Option<RouteFut>) -> Input {
    match f {
        Some(f) => f.await,
        None => std::future::pending().await,
    }
}

async fn sleep_until(wait: Option<Duration>) {
    match wait {
        Some(d) => tokio::time::sleep(d).await,
        None => std::future::pending().await,
    }
}

/// IE-059: a leader change's `Reattach`, as the machine's input (None: it failed).
fn resync_input(
    counter: &Counter<u64>,
    r: Result<ie_routing_client::Reattached, RouteError>,
) -> Option<RouteSync> {
    let result = match &r {
        Ok(_) => "ok",
        Err(RouteError::Fenced) => "fenced",
        Err(RouteError::Unavailable(_)) => "unavailable",
        Err(_) => "refused",
    };
    counter.add(
        1,
        &[
            KeyValue::new("rpc", "Reattach"),
            KeyValue::new("result", result),
        ],
    );
    match r {
        Ok(a) => {
            info!(
                event = "routing.reattached",
                op = "Reattach",
                outcome = "ok",
                entry = a.entry_id.is_some(),
                reservations = a.reservations.len(),
                "re-attached after routing-engine's leader change"
            );
            Some(RouteSync {
                entry_id: a.entry_id,
                reservations: a
                    .reservations
                    .into_iter()
                    .map(|x| (x.reservation_id, x.connected))
                    .collect(),
            })
        }
        Err(e) => {
            warn!(event = "routing.rpc_failed", op = "Reattach", outcome = "error", error = %e,
                "the Reattach after a leader change failed; the call goes on");
            None
        }
    }
}

fn route_result(
    rpc: &'static str,
    counter: &Counter<u64>,
    r: Result<RouteResult, RouteError>,
) -> Input {
    let (result, input) = match r {
        Ok(v) => ("ok", v),
        Err(RouteError::Fenced) => ("fenced", RouteResult::Fenced),
        Err(RouteError::Refused(_)) => ("refused", RouteResult::Refused),
        Err(RouteError::Bug(_)) => ("bug", RouteResult::Refused),
        Err(RouteError::Unavailable(_)) => ("unavailable", RouteResult::Unavailable),
    };
    counter.add(
        1,
        &[KeyValue::new("rpc", rpc), KeyValue::new("result", result)],
    );
    Input::Route(input)
}

/// Play and collection ids: UUIDv7.
struct UuidIds;
impl Ids for UuidIds {
    fn next_id(&mut self) -> String {
        uuid::Uuid::now_v7().to_string()
    }
}

/// The machine's clock: monotonic milliseconds since the process started.
fn mono_ms() -> u64 {
    static START: OnceLock<Instant> = OnceLock::new();
    u64::try_from(START.get_or_init(Instant::now).elapsed().as_millis()).unwrap_or(u64::MAX)
}

fn wall_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}

/// The interaction's routing owner: the number map's tenant (never X-Tenant-ID), the etcd epoch.
fn routing_owner(io: &Io) -> Option<ie_routing_client::Owner> {
    let (o, rec) = (io.owner.as_ref()?, io.record.as_ref()?);
    Some(ie_routing_client::Owner {
        tenant_id: rec.tenant_id.clone(),
        interaction_id: o.key.interaction_id.clone(),
        owner_epoch: o.owner_epoch,
    })
}

fn result_of(o: &Outcome) -> CmdResult {
    match o {
        Outcome::Done(_) => CmdResult::Done,
        Outcome::Fenced | Outcome::ArrivalClosed => CmdResult::Fenced,
        Outcome::Ended(_) | Outcome::Lost => CmdResult::Gone,
        Outcome::Config(_) => CmdResult::PromptUnknown,
        _ => CmdResult::Failed,
    }
}

/// The outcome's name for a log line (never its snapshot).
fn outcome_name(o: &Outcome) -> String {
    match o {
        Outcome::Done(_) => "done".into(),
        Outcome::Resync(_) => "resync".into(),
        Outcome::Ended(_) => "ended".into(),
        other => format!("{other:?}"),
    }
}

/// One call's I/O state, next to its machine.
struct Io {
    owner: Option<Owner>,
    /// The routing owner: the number map's tenant, the etcd epoch.
    routing_owner: Option<ie_routing_client::Owner>,
    record: Option<OwnerRecord>,
    ended: Option<(EndReason, EndedAck)>,
    customer_ended_by: Option<EndedBy>,
    /// Counted in `interaction_engine_calls_ending_stuck` (IE-056).
    stuck: bool,
    /// The CDR's row is written: later snapshots are dropped.
    finished: bool,
    /// The call's snapshots so far (`cdr_progress.progress_seq`).
    progress_seq: i32,
    wall0: i64,
    mono0: u64,
}

impl Io {
    fn wall(&self, mono: u64) -> i64 {
        self.wall0 + i64::try_from(mono.saturating_sub(self.mono0)).unwrap_or(0)
    }
    /// A wall-clock time as the machine's (0 stays 0: none given).
    fn mono(&self, wall: i64) -> u64 {
        if wall <= 0 {
            return 0;
        }
        let d = wall.saturating_sub(self.wall0);
        if d >= 0 {
            self.mono0.saturating_add(u64::try_from(d).unwrap_or(0))
        } else {
            self.mono0.saturating_sub(d.unsigned_abs())
        }
    }
}

impl<T: Telephony + 'static> Calls<T> {
    /// Hands each arrival to its own task until the client's event channel closes.
    pub async fn dispatch(self: Arc<Self>, mut events: mpsc::Receiver<ClientEvent>) {
        while let Some(ev) = events.recv().await {
            match ev {
                ClientEvent::Arrived(arrival, mailbox) => {
                    self.metrics.arrivals.add(1, &[]);
                    // The tenant is the number map's (IE-030); X-Tenant-ID is only a claim.
                    let route = self.routes.get(arrival.called_number.expose()).cloned();
                    let tenant = route
                        .as_ref()
                        .map(|r| r.tenant_id.clone())
                        .or_else(|| self.routes.tenant_of(arrival.called_number.expose()))
                        .unwrap_or_default();
                    let span = info_span!(
                        "call",
                        tenant_id = %tenant,
                        claimed_tenant_id = %arrival.key.tenant_id,
                        interaction_id = %arrival.key.interaction_id,
                        node = %arrival.node,
                        leg_id = %arrival.leg_id,
                    );
                    tokio::spawn(
                        Arc::clone(&self)
                            .call(*arrival, route, mailbox)
                            .instrument(span),
                    );
                }
                ClientEvent::Node { node, link } => {
                    info!(event = "telephony.node_link_changed",  node = %node, link = ?link, "a media node's link changed");
                }
                ClientEvent::Orphan { node, key } => warn!(event = "session.orphaned",
                    node = %node,
                    session_tenant_id = %key.tenant_id,
                    interaction_id = %key.interaction_id,
                    "events for a session this replica doesn't hold (adoption is step 8)"
                ),
                ClientEvent::OrphanEnded {
                    node,
                    key,
                    owner_epoch,
                    reason,
                    sip_status,
                    at_ms,
                    ack,
                } => {
                    let span = info_span!(
                        "orphan_end",
                        tenant_id = %key.tenant_id,
                        interaction_id = %key.interaction_id,
                        node = %node,
                        owner_epoch,
                    );
                    tokio::spawn(
                        Arc::clone(&self)
                            .orphan_ended(key, reason, sip_status, at_ms, ack)
                            .instrument(span),
                    );
                }
            }
        }
    }

    async fn call(
        self: Arc<Self>,
        arrival: Arrival,
        route: Option<Route>,
        mut mailbox: SessionEvents,
    ) {
        info!(event = "call.arrived",
            called_number = %arrival.called_number,
            caller_number = %arrival.caller_number,
            attempt = arrival.attempt,
            flow = route.as_ref().map_or("", |r| r.flow_name.as_str()),
            "a call arrived"
        );
        let mono0 = mono_ms();
        let mut io = Io {
            owner: None,
            // The owner record's tenant is the map's (IE-030); only claimed past the check.
            record: route.as_ref().map(|r| OwnerRecord {
                tenant_id: r.tenant_id.clone(),
                bin: self.bin.clone(),
                replica_id: self.replica_id.clone(),
                node_id: arrival.node.0.clone(),
            }),
            routing_owner: None,
            ended: None,
            customer_ended_by: None,
            stuck: false,
            finished: false,
            progress_seq: 0,
            wall0: wall_ms(),
            mono0,
        };
        let (mut call, first) =
            Call::arrive(mono0, route, &arrival.key.tenant_id, Box::new(UuidIds));
        let mut effects: VecDeque<Effect> = first.into();
        // Routing (step 5a): its requests run beside TC's events, one at a time (the machine's
        // rule); its messages come through the interaction's routing mailbox.
        let mut route_fut: Option<RouteFut> = None;
        let mut routing_mail: Option<mpsc::Receiver<Delivery>> = None;
        // IE-059: leader changes, watched from before the call's first routing request.
        let mut leader = self.routing.as_ref().map(|rc| {
            let mut rx = rc.subscribe_leader();
            rx.borrow_and_update();
            rx
        });
        let mut resync_fut: Option<RouteFut> = None;
        let mut tc_open = true;
        loop {
            while let Some(effect) = effects.pop_front() {
                let input = match effect {
                    Effect::Route(cmd) => {
                        if route_fut.is_some() {
                            error!(
                                event = "routing.machine_bug",
                                "a routing request while another is in flight (a machine bug)"
                            );
                        }
                        route_fut = Some(self.route_request(&io, cmd));
                        None
                    }
                    other => self.run(&arrival, &mut io, other).await,
                };
                if let Some(i) = input {
                    effects.extend(call.step(mono_ms(), i));
                }
                // Bind the interaction's routing mailbox once the claim gives it an epoch.
                if routing_mail.is_none()
                    && let (Some(rc), Some(o)) = (&self.routing, routing_owner(&io))
                {
                    routing_mail = Some(rc.bind(&o));
                    io.routing_owner = Some(o);
                }
            }
            if call.done() {
                break;
            }
            let wait = call
                .deadline()
                .map(|at| Duration::from_millis(at.saturating_sub(mono_ms())));
            let next = tokio::select! {
                ev = mailbox.recv(), if tc_open => Next::Tc(ev),
                d = recv_routing(&mut routing_mail) => Next::Routing(d),
                i = routed(&mut route_fut) => Next::Routed(i),
                ok = leader_changed(&mut leader), if resync_fut.is_none() => Next::Leader(ok),
                i = routed(&mut resync_fut) => Next::Resynced(i),
                () = sleep_until(wait) => Next::Tick,
            };
            let input = match next {
                Next::Tick => Some(Input::Tick),
                Next::Leader(false) => {
                    leader = None;
                    None
                }
                Next::Leader(true) => {
                    // IE-059: the call keeps going; if RE may hold it, it re-attaches (after a
                    // random part of the spread, bounded in number by the client).
                    if let (true, Some(rc), Some(o)) = (
                        call.reattach_wanted(),
                        self.routing.clone(),
                        io.routing_owner.clone(),
                    ) {
                        let counter = self.metrics.routing.clone();
                        resync_fut = Some(Box::pin(async move {
                            let r = rc.resync(&o).await;
                            Input::Reattached(resync_input(&counter, r))
                        }));
                    }
                    None
                }
                Next::Resynced(i) => {
                    resync_fut = None;
                    Some(i)
                }
                Next::Routed(i) => {
                    route_fut = None;
                    Some(i)
                }
                Next::Tc(None) if call.phase() == Phase::Closing => {
                    // The session is over; only routing's cleanup is left.
                    tc_open = false;
                    None
                }
                Next::Tc(None) => {
                    // Fenced, or the node declared dead (the client ends the session for us).
                    warn!(event = "call.mailbox_closed",  phase = ?call.phase(), "the session's mailbox closed");
                    break;
                }
                Next::Tc(Some(ev)) => self.event(&arrival, &mut io, ev),
                Next::Routing(None) => {
                    routing_mail = None;
                    None
                }
                Next::Routing(Some(d)) => {
                    let seq = d.delivery_seq;
                    if let Some(i) = self.routing_input(&io, d.event) {
                        effects.extend(call.step(mono_ms(), i));
                    }
                    // Acknowledged once the machine took it.
                    if let (Some(rc), Some(o)) = (&self.routing, &io.routing_owner) {
                        rc.ack(o, seq);
                    }
                    None
                }
            };
            if let Some(i) = input {
                effects.extend(call.step(mono_ms(), i));
            }
        }
        if let (Some(rc), Some(o)) = (&self.routing, &io.routing_owner) {
            rc.release(o);
        }
        if io.stuck {
            self.metrics
                .stuck
                .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        }
        info!(event = "call.task_done",  phase = ?call.phase(), "call task done");
    }

    /// A routing message as the machine's input (None: nothing for it).
    fn routing_input(&self, io: &Io, ev: RoutingEvent) -> Option<Input> {
        match ev {
            RoutingEvent::Assigned {
                reservation_id,
                entry_id,
                agent_id,
                agent_contact,
                deadline_ms,
                ..
            } => {
                info!(event = "routing.assigned",  reservation_id = %reservation_id, entry_id = %entry_id, agent_id = %agent_id, deadline_ms, "routing-engine assigned an agent");
                Some(Input::Assigned {
                    reservation_id,
                    entry_id,
                    agent_id,
                    agent_contact,
                    deadline_ms: io.mono(deadline_ms),
                })
            }
            RoutingEvent::Revoke {
                reservation_id,
                reason,
            } => {
                info!(event = "routing.revoked",  reservation_id = %reservation_id, reason = %reason, "reservation revoked");
                Some(Input::Revoked { reservation_id })
            }
            RoutingEvent::Overflow { entry_id, action } => {
                info!(event = "routing.overflowed",  entry_id = %entry_id, action = ?action, "routing-engine's overflow for the entry");
                let action = match action {
                    ie_routing_client::OverflowAction::Queue { .. } => OverflowAction::Queue,
                    ie_routing_client::OverflowAction::CallbackOffer => {
                        OverflowAction::CallbackOffer
                    }
                    ie_routing_client::OverflowAction::Voicemail => OverflowAction::Voicemail,
                    ie_routing_client::OverflowAction::Disconnect => OverflowAction::Disconnect,
                    ie_routing_client::OverflowAction::FlowStep(s) => OverflowAction::FlowStep(s),
                    ie_routing_client::OverflowAction::Unknown => return None,
                };
                Some(Input::Overflow { entry_id, action })
            }
            RoutingEvent::Position {
                entry_id,
                position,
                expected_wait_s,
            } => {
                info!(event = "queue.position_changed",  entry_id = %entry_id, position, expected_wait_s, "queue position");
                Some(Input::Position { entry_id, position })
            }
            RoutingEvent::WrapUpEnded {
                reservation_id,
                agent_id,
            } => {
                self.metrics
                    .flow
                    .add(1, &[KeyValue::new("event", "wrap_up_ended")]);
                info!(event = "agent.wrap_up_ended",  reservation_id = %reservation_id, agent_id = %agent_id, "agent wrap-up ended");
                None
            }
        }
    }

    /// One routing request, run beside the call's other events; its answer as an input.
    fn route_request(&self, io: &Io, cmd: RouteCmd) -> RouteFut {
        let (Some(client), Some(owner)) = (self.routing.clone(), io.routing_owner.clone()) else {
            warn!(
                event = "routing.request_refused",
                "a routing request without routing or an owner: refused"
            );
            return Box::pin(async { Input::Route(RouteResult::Refused) });
        };
        let counter = self.metrics.routing.clone();
        match cmd {
            RouteCmd::Enqueue {
                enqueue_seq,
                queue_id,
                priority,
                first_queued_ms,
                ordering_ms,
                reason,
            } => {
                tracing::debug!(event = "routing.request_sent", op = "Enqueue", queue_id = %queue_id, enqueue_seq, priority, "enqueue");
                let e = ie_routing_client::Enqueue {
                    enqueue_seq,
                    queue_id,
                    priority,
                    first_queued_at_ms: io.wall(first_queued_ms),
                    ordering_at_ms: io.wall(ordering_ms),
                    reason: match reason {
                        EnqueueReason::New => ie_routing_client::EnqueueReason::New,
                        EnqueueReason::Transfer => ie_routing_client::EnqueueReason::Transfer,
                    },
                    language: String::new(),
                };
                Box::pin(async move {
                    let r = client.enqueue(&owner, &e).await;
                    match &r {
                        Ok((entry_id, existing, position)) => {
                            info!(event = "routing.enqueued", op = "Enqueue", outcome = "ok", queue_id = %e.queue_id, entry_id = %entry_id, existing, position, "enqueued")
                        }
                        Err(err) => {
                            warn!(event = "routing.rpc_failed", op = "Enqueue", outcome = "error", error = %err, "enqueue failed")
                        }
                    }
                    let r = r.map(|(entry_id, _, position)| RouteResult::Enqueued {
                        entry_id,
                        position,
                    });
                    route_result("Enqueue", &counter, r)
                })
            }
            RouteCmd::Dequeue { entry_id, reason } => {
                let reason = match reason {
                    DequeueReason::Abandoned => ie_routing_client::DequeueReason::Abandoned,
                    DequeueReason::Ended => ie_routing_client::DequeueReason::Ended,
                    DequeueReason::Overflow => ie_routing_client::DequeueReason::Overflow,
                };
                tracing::debug!(event = "routing.request_sent", op = "Dequeue", entry_id = %entry_id, reason = ?reason, "dequeue");
                Box::pin(async move {
                    let r = client.dequeue(&owner, &entry_id, reason).await;
                    match &r {
                        Ok(already_gone) => {
                            info!(event = "routing.dequeued", op = "Dequeue", outcome = "ok", reason = ?reason, already_gone, "dequeued")
                        }
                        Err(err) => {
                            warn!(event = "routing.rpc_failed", op = "Dequeue", outcome = "error", error = %err, "dequeue failed")
                        }
                    }
                    route_result("Dequeue", &counter, r.map(|_| RouteResult::Done))
                })
            }
            RouteCmd::ReportOutcome {
                reservation_id,
                outcome,
                detail,
            } => {
                let outcome = match outcome {
                    AgentOutcome::Connected => ie_routing_client::Outcome::Connected,
                    AgentOutcome::AgentNoAnswer => ie_routing_client::Outcome::AgentNoAnswer,
                    AgentOutcome::AgentLegFailed => ie_routing_client::Outcome::AgentLegFailed,
                    AgentOutcome::CallerGone => ie_routing_client::Outcome::CallerGone,
                    AgentOutcome::AlreadyAssigned => ie_routing_client::Outcome::AlreadyAssigned,
                };
                tracing::debug!(event = "routing.request_sent", op = "ReportOutcome", reservation_id = %reservation_id, outcome = ?outcome, detail, "report outcome");
                Box::pin(async move {
                    let r = client
                        .report_outcome(&owner, &reservation_id, outcome, detail)
                        .await;
                    match &r {
                        Ok(recorded) => {
                            info!(event = "routing.outcome_recorded", op = "ReportOutcome", outcome = "ok", reported = ?outcome, recorded = ?recorded, "outcome recorded")
                        }
                        Err(err) => {
                            warn!(event = "routing.rpc_failed", op = "ReportOutcome", outcome = "error", error = %err, "report outcome failed")
                        }
                    }
                    let r = r.map(|rec| RouteResult::Recorded {
                        connected: rec == ie_routing_client::Recorded::Connected,
                    });
                    route_result("ReportOutcome", &counter, r)
                })
            }
            RouteCmd::CallEnded {
                agent_id,
                reservation_id,
                ended_ms,
            } => {
                let ended_at_ms = io.wall(ended_ms);
                tracing::debug!(event = "routing.request_sent", op = "CallEnded", reservation_id = %reservation_id, agent_id = %agent_id, "call ended for the agent");
                Box::pin(async move {
                    let r = client
                        .call_ended(&owner, &agent_id, &reservation_id, ended_at_ms)
                        .await;
                    if let Err(err) = &r {
                        warn!(event = "routing.rpc_failed", op = "CallEnded", outcome = "error", error = %err, "CallEnded failed");
                    }
                    route_result("CallEnded", &counter, r.map(|()| RouteResult::Done))
                })
            }
        }
    }

    /// Routing messages for released interactions and strangers (the client's events).
    pub async fn routing_dispatch(
        self: Arc<Self>,
        mut events: mpsc::Receiver<ie_routing_client::ClientEvent>,
    ) {
        use ie_routing_client::ClientEvent as Ev;
        while let Some(ev) = events.recv().await {
            let Some(rc) = self.routing.clone() else {
                continue;
            };
            match ev {
                Ev::AfterEnd { owner, delivery } => {
                    let seq = delivery.delivery_seq;
                    if let RoutingEvent::Assigned { reservation_id, .. } = delivery.event {
                        // Abandon vs assign: the caller is gone; RE frees the agent.
                        let counter = self.metrics.routing.clone();
                        tokio::spawn(async move {
                            let r = rc
                                .report_outcome(
                                    &owner,
                                    &reservation_id,
                                    ie_routing_client::Outcome::CallerGone,
                                    "ended",
                                )
                                .await;
                            info!(event = "routing.caller_gone_reported",
                                tenant_id = %owner.tenant_id,
                                interaction_id = %owner.interaction_id,
                                reservation_id = %reservation_id,
                                result = ?r,
                                "an Assigned after the call ended: reported CALLER_GONE"
                            );
                            let _ = route_result(
                                "ReportOutcome",
                                &counter,
                                r.map(|_| RouteResult::Done),
                            );
                            rc.ack_released(&owner, seq);
                        });
                    } else {
                        rc.ack_released(&owner, seq);
                    }
                }
                Ev::Orphan {
                    tenant_id,
                    interaction_id,
                } => warn!(event = "routing.orphaned",
                    session_tenant_id = %tenant_id,
                    interaction_id = %interaction_id,
                    "routing messages for an interaction this replica doesn't hold (adoption is step 8)"
                ),
                Ev::RunCallback {
                    tenant_id,
                    interaction_id,
                } => warn!(event = "routing.callback_ignored",
                    session_tenant_id = %tenant_id,
                    interaction_id = %interaction_id,
                    "RunCallback isn't handled before step 12: left to RE's redelivery"
                ),
            }
        }
    }

    /// One event from TC, as the machine's input (None: nothing for it).
    fn event(&self, arrival: &Arrival, io: &mut Io, ev: SessionEvent) -> Option<Input> {
        match ev {
            SessionEvent::PlayFinished {
                play_id,
                reason,
                played_ms,
                ..
            } => {
                tracing::debug!(event = "prompt.finished",  play_id = %play_id, reason = ?reason, played_ms, "play finished");
                Some(Input::PlayFinished {
                    play_id,
                    completed: reason == PlayEndReason::Completed,
                })
            }
            SessionEvent::PlayStarted { play_id, .. } => {
                tracing::debug!(event = "play.started", play_id = %play_id, "TC reported the play started");
                None
            }
            SessionEvent::PlayFailed {
                play_id, reason, ..
            } => {
                tracing::info!(event = "play.failed", play_id = %play_id, reason = ?reason, "a play ended without starting");
                Some(Input::PlayFailed {
                    play_id,
                    prompt_unknown: reason == PlayFailReason::PromptUnknown,
                })
            }
            SessionEvent::Digit {
                digit,
                collection_id,
                ..
            } => {
                // PCI: the digit goes to the machine only; nothing about it is logged.
                let d = digit.expose().chars().next()?;
                Some(Input::Digit {
                    collection_id,
                    digit: d,
                })
            }
            SessionEvent::Leg(l) => {
                info!(event = "leg.updated",  leg_id = %l.leg_id, role = ?l.role, state = ?l.state, cause = ?l.cause, sip_status = l.sip_status, "leg update");
                if l.leg_id == arrival.leg_id {
                    if l.state == LegState::Ended {
                        io.customer_ended_by = Some(l.ended_by);
                    }
                    return None;
                }
                // An agent's leg this call placed (the machine checks the id).
                let leg = match l.state {
                    LegState::Answered => AgentLeg::Answered,
                    LegState::Ended if l.answered_at_ms > 0 => AgentLeg::Ended,
                    LegState::Ended if l.cause == ie_telephony_client::EndCause::NoAnswer => {
                        AgentLeg::NoAnswer
                    }
                    LegState::Ended => AgentLeg::Failed,
                    _ => return None,
                };
                Some(Input::AgentLeg {
                    leg_id: l.leg_id,
                    leg,
                })
            }
            SessionEvent::DtmfAction {
                leg_id,
                role,
                action,
                result,
                state,
            } => {
                // What parity-ms did with an action code (P5): never the digits or the code.
                info!(event = "dtmf.action_reported",  leg_id = %leg_id, role = ?role, action = ?action, result = ?result,
                    recording = ?state.recording, muted = state.muted, "dtmf action");
                if role != LegRole::Agent {
                    return None;
                }
                Some(Input::DtmfAction {
                    action: match action {
                        DtmfActionKind::Mute => DtmfKind::Mute,
                        DtmfActionKind::Unmute => DtmfKind::Unmute,
                        DtmfActionKind::MuteToggle => DtmfKind::MuteToggle,
                        DtmfActionKind::RecordingPause => DtmfKind::RecordingPause,
                        DtmfActionKind::RecordingResume => DtmfKind::RecordingResume,
                        DtmfActionKind::RecordingToggle => DtmfKind::RecordingToggle,
                        _ => DtmfKind::Other,
                    },
                    result: match result {
                        DtmfActionResult::Applied => DtmfResult::Applied,
                        DtmfActionResult::Noop => DtmfResult::Noop,
                        DtmfActionResult::Locked => DtmfResult::Locked,
                        _ => DtmfResult::Refused,
                    },
                    recording: match state.recording {
                        Some(RecordingState::Active) => RecState::Active,
                        Some(RecordingState::Paused) => RecState::Paused,
                        Some(RecordingState::Stopped) => RecState::Stopped,
                        _ => RecState::Unknown,
                    },
                    muted: state.muted,
                })
            }
            SessionEvent::RecordingStarted {
                recording_id,
                leg_id,
            } => {
                info!(event = "recording.confirmed",  recording_id = %recording_id, leg_id = %leg_id, "recording started");
                None
            }
            SessionEvent::RecordingStopped(r) => {
                // The null sink writes nothing: its path, size and hash are empty (MS-012).
                info!(event = "recording.stopped",  recording_id = %r.recording_id, leg_id = %r.leg_id, duration_ms = r.duration_ms,
                    pauses = r.pauses.len(), agent_code_pauses = r.pauses.iter().filter(|p| p.2).count(),
                    size_bytes = r.size_bytes, "recording stopped");
                None
            }
            SessionEvent::Ended {
                reason,
                sip_status,
                ack,
            } => {
                info!(event = "session.ended",  reason = ?reason, sip_status, "session ended");
                let text = match reason {
                    EndReason::Reported(r) => format!("{r:?}"),
                    other => format!("{other:?}"),
                };
                io.ended = Some((reason, ack));
                Some(Input::Ended {
                    reason: text,
                    sip_status,
                })
            }
            SessionEvent::Resync(snap) => {
                let customer = snap
                    .legs
                    .iter()
                    .find(|l| l.leg_id == arrival.leg_id && l.role == LegRole::Customer)
                    .map(|l| l.state);
                info!(event = "session.resynced",  state = ?snap.state, customer = ?customer, "resynced from TC's snapshot");
                Some(Input::Resync {
                    ended: snap.state == SessionState::Ended,
                })
            }
            SessionEvent::Unmanaged => {
                warn!(
                    event = "session.unmanaged",
                    "unmanaged: the node's stream is lost; the call keeps its audio, no command and no timer until it is back"
                );
                Some(Input::Unmanaged)
            }
            SessionEvent::Managed => {
                info!(
                    event = "session.managed",
                    "managed again: the node's stream is back"
                );
                Some(Input::Managed)
            }
            other => {
                tracing::debug!(event = "session.event_ignored", kind = ?std::mem::discriminant(&other), "a telephony event the flow doesn't use");
                None
            }
        }
    }

    /// Runs one effect; its result as the machine's input.
    async fn run(&self, arrival: &Arrival, io: &mut Io, effect: Effect) -> Option<Input> {
        match effect {
            Effect::Decline { sip_status } => {
                let o = self
                    .client
                    .decline_arrival(
                        arrival,
                        ie_telephony_client::Decline::Refused { sip_status },
                    )
                    .await;
                self.metrics
                    .declined
                    .add(1, &[KeyValue::new("sip_status", i64::from(sip_status))]);
                info!(event = "call.declined",  sip_status, outcome = %outcome_name(&o), "arrival declined");
                None
            }
            Effect::TenantMismatch => {
                self.metrics.tenant_mismatch.add(1, &[]);
                error!(event = "call.tenant_mismatch",
                    called_number = %arrival.called_number,
                    "tenant_mismatch: X-Tenant-ID disagrees with the number map; refusing the call"
                );
                None
            }
            Effect::Claim => {
                let Some(record) = io.record.as_ref() else {
                    return Some(Input::ClaimUnavailable);
                };
                let Some(lease) = self.lease.current() else {
                    // IE-023: no lease until a new one is granted; declined (503) at once.
                    warn!(
                        event = "call.declined",
                        "no etcd lease (a new one is being granted): declining the arrival"
                    );
                    return Some(Input::ClaimUnavailable);
                };
                match self
                    .etcd
                    .claim(&arrival.key.interaction_id, record, lease)
                    .await
                {
                    Ok(Claim::Claimed(epoch) | Claim::Ours(epoch)) => {
                        let o = Owner {
                            key: arrival.key.clone(),
                            owner_epoch: epoch,
                        };
                        if !self.client.bind(&arrival.node, &o) {
                            warn!(
                                event = "call.arrival_lost",
                                owner_epoch = epoch,
                                "the client no longer holds the arrival"
                            );
                        }
                        info!(
                            event = "interaction.created",
                            origin = "pstn",
                            owner_epoch = epoch,
                            "claimed ie/owner in etcd"
                        );
                        io.owner = Some(o);
                        Some(Input::Claimed(epoch))
                    }
                    Ok(Claim::Taken) => {
                        warn!(
                            event = "call.claim_taken",
                            "another owner holds the interaction: sending nothing"
                        );
                        Some(Input::ClaimTaken)
                    }
                    Err(EtcdError::LeaseNotFound) => {
                        error!(
                            event = "owner.claim_failed",
                            lease, "the owner claim's lease is gone: granting a new one"
                        );
                        self.lease.lost(lease);
                        Some(Input::ClaimUnavailable)
                    }
                    Err(e) => {
                        error!(event = "owner.claim_failed",  error = %e, "the owner claim failed");
                        Some(Input::ClaimUnavailable)
                    }
                }
            }
            Effect::Command(cmd) => {
                let intent = self.intent(arrival, cmd);
                Some(Input::Done(self.command(arrival, io, intent).await))
            }
            Effect::ConsentDecided(d, basis) => {
                let decision = if d == Decision::Granted {
                    "granted"
                } else {
                    "refused"
                };
                self.metrics.consent.add(
                    1,
                    &[
                        KeyValue::new("decision", decision),
                        KeyValue::new("basis", basis.as_str()),
                    ],
                );
                info!(
                    event = "consent.decided",
                    decision,
                    basis = basis.as_str(),
                    "consent decided"
                );
                None
            }
            Effect::StartRecording => {
                // [recording] off: nothing is started (MS-012); the machine only ever gets here
                // after a granting decision.
                self.metrics
                    .flow
                    .add(1, &[KeyValue::new("event", "recording_allowed")]);
                info!(
                    event = "recording.disabled",
                    "consent allows recording: recordings are off ([recording] mode)"
                );
                None
            }
            Effect::Progress(summary) => {
                if !io.finished {
                    io.progress_seq += 1;
                    self.cdr.submit(CdrOp::Progress(
                        Box::new(self.cdr_record(arrival, io, &summary)),
                        io.progress_seq,
                    ));
                }
                None
            }
            Effect::Event(ev) => {
                self.metrics
                    .flow
                    .add(1, &[KeyValue::new("event", ev.name())]);
                match ev {
                    CallEvent::PositionAnnounced(position) => {
                        info!(event = ev.log_event(), position, "call event");
                    }
                    CallEvent::RecordingStarted(agent) => {
                        info!(event = ev.log_event(), agent_leg = agent, "call event");
                    }
                    CallEvent::RecordingFailed(agent) => {
                        // A consented caller not recorded: a failure to report; the call goes on.
                        warn!(
                            event = ev.log_event(),
                            agent_leg = agent,
                            "call event: TC refused the recording (no sink on this node?); the call goes on unrecorded"
                        );
                    }
                    _ => info!(event = ev.log_event(), "call event"),
                }
                None
            }
            Effect::Gathered { to, digits } => {
                // The route taken and a count: never the digits.
                self.metrics
                    .flow
                    .add(1, &[KeyValue::new("event", "gathered")]);
                info!(event = "ivr.input_collected",  step = %to, presses = digits, "gather complete");
                None
            }
            Effect::Count(name) => {
                self.metrics.flow.add(1, &[KeyValue::new("event", name)]);
                if name == "ending_stuck" {
                    // IE-056: not closed without TC's end or the node's death; an operator's case.
                    io.stuck = true;
                    self.metrics
                        .stuck
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    warn!(event = "call.ending_stuck",
                        node = %arrival.node,
                        "call stuck in Ending over 5 min: TC's end hasn't come and TC is unreachable or refuses EndSession; the owner key is kept"
                    );
                } else {
                    info!(event = "flow.counted", counter = name, "flow event");
                }
                None
            }
            Effect::Finish(summary) => {
                self.finish(arrival, io, &summary).await;
                None
            }
            Effect::Reconcile => {
                // IE-050: Ending waited past its deadline for TC's end.
                let r = match io.owner.as_ref() {
                    Some(o) => self.client.reconcile(&arrival.node, o).await,
                    None => ie_telephony_client::Reconcile::Unreachable,
                };
                warn!(event = "session.reconcile_asked",  found = ?r, "the session's end hasn't come: asked TC");
                Some(Input::Reconciled(match r {
                    ie_telephony_client::Reconcile::Ended => ie_core::Reconcile::Ended,
                    ie_telephony_client::Reconcile::Live => ie_core::Reconcile::Live,
                    ie_telephony_client::Reconcile::Fenced => ie_core::Reconcile::Fenced,
                    ie_telephony_client::Reconcile::Unreachable => ie_core::Reconcile::Unreachable,
                }))
            }
            // The call task runs these itself (beside TC's events).
            Effect::Route(_) => {
                error!(
                    event = "routing.machine_bug",
                    "a routing effect reached the TC effect runner (a bug)"
                );
                None
            }
        }
    }

    /// The agent leg's DTMF action codes (P5), and the recording lock when consent refused.
    fn agent_dtmf_actions(&self, record_locked: bool) -> DtmfActions {
        DtmfActions {
            bindings: self
                .agents
                .dtmf_actions
                .iter()
                .map(|b| {
                    (
                        Digits::new(b.sequence.clone()),
                        match b.action {
                            AgentDtmfAction::RecordingPause => DtmfActionKind::RecordingPause,
                            AgentDtmfAction::RecordingResume => DtmfActionKind::RecordingResume,
                            AgentDtmfAction::RecordingToggle => DtmfActionKind::RecordingToggle,
                            AgentDtmfAction::Mute => DtmfActionKind::Mute,
                            AgentDtmfAction::Unmute => DtmfActionKind::Unmute,
                            AgentDtmfAction::MuteToggle => DtmfActionKind::MuteToggle,
                        },
                    )
                })
                .collect(),
            locks: if record_locked {
                vec![Facet::Recording]
            } else {
                Vec::new()
            },
            inter_digit_timeout_ms: self.agents.dtmf_inter_digit_ms,
        }
    }

    fn intent(&self, arrival: &Arrival, cmd: Cmd) -> Intent {
        let leg_id = arrival.leg_id.clone();
        match cmd {
            Cmd::Answer => Intent::Answer { leg_id },
            Cmd::Play {
                play_id,
                prompt_id,
                looped,
            } => {
                info!(event = "prompt.started",  play_id = %play_id, prompt_id = %prompt_id, looped, "a prompt plays to the caller");
                Intent::Play {
                    leg_id,
                    play_id,
                    prompt_id,
                    looped,
                }
            }
            Cmd::AddAgentLeg {
                leg_id,
                agent_contact,
                record_locked,
            } => {
                info!(event = "leg.added", role = "agent", agent_leg_id = %leg_id, record_locked, "placing the agent's leg");
                Intent::AddLeg(Box::new(AddLeg {
                    leg_id,
                    role: LegRole::Agent,
                    destination: Destination::Contact(agent_contact),
                    caller_id: Number::default(),
                    ring_timeout_s: AGENT_RING_S,
                    endpoint: Endpoint::Webrtc,
                    codec_policy: CodecPolicy::G711Only,
                    start_muted: false,
                    monitor_mode: MonitorMode::Unspecified,
                    whisper_to_leg_id: String::new(),
                    // CONVENTIONS: only once the node lists `dtmf_actions` (TC refuses them before).
                    dtmf_actions: if self.client.has_capability(&arrival.node, "dtmf_actions") {
                        Some(self.agent_dtmf_actions(record_locked))
                    } else {
                        warn!(
                            event = "leg.dtmf_actions_off",
                            "the node doesn't list dtmf_actions: the agent's codes are off on this leg"
                        );
                        None
                    },
                }))
            }
            Cmd::RemoveLeg { leg_id } => {
                info!(event = "leg.removed", reason = "not_connected", agent_leg_id = %leg_id, "hanging up the agent's leg");
                Intent::RemoveLeg {
                    leg_id,
                    cause: ie_telephony_client::EndCause::Normal,
                }
            }
            Cmd::StartRecording {
                agent_leg,
                recording_id,
            } => {
                let agent = agent_leg.is_some();
                let leg_id = agent_leg.unwrap_or(leg_id);
                info!(event = "recording.requested",  recording_id = %recording_id, leg_id = %leg_id, agent_leg = agent, "starting a recording (null sink)");
                match null_sink_key() {
                    Some(key) => Intent::StartRecording {
                        leg_id,
                        recording_id,
                        key,
                    },
                    None => {
                        // No key: the client refuses the empty one (nothing sent).
                        error!(
                            event = "recording.key_failed",
                            "the OS RNG failed: the recording is not started"
                        );
                        Intent::StartRecording {
                            leg_id,
                            recording_id,
                            key: RecordingKey {
                                dek: Vec::new(),
                                wrapped_dek: Vec::new(),
                                kek_id: String::new(),
                            },
                        }
                    }
                }
            }
            Cmd::StopPlay { play_id } => Intent::StopPlay { leg_id, play_id },
            Cmd::DigitsOn {
                collection_id,
                max_duration_s,
            } => Intent::SetDigitReporting {
                leg_id,
                mode: DigitReporting::On,
                collection_id,
                max_duration_s,
            },
            Cmd::DigitsOff { collection_id } => Intent::SetDigitReporting {
                leg_id,
                mode: DigitReporting::Off,
                collection_id,
                max_duration_s: 0,
            },
            Cmd::EndSession(cause) => Intent::EndSession {
                cause: match cause {
                    EndCause::Normal => ie_telephony_client::EndCause::Normal,
                    EndCause::System => ie_telephony_client::EndCause::System,
                },
            },
        }
    }

    async fn command(&self, arrival: &Arrival, io: &Io, intent: Intent) -> CmdResult {
        let Some(o) = &io.owner else {
            return CmdResult::Failed;
        };
        let name = intent.name();
        let outcome = self.client.command(&arrival.node, o, intent).await;
        let r = result_of(&outcome);
        if r == CmdResult::Done {
            if name == "AnswerLeg" {
                self.metrics.answered.add(1, &[]);
            }
            tracing::debug!(
                event = "telephony.command_done",
                command = name,
                "command done"
            );
        } else {
            warn!(event = "telephony.command_not_done",  command = name, outcome = %outcome_name(&outcome), "command not done");
        }
        r
    }

    /// The CDR's row from the call's summary (open: no end).
    fn cdr_record(&self, arrival: &Arrival, io: &Io, s: &ie_core::Summary) -> CdrRecord {
        let (consent, basis) = match s.consent {
            Some((Decision::Granted, b)) => ("granted", b.as_str()),
            Some((Decision::Refused, b)) => ("refused", b.as_str()),
            None => ("undecided", ConsentBasis::Never.as_str()),
        };
        let n = |v: u32| i32::try_from(v).unwrap_or(i32::MAX);
        CdrRecord {
            tenant_id: io
                .record
                .as_ref()
                .map(|r| r.tenant_id.clone())
                .unwrap_or_default(),
            interaction_id: arrival.key.interaction_id.clone(),
            node_id: arrival.node.0.clone(),
            direction: "inbound",
            called_number: arrival.called_number.expose().to_owned(),
            caller_number: arrival.caller_number.expose().to_owned(),
            flow: s.flow.clone(),
            last_step: s.last_step.clone(),
            consent,
            consent_basis: basis,
            started_at_ms: io.wall(s.started_ms),
            answered_at_ms: s.answered_ms.map(|m| io.wall(m)),
            end: None,
            owner_epoch: i64::try_from(s.owner_epoch).unwrap_or(0),
            gathers: n(s.gathers),
            no_inputs: n(s.no_inputs),
            queue_id: s.queue_id.clone(),
            entry_id: s.entry_id.clone(),
            queued_at_ms: s.queued_ms.map(|m| io.wall(m)),
            dequeued_at_ms: s.dequeued_ms.map(|m| io.wall(m)),
            queue_outcome: s.queue_outcome.as_str(),
            offers: n(s.offers),
            agent_id: s.agent_id.clone(),
            reservation_id: s.reservation_id.clone(),
            connected_at_ms: s.connected_ms.map(|m| io.wall(m)),
            recorded_legs: n(s.recorded_legs),
            recording_failures: n(s.recording_failures),
            recording_pauses: n(s.recording_pauses),
            mutes: n(s.mutes),
            // An inbound call has no parent (consults and callbacks: steps 11, 12).
            parent_interaction_id: String::new(),
        }
    }

    /// The end of a call this replica id owned before a restart (its process killed mid-call;
    /// DEMO step 5): close its CDR from TC's end, free its agent in routing-engine (`CallEnded`,
    /// or `Dequeue(ABANDONED)` for a caller still waiting), then acknowledge the end. Nothing is
    /// sent to TC: the call is over. Full adoption is step 8's.
    async fn orphan_ended(
        self: Arc<Self>,
        key: ie_telephony_client::SessionKey,
        reason: SessionEndReason,
        sip_status: u32,
        at_ms: i64,
        ack: EndedAck,
    ) {
        let end = CdrEnd {
            ended_at_ms: if at_ms > 0 { at_ms } else { wall_ms() },
            end_reason: format!("{reason:?}"),
            ended_by: if reason == SessionEndReason::AllLegsGone {
                "remote"
            } else {
                "system"
            },
            sip_status: i32::try_from(sip_status).unwrap_or(0),
        };
        let row = self
            .cdr
            .close_orphan(
                key.tenant_id.clone(),
                key.interaction_id.clone(),
                end.clone(),
            )
            .await;
        let Some(row) = row else {
            // No snapshot (the call never got that far) or its CDR is there already: nothing of
            // the call is left here; acknowledge so TC drops the tombstone.
            info!(
                event = "interaction.closed_after_restart",
                "the end of a call owned before the restart: no CDR to write"
            );
            ack.durable();
            return;
        };
        info!(
            event = "interaction.closed_after_restart",
            agent_connected = row.connected.is_some(),
            "the end of a call owned before the restart: its CDR is written"
        );
        if let (Some(rc), Ok(epoch)) = (self.routing.as_ref(), u64::try_from(row.owner_epoch)) {
            let owner = ie_routing_client::Owner {
                tenant_id: key.tenant_id.clone(),
                interaction_id: key.interaction_id.clone(),
                owner_epoch: epoch,
            };
            // Held as released: RE's later messages for it (the agent's WrapUpEnded, replays) go
            // to the dispatcher and are acknowledged, instead of staying unacked orphans.
            drop(rc.bind(&owner));
            rc.release(&owner);
            if let Some((agent_id, reservation_id)) = &row.connected {
                let r = rc
                    .call_ended(&owner, agent_id, reservation_id, end.ended_at_ms)
                    .await;
                info!(event = "routing.call_ended", op = "CallEnded", reservation_id = %reservation_id, agent_id = %agent_id, result = ?r,
                    "CallEnded for a call owned before the restart (the agent's wrap-up)");
                let _ = route_result(
                    "CallEnded",
                    &self.metrics.routing,
                    r.map(|()| RouteResult::Done),
                );
            } else if let Some(entry_id) = &row.entry_id {
                let r = rc
                    .dequeue(
                        &owner,
                        entry_id,
                        ie_routing_client::DequeueReason::Abandoned,
                    )
                    .await;
                info!(event = "routing.dequeued", op = "Dequeue", reason = "abandoned", entry_id = %entry_id, result = ?r,
                    "Dequeue(ABANDONED) for a caller of the previous process");
                let _ = route_result(
                    "Dequeue",
                    &self.metrics.routing,
                    r.map(|_| RouteResult::Done),
                );
            }
        }
        ack.durable();
    }

    /// The ENDED record, then the ack (D5: never before the record), then the owner key, then
    /// the CDR (off the call path).
    async fn finish(&self, arrival: &Arrival, io: &mut Io, s: &ie_core::Summary) {
        let Some((reason, ack)) = io.ended.take() else {
            return;
        };
        match self.ended_log.record(&arrival.key, reason).await {
            Ok(()) => {
                ack.durable();
                self.metrics.ended.add(1, &[]);
            }
            Err(e) => {
                // Unredeemed: TC keeps the tombstone and replays the end.
                error!(event = "session.end_record_failed",  error = %e, "the ENDED record failed; not acknowledging");
                return;
            }
        }
        if let (Some(o), Some(rec)) = (io.owner.as_ref(), io.record.as_ref()) {
            match self
                .etcd
                .release(&arrival.key.interaction_id, o.owner_epoch, rec)
                .await
            {
                Ok(released) => info!(event = "owner.released", released, "owner key released"),
                Err(e) => {
                    warn!(event = "owner.release_failed",  error = %e, "the owner key couldn't be released (the lease ends it)")
                }
            }
        }
        let mut r = self.cdr_record(arrival, io, s);
        let ended_by = match (s.ended_by_ie, io.customer_ended_by) {
            (Some(_), _) => "ie",
            (None, Some(EndedBy::Remote)) => "caller",
            (None, Some(EndedBy::Command)) => "ie",
            _ => "system",
        };
        r.end = Some(CdrEnd {
            ended_at_ms: io.wall(s.ended_ms),
            end_reason: s.end_reason.clone(),
            ended_by,
            sip_status: i32::try_from(s.sip_status).unwrap_or(0),
        });
        io.finished = true;
        if let Some(end) = r.end.as_ref() {
            info!(
                event = "interaction.ended",
                end_reason = %end.end_reason,
                ended_by = end.ended_by,
                queue_outcome = r.queue_outcome,
                duration_ms = end.ended_at_ms.saturating_sub(r.started_at_ms).max(0),
                "the call ended"
            );
        }
        self.cdr.submit(CdrOp::Close(Box::new(r)));
    }
}
