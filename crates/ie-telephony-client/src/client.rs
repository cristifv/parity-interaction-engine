//! The client (ADR 0003 §4): one Watch task and one evidence task per node, one ack task, one
//! resync worker, one control task (redeemed ends, dropped mailboxes), and the commands, which run
//! in the caller's task. Each node's state sits behind one short-held lock; nothing awaits while
//! holding it, and no periodic task walks every session (IE-022): acks and alerts are queued per
//! session, and a session is forgotten once nothing is left to do for it.

use crate::api::{
    Arrival, ClientEvent, Decline, EndReason, ErrorCode, Intent, Link, NodeId, NodeStatus, Outcome,
    Owner, Reconcile, Reconciled, Reply, SessionEvent, SessionKey, Snapshot,
};
use crate::clock::Clock;
use crate::commands::{Class, backoff, classify, command_id, timed_out};
use crate::config::{ClientConfig, ConfigError, NodeConfig, Timing};
use crate::delivery::{Delivery, Offer, Resync};
use crate::ended::{Control, EndedAck, Redeem};
use crate::evidence::{Evaluator, NodeEvidence};
use crate::forgotten::Forgotten;
use crate::queue::{Queue, SessionEvents};
use crate::stats::Stats;
use crate::wire::{self, Incoming, Meta};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::future::Future;
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::time::{Duration, Instant};
use tokio::sync::{Semaphore, mpsc, oneshot, watch};
use tracing::{error, info, warn};

/// What ie-core uses (ADR 0003 §4.8). `impl Future + Send`, not `async fn` (R1-6).
pub trait Telephony: Send + Sync {
    /// One intent for one session; at most one in flight per session.
    fn command(
        &self,
        node: &NodeId,
        owner: &Owner,
        intent: Intent,
    ) -> impl Future<Output = Outcome> + Send;
    /// Adopts a session: TC verifies the epoch, raises the fences and answers with a snapshot.
    fn claim(
        &self,
        node: &NodeId,
        owner: &Owner,
    ) -> impl Future<Output = Result<(Snapshot, SessionEvents), Outcome>> + Send;
    /// TC's snapshot, with no fence change.
    fn get_session(
        &self,
        node: &NodeId,
        key: &SessionKey,
    ) -> impl Future<Output = Result<Snapshot, Outcome>> + Send;
    /// Declines an arrival offered to this replica (with the redirect's fallback, R1-5).
    fn decline_arrival(
        &self,
        arrival: &Arrival,
        decline: Decline,
    ) -> impl Future<Output = Outcome> + Send;
    /// The owner's epoch for a session it just claimed in etcd (an arrival's claim).
    fn bind(&self, node: &NodeId, owner: &Owner) -> bool;
    /// The mailbox of an outbound session this replica starts (before its first `AddLeg`).
    fn open(&self, node: &NodeId, owner: &Owner) -> Option<SessionEvents>;
    /// A node for a new outbound session.
    fn choose_node(&self) -> Option<NodeId>;
    /// IE-050: an ending session whose end hasn't come: `GetSession`; ENDED closes it from the
    /// tombstone (its end goes to the mailbox), as does a session TC has lost.
    fn reconcile(&self, node: &NodeId, owner: &Owner) -> impl Future<Output = Reconcile> + Send;
    /// Whether the node's last `NodeStatus` lists this capability (CONVENTIONS: IE sends
    /// `AddLeg.dtmf_actions` only once it lists `dtmf_actions`).
    fn has_capability(&self, node: &NodeId, capability: &str) -> bool {
        let _ = (node, capability);
        false
    }
}

/// What the client holds (the `interaction_engine_telephony_sessions_held` gauge, IE-022).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Held {
    /// Sessions with state: live, ending, or with an ack still to send.
    pub sessions: usize,
    /// Forgotten sessions' keys (duplicate detection, a short window).
    pub forgotten: usize,
    /// Sessions queued for the next `Acknowledge`.
    pub acks_queued: usize,
}

/// Readiness failed (R1-9).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum NotReady {
    /// Another process watches a node with this replica id.
    #[error("another process uses replica id {replica_id} on node {node}")]
    ReplicaConflict { replica_id: String, node: NodeId },
}

/// The started client and its event stream.
pub struct Started {
    /// The client (cheap to clone).
    pub client: TelephonyClient,
    /// Arrivals, node changes, orphaned sessions.
    pub events: mpsc::Receiver<ClientEvent>,
}

/// The client; clones share one state.
#[derive(Clone)]
pub struct TelephonyClient {
    inner: Arc<Inner>,
}

struct Inner {
    cfg: ClientConfig,
    clock: Arc<dyn Clock>,
    nodes: BTreeMap<NodeId, Arc<Node>>,
    events_tx: mpsc::Sender<ClientEvent>,
    control_tx: mpsc::UnboundedSender<Control>,
    resync_tx: mpsc::UnboundedSender<(NodeId, SessionKey)>,
    evidence: Vec<Arc<dyn NodeEvidence>>,
    stats: Stats,
    start_ms: u128,
    not_ready: Mutex<Option<NotReady>>,
    shutdown: watch::Sender<bool>,
    /// Whether arrivals are taken (IE-055: not while the owner has no etcd lease).
    accepting: std::sync::atomic::AtomicBool,
}

struct Node {
    id: NodeId,
    cfg: NodeConfig,
    watch_client: wire::Client,
    cmd_client: wire::Client,
    st: Mutex<NodeSt>,
    incarnation: watch::Sender<String>,
}

struct NodeSt {
    link: Link,
    incarnation: String,
    last_msg_ms: u64,
    status: NodeStatus,
    sessions: HashMap<SessionKey, Sess>,
    zombies: Vec<Owner>,
    evaluator: Evaluator,
    /// Incarnations this node's stream has left behind: their snapshots are stale (fix 2).
    past_incarnations: Vec<String>,
    /// TC's clock minus ours, from live heartbeats' `at_ms` (the latest 16; each is low by the
    /// network delay, so the largest is the best estimate).
    skew_samples: std::collections::VecDeque<i64>,
    skew_alerted: bool,
    /// Sessions that may have an ack to send, each once (`Sess::ack_queued`): the ack task takes
    /// them from here instead of walking every session (IE-022).
    ack_dirty: VecDeque<SessionKey>,
    /// Forgotten sessions' replayed `SessionEnded`s to acknowledge again: (key, epoch, seq, the
    /// replay's incarnation).
    reacks: Vec<(SessionKey, u64, u64, String)>,
    /// Degraded sessions' alert times, in order: (due ms, key, degraded since ms).
    degraded_due: VecDeque<(u64, SessionKey, u64)>,
    /// Sessions no longer held, by key, for `forget_after_ms` (IE-022).
    forgotten: Forgotten,
}

/// At most this many acks per `Acknowledge`.
const ACK_BATCH: usize = 500;
/// At most this many repeated end acks waiting (a TC restart that lost acks replays its ends).
const REACKS_MAX: usize = 10_000;

impl NodeSt {
    /// The best estimate of TC's clock minus ours.
    fn skew_ms(&self) -> i64 {
        self.skew_samples.iter().copied().max().unwrap_or(0)
    }

    /// Queues the session for the next `Acknowledge` if it has an ack to send (once).
    fn touch_ack(&mut self, key: &SessionKey) {
        if let Some(s) = self.sessions.get_mut(key)
            && !s.ack_queued
            && s.delivery.pending_ack().is_some()
        {
            s.ack_queued = true;
            self.ack_dirty.push_back(key.clone());
        }
    }

    /// Forgets the session if nothing is left to do for it (IE-022): its end is acked; or its
    /// owner dropped the mailbox and it was never owned here, or is over with no redeemed end
    /// still to ack; or it is an orphan (no mailbox, no owner) whose end came.
    fn maybe_forget(&mut self, key: &SessionKey, now_ms: u64, t: &Timing, stats: &Stats) {
        let Some(s) = self.sessions.get(key) else {
            return;
        };
        let ack_due = s.delivery.end_redeemed() && !s.delivery.end_acked();
        let done = s.delivery.end_acked()
            || (!ack_due && s.abandoned && (!s.owned() || s.delivery.ended()))
            || (!ack_due && s.queue.is_none() && !s.owned() && s.delivery.ended());
        if done {
            self.forget(key, now_ms, t, stats);
        }
    }

    /// Stops holding the session; its key is remembered for `forget_after_ms`, with the epoch
    /// its end was acked with (to ack a replay of it again).
    fn forget(&mut self, key: &SessionKey, now_ms: u64, t: &Timing, stats: &Stats) {
        if let Some(mut s) = self.sessions.remove(key) {
            s.settle(&Err(Outcome::Lost));
            let epoch = if s.delivery.end_acked() {
                s.owner_epoch
            } else {
                0
            };
            self.forgotten.insert(
                key.clone(),
                epoch,
                false,
                now_ms,
                t.forget_after_ms,
                t.forgotten_max,
            );
            Stats::inc(&stats.sessions_forgotten);
        }
    }
}

struct Sess {
    delivery: Delivery,
    queue: Option<Arc<Queue>>,
    taken: bool,
    owner_epoch: u64,
    arrival: bool,
    inflight: Arc<tokio::sync::Mutex<()>>,
    collection: Option<String>,
    degraded_since_ms: Option<u64>,
    degraded_alerted: bool,
    /// Commands whose error needs TC's view: they join the session's one resync (fix 1).
    waiters: Vec<oneshot::Sender<Result<Snapshot, Outcome>>>,
    /// A command waits on the resync: start it without waiting for the low-water mark.
    resync_urgent: bool,
    /// The latest `parity-attempt-expires-ms` sent for this session (0.2.3).
    last_attempt_expires_ms: i64,
    /// In the node's `ack_dirty` queue.
    ack_queued: bool,
    /// The owner dropped its mailbox (IE-022).
    abandoned: bool,
}

impl Sess {
    fn new(delivery: Delivery, queue: Option<Arc<Queue>>, owner_epoch: u64, arrival: bool) -> Self {
        Self {
            delivery,
            queue,
            taken: false,
            owner_epoch,
            arrival,
            inflight: Arc::new(tokio::sync::Mutex::new(())),
            collection: None,
            degraded_since_ms: None,
            degraded_alerted: false,
            waiters: Vec::new(),
            resync_urgent: false,
            last_attempt_expires_ms: 0,
            ack_queued: false,
            abandoned: false,
        }
    }

    /// Answers every command waiting on this session's resync.
    fn settle(&mut self, r: &Result<Snapshot, Outcome>) {
        for w in self.waiters.drain(..) {
            let _ = w.send(r.clone());
        }
    }
    fn owned(&self) -> bool {
        self.owner_epoch > 0
    }
}

/// TC's skew budget for attempt expiry (CONVENTIONS, 0.2.3).
const SKEW_BUDGET_MS: i64 = 250;
/// Both sides alert above this clock offset (CONVENTIONS, 0.2.3).
const SKEW_ALERT_MS: i64 = 100;

fn lock(n: &Node) -> MutexGuard<'_, NodeSt> {
    // A panic while holding the lock is a bug; the state is still the best there is.
    n.st.lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

impl Inner {
    fn meta(&self, tenant_id: &str) -> Meta {
        Meta {
            expires_ms: None,
            tenant_id: tenant_id.to_owned(),
            test_peer: self
                .cfg
                .test_peer_header
                .then(|| format!("parity-interaction-engine.{}", self.cfg.bin)),
        }
    }

    fn t(&self) -> &Timing {
        &self.cfg.timing
    }

    fn new_queue(self: &Arc<Self>, node: &NodeId, key: &SessionKey) -> Arc<Queue> {
        let tx = self.resync_tx.clone();
        let control = self.control_tx.clone();
        let (n, k) = (node.clone(), key.clone());
        let (n2, k2) = (node.clone(), key.clone());
        Arc::new_cyclic(|me: &Weak<Queue>| {
            let me = me.clone();
            Queue::new(
                self.t().mailbox,
                self.t().low_water,
                Box::new(move || {
                    let _ = tx.send((n.clone(), k.clone()));
                }),
                Box::new(move || {
                    let _ = control.send(Control::Abandoned(n2.clone(), k2.clone(), me.clone()));
                }),
            )
        })
    }

    fn node(&self, id: &NodeId) -> Option<&Arc<Node>> {
        self.nodes.get(id)
    }

    // ---- the Watch stream -------------------------------------------------------------------

    async fn watch_loop(self: Arc<Self>, node: Arc<Node>) {
        let mut shutdown = self.shutdown.subscribe();
        let mut attempt: u32 = 0;
        loop {
            if *shutdown.borrow() {
                return;
            }
            let meta = self.meta("");
            let mut c = node.watch_client.clone();
            let opened = tokio::select! {
                r = wire::watch(&mut c, &meta, &self.cfg.replica_id, &self.cfg.bin) => r,
                _ = shutdown.changed() => return,
            };
            let ended: Option<tonic::Status> = match opened {
                Ok(mut stream) => {
                    lock(&node).last_msg_ms = self.clock.now_ms();
                    let mut tick = tokio::time::interval(Duration::from_millis(50));
                    loop {
                        tokio::select! {
                            m = stream.message() => match m {
                                Ok(Some(msg)) => {
                                    attempt = 0;
                                    self.on_message(&node, msg);
                                }
                                Ok(None) => break None,
                                Err(s) => break Some(s),
                            },
                            _ = tick.tick() => {
                                let last = lock(&node).last_msg_ms;
                                if self.clock.now_ms().saturating_sub(last) > self.t().liveness_ms {
                                    info!(event = "telephony.stream_silent",  node = %node.id, "telephony stream silent past liveness");
                                    break None;
                                }
                            }
                            _ = shutdown.changed() => return,
                        }
                    }
                }
                Err(s) => Some(s),
            };
            if let Some(s) = &ended
                && classify(s) == Class::Superseded
            {
                self.conflict(&node);
                return;
            }
            self.on_stream_lost(&node);
            Stats::inc(&self.stats.reconnects);
            let wait = backoff(
                attempt,
                self.t().reconnect_min_ms,
                self.t().reconnect_max_ms,
            )
            .max(Duration::from_millis(self.t().reconnect_min_ms / 2));
            attempt = attempt.saturating_add(1);
            tokio::select! {
                _ = tokio::time::sleep(wait) => {}
                _ = shutdown.changed() => return,
            }
        }
    }

    fn conflict(&self, node: &Arc<Node>) {
        Stats::inc(&self.stats.replica_conflicts);
        error!(event = "telephony.stream_superseded",  node = %node.id, replica_id = %self.cfg.replica_id, "telephony stream superseded by another process with this replica id; readiness failed");
        if let Ok(mut g) = self.not_ready.lock() {
            *g = Some(NotReady::ReplicaConflict {
                replica_id: self.cfg.replica_id.clone(),
                node: node.id.clone(),
            });
        }
        self.unmanage(node, Link::Conflict);
    }

    fn on_stream_lost(&self, node: &Arc<Node>) {
        self.unmanage(node, Link::Disconnected);
    }

    fn unmanage(&self, node: &Arc<Node>, to: Link) {
        let changed = {
            let mut st = lock(node);
            if matches!(st.link, Link::Live | Link::Connecting) || to == Link::Conflict {
                st.link = to;
                st.evaluator.reset();
                for s in st.sessions.values() {
                    if let Some(q) = &s.queue {
                        q.force(SessionEvent::Unmanaged);
                    }
                }
                true
            } else {
                false
            }
        };
        if changed {
            warn!(event = "telephony.node_lost",  node = %node.id, link = ?to, "telephony node lost: its sessions are unmanaged");
            let _ = self.events_tx.try_send(ClientEvent::Node {
                node: node.id.clone(),
                link: to,
            });
        }
    }

    fn on_message(self: &Arc<Self>, node: &Arc<Node>, msg: wire::WatchResponse) {
        let now = self.clock.now_ms();
        let mut out: Vec<ClientEvent> = Vec::new();
        let mut resyncs: Vec<SessionKey> = Vec::new();
        let mut reconcile = false;
        {
            let mut st = lock(node);
            st.last_msg_ms = now;
            if msg.node_id != node.id.0 && !msg.node_id.is_empty() {
                warn!(event = "telephony.event_dropped",  node = %node.id, "telephony event names another node: dropped");
                return;
            }
            if msg.incarnation != st.incarnation {
                let first = st.incarnation.is_empty();
                let old = std::mem::replace(&mut st.incarnation, msg.incarnation.clone());
                if !old.is_empty() {
                    st.past_incarnations.push(old);
                    if st.past_incarnations.len() > 16 {
                        st.past_incarnations.remove(0);
                    }
                }
                node.incarnation.send_replace(msg.incarnation.clone());
                if !first {
                    info!(event = "telephony.node_restarted",  node = %node.id, "telephony node has a new incarnation: resyncing its sessions");
                    for (k, s) in &mut st.sessions {
                        s.delivery = Delivery::awaiting_snapshot();
                        s.collection = None;
                        if s.owned() || s.arrival {
                            resyncs.push(k.clone());
                        }
                    }
                }
            }
            if st.link != Link::Live {
                let was = st.link;
                st.link = Link::Live;
                if was == Link::Dead {
                    reconcile = true;
                }
                for s in st.sessions.values() {
                    if let Some(q) = &s.queue
                        && was == Link::Disconnected
                    {
                        q.force(SessionEvent::Managed);
                    }
                }
                out.push(ClientEvent::Node {
                    node: node.id.clone(),
                    link: Link::Live,
                });
            }
            if msg.delivery_seq == 0 && msg.at_ms > 0 {
                // Heartbeats and NodeStatus are never replayed, so their at_ms is TC's clock now.
                st.skew_samples.push_back(msg.at_ms - wire::epoch_ms());
                if st.skew_samples.len() > 16 {
                    st.skew_samples.pop_front();
                }
                let skew = st.skew_ms();
                if skew.abs() > SKEW_ALERT_MS && !st.skew_alerted {
                    st.skew_alerted = true;
                    Stats::inc(&self.stats.clock_skew_alerts);
                    warn!(event = "telephony.clock_skewed",  node = %node.id, skew_ms = skew, "telephony node's clock is off by more than 100 ms");
                } else if skew.abs() <= SKEW_ALERT_MS {
                    st.skew_alerted = false;
                }
            }
            let incoming = wire::incoming(msg.event);
            match incoming {
                Incoming::Status(s) => st.status = s,
                Incoming::Heartbeat => {}
                other => {
                    if let Some(sref) = msg.session.as_ref() {
                        let key = wire::key_of(sref);
                        let seq = msg.delivery_seq;
                        self.on_session_event(
                            node,
                            &mut st,
                            key.clone(),
                            (seq, sref.owner_epoch, msg.at_ms),
                            other,
                            &mut out,
                            &mut resyncs,
                        );
                        st.touch_ack(&key);
                        st.maybe_forget(&key, now, self.t(), &self.stats);
                    }
                }
            }
        }
        for e in out {
            if let ClientEvent::Arrived(a, _) = &e {
                let (node_id, key) = (a.node.clone(), a.key.clone());
                if self.events_tx.try_send(e).is_err() {
                    // No room: forget it unacked; TC offers it to another replica after 1 s.
                    lock(node).sessions.remove(&key);
                    warn!(event = "arrival.dropped",  node = %node_id, "arrival dropped: the arrivals channel is full");
                }
            } else if let ClientEvent::OrphanEnded { key, .. } = &e {
                let key = key.clone();
                if self.events_tx.try_send(e).is_err() {
                    // No room: nobody will redeem it; back to an unowned orphan (forgotten).
                    let now = self.clock.now_ms();
                    let mut g = lock(node);
                    if let Some(s) = g.sessions.get_mut(&key) {
                        s.owner_epoch = 0;
                    }
                    g.maybe_forget(&key, now, self.t(), &self.stats);
                    warn!(event = "session.end_dropped",  node = %node.id, "an orphan's end dropped: the events channel is full");
                }
            } else {
                let _ = self.events_tx.try_send(e);
            }
        }
        for k in resyncs {
            let _ = self.resync_tx.send((node.id.clone(), k));
        }
        if reconcile {
            let me = Arc::clone(self);
            let n = Arc::clone(node);
            tokio::spawn(async move { me.reconcile_zombies(n).await });
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn on_session_event(
        self: &Arc<Self>,
        node: &Arc<Node>,
        st: &mut NodeSt,
        key: SessionKey,
        (seq, fence, at_ms): (u64, u64, i64),
        incoming: Incoming,
        out: &mut Vec<ClientEvent>,
        resyncs: &mut Vec<SessionKey>,
    ) {
        let incarnation = st.incarnation.clone();
        let taking = self.accepting.load(std::sync::atomic::Ordering::Relaxed);
        if !st.sessions.contains_key(&key)
            && let Some(gone) = st.forgotten.get(&key)
        {
            if gone.retake && taking && matches!(incoming, Incoming::Arrived(_)) {
                // Left unacked while no arrival was taken (IE-055); offered again: a new arrival.
                st.forgotten.remove(&key);
            } else {
                // A late duplicate of a session this replica is done with (IE-022): no orphan.
                // A replayed end it had acked (a TC restart lost the ack) is acked again.
                Stats::inc(&self.stats.replays_dropped);
                if matches!(incoming, Incoming::Ended { .. })
                    && gone.owner_epoch > 0
                    && st.reacks.len() < REACKS_MAX
                {
                    st.reacks.push((key, gone.owner_epoch, seq, incarnation));
                }
                return;
            }
        }
        if !st.sessions.contains_key(&key) {
            match &incoming {
                Incoming::Arrived(_) if !taking => {
                    // IE-055: no arrival is taken (this replica has no etcd lease): neither acked
                    // nor declined, so TC offers it to another replica after its 1 s. Its key is
                    // kept so its end isn't an orphan, and an offer once taking again is new.
                    Stats::inc(&self.stats.arrivals_not_taken);
                    st.forgotten.insert(
                        key,
                        0,
                        true,
                        self.clock.now_ms(),
                        self.t().forget_after_ms,
                        self.t().forgotten_max,
                    );
                    return;
                }
                Incoming::Arrived(_) => {
                    let q = self.new_queue(&node.id, &key);
                    st.sessions.insert(
                        key.clone(),
                        Sess::new(Delivery::starting_before(seq), Some(q), 0, true),
                    );
                }
                _ => {
                    st.sessions.insert(
                        key.clone(),
                        Sess::new(Delivery::starting_before(seq), None, 0, false),
                    );
                    out.push(ClientEvent::Orphan {
                        node: node.id.clone(),
                        key: key.clone(),
                    });
                }
            }
        }
        let Some(s) = st.sessions.get_mut(&key) else {
            return;
        };
        match s.delivery.offer(seq) {
            Offer::Replay => Stats::inc(&self.stats.replays_dropped),
            Offer::Hold => {
                Stats::inc(&self.stats.held);
                // The resync worker alone starts it, once, below the low-water mark (R1-4).
                if s.delivery.resync() == Resync::Wanted {
                    resyncs.push(key.clone());
                }
            }
            Offer::Deliver => match incoming {
                Incoming::Arrived(a) => {
                    s.delivery.delivered(seq, false);
                    if !s.taken
                        && let Some(q) = &s.queue
                    {
                        s.taken = true;
                        let arrival = Arrival {
                            node: node.id.clone(),
                            key: key.clone(),
                            bin: a.bin,
                            leg_id: a.leg_id,
                            sip_call_id: a.sip_call_id,
                            called_number: a.called_number,
                            caller_number: a.caller_number,
                            attempt: a.attempt,
                            redirected_from_bin: a.redirected_from_bin,
                            deadline_ms: a.deadline_ms,
                            trunk_id: a.trunk_id,
                        };
                        out.push(ClientEvent::Arrived(
                            Box::new(arrival),
                            SessionEvents(Arc::clone(q)),
                        ));
                    }
                }
                Incoming::Ended { reason, sip_status } => {
                    let ack = EndedAck::new(
                        self.control_tx.clone(),
                        Redeem {
                            node: node.id.clone(),
                            key: key.clone(),
                            incarnation,
                            seq,
                        },
                    );
                    if let Some(q) = &s.queue {
                        q.force(SessionEvent::Ended {
                            reason: EndReason::Reported(reason),
                            sip_status,
                            ack,
                        });
                    } else if !s.arrival && s.owner_epoch == 0 && fence > 0 {
                        // Step 6: the end of a call this replica id owned before a restart (TC
                        // sends a session's events to its owner's replica). The owner closes its
                        // CDR, then redeems: the ack carries TC's fence and this replica id, as
                        // the old process's would (`ackFromAnyone`). No command is ever sent.
                        s.owner_epoch = fence;
                        out.push(ClientEvent::OrphanEnded {
                            node: node.id.clone(),
                            key: key.clone(),
                            owner_epoch: fence,
                            reason,
                            sip_status,
                            at_ms,
                            ack,
                        });
                    }
                    s.delivery.delivered(seq, true);
                }
                Incoming::Event(ev) => {
                    if let SessionEvent::Digit { collection_id, .. } = &ev
                        && s.collection.as_deref() != Some(collection_id.as_str())
                    {
                        // A digit of a collection that is over (X15): processed, never passed on.
                        s.delivery.delivered(seq, false);
                        return;
                    }
                    match &s.queue {
                        Some(q) => {
                            if q.push(ev) {
                                s.delivery.delivered(seq, false);
                                Stats::inc(&self.stats.events);
                            } else {
                                Stats::inc(&self.stats.mailbox_overflows);
                                s.delivery.want_resync();
                                if s.degraded_since_ms.is_none() {
                                    let now = self.clock.now_ms();
                                    s.degraded_since_ms = Some(now);
                                    st.degraded_due.push_back((
                                        now.saturating_add(self.t().degraded_alert_ms),
                                        key.clone(),
                                        now,
                                    ));
                                }
                            }
                        }
                        None => s.delivery.delivered(seq, false),
                    }
                }
                Incoming::Unknown => {
                    Stats::inc(&self.stats.unknown_events);
                    s.delivery.delivered(seq, false);
                    if let Some(q) = &s.queue {
                        q.force(SessionEvent::Unknown);
                    }
                }
                Incoming::Status(_) | Incoming::Heartbeat => {}
            },
        }
    }

    // ---- resyncs ------------------------------------------------------------------------------

    async fn resync_worker(self: Arc<Self>, mut rx: mpsc::UnboundedReceiver<(NodeId, SessionKey)>) {
        let sem = Arc::new(Semaphore::new(64));
        let gap = Duration::from_secs(1) / self.t().resyncs_per_s.max(1);
        let mut next = Instant::now();
        while let Some((node_id, key)) = rx.recv().await {
            let Some(node) = self.node(&node_id).cloned() else {
                continue;
            };
            let start = {
                let mut st = lock(&node);
                match st.sessions.get_mut(&key) {
                    Some(s) => {
                        let below = s.queue.as_ref().is_none_or(|q| q.below_low_water());
                        let started = (below || s.resync_urgent) && s.delivery.start_resync();
                        if started {
                            s.resync_urgent = false;
                        }
                        started
                    }
                    None => false,
                }
            };
            if !start {
                continue;
            }
            // The limit is per replica, across all nodes (R1-10).
            let now = Instant::now();
            if next > now {
                tokio::time::sleep(next - now).await;
            }
            next = Instant::now().max(next) + gap;
            let Ok(permit) = Arc::clone(&sem).acquire_owned().await else {
                return;
            };
            let me = Arc::clone(&self);
            tokio::spawn(async move {
                me.resync(&node, &key).await;
                drop(permit);
            });
        }
    }

    /// One resync: `GetSession`, matched to its incarnation, replaces the queued events.
    async fn resync(self: &Arc<Self>, node: &Arc<Node>, key: &SessionKey) {
        Stats::inc(&self.stats.resyncs);
        match self.snapshot_now(node, key).await {
            Ok((snap, inc)) => self.apply_snapshot(node, key, snap, &inc),
            Err(Outcome::Lost) => {}
            Err(_) => {
                Stats::inc(&self.stats.resync_failures);
                let mut st = lock(node);
                if let Some(s) = st.sessions.get_mut(key) {
                    s.delivery.resync_failed();
                }
                drop(st);
                // Try again later; never a tight loop.
                let tx = self.resync_tx.clone();
                let (n, k) = (node.id.clone(), key.clone());
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    let _ = tx.send((n, k));
                });
            }
        }
    }

    /// `GetSession` for a resync. `session_unknown` on a live, rebuilt node ends the session (R1-1).
    async fn snapshot_now(
        self: &Arc<Self>,
        node: &Arc<Node>,
        key: &SessionKey,
    ) -> Result<(Snapshot, String), Outcome> {
        match self.read_session(node, key).await {
            Ok(v) => Ok(v),
            Err(Class::Resync(ErrorCode::SessionUnknown)) => {
                let lost = {
                    let st = lock(node);
                    st.link == Link::Live && !st.status.rebuilding
                };
                if lost {
                    self.lose(node, key, EndReason::SessionUnknown);
                    Err(Outcome::Lost)
                } else {
                    Err(Outcome::Unavailable(ErrorCode::Rebuilding))
                }
            }
            Err(Class::Fenced) => {
                self.fence(node, key);
                Err(Outcome::Fenced)
            }
            Err(_) => Err(Outcome::Unavailable(ErrorCode::Unspecified)),
        }
    }

    /// `GetSession` with retries, and the snapshot's incarnation matched to the stream's (R1-11).
    async fn read_session(
        self: &Arc<Self>,
        node: &Arc<Node>,
        key: &SessionKey,
    ) -> Result<(Snapshot, String), Class> {
        let started = Instant::now();
        let mut attempt = 0u32;
        loop {
            let mut c = node.cmd_client.clone();
            let deadline = Timing::ms(self.t().command_deadline_ms);
            let r = tokio::time::timeout(
                deadline + Duration::from_millis(50),
                wire::get_session(
                    &mut c,
                    &self.meta(&key.tenant_id),
                    deadline,
                    key,
                    &self.cfg.replica_id,
                ),
            )
            .await;
            let class = match r {
                Ok(Ok((snap, inc))) => match self.match_incarnation(node, &inc).await {
                    Some(i) => return Ok((snap, i)),
                    None => timed_out(),
                },
                Ok(Err(s)) => classify(&s),
                Err(_) => timed_out(),
            };
            match class {
                Class::Retry(_) if attempt < 3 => {}
                Class::RetryLong(_)
                    if started.elapsed() < Timing::ms(self.t().rebuilding_retry_ms) => {}
                other => return Err(other),
            }
            tokio::time::sleep(backoff(attempt, 50, 1_000)).await;
            attempt += 1;
        }
    }

    /// The incarnation a snapshot belongs to (fix 2: never ordered as strings). The stream's own:
    /// trusted. One the stream has left behind: stale (`None`: ask again). Any other: wait (up to
    /// 10 s) for the stream to change, then decide again. An empty one (a TC before 0.2.2) is the
    /// stream's current one.
    async fn match_incarnation(&self, node: &Arc<Node>, inc: &str) -> Option<String> {
        let mut rx = node.incarnation.subscribe();
        let current = rx.borrow_and_update().clone();
        if inc.is_empty() || inc == current {
            return Some(current);
        }
        if lock(node).past_incarnations.iter().any(|p| p == inc) {
            return None;
        }
        match tokio::time::timeout(Duration::from_secs(10), rx.changed()).await {
            Ok(Ok(())) => {
                let now = rx.borrow().clone();
                (now == inc).then_some(now)
            }
            _ => None,
        }
    }

    fn apply_snapshot(
        self: &Arc<Self>,
        node: &Arc<Node>,
        key: &SessionKey,
        snap: Snapshot,
        inc: &str,
    ) {
        let mut st = lock(node);
        if st.incarnation != inc {
            // The stream moved on meanwhile: this snapshot is for another incarnation.
            if let Some(s) = st.sessions.get_mut(key) {
                s.delivery.resync_failed();
            }
            drop(st);
            let _ = self.resync_tx.send((node.id.clone(), key.clone()));
            return;
        }
        let Some(s) = st.sessions.get_mut(key) else {
            return;
        };
        let ended = snap.state == crate::api::SessionState::Ended;
        let last = snap.last_delivery_seq;
        if !s.delivery.resynced(last, ended) {
            // Older than what the owner already has (fix 1): ignored; a fresh one is asked for.
            warn!(event = "session.snapshot_ignored",  node = %node.id, tenant_id = %key.tenant_id, interaction_id = %key.interaction_id,
                snapshot_seq = last, processed = s.delivery.processed(), "older telephony snapshot ignored");
            s.delivery.resync_failed();
            drop(st);
            let _ = self.resync_tx.send((node.id.clone(), key.clone()));
            return;
        }
        s.settle(&Ok(snap.clone()));
        s.degraded_since_ms = None;
        s.degraded_alerted = false;
        s.collection = None;
        let mut evs = Vec::with_capacity(2);
        let reason = snap.end_reason;
        evs.push(SessionEvent::Resync(Box::new(snap)));
        if ended {
            let ack = EndedAck::new(
                self.control_tx.clone(),
                Redeem {
                    node: node.id.clone(),
                    key: key.clone(),
                    incarnation: inc.to_owned(),
                    seq: last.max(1),
                },
            );
            evs.push(SessionEvent::Ended {
                reason: EndReason::Reported(reason),
                sip_status: 0,
                ack,
            });
        }
        if let Some(q) = &s.queue {
            q.replace_with(evs);
        }
        st.touch_ack(key);
        st.maybe_forget(key, self.clock.now_ms(), self.t(), &self.stats);
    }

    /// An ENDED snapshot for a held session whose end hasn't reached its owner: applied as a
    /// resync's (Resync, then Ended with an ack token), so the owner closes from the tombstone.
    fn close_from_tombstone(
        self: &Arc<Self>,
        node: &Arc<Node>,
        key: &SessionKey,
        snap: &Snapshot,
        inc: &str,
    ) {
        if snap.state != crate::api::SessionState::Ended {
            return;
        }
        let due = lock(node)
            .sessions
            .get(key)
            .is_some_and(|s| s.queue.is_some() && !s.delivery.ended());
        if due {
            info!(event = "session.closed_from_tombstone",  node = %node.id, tenant_id = %key.tenant_id, interaction_id = %key.interaction_id,
                "closing from the tombstone: the session's end never reached its owner");
            self.apply_snapshot(node, key, snap.clone(), inc);
        }
    }

    /// IE-050: asks TC about an ending session whose end hasn't come.
    async fn reconcile(self: &Arc<Self>, node_id: &NodeId, owner: &Owner) -> Reconcile {
        let Some(node) = self.node(node_id).cloned() else {
            return Reconcile::Unreachable;
        };
        if lock(&node).link != Link::Live {
            return Reconcile::Unreachable;
        }
        match self.snapshot_now(&node, &owner.key).await {
            Ok((snap, inc)) if snap.state == crate::api::SessionState::Ended => {
                self.close_from_tombstone(&node, &owner.key, &snap, &inc);
                Reconcile::Ended
            }
            Ok(_) => Reconcile::Live,
            // `lose` put Ended(SessionUnknown) in the mailbox; `fence` closed it.
            Err(Outcome::Lost) => Reconcile::Ended,
            Err(Outcome::Fenced) => Reconcile::Fenced,
            Err(_) => Reconcile::Unreachable,
        }
    }

    fn lose(&self, node: &Arc<Node>, key: &SessionKey, reason: EndReason) {
        Stats::inc(&self.stats.sessions_lost);
        let mut st = lock(node);
        if let Some(mut s) = st.sessions.remove(key) {
            s.settle(&Err(Outcome::Lost));
            if let Some(q) = &s.queue {
                q.force(SessionEvent::Ended {
                    reason,
                    sip_status: 0,
                    ack: EndedAck::nothing(),
                });
                q.close();
            }
        }
        warn!(event = "session.gone",  node = %node.id, tenant_id = %key.tenant_id, interaction_id = %key.interaction_id, ?reason, "telephony session gone on a live node: ended");
    }

    fn fence(&self, node: &Arc<Node>, key: &SessionKey) {
        Stats::inc(&self.stats.fence_refusals);
        let mut st = lock(node);
        if let Some(mut s) = st.sessions.remove(key) {
            s.settle(&Err(Outcome::Fenced));
            if let Some(q) = &s.queue {
                q.close();
            }
        }
        info!(event = "session.fenced",  node = %node.id, tenant_id = %key.tenant_id, interaction_id = %key.interaction_id, "telephony session fenced: a newer owner has it");
    }

    // ---- acks and redemptions -----------------------------------------------------------------

    async fn ack_loop(self: Arc<Self>) {
        let mut shutdown = self.shutdown.subscribe();
        let mut tick = tokio::time::interval(Timing::ms(self.t().ack_batch_ms));
        loop {
            tokio::select! {
                _ = tick.tick() => {}
                _ = shutdown.changed() => return,
            }
            for node in self.nodes.values() {
                self.flush_acks(node).await;
                self.housekeeping(node);
            }
        }
    }

    /// One `Acknowledge` of what the sessions queued since the last (IE-022: never a walk of
    /// every session), plus the repeated acks of forgotten sessions' replayed ends.
    async fn flush_acks(&self, node: &Arc<Node>) {
        let (inc, batch, again) = {
            let mut g = lock(node);
            let st = &mut *g;
            if st.link != Link::Live {
                return;
            }
            let mut batch: Vec<(SessionKey, u64, u64)> = Vec::new();
            while batch.len() < ACK_BATCH {
                let Some(k) = st.ack_dirty.pop_front() else {
                    break;
                };
                let Some(s) = st.sessions.get_mut(&k) else {
                    continue;
                };
                s.ack_queued = false;
                if !(s.owned() || s.arrival) {
                    continue;
                }
                if let Some(a) = s.delivery.pending_ack() {
                    batch.push((k, s.owner_epoch, a));
                }
            }
            let room = (ACK_BATCH - batch.len()).min(st.reacks.len());
            let again: Vec<(SessionKey, u64, u64, String)> = st.reacks.drain(..room).collect();
            (st.incarnation.clone(), batch, again)
        };
        if batch.is_empty() && again.is_empty() {
            return;
        }
        // A repeated end ack carries its replay's incarnation (IE-054).
        let acks = batch
            .iter()
            .map(|(k, epoch, a)| (k, *epoch, *a, inc.as_str()))
            .chain(again.iter().map(|(k, e, a, i)| (k, *e, *a, i.as_str())))
            .map(|(k, epoch, a, i)| wire::SessionOwner {
                tenant_id: k.tenant_id.clone(),
                interaction_id: k.interaction_id.clone(),
                owner_epoch: epoch,
                replica_id: self.cfg.replica_id.clone(),
                acked_delivery_seq: a,
                acked_incarnation: i.to_owned(),
            })
            .collect();
        let mut c = node.cmd_client.clone();
        let deadline = Timing::ms(self.t().command_deadline_ms);
        let sent = matches!(
            tokio::time::timeout(
                deadline,
                wire::acknowledge(&mut c, &self.meta(""), deadline, acks),
            )
            .await,
            Ok(Ok(_))
        );
        let now = self.clock.now_ms();
        let mut g = lock(node);
        let st = &mut *g;
        if !sent {
            // Queued again for the next tick; a repeated end ack is best effort (TC replays it).
            for (k, _, _) in &batch {
                st.touch_ack(k);
            }
            return;
        }
        if st.incarnation != inc {
            return;
        }
        for (k, _, a) in &batch {
            if let Some(s) = st.sessions.get_mut(k) {
                s.delivery.ack_confirmed(*a);
            }
            st.maybe_forget(k, now, self.t(), &self.stats);
        }
        for _ in &again {
            Stats::inc(&self.stats.ends_acked_again);
        }
    }

    /// Per tick, only what is due (IE-022): forgotten keys past their window go, and degraded
    /// sessions past the alert threshold raise it.
    fn housekeeping(&self, node: &Arc<Node>) {
        let now = self.clock.now_ms();
        let mut g = lock(node);
        let st = &mut *g;
        st.forgotten.expire(now);
        while st
            .degraded_due
            .front()
            .is_some_and(|(due, _, _)| *due <= now)
        {
            let Some((_, k, since)) = st.degraded_due.pop_front() else {
                break;
            };
            if let Some(s) = st.sessions.get_mut(&k)
                && s.degraded_since_ms == Some(since)
                && !s.degraded_alerted
            {
                s.degraded_alerted = true;
                Stats::inc(&self.stats.degraded_alerts);
                warn!(event = "session.degraded",  node = %node.id, tenant_id = %k.tenant_id, interaction_id = %k.interaction_id, "telephony session degraded: its owner is not keeping up");
            }
        }
    }

    /// Owners' redeemed ends and dropped mailboxes, in the order they happened.
    async fn control_loop(self: Arc<Self>, mut rx: mpsc::UnboundedReceiver<Control>) {
        while let Some(c) = rx.recv().await {
            match c {
                Control::Redeem(r) => {
                    if let Some(node) = self.node(&r.node) {
                        let mut st = lock(node);
                        if st.incarnation == r.incarnation
                            && let Some(s) = st.sessions.get_mut(&r.key)
                        {
                            s.delivery.redeem(r.seq);
                            st.touch_ack(&r.key);
                        }
                    }
                }
                Control::Abandoned(node_id, key, q) => {
                    let Some(node) = self.node(&node_id) else {
                        continue;
                    };
                    let now = self.clock.now_ms();
                    let mut g = lock(node);
                    let st = &mut *g;
                    if let Some(s) = st.sessions.get_mut(&key)
                        && s.queue
                            .as_ref()
                            .is_some_and(|mine| std::ptr::eq(Arc::as_ptr(mine), q.as_ptr()))
                    {
                        s.abandoned = true;
                        st.maybe_forget(&key, now, self.t(), &self.stats);
                    }
                }
            }
        }
    }

    // ---- lost contact: evidence, Dead, zombies -------------------------------------------------

    async fn evidence_loop(self: Arc<Self>, node: Arc<Node>) {
        let mut shutdown = self.shutdown.subscribe();
        let mut tick = tokio::time::interval(Timing::ms(self.t().probe_every_ms));
        loop {
            tokio::select! {
                _ = tick.tick() => {}
                _ = shutdown.changed() => return,
            }
            if lock(&node).link != Link::Disconnected {
                continue;
            }
            let mut samples = Vec::with_capacity(self.evidence.len());
            for e in &self.evidence {
                samples.push((e.kind(), e.check(&node.cfg).await));
            }
            let confirmed = {
                let mut st = lock(&node);
                st.link == Link::Disconnected
                    && st
                        .evaluator
                        .observe(self.clock.now_ms(), self.t().dead_after_ms, &samples)
            };
            if confirmed {
                self.declare_dead(&node, "evidence");
            }
        }
    }

    fn declare_dead(&self, node: &Arc<Node>, why: &'static str) {
        let n = {
            let mut st = lock(node);
            if st.link == Link::Dead {
                return;
            }
            st.link = Link::Dead;
            st.ack_dirty.clear();
            st.degraded_due.clear();
            let sessions: Vec<(SessionKey, Sess)> = st.sessions.drain().collect();
            let mut n = 0;
            for (k, mut s) in sessions {
                s.settle(&Err(Outcome::Lost));
                if s.owned() {
                    st.zombies.push(Owner {
                        key: k,
                        owner_epoch: s.owner_epoch,
                    });
                    n += 1;
                }
                if let Some(q) = &s.queue {
                    q.force(SessionEvent::Ended {
                        reason: EndReason::NodeDead,
                        sip_status: 0,
                        ack: EndedAck::nothing(),
                    });
                    q.close();
                }
            }
            n
        };
        Stats::inc(&self.stats.nodes_dead);
        error!(event = "telephony.node_declared_dead",  node = %node.id, sessions = n, why, "telephony node declared dead: its sessions are ended");
        let _ = self.events_tx.try_send(ClientEvent::Node {
            node: node.id.clone(),
            link: Link::Dead,
        });
    }

    /// R1-2: the node came back after `Dead`. Whatever IE ended there and TC still has is ended.
    async fn reconcile_zombies(self: Arc<Self>, node: Arc<Node>) {
        let zombies: Vec<Owner> = std::mem::take(&mut lock(&node).zombies);
        let mut keep = Vec::new();
        for owner in zombies {
            match self.read_session(&node, &owner.key).await {
                Ok((snap, _)) if snap.state != crate::api::SessionState::Ended => {
                    let o = self
                        .send_with_retries(
                            &node,
                            &owner,
                            &Intent::EndSession {
                                cause: crate::api::EndCause::System,
                            },
                        )
                        .await;
                    match o {
                        Ok(_) => {
                            Stats::inc(&self.stats.zombies_ended);
                            error!(event = "session.zombie_ended",  node = %node.id, tenant_id = %owner.key.tenant_id, interaction_id = %owner.key.interaction_id, "zombie session ended: IE had ended it as dead, TC still had it live");
                        }
                        Err(Class::Fenced) => {}
                        Err(_) => keep.push(owner),
                    }
                }
                Ok(_) | Err(Class::Resync(ErrorCode::SessionUnknown)) | Err(Class::Fenced) => {}
                Err(_) => keep.push(owner),
            }
        }
        lock(&node).zombies.extend(keep);
    }

    // ---- commands ----------------------------------------------------------------------------

    fn deadline_for(&self, intent: &Intent) -> Duration {
        let t = self.t();
        Timing::ms(match intent {
            Intent::AddLeg(_) | Intent::EndSession { .. } => t.long_deadline_ms,
            Intent::RemoveLeg { .. } => t.remove_deadline_ms,
            _ => t.command_deadline_ms,
        })
    }

    /// One intent with the contract's retries; the piggybacked ack is confirmed only on success.
    async fn send_with_retries(
        &self,
        node: &Arc<Node>,
        owner: &Owner,
        intent: &Intent,
    ) -> Result<Reply, Class> {
        let cid = command_id(&self.cfg.replica_id, self.start_ms);
        let deadline = self.deadline_for(intent);
        let started = Instant::now();
        let mut attempt = 0u32;
        loop {
            let (piggy, live) = {
                let st = lock(node);
                let p = st
                    .sessions
                    .get(&owner.key)
                    .and_then(|s| s.delivery.pending_ack())
                    .map(|a| (a, st.incarnation.clone()));
                (p, st.link == Link::Live)
            };
            if !live {
                return Err(Class::Retry(ErrorCode::Unspecified));
            }
            let block = wire::owner_block(
                owner,
                &self.cfg.replica_id,
                piggy.as_ref().map(|(a, i)| (*a, i.as_str())),
            );
            // A fresh absolute expiry for every attempt, the same command id (0.2.3).
            let mut meta = self.meta(&owner.key.tenant_id);
            let expires = wire::epoch_ms() + i64::try_from(deadline.as_millis()).unwrap_or(0);
            meta.expires_ms = Some(expires);
            if let Some(s) = lock(node).sessions.get_mut(&owner.key) {
                s.last_attempt_expires_ms = s.last_attempt_expires_ms.max(expires);
            }
            let mut c = node.cmd_client.clone();
            let r = tokio::time::timeout(
                deadline + Duration::from_millis(50),
                wire::send(&mut c, &meta, deadline, intent, block, &cid),
            )
            .await;
            let class = match r {
                Ok(Ok(reply)) => {
                    if let Some((a, inc)) = piggy {
                        let mut st = lock(node);
                        if st.incarnation == inc
                            && let Some(s) = st.sessions.get_mut(&owner.key)
                        {
                            s.delivery.ack_confirmed(a);
                            st.maybe_forget(&owner.key, self.clock.now_ms(), self.t(), &self.stats);
                        }
                    }
                    return Ok(reply);
                }
                Ok(Err(s)) => classify(&s),
                Err(_) => timed_out(),
            };
            match class {
                Class::Retry(_) if attempt < 3 => {}
                Class::RetryLong(_)
                    if started.elapsed() < Timing::ms(self.t().rebuilding_retry_ms) => {}
                other => return Err(other),
            }
            tokio::time::sleep(backoff(attempt, 50, 1_000)).await;
            attempt += 1;
        }
    }

    /// R1-3: an `AddLeg` that may or may not have acted is settled before ie-core may fail over.
    async fn reconcile_add_leg(
        self: &Arc<Self>,
        node: &Arc<Node>,
        owner: &Owner,
        leg_id: &str,
    ) -> Reconciled {
        Stats::inc(&self.stats.indeterminate);
        // "Leg absent" is trusted only once every attempt's expiry plus TC's 250 ms skew budget
        // has passed, and longer if TC's clock runs behind ours (bounded at 1 s), 0.2.3.
        let until = {
            let st = lock(node);
            let behind = (-st.skew_ms()).clamp(0, 1_000);
            st.sessions
                .get(&owner.key)
                .map_or(0, |s| s.last_attempt_expires_ms)
                + SKEW_BUDGET_MS
                + behind
        };
        let now = wire::epoch_ms();
        if until > now {
            tokio::time::sleep(Duration::from_millis(
                u64::try_from(until - now).unwrap_or(0),
            ))
            .await;
        }
        let Ok((snap, _)) = self.read_session(node, &owner.key).await else {
            return Reconciled::Unreconciled;
        };
        let Some(leg) = snap.legs.iter().find(|l| l.leg_id == leg_id) else {
            return Reconciled::LegAbsent;
        };
        if leg.state == crate::api::LegState::Ended {
            return Reconciled::LegEnded;
        }
        let remove = Intent::RemoveLeg {
            leg_id: leg_id.to_owned(),
            cause: crate::api::EndCause::System,
        };
        match self.send_with_retries(node, owner, &remove).await {
            Ok(_) => Reconciled::LegRemoved,
            Err(_) => Reconciled::Unreconciled,
        }
    }

    fn after_success(&self, node: &Arc<Node>, owner: &Owner, intent: &Intent) {
        if let Intent::SetDigitReporting {
            mode,
            collection_id,
            ..
        } = intent
        {
            let mut st = lock(node);
            if let Some(s) = st.sessions.get_mut(&owner.key) {
                s.collection = match mode {
                    crate::api::DigitReporting::On | crate::api::DigitReporting::Masked => {
                        Some(collection_id.clone())
                    }
                    _ => None,
                };
            }
        }
    }

    async fn command(self: &Arc<Self>, node_id: &NodeId, owner: &Owner, intent: Intent) -> Outcome {
        if let Err(e) = crate::validate::intent(owner, &intent) {
            warn!(event = "telephony.intent_refused",  command = intent.name(), rule = %e, "telephony intent refused before sending");
            return Outcome::Refused(e);
        }
        let Some(node) = self.node(node_id).cloned() else {
            return Outcome::Bug(ErrorCode::WrongNode);
        };
        let gate = {
            let mut st = lock(&node);
            if st.link != Link::Live {
                return Outcome::Unmanaged;
            }
            if !st.sessions.contains_key(&owner.key) && st.forgotten.get(&owner.key).is_some() {
                // Over and done here (IE-022): no entry is made again for it.
                return Outcome::Lost;
            }
            let s = st.sessions.entry(owner.key.clone()).or_insert_with(|| {
                Sess::new(Delivery::starting_before(1), None, owner.owner_epoch, false)
            });
            if s.owner_epoch < owner.owner_epoch {
                s.owner_epoch = owner.owner_epoch;
            }
            Arc::clone(&s.inflight)
        };
        let _one = gate.lock().await;
        match self.send_with_retries(&node, owner, &intent).await {
            Ok(reply) => {
                self.after_success(&node, owner, &intent);
                Outcome::Done(reply)
            }
            Err(class) => self.outcome(&node, owner, &intent, class).await,
        }
    }

    /// Fix 1: a command's resync joins the session's one resync (the worker's), never a second.
    async fn join_resync(self: &Arc<Self>, node: &Arc<Node>, key: &SessionKey) -> Outcome {
        let rx = {
            let mut st = lock(node);
            let Some(s) = st.sessions.get_mut(key) else {
                return Outcome::Lost;
            };
            let (tx, rx) = oneshot::channel();
            s.waiters.push(tx);
            s.delivery.want_resync();
            s.resync_urgent = true;
            rx
        };
        let _ = self.resync_tx.send((node.id.clone(), key.clone()));
        let wait = Timing::ms(self.t().rebuilding_retry_ms) + Duration::from_secs(5);
        match tokio::time::timeout(wait, rx).await {
            Ok(Ok(Ok(snap))) => Outcome::Resync(Box::new(snap)),
            Ok(Ok(Err(o))) => o,
            _ => Outcome::Unavailable(ErrorCode::Unspecified),
        }
    }

    async fn outcome(
        self: &Arc<Self>,
        node: &Arc<Node>,
        owner: &Owner,
        intent: &Intent,
        class: Class,
    ) -> Outcome {
        match class {
            Class::Fenced => {
                self.fence(node, &owner.key);
                Outcome::Fenced
            }
            Class::Bug(code) => {
                error!(event = "telephony.command_failed",  node = %node.id, command = intent.name(), ?code, "telephony command refused as a bug");
                Outcome::Bug(code)
            }
            Class::Superseded => Outcome::Bug(ErrorCode::StreamSuperseded),
            Class::Resync(_) => self.join_resync(node, &owner.key).await,
            Class::Ended => match self.read_session(node, &owner.key).await {
                Ok((snap, inc)) => {
                    // IE-049: close from the tombstone if its end never reached the owner (an
                    // ownerless end TC sent elsewhere): Resync, then Ended with its ack token.
                    self.close_from_tombstone(node, &owner.key, &snap, &inc);
                    Outcome::Ended(Box::new(snap))
                }
                Err(_) => Outcome::Unavailable(ErrorCode::SessionEnded),
            },
            Class::ArrivalClosed => Outcome::ArrivalClosed,
            Class::Config(c) => Outcome::Config(c),
            Class::Capacity(c) => Outcome::Capacity(c),
            Class::Unsupported => Outcome::Unsupported,
            Class::Retry(_) | Class::RetryLong(_) => {
                if lock(node).link != Link::Live && !matches!(intent, Intent::AddLeg(_)) {
                    return Outcome::Unmanaged;
                }
                let how = match intent {
                    Intent::AddLeg(a) => self.reconcile_add_leg(node, owner, &a.leg_id).await,
                    _ => {
                        let _ = self.resync_tx.send((node.id.clone(), owner.key.clone()));
                        Reconciled::NotApplicable
                    }
                };
                warn!(event = "telephony.command_unknown",  node = %node.id, command = intent.name(), reconciled = ?how, "telephony command result unknown");
                Outcome::Indeterminate(how)
            }
        }
    }
}

impl TelephonyClient {
    /// Starts the tasks (inside a tokio runtime).
    pub fn start(
        cfg: ClientConfig,
        clock: Arc<dyn Clock>,
        evidence: Vec<Arc<dyn NodeEvidence>>,
    ) -> Result<Started, ConfigError> {
        cfg.validate()?;
        let (events_tx, events) = mpsc::channel(1024);
        let (control_tx, control_rx) = mpsc::unbounded_channel();
        let (resync_tx, resync_rx) = mpsc::unbounded_channel();
        let mut nodes = BTreeMap::new();
        for n in &cfg.nodes {
            let endpoint = tonic::transport::Endpoint::from_shared(n.grpc.clone())
                .map_err(|e| ConfigError::Field("nodes.grpc", e.to_string()))?
                .http2_keep_alive_interval(Duration::from_secs(10))
                .keep_alive_while_idle(true)
                .tcp_keepalive(Some(Duration::from_secs(10)))
                .connect_timeout(Duration::from_secs(2));
            let node = Node {
                id: NodeId(n.id.clone()),
                cfg: n.clone(),
                // Two connections: a slow event reader never stalls commands (D2).
                watch_client: wire::Client::new(endpoint.connect_lazy()),
                cmd_client: wire::Client::new(endpoint.connect_lazy()),
                st: Mutex::new(NodeSt {
                    link: Link::Connecting,
                    incarnation: String::new(),
                    last_msg_ms: 0,
                    status: NodeStatus::default(),
                    sessions: HashMap::new(),
                    zombies: Vec::new(),
                    evaluator: Evaluator::default(),
                    past_incarnations: Vec::new(),
                    skew_samples: std::collections::VecDeque::new(),
                    skew_alerted: false,
                    ack_dirty: VecDeque::new(),
                    reacks: Vec::new(),
                    degraded_due: VecDeque::new(),
                    forgotten: Forgotten::default(),
                }),
                incarnation: watch::channel(String::new()).0,
            };
            nodes.insert(NodeId(n.id.clone()), Arc::new(node));
        }
        let start_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0);
        let inner = Arc::new(Inner {
            cfg,
            clock,
            nodes,
            events_tx,
            control_tx,
            resync_tx,
            evidence,
            stats: Stats::default(),
            start_ms,
            not_ready: Mutex::new(None),
            shutdown: watch::channel(false).0,
            accepting: std::sync::atomic::AtomicBool::new(true),
        });
        for node in inner.nodes.values() {
            tokio::spawn(Arc::clone(&inner).watch_loop(Arc::clone(node)));
            tokio::spawn(Arc::clone(&inner).evidence_loop(Arc::clone(node)));
        }
        tokio::spawn(Arc::clone(&inner).ack_loop());
        tokio::spawn(Arc::clone(&inner).control_loop(control_rx));
        tokio::spawn(Arc::clone(&inner).resync_worker(resync_rx));
        Ok(Started {
            client: Self { inner },
            events,
        })
    }

    /// Stops the tasks.
    pub fn shutdown(&self) {
        let _ = self.inner.shutdown.send(true);
    }

    /// Readiness: false once another process uses this replica id (R1-9).
    pub fn ready(&self) -> Result<(), NotReady> {
        match self.inner.not_ready.lock().map(|g| g.clone()) {
            Ok(Some(e)) => Err(e),
            _ => Ok(()),
        }
    }

    /// A node's link.
    pub fn link(&self, node: &NodeId) -> Option<Link> {
        self.inner.node(node).map(|n| lock(n).link)
    }

    /// Sessions of a node that have an owner and no stream (the `unmanaged_calls` gauge).
    pub fn unmanaged_calls(&self, node: &NodeId) -> usize {
        self.inner.node(node).map_or(0, |n| {
            let st = lock(n);
            if matches!(st.link, Link::Disconnected | Link::Conflict) {
                st.sessions.values().filter(|s| s.owned()).count()
            } else {
                0
            }
        })
    }

    /// Sessions whose mailbox overflowed and wait for a resync.
    pub fn degraded_sessions(&self) -> usize {
        self.inner
            .nodes
            .values()
            .map(|n| {
                lock(n)
                    .sessions
                    .values()
                    .filter(|s| s.degraded_since_ms.is_some())
                    .count()
            })
            .sum()
    }

    /// What the client holds now, across nodes (IE-022: bounded by live sessions, plus the
    /// forgotten keys of the last `forget_after_ms`, at most `forgotten_max` per node).
    pub fn held(&self) -> Held {
        let mut h = Held::default();
        for n in self.inner.nodes.values() {
            let st = lock(n);
            h.sessions += st.sessions.len();
            h.forgotten += st.forgotten.len();
            h.acks_queued += st.ack_dirty.len();
        }
        h
    }

    /// IE-055: whether new arrivals are taken. While not, an arrival offered to this replica is
    /// neither acked nor declined, so TC offers it to another replica after its 1 s no-ack timer
    /// (the caller gets TC's 503 at the 5 s deadline only when no replica takes it).
    pub fn accept_arrivals(&self, on: bool) {
        let was = self
            .inner
            .accepting
            .swap(on, std::sync::atomic::Ordering::Relaxed);
        if was != on {
            info!(
                event = "telephony.arrivals_toggled",
                accepting = on,
                "telephony arrivals taken or paused"
            );
        }
    }

    /// Whether new arrivals are taken.
    pub fn accepting_arrivals(&self) -> bool {
        self.inner
            .accepting
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// The counters.
    pub fn stats(&self) -> &Stats {
        &self.inner.stats
    }

    /// An operator's declaration: ends the node's sessions now, and reconciles zombies when the
    /// node returns, as on evidence (R1-11). Only behind the `operator-override` feature: step 7's
    /// audited admin command turns it on; nothing else may call it.
    #[cfg(feature = "operator-override")]
    pub fn declare_node_dead(&self, node: &NodeId) {
        if let Some(n) = self.inner.node(node) {
            self.inner.declare_dead(n, "operator");
        }
    }
}

impl Telephony for TelephonyClient {
    fn has_capability(&self, node: &NodeId, capability: &str) -> bool {
        self.inner
            .node(node)
            .is_some_and(|n| lock(n).status.capabilities.iter().any(|c| c == capability))
    }

    fn command(
        &self,
        node: &NodeId,
        owner: &Owner,
        intent: Intent,
    ) -> impl Future<Output = Outcome> + Send {
        let inner = Arc::clone(&self.inner);
        let (node, owner) = (node.clone(), owner.clone());
        async move { inner.command(&node, &owner, intent).await }
    }

    fn claim(
        &self,
        node: &NodeId,
        owner: &Owner,
    ) -> impl Future<Output = Result<(Snapshot, SessionEvents), Outcome>> + Send {
        let inner = Arc::clone(&self.inner);
        let (node_id, owner) = (node.clone(), owner.clone());
        async move {
            crate::validate::owner(&owner).map_err(Outcome::Refused)?;
            let node = inner
                .node(&node_id)
                .cloned()
                .ok_or(Outcome::Bug(ErrorCode::WrongNode))?;
            if lock(&node).link != Link::Live {
                return Err(Outcome::Unmanaged);
            }
            let started = Instant::now();
            let mut attempt = 0u32;
            let (snap, inc) = loop {
                let deadline = Timing::ms(inner.t().claim_deadline_ms);
                let mut c = node.cmd_client.clone();
                let block = wire::owner_block(&owner, &inner.cfg.replica_id, None);
                let r = tokio::time::timeout(
                    deadline + Duration::from_millis(50),
                    wire::claim(&mut c, &inner.meta(&owner.key.tenant_id), deadline, block),
                )
                .await;
                let class = match r {
                    Ok(Ok((snap, inc))) => match inner.match_incarnation(&node, &inc).await {
                        Some(i) => break (snap, i),
                        None => timed_out(),
                    },
                    Ok(Err(s)) => classify(&s),
                    Err(_) => timed_out(),
                };
                match class {
                    Class::Retry(_) if attempt < 3 => {}
                    Class::RetryLong(_)
                        if started.elapsed() < Timing::ms(inner.t().rebuilding_retry_ms) => {}
                    Class::Fenced => return Err(Outcome::Fenced),
                    Class::Bug(c) => return Err(Outcome::Bug(c)),
                    Class::Resync(_) => return Err(Outcome::Lost),
                    _ => return Err(Outcome::Unavailable(ErrorCode::Unspecified)),
                }
                tokio::time::sleep(backoff(attempt, 50, 1_000)).await;
                attempt += 1;
            };
            let q = inner.new_queue(&node.id, &owner.key);
            {
                let mut st = lock(&node);
                if st.incarnation != inc {
                    return Err(Outcome::Unavailable(ErrorCode::Rebuilding));
                }
                let mut s = Sess::new(
                    Delivery::starting_before(1),
                    Some(Arc::clone(&q)),
                    owner.owner_epoch,
                    false,
                );
                s.taken = true;
                st.forgotten.remove(&owner.key);
                st.sessions.insert(owner.key.clone(), s);
            }
            // The snapshot reflects every event up to its last_delivery_seq; an ENDED one is a tombstone.
            inner.apply_snapshot(&node, &owner.key, snap.clone(), &inc);
            Ok((snap, SessionEvents(q)))
        }
    }

    fn get_session(
        &self,
        node: &NodeId,
        key: &SessionKey,
    ) -> impl Future<Output = Result<Snapshot, Outcome>> + Send {
        let inner = Arc::clone(&self.inner);
        let (node_id, key) = (node.clone(), key.clone());
        async move {
            let node = inner
                .node(&node_id)
                .cloned()
                .ok_or(Outcome::Bug(ErrorCode::WrongNode))?;
            inner
                .read_session(&node, &key)
                .await
                .map(|(s, _)| s)
                .map_err(|c| match c {
                    Class::Resync(code) => Outcome::Bug(code),
                    Class::Fenced => Outcome::Fenced,
                    _ => Outcome::Unavailable(ErrorCode::Unspecified),
                })
        }
    }

    fn decline_arrival(
        &self,
        arrival: &Arrival,
        decline: Decline,
    ) -> impl Future<Output = Outcome> + Send {
        let inner = Arc::clone(&self.inner);
        let arrival = arrival.clone();
        async move {
            if let Err(e) = crate::validate::decline(
                &arrival.key.tenant_id,
                &arrival.key.interaction_id,
                &arrival.leg_id,
                &decline,
            ) {
                return Outcome::Refused(e);
            }
            let Some(node) = inner.node(&arrival.node).cloned() else {
                return Outcome::Bug(ErrorCode::WrongNode);
            };
            let deadline = Timing::ms(inner.t().command_deadline_ms);
            let once = |d: Decline| {
                let inner = Arc::clone(&inner);
                let node = Arc::clone(&node);
                let arrival = arrival.clone();
                async move {
                    let mut c = node.cmd_client.clone();
                    match tokio::time::timeout(
                        deadline + Duration::from_millis(50),
                        wire::decline(
                            &mut c,
                            &inner.meta(&arrival.key.tenant_id),
                            deadline,
                            &arrival.key,
                            &inner.cfg.replica_id,
                            &arrival.leg_id,
                            &d,
                        ),
                    )
                    .await
                    {
                        Ok(Ok(r)) => Ok(r),
                        Ok(Err(s)) => Err(classify(&s)),
                        Err(_) => Err(timed_out()),
                    }
                }
            };
            let first = once(decline.clone()).await;
            let result = match (first, &decline) {
                (Ok(r), _) => Ok(r),
                (Err(Class::ArrivalClosed), _) => return Outcome::ArrivalClosed,
                // R1-5: a failed redirect falls back to refusing the call; failing that, TC's deadline answers 503.
                (Err(_), Decline::WrongBin { .. }) => {
                    warn!(event = "arrival.redirect_failed",  node = %arrival.node, "redirect failed: refusing the arrival instead");
                    once(Decline::Refused { sip_status: 503 }).await
                }
                (Err(c), Decline::Refused { .. }) => Err(c),
            };
            match result {
                Ok(r) => {
                    // Forgotten, not just removed: its SessionEnded may still come (IE-016).
                    let now = inner.clock.now_ms();
                    lock(&node).forget(&arrival.key, now, inner.t(), &inner.stats);
                    Outcome::Done(r)
                }
                Err(Class::ArrivalClosed) => Outcome::ArrivalClosed,
                Err(Class::Bug(c)) => Outcome::Bug(c),
                Err(_) => Outcome::Unavailable(ErrorCode::Unspecified),
            }
        }
    }

    fn bind(&self, node: &NodeId, owner: &Owner) -> bool {
        let Some(n) = self.inner.node(node) else {
            return false;
        };
        let mut st = lock(n);
        match st.sessions.get_mut(&owner.key) {
            Some(s) => {
                s.owner_epoch = s.owner_epoch.max(owner.owner_epoch);
                s.arrival = false;
                st.touch_ack(&owner.key);
                true
            }
            None => false,
        }
    }

    fn open(&self, node: &NodeId, owner: &Owner) -> Option<SessionEvents> {
        let n = self.inner.node(node)?;
        let q = self.inner.new_queue(node, &owner.key);
        let mut st = lock(n);
        if !st.sessions.contains_key(&owner.key) && st.forgotten.get(&owner.key).is_some() {
            return None;
        }
        let s = st.sessions.entry(owner.key.clone()).or_insert_with(|| {
            Sess::new(Delivery::starting_before(1), None, owner.owner_epoch, false)
        });
        s.owner_epoch = s.owner_epoch.max(owner.owner_epoch);
        if s.taken {
            return None;
        }
        let q = Arc::clone(s.queue.get_or_insert(q));
        s.taken = true;
        Some(SessionEvents(q))
    }

    fn reconcile(&self, node: &NodeId, owner: &Owner) -> impl Future<Output = Reconcile> + Send {
        let inner = Arc::clone(&self.inner);
        let (node, owner) = (node.clone(), owner.clone());
        async move { inner.reconcile(&node, &owner).await }
    }

    fn choose_node(&self) -> Option<NodeId> {
        let candidates: Vec<(NodeId, u32)> = self
            .inner
            .nodes
            .values()
            .filter_map(|n| {
                let st = lock(n);
                (st.link == Link::Live
                    && !st.status.draining
                    && !st.status.rebuilding
                    && st.status.free_legs > 0)
                    .then(|| (n.id.clone(), st.status.free_legs))
            })
            .collect();
        let total: u64 = candidates.iter().map(|(_, w)| u64::from(*w)).sum();
        if total == 0 {
            return None;
        }
        let mut pick = fastrand::u64(0..total);
        for (id, w) in candidates {
            if pick < u64::from(w) {
                return Some(id);
            }
            pick -= u64::from(w);
        }
        None
    }
}
