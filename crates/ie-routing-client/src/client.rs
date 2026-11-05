//! The client: requests with retries, the Watch stream, acks. Nothing here blocks a call task for
//! longer than a request's retries (300 ms × 3 with jitter; `Enqueue` up to its 60 s budget; the
//! outcome RPCs up to their 15 s budget across a leader change), which the call task runs next to
//! its other events, never in their way.
//!
//! IE-059 (CONVENTIONS "A leader change"): the bin's instances are tried in turn; a standby's
//! `not_leader` names the leader (`parity-leader`), which is followed. A rising `leader_epoch` is a
//! leader change: the stream goes on, every bound call hears of it ([`RoutingClient::subscribe_leader`])
//! and re-attaches ([`RoutingClient::resync`]); replays are dropped by `delivery_seq`. IE never
//! exits or restarts on it.

use crate::config::{Config, ConfigError, MAX_ENDPOINTS, authority, valid_authority};
use crate::types::{
    ClientEvent, Delivery, DequeueReason, Enqueue, EnqueueReason, Link, Outcome, OverflowAction,
    Owner, Reattached, ReattachedReservation, Recorded, RouteError, RoutingEvent,
};
use parity_proto::routing::v1 as pb;
use pb::routing_service_client::RoutingServiceClient;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};
use tokio::sync::{Semaphore, mpsc, watch};
use tonic::metadata::{AsciiMetadataValue, MetadataMap};
use tonic::transport::Channel;
use tracing::{debug, error, info, warn};

/// A call's mailbox: RE redelivers what doesn't fit (it isn't acknowledged).
const MAILBOX: usize = 64;
/// Acks per `Acknowledge` (CONVENTIONS: at most 500).
const MAX_ACKS: usize = 500;
/// Released interactions and orphans remembered at most (memory bound).
const MAX_REMEMBERED: usize = 100_000;
/// The dispatcher's channel.
const EVENTS: usize = 1024;

/// What [`RoutingClient::start`] returns.
pub struct Started {
    pub client: RoutingClient,
    /// Messages for released interactions and orphans.
    pub events: mpsc::Receiver<ClientEvent>,
}

/// Messages the client dropped, by reason (for tests and the logs).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Dropped {
    /// The message's tenant isn't the bound call's (never handed over; logged as an error).
    pub tenant_mismatch: u64,
    /// An older leader epoch, or another owner epoch than the call's.
    pub stale: u64,
    /// A `delivery_seq` already handed over.
    pub duplicate: u64,
    /// A `delivery_seq` past the next one expected (CR-053): not handed over, not acknowledged;
    /// RE redelivers the missing ones in order.
    pub gap: u64,
}

/// The client; cheap to clone.
#[derive(Clone)]
pub struct RoutingClient {
    inner: Arc<Inner>,
}

struct Inner {
    cfg: Config,
    state: Mutex<State>,
    events: mpsc::Sender<ClientEvent>,
    stop: watch::Sender<bool>,
    /// The leader epoch after each leader change (0 until the first one): every bound call
    /// watches it and re-attaches.
    leader: watch::Sender<u64>,
    /// Leader-change `Reattach`es in flight (`resync_concurrency`).
    resyncs: Semaphore,
}

/// One routing-engine instance of the bin.
struct Ep {
    /// `host:port`, as `parity-leader` names it.
    authority: String,
    rpc: RoutingServiceClient<Channel>,
}

/// How long a request is retried.
#[derive(Clone, Copy)]
enum Budget {
    /// `tries` tries (the contract's 3).
    Tries,
    /// Until this much time has passed.
    For(Duration),
}

struct Binding {
    tenant: String,
    epoch: u64,
    tx: mpsc::Sender<Delivery>,
    /// The next `delivery_seq` to hand over; 0 until the first message (CR-053: RE sends an
    /// interaction's messages in order from the oldest unacknowledged one, so the first message
    /// sets the base and anything above `next` is a gap, never progress).
    next: u64,
    acked: u64,
}

struct Released {
    tenant: String,
    epoch: u64,
    next: u64,
    acked: u64,
}

/// What [`seq_check`] decided for one message.
enum SeqCheck {
    /// Hand it over.
    Deliver,
    /// Processed already: acknowledge again so RE stops redelivering.
    Reack,
    /// Handed over already, not yet processed.
    Duplicate,
    /// Past the next one expected.
    Gap,
}

/// CR-053's gap rule for an interaction's `delivery_seq` (0, never replayed, always delivers).
fn seq_check(seq: u64, next: u64, acked: u64) -> SeqCheck {
    if seq == 0 {
        SeqCheck::Deliver
    } else if seq <= acked {
        SeqCheck::Reack
    } else if next != 0 && seq < next {
        SeqCheck::Duplicate
    } else if next != 0 && seq > next {
        SeqCheck::Gap
    } else {
        SeqCheck::Deliver
    }
}

struct State {
    bound: HashMap<String, Binding>,
    released: HashMap<String, Released>,
    released_order: VecDeque<(String, Instant)>,
    orphans: HashSet<String>,
    orphan_order: VecDeque<String>,
    /// interaction → (tenant, epoch, acked seq) waiting for an `Acknowledge`.
    pending: HashMap<String, (String, u64, u64)>,
    leader_epoch: u64,
    /// Leader changes seen (a rising `leader_epoch` after the first one).
    leader_changes: u64,
    link: Link,
    dropped: Dropped,
    /// The bin's instances (configured first, then leaders learned from `parity-leader`) and
    /// the one requests and the stream go to now.
    eps: Vec<Ep>,
    current: usize,
}

fn channel(uri: &str) -> Result<RoutingServiceClient<Channel>, ConfigError> {
    let endpoint = tonic::transport::Endpoint::from_shared(uri.to_owned())
        .map_err(|e| ConfigError(e.to_string()))?
        .connect_timeout(Duration::from_secs(2));
    Ok(RoutingServiceClient::new(endpoint.connect_lazy()))
}

impl RoutingClient {
    /// Starts the Watch stream and the ack flusher (needs a tokio runtime).
    pub fn start(cfg: Config) -> Result<Started, ConfigError> {
        cfg.validate()?;
        let mut eps = Vec::new();
        for uri in &cfg.endpoints {
            let a = authority(uri).ok_or_else(|| ConfigError(format!("endpoint {uri}")))?;
            if eps.iter().any(|e: &Ep| e.authority == a) {
                continue;
            }
            eps.push(Ep {
                authority: a.to_owned(),
                rpc: channel(uri)?,
            });
        }
        let (events, rx) = mpsc::channel(EVENTS);
        let (stop, stop_rx) = watch::channel(false);
        let (leader, _) = watch::channel(0);
        let inner = Arc::new(Inner {
            resyncs: Semaphore::new(cfg.resync_concurrency),
            leader,
            cfg,
            state: Mutex::new(State {
                bound: HashMap::new(),
                released: HashMap::new(),
                released_order: VecDeque::new(),
                orphans: HashSet::new(),
                orphan_order: VecDeque::new(),
                pending: HashMap::new(),
                leader_epoch: 0,
                leader_changes: 0,
                link: Link::Connecting,
                dropped: Dropped::default(),
                eps,
                current: 0,
            }),
            events,
            stop,
        });
        tokio::spawn(watch_loop(Arc::clone(&inner), stop_rx.clone()));
        tokio::spawn(ack_loop(Arc::clone(&inner), stop_rx));
        Ok(Started {
            client: Self { inner },
            events: rx,
        })
    }

    /// Stops the stream and the flusher.
    pub fn shutdown(&self) {
        let _ = self.inner.stop.send(true);
    }

    /// The stream's state.
    pub fn link(&self) -> Link {
        self.inner.lock().link
    }

    /// What was dropped so far.
    pub fn dropped(&self) -> Dropped {
        self.inner.lock().dropped
    }

    /// The leader epoch of the latest message (0 before the first).
    pub fn leader_epoch(&self) -> u64 {
        self.inner.lock().leader_epoch
    }

    /// Leader changes seen so far (for the metric).
    pub fn leader_changes(&self) -> u64 {
        self.inner.lock().leader_changes
    }

    /// The instance requests and the stream go to now (`host:port`).
    pub fn current_endpoint(&self) -> String {
        let st = self.inner.lock();
        st.eps[st.current].authority.clone()
    }

    /// IE-059: changes once per leader change (the new `leader_epoch`). A call that RE may hold
    /// re-attaches when it changes ([`Self::resync`]); take it before the call's first request.
    pub fn subscribe_leader(&self) -> watch::Receiver<u64> {
        self.inner.leader.subscribe()
    }

    /// IE-059: after a leader change, `Reattach` with the interaction's unchanged owner: what
    /// the new leader holds (the entry, the live reservations). The binding is kept (its mailbox,
    /// its next and acknowledged `delivery_seq`), so the replay RE sends is dropped by
    /// `delivery_seq` where processed. Waits a random part of `resync_spread` first and is
    /// retried for `outcome_budget`; at most `resync_concurrency` run at once.
    pub async fn resync(&self, owner: &Owner) -> Result<Reattached, RouteError> {
        tokio::time::sleep(jitter(self.inner.cfg.resync_spread)).await;
        let _permit = self
            .inner
            .resyncs
            .acquire()
            .await
            .map_err(|_| RouteError::Unavailable("the client stopped".into()))?;
        let r = self
            .reattach_rpc(owner, Budget::For(self.inner.cfg.outcome_budget))
            .await?;
        self.rebase(owner, &r);
        Ok(r)
    }

    /// MF-1a: a routing engine that restarted without its state (HA off: no re.state) holds
    /// nothing of the interaction and numbers its messages afresh (from its incarnation's base,
    /// `last_delivery_seq`). If the binding still expected IE's old numbering, every new message
    /// would look like a gap and be held back for ever (CR-053's rule), and RE drops the entry
    /// unacknowledged after its owner-loss timeout: a stranded caller. So when RE shows no entry
    /// and no reservation and its numbering isn't the binding's (behind what IE acknowledged, or
    /// at or past what IE expects next), the binding is re-based: the next message sets the base
    /// again (RE replays from the oldest unacknowledged, in order), acknowledged at most RE's
    /// `last_delivery_seq`.
    fn rebase(&self, owner: &Owner, r: &Reattached) {
        if r.entry_id.is_some() || !r.reservations.is_empty() {
            return;
        }
        let mut st = self.inner.lock();
        let Some(b) = st.bound.get_mut(&owner.interaction_id) else {
            return;
        };
        let last = r.last_delivery_seq;
        if last < b.acked || last >= b.next {
            info!(
                event = "routing.delivery_rebased",
                tenant_id = %owner.tenant_id,
                interaction_id = %owner.interaction_id,
                previous_next = b.next,
                previous_acked = b.acked,
                last_delivery_seq = last,
                "routing-engine holds nothing of the call and numbers afresh: the delivery sequence restarts from its numbering"
            );
            b.next = 0;
            b.acked = b.acked.min(last);
            st.pending.remove(&owner.interaction_id);
        }
    }

    /// Binds an interaction to this replica's stream: its messages go to the returned mailbox.
    pub fn bind(&self, owner: &Owner) -> mpsc::Receiver<Delivery> {
        let (tx, rx) = mpsc::channel(MAILBOX);
        let mut st = self.inner.lock();
        st.released.remove(&owner.interaction_id);
        st.bound.insert(
            owner.interaction_id.clone(),
            Binding {
                tenant: owner.tenant_id.clone(),
                epoch: owner.owner_epoch,
                tx,
                next: 0,
                acked: 0,
            },
        );
        rx
    }

    /// The call is done: later messages for it go to the dispatcher for a while.
    pub fn release(&self, owner: &Owner) {
        let ttl = self.inner.cfg.ended_ttl;
        let mut st = self.inner.lock();
        let Some(b) = st.bound.remove(&owner.interaction_id) else {
            return;
        };
        let now = Instant::now();
        while st
            .released_order
            .front()
            .is_some_and(|(_, at)| now.duration_since(*at) > ttl)
            || st.released_order.len() >= MAX_REMEMBERED
        {
            if let Some((id, _)) = st.released_order.pop_front() {
                st.released.remove(&id);
            }
        }
        st.released.insert(
            owner.interaction_id.clone(),
            Released {
                tenant: b.tenant,
                epoch: b.epoch,
                next: b.next,
                acked: b.acked,
            },
        );
        st.released_order
            .push_back((owner.interaction_id.clone(), now));
    }

    /// The call's machine took the message with this `delivery_seq`: acknowledge it.
    pub fn ack(&self, owner: &Owner, delivery_seq: u64) {
        if delivery_seq == 0 {
            return;
        }
        let mut st = self.inner.lock();
        let Some(b) = st.bound.get_mut(&owner.interaction_id) else {
            return;
        };
        b.acked = b.acked.max(delivery_seq);
        let entry = (b.tenant.clone(), b.epoch, b.acked);
        st.pending.insert(owner.interaction_id.clone(), entry);
    }

    /// The dispatcher handled a message of a released interaction.
    pub fn ack_released(&self, owner: &Owner, delivery_seq: u64) {
        if delivery_seq == 0 {
            return;
        }
        let mut st = self.inner.lock();
        let Some(r) = st.released.get_mut(&owner.interaction_id) else {
            return;
        };
        r.acked = r.acked.max(delivery_seq);
        let entry = (r.tenant.clone(), r.epoch, r.acked);
        st.pending.insert(owner.interaction_id.clone(), entry);
    }

    /// `Enqueue`, retried while RE is unreachable for up to the budget (repeat-safe by
    /// `(interaction, enqueue_seq)`). The entry's id, whether it existed already, and its
    /// position (0 once reserved).
    pub async fn enqueue(
        &self,
        owner: &Owner,
        e: &Enqueue,
    ) -> Result<(String, bool, u32), RouteError> {
        let req = pb::EnqueueRequest {
            owner: Some(self.inner.owner(owner)),
            enqueue_seq: e.enqueue_seq,
            target: Some(pb::Target {
                kind: Some(pb::target::Kind::QueueId(e.queue_id.clone())),
            }),
            requirements: Vec::new(),
            priority: e.priority,
            first_queued_at_ms: e.first_queued_at_ms,
            ordering_at_ms: e.ordering_at_ms,
            channel: pb::Channel::Voice as i32,
            context: Some(pb::EnqueueContext {
                language: e.language.clone(),
                customer_key: String::new(),
            }),
            reason: match e.reason {
                EnqueueReason::New => pb::EnqueueReason::New,
                EnqueueReason::Transfer => pb::EnqueueReason::Transfer,
            } as i32,
            placement: pb::Placement::ByOrderingTime as i32,
            // contracts 0.12.0: IE's flows don't mark VIP callers yet (every caller normal).
            caller_class: pb::CallerClass::Unspecified as i32,
        };
        let budget = Budget::For(self.inner.cfg.enqueue_budget);
        let resp = self
            .inner
            .retry("Enqueue", budget, |mut c| {
                let r = self.inner.request(&owner.tenant_id, req.clone());
                async move { c.enqueue(r).await }
            })
            .await?
            .into_inner();
        let entry = resp
            .entry
            .ok_or_else(|| RouteError::Bug("Enqueue answered without an entry".into()))?;
        Ok((entry.entry_id, resp.existing, entry.position))
    }

    /// `Dequeue`; an entry already gone (or unknown to RE) is a success: `true`.
    pub async fn dequeue(
        &self,
        owner: &Owner,
        entry_id: &str,
        reason: DequeueReason,
    ) -> Result<bool, RouteError> {
        let req = pb::DequeueRequest {
            owner: Some(self.inner.owner(owner)),
            entry_id: entry_id.to_owned(),
            reason: match reason {
                DequeueReason::Abandoned => pb::DequeueReason::Abandoned,
                DequeueReason::Ended => pb::DequeueReason::Ended,
                DequeueReason::Overflow => pb::DequeueReason::Overflow,
            } as i32,
        };
        let r = self
            .inner
            .retry("Dequeue", self.inner.outcome_budget(), |mut c| {
                let r = self.inner.request(&owner.tenant_id, req.clone());
                async move { c.dequeue(r).await }
            })
            .await;
        match r {
            Ok(resp) => Ok(resp.into_inner().already_gone),
            Err(RouteError::Refused(code)) if code == "entry_unknown" => Ok(true),
            Err(e) => Err(e),
        }
    }

    /// `ReportOutcome`: the outcome RE recorded first.
    pub async fn report_outcome(
        &self,
        owner: &Owner,
        reservation_id: &str,
        outcome: Outcome,
        detail: &str,
    ) -> Result<Recorded, RouteError> {
        let req = pb::ReportOutcomeRequest {
            owner: Some(self.inner.owner(owner)),
            reservation_id: reservation_id.to_owned(),
            outcome: match outcome {
                Outcome::Connected => pb::Outcome::Connected,
                Outcome::AgentNoAnswer => pb::Outcome::AgentNoAnswer,
                Outcome::AgentLegFailed => pb::Outcome::AgentLegFailed,
                Outcome::CallerGone => pb::Outcome::CallerGone,
                Outcome::AlreadyAssigned => pb::Outcome::AlreadyAssigned,
            } as i32,
            detail: detail.to_owned(),
        };
        let resp = self
            .inner
            .retry("ReportOutcome", self.inner.outcome_budget(), |mut c| {
                let r = self.inner.request(&owner.tenant_id, req.clone());
                async move { c.report_outcome(r).await }
            })
            .await?
            .into_inner();
        Ok(if resp.recorded == pb::Outcome::Connected as i32 {
            Recorded::Connected
        } else {
            Recorded::Other
        })
    }

    /// `CallEnded`: the agent's part ended; RE starts the agent's wrap-up.
    pub async fn call_ended(
        &self,
        owner: &Owner,
        agent_id: &str,
        reservation_id: &str,
        ended_at_ms: i64,
    ) -> Result<(), RouteError> {
        let req = pb::CallEndedRequest {
            owner: Some(self.inner.owner(owner)),
            agent_id: agent_id.to_owned(),
            reservation_id: reservation_id.to_owned(),
            ended_at_ms,
        };
        self.inner
            .retry("CallEnded", self.inner.outcome_budget(), |mut c| {
                let r = self.inner.request(&owner.tenant_id, req.clone());
                async move { c.call_ended(r).await }
            })
            .await
            .map(|_| ())
    }

    /// `Reattach` with `owner`'s (new) epoch: what RE has for the interaction. The interaction is
    /// bound at that epoch **before** the request goes out: RE re-stamps the unacknowledged
    /// messages with the new fence and replays them at the epoch raise, before it answers
    /// (CONVENTIONS "Watch delivery", CR-054), and a binding made after the answer would drop
    /// them as another owner's (redelivered only 2 s later). Returns the new mailbox, which
    /// replaces any earlier one for the interaction.
    pub async fn reattach(
        &self,
        owner: &Owner,
    ) -> (mpsc::Receiver<Delivery>, Result<Reattached, RouteError>) {
        let rx = self.bind(owner);
        let r = self.reattach_rpc(owner, Budget::Tries).await;
        (rx, r)
    }

    async fn reattach_rpc(&self, owner: &Owner, budget: Budget) -> Result<Reattached, RouteError> {
        let req = pb::ReattachRequest {
            owner: Some(self.inner.owner(owner)),
        };
        self.inner
            .retry("Reattach", budget, |mut c| {
                let mut r = self.inner.request(&owner.tenant_id, req.clone());
                // CONVENTIONS "Timeouts and retries": 500 ms per try for Reattach.
                r.set_timeout(Duration::from_millis(500).max(self.inner.cfg.rpc_timeout));
                async move { c.reattach(r).await }
            })
            .await
            .map(|resp| {
                let resp = resp.into_inner();
                Reattached {
                    entry_id: resp.entry.map(|e| e.entry_id),
                    reservations: resp
                        .reservations
                        .into_iter()
                        .map(|r| ReattachedReservation {
                            connected: r.state == pb::ReservationState::Connected as i32,
                            reservation_id: r.reservation_id,
                            agent_id: r.agent_id,
                            agent_contact: r.agent_contact,
                            deadline_ms: r.deadline_ms,
                        })
                        .collect(),
                    last_delivery_seq: resp.last_delivery_seq,
                }
            })
    }
}

/// What a failed request leads to.
enum Class {
    Retry,
    Fail(RouteError),
}

fn classify(s: &tonic::Status) -> Class {
    match parity_error(s).as_deref() {
        Some("stale_owner") => Class::Fail(RouteError::Fenced),
        // A standby (IE-059): retried at the leader it names, else at the next instance.
        Some("not_leader") => Class::Retry,
        Some(c @ ("tenant_mismatch" | "invalid_request" | "enqueue_conflict" | "internal")) => {
            Class::Fail(RouteError::Bug(c.to_owned()))
        }
        Some(c) => Class::Fail(RouteError::Refused(c.to_owned())),
        None => match s.code() {
            tonic::Code::Unavailable | tonic::Code::DeadlineExceeded | tonic::Code::Cancelled => {
                Class::Retry
            }
            other => Class::Fail(RouteError::Refused(format!("{other:?}"))),
        },
    }
}

fn parity_error(s: &tonic::Status) -> Option<String> {
    s.metadata()
        .get("parity-error")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned)
}

/// Where a failed request or stream should go next (IE-059).
#[derive(Debug, Clone, PartialEq, Eq)]
enum Steer {
    /// Stay: the instance answered (a refusal, a store error).
    Stay,
    /// A standby named the leader (`parity-leader`).
    Leader(String),
    /// A standby without a leader to name, or no answer at all: the bin's next instance.
    Next,
}

fn steer(s: &tonic::Status) -> Steer {
    match parity_error(s).as_deref() {
        Some("not_leader") => match s
            .metadata()
            .get("parity-leader")
            .and_then(|v| v.to_str().ok())
            .filter(|a| valid_authority(a))
        {
            Some(a) => Steer::Leader(a.to_owned()),
            None => Steer::Next,
        },
        Some(_) => Steer::Stay,
        None => match s.code() {
            tonic::Code::Unavailable | tonic::Code::DeadlineExceeded | tonic::Code::Cancelled => {
                Steer::Next
            }
            _ => Steer::Stay,
        },
    }
}

fn traceparent() -> String {
    format!(
        "00-{:016x}{:016x}-{:016x}-01",
        fastrand::u64(1..),
        fastrand::u64(..),
        fastrand::u64(1..)
    )
}

fn put(m: &mut MetadataMap, k: &'static str, v: &str) {
    if let Ok(v) = AsciiMetadataValue::try_from(v) {
        m.insert(k, v);
    }
}

/// Full jitter: a random wait up to `cap`.
fn jitter(cap: Duration) -> Duration {
    let ms = u64::try_from(cap.as_millis()).unwrap_or(u64::MAX).max(1);
    Duration::from_millis(fastrand::u64(0..=ms))
}

impl Inner {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn outcome_budget(&self) -> Budget {
        Budget::For(self.cfg.outcome_budget)
    }

    /// The instance to use now: its index and a client for it.
    fn rpc(&self) -> (usize, RoutingServiceClient<Channel>) {
        let st = self.lock();
        (st.current, st.eps[st.current].rpc.clone())
    }

    /// IE-059: after a failure at instance `used`, follow the named leader or move on to the next
    /// instance (only if nothing moved it meanwhile, so concurrent failures move it once).
    fn steer(&self, used: usize, s: &tonic::Status) {
        let to = steer(s);
        let mut st = self.lock();
        match to {
            Steer::Stay => {}
            Steer::Leader(a) => {
                let idx = match st.eps.iter().position(|e| e.authority == a) {
                    Some(i) => Some(i),
                    None if st.eps.len() < MAX_ENDPOINTS => match channel(&format!("http://{a}")) {
                        Ok(rpc) => {
                            info!(event = "routing.leader_learned", leader = %a,
                                    "routing-engine named a leader outside the configured instances: added");
                            st.eps.push(Ep { authority: a, rpc });
                            Some(st.eps.len() - 1)
                        }
                        Err(_) => None,
                    },
                    None => None,
                };
                if let Some(i) = idx
                    && st.current != i
                {
                    debug!(event = "routing.endpoint_changed", endpoint = %st.eps[i].authority, "following the leader routing-engine named");
                    st.current = i;
                }
            }
            Steer::Next => {
                if st.eps.len() > 1 && st.current == used {
                    st.current = (used + 1) % st.eps.len();
                    debug!(event = "routing.endpoint_changed", endpoint = %st.eps[st.current].authority, "trying the bin's next routing-engine instance");
                }
            }
        }
    }

    /// The request's owner: our replica, the interaction's acked `delivery_seq`.
    fn owner(&self, o: &Owner) -> pb::InteractionOwner {
        let st = self.lock();
        let acked = st
            .bound
            .get(&o.interaction_id)
            .map(|b| b.acked)
            .or_else(|| st.released.get(&o.interaction_id).map(|r| r.acked))
            .unwrap_or(0);
        pb::InteractionOwner {
            tenant_id: o.tenant_id.clone(),
            interaction_id: o.interaction_id.clone(),
            owner_epoch: o.owner_epoch,
            replica_id: self.cfg.replica_id.clone(),
            acked_delivery_seq: acked,
        }
    }

    /// Metadata: `tenant-id` (the owner's: the number map's) and `traceparent`; the deadline.
    fn request<T>(&self, tenant: &str, msg: T) -> tonic::Request<T> {
        let mut r = tonic::Request::new(msg);
        r.set_timeout(self.cfg.rpc_timeout);
        put(r.metadata_mut(), "tenant-id", tenant);
        put(r.metadata_mut(), "traceparent", &traceparent());
        r
    }

    /// Runs a request with the contract's retries: `tries` tries, or until the budget passes,
    /// following the leader across a leader change (IE-059).
    async fn retry<T, F>(
        &self,
        rpc: &'static str,
        budget: Budget,
        mut call: impl FnMut(RoutingServiceClient<Channel>) -> F,
    ) -> Result<T, RouteError>
    where
        F: std::future::Future<Output = Result<T, tonic::Status>>,
    {
        let start = Instant::now();
        let mut attempt = 0u32;
        loop {
            let (used, client) = self.rpc();
            let status = match call(client).await {
                Ok(v) => return Ok(v),
                Err(s) => s,
            };
            match classify(&status) {
                Class::Fail(e) => {
                    if matches!(e, RouteError::Bug(_)) {
                        error!(event = "routing.rpc_failed", op = rpc, outcome = "refused", error = %e,
                            "routing-engine refused a request IE should never send");
                    }
                    return Err(e);
                }
                Class::Retry => {
                    self.steer(used, &status);
                    attempt += 1;
                    let more = match budget {
                        Budget::For(b) => start.elapsed() < b,
                        Budget::Tries => attempt < self.cfg.tries,
                    };
                    if !more {
                        return Err(RouteError::Unavailable(format!(
                            "{rpc}: {:?} after {attempt} tries",
                            status.code()
                        )));
                    }
                    // Full jitter, 50 ms doubling to at most 1.6 s.
                    let cap = Duration::from_millis(50u64 << attempt.min(5));
                    debug!(event = "routing.rpc_retried", op = rpc, attempt, code = ?status.code(), "retrying");
                    tokio::time::sleep(jitter(cap)).await;
                }
            }
        }
    }

    /// One Watch message: checked, then handed to its call, the dispatcher, or dropped.
    fn on_message(&self, m: pb::WatchResponse) {
        use pb::watch_response::Event;
        let mut st = self.lock();
        if m.leader_epoch < st.leader_epoch {
            // An older leader's late message (it was paused): the new leader replays it.
            st.dropped.stale += 1;
            return;
        }
        if st.leader_epoch != 0 && m.leader_epoch > st.leader_epoch {
            // IE-059 (CONVENTIONS "A leader change"): a new leader holds exactly the old one's
            // state. Every call keeps going; those RE may hold re-attach; replays are dropped by
            // delivery_seq below. Never an exit.
            let from = st.leader_epoch;
            st.leader_changes += 1;
            info!(
                event = "routing.leader_changed",
                from_leader_epoch = from,
                leader_epoch = m.leader_epoch,
                bound = st.bound.len(),
                endpoint = %st.eps[st.current].authority,
                "routing-engine's leader changed: calls keep going and re-attach"
            );
            self.leader.send_replace(m.leader_epoch);
        }
        st.leader_epoch = m.leader_epoch;
        let seq = m.delivery_seq;
        let (target, event) = match m.event {
            None | Some(Event::Heartbeat(_)) => return,
            Some(Event::Assigned(a)) => (
                a.target,
                RoutingEvent::Assigned {
                    reservation_id: a.reservation_id,
                    entry_id: a.entry_id,
                    agent_id: a.agent_id,
                    agent_contact: a.agent_contact,
                    deadline_ms: a.deadline_ms,
                    consult: a.consult,
                },
            ),
            Some(Event::Position(p)) => (
                p.target,
                RoutingEvent::Position {
                    entry_id: p.entry_id,
                    position: p.position,
                    expected_wait_s: p.expected_wait_s,
                },
            ),
            Some(Event::Overflow(o)) => {
                let action = match pb::OverflowAction::try_from(o.action) {
                    Ok(pb::OverflowAction::Queue) => OverflowAction::Queue {
                        queue_id: o.queue_id,
                    },
                    Ok(pb::OverflowAction::Voicemail) => OverflowAction::Voicemail,
                    Ok(pb::OverflowAction::CallbackOffer) => OverflowAction::CallbackOffer,
                    Ok(pb::OverflowAction::Disconnect) => OverflowAction::Disconnect,
                    Ok(pb::OverflowAction::FlowStep) => OverflowAction::FlowStep(o.flow_step),
                    _ => OverflowAction::Unknown,
                };
                (
                    o.target,
                    RoutingEvent::Overflow {
                        entry_id: o.entry_id,
                        action,
                    },
                )
            }
            Some(Event::Revoke(r)) => (
                r.target,
                RoutingEvent::Revoke {
                    reservation_id: r.reservation_id,
                    reason: r.reason,
                },
            ),
            Some(Event::WrapUpEnded(w)) => (
                w.target,
                RoutingEvent::WrapUpEnded {
                    reservation_id: w.reservation_id,
                    agent_id: w.agent_id,
                },
            ),
            Some(Event::RunCallback(c)) => {
                let _ = self.events.try_send(ClientEvent::RunCallback {
                    tenant_id: c.tenant_id,
                    interaction_id: c.interaction_id,
                });
                return;
            }
            // parity-contracts 0.14.0: a lent agent's availability, for a campaign leader (IE step
            // 13a, not built yet); never replayed (delivery_seq 0), nothing to acknowledge.
            Some(Event::AgentAvailability(_)) => return,
        };
        let Some(t) = target else {
            warn!(
                event = "routing.message_dropped",
                "a routing message without its target: dropped"
            );
            return;
        };
        let delivery = Delivery {
            delivery_seq: seq,
            event,
        };
        let State {
            bound,
            released,
            pending,
            dropped,
            orphans,
            orphan_order,
            ..
        } = &mut *st;
        if let Some(b) = bound.get_mut(&t.interaction_id) {
            if t.tenant_id != b.tenant {
                dropped.tenant_mismatch += 1;
                error!(event = "routing.tenant_mismatch",
                    tenant_id = %b.tenant,
                    message_tenant_id = %t.tenant_id,
                    interaction_id = %t.interaction_id,
                    "tenant_mismatch: a routing message names another tenant for this interaction; dropped"
                );
                return;
            }
            if t.owner_epoch != b.epoch {
                dropped.stale += 1;
                warn!(event = "routing.message_dropped",  owner_epoch = b.epoch, message_owner_epoch = t.owner_epoch,
                    interaction_id = %t.interaction_id, "a routing message for another owner epoch: dropped");
                return;
            }
            match seq_check(seq, b.next, b.acked) {
                SeqCheck::Reack => {
                    dropped.duplicate += 1;
                    pending.insert(
                        t.interaction_id.clone(),
                        (b.tenant.clone(), b.epoch, b.acked),
                    );
                    return;
                }
                SeqCheck::Duplicate => {
                    dropped.duplicate += 1;
                    return;
                }
                SeqCheck::Gap => {
                    // CR-053: never past a gap (a cumulative ack would pop the missing one).
                    dropped.gap += 1;
                    debug!(event = "routing.message_held",  interaction_id = %t.interaction_id, delivery_seq = seq, next = b.next,
                        "a routing message past a gap: held back until RE redelivers the missing one");
                    return;
                }
                SeqCheck::Deliver => {}
            }
            match b.tx.try_send(delivery) {
                Ok(()) if seq != 0 => b.next = seq + 1,
                Ok(()) => {}
                // Not acknowledged: RE redelivers it, and nothing after it passes meanwhile.
                Err(_) => {
                    if seq != 0 && b.next == 0 {
                        b.next = seq;
                    }
                    warn!(event = "routing.mailbox_full",  interaction_id = %t.interaction_id, "the call's routing mailbox is full or closed")
                }
            }
            return;
        }
        if let Some(r) = released.get_mut(&t.interaction_id) {
            if t.tenant_id != r.tenant {
                dropped.tenant_mismatch += 1;
                error!(event = "routing.tenant_mismatch",
                    tenant_id = %r.tenant,
                    message_tenant_id = %t.tenant_id,
                    interaction_id = %t.interaction_id,
                    "tenant_mismatch: a routing message names another tenant for this interaction; dropped"
                );
                return;
            }
            if t.owner_epoch != r.epoch {
                dropped.stale += 1;
                return;
            }
            if seq == 0 {
                return;
            }
            match seq_check(seq, r.next, r.acked) {
                SeqCheck::Reack => {
                    dropped.duplicate += 1;
                    pending.insert(
                        t.interaction_id.clone(),
                        (r.tenant.clone(), r.epoch, r.acked),
                    );
                    return;
                }
                SeqCheck::Duplicate => {
                    dropped.duplicate += 1;
                    return;
                }
                SeqCheck::Gap => {
                    dropped.gap += 1;
                    return;
                }
                SeqCheck::Deliver => {}
            }
            let owner = Owner {
                tenant_id: r.tenant.clone(),
                interaction_id: t.interaction_id.clone(),
                owner_epoch: r.epoch,
            };
            if self
                .events
                .try_send(ClientEvent::AfterEnd { owner, delivery })
                .is_ok()
            {
                r.next = seq + 1;
            } else if r.next == 0 {
                r.next = seq;
            }
            return;
        }
        if orphans.insert(t.interaction_id.clone()) {
            orphan_order.push_back(t.interaction_id.clone());
            if orphan_order.len() > MAX_REMEMBERED
                && let Some(old) = orphan_order.pop_front()
            {
                orphans.remove(&old);
            }
            let _ = self.events.try_send(ClientEvent::Orphan {
                tenant_id: t.tenant_id,
                interaction_id: t.interaction_id,
            });
        }
    }

    fn current_authority(&self) -> String {
        let st = self.lock();
        st.eps[st.current].authority.clone()
    }

    fn set_link(&self, link: Link) {
        let mut st = self.lock();
        if !st.link.stopped() {
            st.link = link;
        }
    }
}

/// One stream per replica; reconnects with backoff (full jitter) after an error, an end, or no
/// message for `heartbeat_timeout`. `stream_superseded` stops it.
async fn watch_loop(inner: Arc<Inner>, mut stop: watch::Receiver<bool>) {
    let mut backoff = inner.cfg.reconnect_min;
    loop {
        if *stop.borrow() {
            return;
        }
        inner.set_link(Link::Connecting);
        let mut req = tonic::Request::new(pb::WatchRequest {
            replica_id: inner.cfg.replica_id.clone(),
            bin: inner.cfg.bin.clone(),
        });
        put(req.metadata_mut(), "traceparent", &traceparent());
        let (used, mut client) = inner.rpc();
        let opened = tokio::select! {
            r = client.watch(req) => r,
            _ = stop.changed() => return,
        };
        let ended = match opened {
            Ok(resp) => {
                let mut stream = resp.into_inner();
                loop {
                    let next = tokio::select! {
                        m = tokio::time::timeout(inner.cfg.heartbeat_timeout, stream.message()) => m,
                        _ = stop.changed() => return,
                    };
                    match next {
                        Err(_) => {
                            warn!(
                                event = "routing.stream_lost",
                                reason = "heartbeat_timeout",
                                "no routing message within the heartbeat timeout: reconnecting"
                            );
                            // The leader may be hung or cut off: a standby names the new one.
                            break Some(tonic::Status::deadline_exceeded("heartbeat timeout"));
                        }
                        Ok(Ok(Some(m))) => {
                            if inner.lock().link == Link::Connecting {
                                info!(event = "routing.stream_live", endpoint = %inner.current_authority(),
                                    "routing-engine stream live");
                                inner.set_link(Link::Live);
                                backoff = inner.cfg.reconnect_min;
                            }
                            inner.on_message(m);
                            if inner.lock().link.stopped() {
                                return;
                            }
                        }
                        Ok(Ok(None)) => break None,
                        Ok(Err(s)) => break Some(s),
                    }
                }
            }
            Err(s) => Some(s),
        };
        let mut wait = backoff;
        if let Some(s) = ended {
            if parity_error(&s).as_deref() == Some("stream_superseded") {
                error!(
                    event = "routing.stream_superseded",
                    replica_id = %inner.cfg.replica_id,
                    "stream_superseded: another process watches routing-engine with this replica id; stopped"
                );
                inner.lock().link = Link::Superseded;
                return;
            }
            debug!(event = "routing.stream_ended", code = ?s.code(), "the routing stream ended");
            inner.steer(used, &s);
            if matches!(steer(&s), Steer::Leader(_)) {
                // A standby named the leader: go there at once (the backoff's floor only).
                wait = inner.cfg.reconnect_min;
            }
        }
        inner.set_link(Link::Connecting);
        tokio::select! {
            () = tokio::time::sleep(jitter(wait)) => {}
            _ = stop.changed() => return,
        }
        backoff = (backoff * 2).min(inner.cfg.reconnect_max);
    }
}

/// Sends the pending acks every `ack_flush`, at most 500 per request; a failed batch stays
/// pending (unless a newer ack replaced it).
async fn ack_loop(inner: Arc<Inner>, mut stop: watch::Receiver<bool>) {
    let mut tick = tokio::time::interval(inner.cfg.ack_flush);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = tick.tick() => {}
            _ = stop.changed() => return,
        }
        let batch: Vec<(String, (String, u64, u64))> = {
            let mut st = inner.lock();
            let keys: Vec<String> = st.pending.keys().take(MAX_ACKS).cloned().collect();
            keys.into_iter()
                .filter_map(|k| st.pending.remove(&k).map(|v| (k, v)))
                .collect()
        };
        if batch.is_empty() || inner.lock().link.stopped() {
            continue;
        }
        let acks = batch
            .iter()
            .map(|(iid, (tenant, epoch, seq))| pb::InteractionOwner {
                tenant_id: tenant.clone(),
                interaction_id: iid.clone(),
                owner_epoch: *epoch,
                replica_id: inner.cfg.replica_id.clone(),
                acked_delivery_seq: *seq,
            })
            .collect();
        let mut req = tonic::Request::new(pb::AcknowledgeRequest { acks });
        req.set_timeout(inner.cfg.rpc_timeout);
        put(req.metadata_mut(), "traceparent", &traceparent());
        let (used, mut client) = inner.rpc();
        match client.acknowledge(req).await {
            Ok(r) => {
                let ignored = r.into_inner().ignored;
                if ignored > 0 {
                    warn!(
                        event = "routing.acks_ignored",
                        ignored, "routing-engine ignored acks as stale or foreign"
                    );
                }
            }
            Err(s) => {
                inner.steer(used, &s);
                debug!(event = "routing.rpc_retried", op = "Acknowledge", code = ?s.code(), "Acknowledge failed; kept for the next flush");
                let mut st = inner.lock();
                for (iid, v) in batch {
                    st.pending.entry(iid).or_insert(v);
                }
            }
        }
    }
}
