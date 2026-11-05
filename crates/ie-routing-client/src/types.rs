//! What the client takes and gives: plain Rust types, not the generated ones.

/// An interaction as its owner holds it: the tenant is the number map's, never a header's.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Owner {
    pub tenant_id: String,
    pub interaction_id: String,
    /// The etcd create revision of `ie/owner/{interaction_id}` (at least 1).
    pub owner_epoch: u64,
}

/// `EnqueueReason`, IE's part.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnqueueReason {
    New,
    Transfer,
}

/// What `Enqueue` sends besides the owner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Enqueue {
    pub enqueue_seq: u32,
    pub queue_id: String,
    pub priority: u32,
    /// Epoch ms; the same on every retry.
    pub first_queued_at_ms: i64,
    pub ordering_at_ms: i64,
    pub reason: EnqueueReason,
    /// BCP 47; empty when unknown.
    pub language: String,
}

/// `DequeueReason`, IE's part.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DequeueReason {
    Abandoned,
    Ended,
    Overflow,
}

/// `Outcome` 1–5 (what IE may send).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Connected,
    AgentNoAnswer,
    AgentLegFailed,
    CallerGone,
    AlreadyAssigned,
}

/// The outcome RE recorded first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Recorded {
    Connected,
    /// Another outcome, `EXPIRED` or `REVOKED` (RE's own) included.
    Other,
}

/// `OverflowAction`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OverflowAction {
    Queue { queue_id: String },
    Voicemail,
    CallbackOffer,
    Disconnect,
    FlowStep(String),
    Unknown,
}

/// A Watch message for one interaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RoutingEvent {
    Assigned {
        reservation_id: String,
        entry_id: String,
        agent_id: String,
        agent_contact: String,
        deadline_ms: i64,
        consult: bool,
    },
    /// Never replayed (`delivery_seq` 0).
    Position {
        entry_id: String,
        position: u32,
        expected_wait_s: u32,
    },
    Overflow {
        entry_id: String,
        action: OverflowAction,
    },
    Revoke {
        reservation_id: String,
        reason: String,
    },
    WrapUpEnded {
        reservation_id: String,
        agent_id: String,
    },
}

/// One message handed to a call: acknowledge `delivery_seq` (if not 0) once the machine took it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Delivery {
    pub delivery_seq: u64,
    pub event: RoutingEvent,
}

/// What the dispatcher hears about.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientEvent {
    /// A message for an interaction this replica released: answer it (an `Assigned` with
    /// `CALLER_GONE`) and acknowledge it ([`crate::RoutingClient::ack_released`]).
    AfterEnd { owner: Owner, delivery: Delivery },
    /// A message for an interaction this replica doesn't hold (adoption is step 8); logged once
    /// per interaction, not acknowledged.
    Orphan {
        tenant_id: String,
        interaction_id: String,
    },
    /// `RunCallback` (step 12): not handled, not acknowledged (RE gives it to another stream).
    RunCallback {
        tenant_id: String,
        interaction_id: String,
    },
}

/// Why a request failed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RouteError {
    /// `stale_owner`: a newer owner exists; stop acting on the interaction.
    #[error("stale_owner")]
    Fenced,
    /// A refusal that a retry won't change (`queue_unknown`, `queue_closed`, `queue_full`,
    /// `entry_unknown`, `reservation_unknown`, ...).
    #[error("refused: {0}")]
    Refused(String),
    /// A request IE should never have sent (`tenant_mismatch`, `invalid_request`,
    /// `enqueue_conflict`, `internal`): logged as an error.
    #[error("bug: {0}")]
    Bug(String),
    /// RE didn't answer within the retries (or the budget).
    #[error("unavailable: {0}")]
    Unavailable(String),
}

/// The Watch stream's state, for readiness and the gauge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Link {
    Connecting,
    Live,
    /// Another process watches with this replica id (`stream_superseded`): stopped.
    Superseded,
}

impl Link {
    /// Superseded: the stream stays stopped. (IE-059: a rising `leader_epoch` is a leader change,
    /// never a stop: phase 1's exit rule, CR-080, is gone.)
    pub fn stopped(self) -> bool {
        matches!(self, Self::Superseded)
    }
}

/// What `Reattach` found for an interaction (RE's view after an epoch raise or a re-check).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Reattached {
    /// The live entry's id, if RE has one.
    pub entry_id: Option<String>,
    /// Every live reservation.
    pub reservations: Vec<ReattachedReservation>,
    /// The highest `delivery_seq` RE sent; the unacknowledged ones are replayed on the stream.
    pub last_delivery_seq: u64,
}

/// A live reservation in [`Reattached`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReattachedReservation {
    pub reservation_id: String,
    pub agent_id: String,
    pub agent_contact: String,
    /// `CONNECTED` (else `ASSIGNED`, with its deadline).
    pub connected: bool,
    pub deadline_ms: i64,
}
