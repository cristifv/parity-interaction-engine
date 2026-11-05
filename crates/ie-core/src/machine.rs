//! The voice state machine with its verb runner (PLAN "The voice state machine", "The verb
//! runner"; step 4's part: claim, answer, consent, IVR, end). Pure: [`Call::step`] takes the time
//! and one [`Input`] and returns the [`Effect`]s to run, in order; [`Call::deadline`] is the next
//! time the call needs a [`Input::Tick`]. Commands to TC queue here and go out one at a time: the
//! next only after the previous one's [`Input::Done`] (T16).
//!
//! Digits never leave the machine: a gather's digits pick a route and are dropped. No effect
//! carries one (by type), so no log line, event or CDR can. An agent's DTMF action codes are
//! matched in parity-ms (P5); IE sees only what they did ([`Input::DtmfAction`]).
//!
//! Step 5a adds the queue (`dial` to a queue, with routing on): `Queued → Offering → Connected`
//! (PLAN's QUEUED, OFFERING, CONNECTED), requests to routing-engine as [`Effect::Route`] (one in
//! flight at a time, on their own channel next to TC's commands), and the cleanup when the
//! caller leaves: `Dequeue(ABANDONED)`, `ReportOutcome(CALLER_GONE)` or `CallEnded`, at most once
//! each. The machine stays [`Phase::Closing`] until that cleanup is answered.
//!
//! Step 6 completes DEMO level 1: hold music (a looped prompt) and position announcements while
//! queued; the agent's leg placed through TC ([`Cmd::AddAgentLeg`], a passthrough call bridged by
//! TC when it answers) and hung up when it doesn't connect ([`Cmd::RemoveLeg`]); recording per
//! leg after a granting consent ([`Cmd::StartRecording`]: the customer's leg at the decision, the
//! agent's when it answers); the agent's DTMF recording pause and resume and mute, reported as
//! events ([`Effect::Event`]); the CDR opened at answer and updated at connect
//! ([`Effect::Progress`]). IE-041: an unanswered `ReportOutcome(CONNECTED)` is retried until RE
//! answers or its reservation is surely expired; the agent is never left talking on a
//! reservation RE has freed.
//!
//! IE-059 (CONVENTIONS "A leader change"): after routing-engine's leader changes, a call RE may
//! hold re-attaches ([`Call::reattach_wanted`], [`Input::Reattached`]): nothing about the call's
//! media changes; a waiting caller whose entry the new leader doesn't have is enqueued again with
//! the same `enqueue_seq` and times (its place is kept); an outcome RPC (`ReportOutcome`,
//! `CallEnded`, `Dequeue`) that found RE unavailable is sent again after the `Reattach` and on a
//! retry timer (5 s doubling to 20 s; the timer only retries, RE's answer settles it); a call that
//! has ended keeps them at most [`UNSENT_HOLD_MS`] (L18: RE has dropped the interaction by then).
//!
//! IE-068 (event-based since the play saga, contracts 0.17.0): a prompt whose `Play` fails or
//! that ends with `PlayFailed` (parity-ms busy or unreachable) is tried once more with a new
//! play id, then skipped; one parity-ms doesn't have is skipped at once: an IVR call is never
//! dropped for a failed prompt. IE-069: when the agent's leg answers, any play on the
//! customer's leg is stopped, whatever IE believes plays.
//!
//! **The consent announcement's outcome is a legal fact** (the user, 2026-11-03): a recording
//! starts only once IE has the `PlayFinished(COMPLETED)` of every announcement prompt. A prompt
//! that fails, is stopped, or ends any other way means no recording (the call goes on); a
//! digit during the announcement doesn't cut it short (it waits for the window); a decision is
//! taken once, so a late `PlayFinished` never switches recording on.

use crate::flow::{Consent, ConsentMode, Flow, Gather, Verb};
use crate::prompt::say_prompt_id;
use std::collections::{BTreeSet, VecDeque};
use std::sync::Arc;

/// No-input timeouts in a row before the flow's `no_input` verbs.
pub const MAX_NO_INPUT: u32 = 3;
/// Verbs per step entry before the loop guard.
pub const MAX_VERBS_PER_ENTRY: u32 = 100;
/// `goto`s per minute before the loop guard.
pub const MAX_GOTOS_PER_MINUTE: usize = 20;
/// The interaction's safety cap.
pub const MAX_CALL_MS: u64 = 8 * 3600 * 1000;
/// How long a collection may stay open in TC (prompts plus the gather), seconds (1–300).
pub const COLLECTION_MAX_S: u32 = 120;
/// How long the agent's leg may take to answer after `Assigned` (PLAN "Timers"); also its TC
/// ring timeout.
pub const AGENT_LEG_ANSWER_MS: u64 = 10_000;
/// IE-050: how long `Ending` waits for TC's end before asking TC (`GetSession`); the wait
/// doubles after each ask, up to [`ENDING_MAX_MS`].
pub const ENDING_MS: u64 = 10_000;
/// IE-050/IE-056: the longest wait between asks (and `EndSession`s to a session TC still has).
pub const ENDING_MAX_MS: u64 = 60_000;
/// IE-056: a call in `Ending` this long is reported stuck (once): an operator's case, never closed
/// without TC's end or the node's death (CONVENTIONS "Lost contact is not death").
pub const ENDING_STUCK_MS: u64 = 300_000;
/// IE-041: the first wait before `ReportOutcome(CONNECTED)` is sent again after RE didn't answer;
/// it doubles up to [`CONNECT_RETRY_MAX_MS`].
pub const CONNECT_RETRY_MS: u64 = 500;
/// IE-041: the longest wait between `CONNECTED` retries.
pub const CONNECT_RETRY_MAX_MS: u64 = 4_000;
/// IE-041: past `Assigned.deadline_ms` + this (L7's 2 s plus its 10 s pause while IE's stream is
/// down), RE has surely expired the reservation and freed the agent: an unanswered `CONNECTED`
/// is given up and the agent's leg hung up (no double booking).
pub const CONNECT_GIVE_UP_AFTER_DEADLINE_MS: u64 = 12_000;
/// IE-041: the give-up time when `Assigned` carried no deadline (RE's default assignment
/// deadline, 30 s, plus [`CONNECT_GIVE_UP_AFTER_DEADLINE_MS`]).
pub const CONNECT_GIVE_UP_NO_DEADLINE_MS: u64 = 42_000;
/// Position announcements: at most one per this long.
pub const ANNOUNCE_GAP_MS: u64 = 20_000;
/// IE-059: an ended call keeps its outcome RPCs that found routing-engine unavailable this long
/// (RE's owner-loss timeout, L18: after it RE has dropped the interaction anyway), for the
/// `Reattach` of a leader change to send them again.
pub const UNSENT_HOLD_MS: u64 = 60_000;
/// IE-059: an outcome RPC that found routing-engine unavailable is sent again after this long
/// (doubling to [`UNSENT_RETRY_MAX_MS`]), besides at the next leader change's `Reattach`: the timer
/// only retries; RE's answer settles it.
pub const UNSENT_RETRY_MS: u64 = 5_000;
pub const UNSENT_RETRY_MAX_MS: u64 = 20_000;
/// IE-068: tries of a prompt whose `Play` failed (the first and one more), then it is skipped.
pub const PLAY_TRIES: u32 = 2;
/// IE-069: the plays remembered as possibly running (the latest).
const MAX_OPEN_PLAYS: usize = 32;

/// Where play, collection, leg and recording ids come from (UUIDs; a counter in tests).
pub trait Ids: Send {
    fn next_id(&mut self) -> String;
}

/// A dialled number's route: its tenant (the number map's) and flow.
#[derive(Debug, Clone)]
pub struct Route {
    pub tenant_id: String,
    pub flow_name: String,
    pub flow: Arc<Flow>,
    pub consent: Consent,
    /// `[routing] enabled`: a `dial` to a queue enqueues; off, it is skipped and counted.
    pub routing: bool,
    /// Step 6: recordings can be started (`[recording] mode = "null_sink"` until step 9's keys,
    /// MS-012). Off, a granting consent only counts `recording_allowed` (no audio anywhere).
    pub recording: bool,
}

/// The position prompt for a position (1 is next).
pub fn position_prompt(position: u32) -> String {
    if (1..=9).contains(&position) {
        format!("queue-position-{position}")
    } else {
        "queue-position-more".to_owned()
    }
}

/// Why IE enqueues (routing.v1 `EnqueueReason`, IE's part).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnqueueReason {
    /// The call's first queue: `ordering_at` = `first_queued_at` = now.
    New,
    /// A later `dial` in the flow: the ordering time restarts, `first_queued_at` is kept.
    Transfer,
}

/// Why an entry leaves its queue (routing.v1 `DequeueReason`, IE's part).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DequeueReason {
    /// The caller hung up.
    Abandoned,
    /// IE ended the call (or the flow left the queue).
    Ended,
    /// IE takes over a voicemail, disconnect or flow-step overflow.
    Overflow,
}

/// An outcome IE reports for a reservation (routing.v1 `Outcome` 1–5).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentOutcome {
    Connected,
    AgentNoAnswer,
    AgentLegFailed,
    CallerGone,
    AlreadyAssigned,
}

/// A request to routing-engine about this interaction. The task adds the owner (the number
/// map's tenant, the epoch, the replica) and turns the machine's times into wall-clock times.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteCmd {
    Enqueue {
        enqueue_seq: u32,
        queue_id: String,
        priority: u32,
        /// The machine's time of the call's first queue; never changes.
        first_queued_ms: u64,
        /// The machine's time that orders the entry.
        ordering_ms: u64,
        reason: EnqueueReason,
    },
    Dequeue {
        entry_id: String,
        reason: DequeueReason,
    },
    ReportOutcome {
        reservation_id: String,
        outcome: AgentOutcome,
        /// Why, without personal data.
        detail: &'static str,
    },
    CallEnded {
        agent_id: String,
        reservation_id: String,
        ended_ms: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum RouteKind {
    Enqueue,
    Dequeue,
    Outcome(AgentOutcome, String),
    CallEnded,
}

impl RouteCmd {
    fn kind(&self) -> RouteKind {
        match self {
            Self::Enqueue { .. } => RouteKind::Enqueue,
            Self::Dequeue { .. } => RouteKind::Dequeue,
            Self::ReportOutcome {
                outcome,
                reservation_id,
                ..
            } => RouteKind::Outcome(*outcome, reservation_id.clone()),
            Self::CallEnded { .. } => RouteKind::CallEnded,
        }
    }
}

/// A routing request's result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteResult {
    /// `Enqueue`: the entry, new or the existing one, and its position then (0: none given).
    Enqueued { entry_id: String, position: u32 },
    /// `ReportOutcome`: whether RE recorded `CONNECTED` (false: `EXPIRED`, `REVOKED`, or another
    /// first outcome).
    Recorded { connected: bool },
    /// `Dequeue` (`already_gone` included), `CallEnded`.
    Done,
    /// `stale_owner`: a newer owner exists.
    Fenced,
    /// Refused for good: an unknown, closed or full queue, an unknown entry or reservation, a bug.
    Refused,
    /// RE unreachable within the request's retries.
    Unavailable,
}

/// What became of the agent's leg (TC's `LegUpdate` for it, or the machine's own timer).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentLeg {
    /// Answered: TC bridged it with the customer (a passthrough call).
    Answered,
    /// Not answered in its ring time.
    NoAnswer,
    /// Failed before answering (busy, unregistered, refused).
    Failed,
    /// Ended after it answered: the agent hung up (or the browser went).
    Ended,
}

/// RE's overflow action for an entry (routing.v1 `OverflowAction`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OverflowAction {
    /// RE moved the entry to another queue: nothing for IE.
    Queue,
    /// A callback offer (step 12): the entry keeps its place.
    CallbackOffer,
    Voicemail,
    Disconnect,
    /// Continue the flow at this step.
    FlowStep(String),
}

/// Where a `dial` to a queue is (PLAN's QUEUED, OFFERING, CONNECTED).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueState {
    /// Enqueued (or the `Enqueue` in flight); no agent yet.
    Queued,
    /// An agent accepted (`Assigned`): the agent's leg is being placed, or answered and its
    /// `CONNECTED` not yet recorded by RE.
    Offering,
    /// The agent's leg answered and RE recorded `CONNECTED`.
    Connected,
}

/// Why the call ended, as the CDR records it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EndCause {
    Normal,
    System,
}

/// A command to TC (the task fills in the owner, and the customer's leg where none is named).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Cmd {
    Answer,
    /// A prompt into the customer's leg; `looped`: until stopped (hold music).
    Play {
        play_id: String,
        prompt_id: String,
        looped: bool,
    },
    StopPlay {
        play_id: String,
    },
    DigitsOn {
        collection_id: String,
        max_duration_s: u32,
    },
    DigitsOff {
        collection_id: String,
    },
    EndSession(EndCause),
    /// The agent's leg (`AddLeg`, role AGENT, to the softphone RE named): the task adds the
    /// endpoint, codecs, ring time and the agent's DTMF action codes. `record_locked`: consent
    /// didn't grant recording, so DTMF may not touch the recording facet.
    AddAgentLeg {
        leg_id: String,
        agent_contact: String,
        record_locked: bool,
    },
    /// Hangs up one leg (an agent's leg that didn't connect).
    RemoveLeg {
        leg_id: String,
    },
    /// Records one leg (`None`: the customer's). The task supplies the key.
    StartRecording {
        agent_leg: Option<String>,
        recording_id: String,
    },
}

impl Cmd {
    fn kind(&self) -> Kind {
        match self {
            Self::Answer => Kind::Answer,
            Self::Play { play_id, .. } => Kind::Play(play_id.clone()),
            Self::StopPlay { .. } => Kind::StopPlay,
            Self::DigitsOn { .. } => Kind::DigitsOn,
            Self::DigitsOff { .. } => Kind::DigitsOff,
            Self::EndSession(_) => Kind::EndSession,
            Self::AddAgentLeg { leg_id, .. } => Kind::AddLeg(leg_id.clone()),
            Self::RemoveLeg { .. } => Kind::RemoveLeg,
            Self::StartRecording { agent_leg, .. } => Kind::StartRecording(agent_leg.is_some()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Kind {
    Answer,
    Play(String),
    StopPlay,
    DigitsOn,
    DigitsOff,
    EndSession,
    AddLeg(String),
    RemoveLeg,
    StartRecording(bool),
}

/// A command's result, as the machine needs it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CmdResult {
    Done,
    /// A newer owner has the session, or the arrival isn't ours: stop, send nothing.
    Fenced,
    /// The session has already ended.
    Gone,
    /// `Play` of a prompt parity-ms doesn't have: the verb is skipped.
    PromptUnknown,
    /// Anything else.
    Failed,
}

/// The consent decision (PLAN "Privacy").
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    Granted,
    Refused,
}

/// How it was reached (for the CDR).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsentBasis {
    /// The tenant never records.
    Never,
    /// Announced, not opted out.
    NotOptedOut,
    /// The opt-out digit.
    OptedOut,
    /// The opt-in digit.
    OptedIn,
    /// Asked; no opt-in.
    NoOptIn,
    /// IE-068: the announcement wasn't heard to its end (a prompt failed, is missing, or ended
    /// other than completed): no recording.
    NotAnnounced,
}

impl ConsentBasis {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Never => "never",
            Self::NotOptedOut => "not_opted_out",
            Self::OptedOut => "opted_out",
            Self::OptedIn => "opted_in",
            Self::NoOptIn => "no_opt_in",
            Self::NotAnnounced => "not_announced",
        }
    }
}

/// A DTMF action an agent keyed (parity-ms P5; never its digits).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DtmfKind {
    Mute,
    Unmute,
    MuteToggle,
    RecordingPause,
    RecordingResume,
    RecordingToggle,
    /// A supervisor's mode (step 14) or a kind this release doesn't know.
    Other,
}

/// What the action did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DtmfResult {
    Applied,
    Noop,
    Refused,
    Locked,
}

/// A recording's state as TC reports it (the session's, for a DTMF action).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecState {
    Active,
    Paused,
    Stopped,
    Unknown,
}

/// What the task tells the machine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Input {
    /// The owner key is ours, with this epoch.
    Claimed(u64),
    ClaimTaken,
    ClaimUnavailable,
    /// The result of the command in flight.
    Done(CmdResult),
    /// A play ended; `completed`: it played to its end (`PLAY_END_REASON_COMPLETED`).
    PlayFinished {
        play_id: String,
        completed: bool,
    },
    /// A play ended without starting (`PlayFailed`, contracts 0.17.0); `prompt_unknown`: the
    /// prompt is missing or can't be played (skipped at once), else it is tried once more.
    PlayFailed {
        play_id: String,
        prompt_unknown: bool,
    },
    /// A digit of a collection (never logged or kept beyond its gather).
    Digit {
        collection_id: String,
        digit: char,
    },
    /// The session ended (`SessionEnded`, an ENDED snapshot).
    Ended {
        reason: String,
        sip_status: u32,
    },
    /// TC's snapshot replaced IE's view.
    Resync {
        ended: bool,
    },
    Unmanaged,
    Managed,
    /// A deadline passed.
    Tick,
    /// The result of the routing request in flight.
    Route(RouteResult),
    /// RE: an agent accepted (routing.v1 `Assigned`). `deadline_ms`: RE's deadline for the
    /// outcome in the machine's time (0: none given).
    Assigned {
        reservation_id: String,
        entry_id: String,
        agent_id: String,
        agent_contact: String,
        deadline_ms: u64,
    },
    /// RE withdrew a reservation (`Revoke`).
    Revoked {
        reservation_id: String,
    },
    /// RE's overflow for an entry.
    Overflow {
        entry_id: String,
        action: OverflowAction,
    },
    /// RE: the entry's position (1 is next).
    Position {
        entry_id: String,
        position: u32,
    },
    /// What became of an agent's leg (TC's `LegUpdate` for a leg this call placed).
    AgentLeg {
        leg_id: String,
        leg: AgentLeg,
    },
    /// parity-ms acted (or not) on an action code keyed on the agent's leg (never digits).
    /// `recording`: the session's recording state afterwards; `muted`: the agent leg's.
    DtmfAction {
        action: DtmfKind,
        result: DtmfResult,
        recording: RecState,
        muted: bool,
    },
    /// What `Effect::Reconcile` found (IE-050).
    Reconciled(Reconcile),
    /// IE-059: what the new routing-engine leader holds for this interaction after a leader
    /// change (`Reattach` with the unchanged owner); `None`: the `Reattach` failed.
    Reattached(Option<RouteSync>),
}

/// IE-059: `Reattach`'s answer, as the machine needs it.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RouteSync {
    /// The live entry, if RE holds one.
    pub entry_id: Option<String>,
    /// The live reservations: (id, `CONNECTED`).
    pub reservations: Vec<(String, bool)>,
}

/// What asking TC about an ending session found (IE-050).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reconcile {
    /// Over at TC: its end is on its way through the mailbox (from the tombstone, or as lost).
    Ended,
    /// TC still has it live: end it again.
    Live,
    /// A newer owner has it: stop.
    Fenced,
    /// TC can't be asked now (the node is unmanaged or doesn't answer).
    Unreachable,
}

/// How a `dial` to a queue went, for the CDR (IE-046).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum QueueOutcome {
    /// No `dial` to a queue.
    #[default]
    None,
    /// Still waiting (the CDR's open row).
    Waiting,
    /// An agent connected (RE recorded `CONNECTED`).
    Connected,
    /// The caller hung up before an agent connected.
    Abandoned,
    /// RE's overflow took the call out of the queue.
    Overflow,
    /// The queue refused the call.
    Refused,
    /// RE was unreachable.
    Unavailable,
    /// The flow left the queue otherwise (IE ended the call).
    Ended,
}

impl QueueOutcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "",
            Self::Waiting => "waiting",
            Self::Connected => "connected",
            Self::Abandoned => "abandoned",
            Self::Overflow => "overflow",
            Self::Refused => "refused",
            Self::Unavailable => "unavailable",
            Self::Ended => "ended",
        }
    }
}

/// What the call did, for its ENDED record and CDR.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Summary {
    pub flow: String,
    pub last_step: String,
    pub consent: Option<(Decision, ConsentBasis)>,
    pub started_ms: u64,
    pub answered_ms: Option<u64>,
    /// 0 until the call ended (the CDR's open row).
    pub ended_ms: u64,
    pub end_reason: String,
    pub ended_by_ie: Option<EndCause>,
    pub sip_status: u32,
    pub owner_epoch: u64,
    pub gathers: u32,
    pub no_inputs: u32,
    /// IE-046: the last queue the call entered, its entry in RE, when, and how it went.
    pub queue_id: String,
    pub entry_id: String,
    pub queued_ms: Option<u64>,
    /// When the call left the queue (connected, abandoned, overflow...).
    pub dequeued_ms: Option<u64>,
    pub queue_outcome: QueueOutcome,
    /// Agents that accepted the call (`Assigned`s taken).
    pub offers: u32,
    /// The agent who connected, and the reservation.
    pub agent_id: String,
    pub reservation_id: String,
    pub connected_ms: Option<u64>,
    /// Legs a recording was started on (step 6: a null sink until step 9).
    pub recorded_legs: u32,
    /// Legs whose recording TC refused (no sink on the node, a refused key): a consented caller
    /// not recorded is a recording failure to report, never a reason to drop the call.
    pub recording_failures: u32,
    /// DTMF recording pauses and mutes the agent applied.
    pub recording_pauses: u32,
    pub mutes: u32,
}

/// A named moment of the call, for its log line now and `interaction.events` in step 9. Never
/// digits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CallEvent {
    /// The agent paused the recording (by DTMF).
    RecordingPaused,
    RecordingResumed,
    Muted,
    Unmuted,
    /// The caller heard this position.
    PositionAnnounced(u32),
    /// The agent's leg answered and RE recorded `CONNECTED`.
    AgentConnected,
    /// A recording started on a leg (`true`: the agent's).
    RecordingStarted(bool),
    /// TC refused a leg's recording (`true`: the agent's); the call goes on unrecorded.
    RecordingFailed(bool),
}

impl CallEvent {
    /// The log record's catalogue event (log-record.v1: `noun.verb`).
    pub fn log_event(self) -> &'static str {
        match self {
            Self::RecordingPaused => "recording.paused",
            Self::RecordingResumed => "recording.resumed",
            Self::Muted => "agent.muted",
            Self::Unmuted => "agent.unmuted",
            Self::PositionAnnounced(_) => "queue.position_announced",
            Self::AgentConnected => "agent.connected",
            Self::RecordingStarted(_) => "recording.started",
            Self::RecordingFailed(_) => "recording.failed",
        }
    }

    /// The metric label (`interaction_engine_flow_events_total{event}`).
    pub fn name(self) -> &'static str {
        match self {
            Self::RecordingPaused => "recording_paused",
            Self::RecordingResumed => "recording_resumed",
            Self::Muted => "muted",
            Self::Unmuted => "unmuted",
            Self::PositionAnnounced(_) => "position_announced",
            Self::AgentConnected => "agent_connected",
            Self::RecordingStarted(_) => "recording_started",
            Self::RecordingFailed(_) => "recording_failed",
        }
    }
}

/// What the task does, in order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Effect {
    /// Refuse the arrival (nothing claimed).
    Decline { sip_status: u32 },
    /// `X-Tenant-ID` disagreed with the number map: count and alert.
    TenantMismatch,
    /// Claim the owner key.
    Claim,
    /// One command to TC; the next waits for this one's `Done`.
    Command(Cmd),
    /// The consent decision, once.
    ConsentDecided(Decision, ConsentBasis),
    /// Recording is allowed (after a granting decision only) but recordings are off
    /// (`Route::recording` false): counted, nothing started.
    StartRecording,
    /// A counter (a skipped verb, a dropped digit, a loop guard).
    Count(&'static str),
    /// A gather ended: the step it routes to (`""`: the next verb) and how many digits it had.
    /// Never the digits.
    Gathered { to: String, digits: u32 },
    /// The CDR's open row (no end yet): written at answer, updated at the queue's moments.
    Progress(Box<Summary>),
    /// A named moment of the call (a log line; `interaction.events` from step 9).
    Event(CallEvent),
    /// Write the ENDED record, redeem the ack, release the owner key, close the CDR. Once.
    Finish(Box<Summary>),
    /// `Ending` waited past its deadline for TC's end (IE-050): ask TC (`GetSession`) and close
    /// from what it says; the answer comes back as [`Input::Reconciled`].
    Reconcile,
    /// One request to routing-engine; the next waits for this one's [`Input::Route`].
    Route(RouteCmd),
}

/// Where the call is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    Claiming,
    Answering,
    Consent,
    Ivr,
    Ending,
    /// The session has ended (the ENDED record is out); routing's cleanup is still in flight.
    Closing,
    Ended,
    Dropped,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Source {
    Consent,
    Step(String),
    NoInput,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Activity {
    Idle,
    Playing {
        play_id: String,
    },
    Pausing {
        until: u64,
    },
    Gathering {
        spec: Gather,
        digits: String,
        deadline: u64,
    },
    /// In a `dial` to a queue (its state is [`Call::dial`]).
    Dialing,
}

/// Where a reservation's `CONNECTED` is (IE-041).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Connect {
    /// The agent's leg hasn't answered.
    NotSent,
    /// Sent; its answer is awaited.
    InFlight,
    /// RE didn't answer: sent again at `at`.
    Retry { at: u64 },
    /// RE recorded it.
    Recorded,
}

/// A reservation of an agent for this call.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Res {
    id: String,
    agent_id: String,
    /// The agent's leg (made here, placed with `AddLeg`).
    leg_id: String,
    /// The leg's answer timer.
    leg_deadline: u64,
    answered: bool,
    connect: Connect,
    /// IE-041: the wait before the last retry (doubles, capped).
    retry_gap: u64,
    /// IE-041: past this an unanswered `CONNECTED` is given up (RE has freed the agent).
    give_up_at: u64,
    /// The conversation is over (the agent's leg ended after answering, or the caller left)
    /// while `CONNECTED` was unsettled: its answer decides between `CallEnded` and `Dequeue`.
    over: Option<DequeueReason>,
}

/// What the caller hears while waiting.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Media {
    None,
    Music(String),
    Announcing(String),
}

/// A `dial` to a queue.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Dialing {
    entry: Option<String>,
    res: Option<Res>,
    /// `Dequeue(OVERFLOW)` is out; this action runs on its answer.
    overflow: Option<OverflowAction>,
    /// The entry is leaving: the caller left (or IE ended the call) and the cleanup is queued
    /// or waits for the entry, or a `Dequeue(OVERFLOW)` is out.
    left: bool,
    /// A `Dequeue` of the entry is queued or sent (at most one per dial).
    dequeued: bool,
    music: Option<String>,
    announce: bool,
    media: Media,
    /// The latest position RE sent, the last one announced, and when.
    position: Option<u32>,
    announced: Option<u32>,
    announced_at: Option<u64>,
    /// MF-1b: a re-attach found nothing of this dial in RE (no entry, no reservation) while an
    /// agent's reservation was being placed: when it ends without connecting, enqueue again.
    forgotten: bool,
}

impl Dialing {
    fn queued(&self) -> bool {
        self.res.is_none() && !self.left && self.overflow.is_none()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Collection {
    id: String,
    /// The gather it feeds: its index in the current source.
    gather_idx: usize,
    /// Digits pressed before the gather started (barge-in).
    buffer: String,
}

/// One call.
pub struct Call {
    phase: Phase,
    route: Option<Route>,
    ids: Box<dyn Ids>,
    claimed: bool,
    /// IE-050: when `Ending` began, the next ask, the wait after it, and whether it was reported stuck.
    ending_since: Option<u64>,
    ending_next: u64,
    ending_gap: u64,
    ending_stuck: bool,
    outbox: VecDeque<Cmd>,
    awaiting: Option<Kind>,
    source: Source,
    idx: usize,
    entry_verbs: u32,
    gotos: VecDeque<u64>,
    no_input_run: u32,
    activity: Activity,
    collection: Option<Collection>,
    consent_verbs: Vec<Verb>,
    unmanaged: bool,
    recording: bool,
    now: u64,
    summary: Summary,
    dial: Option<Dialing>,
    route_outbox: VecDeque<RouteCmd>,
    route_awaiting: Option<RouteKind>,
    enqueue_seq: u32,
    first_queued: Option<u64>,
    /// Reservations this call has reported an outcome for (at most one kind each; `CONNECTED`
    /// may be sent again while unanswered, IE-041).
    reported: BTreeSet<String>,
    /// The agent legs this call placed (each once).
    legs: BTreeSet<String>,
    /// The routing request in flight (its kind is `route_awaiting`).
    route_sent: Option<RouteCmd>,
    /// IE-059: the dial's latest `Enqueue` (sent again, unchanged, when a new leader lacks it).
    last_enqueue: Option<RouteCmd>,
    /// IE-059: outcome RPCs that found routing-engine unavailable, and since when.
    unsent: Vec<RouteCmd>,
    unsent_since: Option<u64>,
    /// IE-059: when the unsent ones go again (0: not scheduled), and the wait after that.
    unsent_retry_at: u64,
    unsent_gap: u64,
    /// IE-068: the failed tries of the flow's prompt in flight.
    play_failures: u32,
    /// IE-069: plays on the customer's leg that may still run (sent, no `PlayFinished` yet; a
    /// failed one included: TC may have started it as its timeout fired). Stopped on attach.
    open_plays: VecDeque<String>,
    /// The consent announcement's prompts heard to their end (`PlayFinished(COMPLETED)`): a
    /// granting decision needs all of them.
    consent_heard: usize,
}

impl std::fmt::Debug for Call {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never the digits a gather holds.
        f.debug_struct("Call")
            .field("phase", &self.phase)
            .field("source", &self.source)
            .field("idx", &self.idx)
            .finish_non_exhaustive()
    }
}

impl Call {
    /// An arrival: `route` is the dialled number's (None: not in the map); `claimed_tenant` is
    /// `X-Tenant-ID`, which must equal the map's tenant.
    pub fn arrive(
        now: u64,
        route: Option<Route>,
        claimed_tenant: &str,
        ids: Box<dyn Ids>,
    ) -> (Self, Vec<Effect>) {
        let consent_verbs = route
            .as_ref()
            .map(|r| consent_verbs(&r.consent))
            .unwrap_or_default();
        let mut c = Self {
            phase: Phase::Claiming,
            summary: Summary {
                flow: route
                    .as_ref()
                    .map(|r| r.flow_name.clone())
                    .unwrap_or_default(),
                started_ms: now,
                ..Summary::default()
            },
            route,
            ids,
            claimed: false,
            ending_since: None,
            ending_next: 0,
            ending_gap: ENDING_MS,
            ending_stuck: false,
            outbox: VecDeque::new(),
            awaiting: None,
            source: Source::Consent,
            idx: 0,
            entry_verbs: 0,
            gotos: VecDeque::new(),
            no_input_run: 0,
            activity: Activity::Idle,
            collection: None,
            consent_verbs,
            unmanaged: false,
            recording: false,
            now,
            dial: None,
            route_outbox: VecDeque::new(),
            route_awaiting: None,
            enqueue_seq: 0,
            first_queued: None,
            reported: BTreeSet::new(),
            legs: BTreeSet::new(),
            route_sent: None,
            last_enqueue: None,
            unsent: Vec::new(),
            unsent_since: None,
            unsent_retry_at: 0,
            unsent_gap: UNSENT_RETRY_MS,
            play_failures: 0,
            open_plays: VecDeque::new(),
            consent_heard: 0,
        };
        let fx = match &c.route {
            None => {
                c.phase = Phase::Dropped;
                vec![Effect::Decline { sip_status: 404 }]
            }
            Some(r) if r.tenant_id != claimed_tenant => {
                c.phase = Phase::Dropped;
                vec![Effect::TenantMismatch, Effect::Decline { sip_status: 403 }]
            }
            Some(_) => vec![Effect::Claim],
        };
        (c, fx)
    }

    pub fn phase(&self) -> Phase {
        self.phase
    }

    /// Nothing more will happen.
    pub fn done(&self) -> bool {
        matches!(self.phase, Phase::Ended | Phase::Dropped)
    }

    /// This replica holds the owner key.
    pub fn claimed(&self) -> bool {
        self.claimed
    }

    /// Whether recording has been allowed and started (the consent invariant).
    pub fn recording(&self) -> bool {
        self.recording
    }

    /// IE-059: whether routing-engine may hold something of this call (it was ever enqueued
    /// and isn't over): after a leader change the task re-attaches it.
    pub fn reattach_wanted(&self) -> bool {
        self.enqueue_seq > 0 && !self.done()
    }

    /// What the call did so far.
    pub fn summary(&self) -> &Summary {
        &self.summary
    }

    /// Where the call's `dial` to a queue is; None outside one (or once the caller left).
    pub fn queue_state(&self) -> Option<QueueState> {
        let d = self.dial.as_ref()?;
        if d.left || d.overflow.is_some() || self.phase != Phase::Ivr {
            return None;
        }
        Some(match &d.res {
            None => QueueState::Queued,
            Some(r) if r.connect == Connect::Recorded => QueueState::Connected,
            Some(_) => QueueState::Offering,
        })
    }

    /// The reservation's timers: the leg's answer timer, a `CONNECTED` retry, its give-up.
    fn res_deadline(&self) -> Option<u64> {
        let r = self.dial.as_ref()?.res.as_ref()?;
        let mut at = None::<u64>;
        let mut put = |t: u64| at = Some(at.map_or(t, |a| a.min(t)));
        if !r.answered {
            put(r.leg_deadline);
        }
        // In flight: its answer comes within the client's retries (and is given up then if late).
        if let Connect::Retry { at: t } = r.connect {
            put(t);
            put(r.give_up_at);
        }
        at
    }

    /// The next time a `Tick` is due; none while unmanaged (T14). `Ending` has its own, unmanaged
    /// or not (IE-050): TC's end may never come. `Closing` ticks only for an unsettled
    /// `CONNECTED` (IE-041).
    pub fn deadline(&self) -> Option<u64> {
        if self.phase == Phase::Ending {
            return self.ending_since.map(|_| self.ending_next);
        }
        let min = |a: Option<u64>, b: Option<u64>| match (a, b) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        // IE-059: the retry of outcome RPCs RE didn't answer (TC's state doesn't matter to it).
        let retry =
            (!self.unsent.is_empty() && self.unsent_retry_at > 0).then_some(self.unsent_retry_at);
        if self.phase == Phase::Closing {
            let hold = self.unsent_since.map(|t| t.saturating_add(UNSENT_HOLD_MS));
            return min(min(self.res_deadline(), hold), retry);
        }
        if self.unmanaged || !matches!(self.phase, Phase::Consent | Phase::Ivr | Phase::Answering) {
            return retry;
        }
        let cap = self.summary.started_ms.saturating_add(MAX_CALL_MS);
        let act = match &self.activity {
            Activity::Pausing { until } => Some(*until),
            Activity::Gathering { deadline, .. } => Some(*deadline),
            Activity::Dialing => {
                let announce = self.dial.as_ref().and_then(|d| {
                    (d.queued()
                        && d.announce
                        && d.position.is_some_and(|p| p > 0)
                        && d.position != d.announced)
                        .then(|| d.announced_at.map_or(0, |t| t + ANNOUNCE_GAP_MS))
                });
                match (self.res_deadline(), announce) {
                    (Some(a), Some(b)) => Some(a.min(b)),
                    (a, b) => a.or(b),
                }
            }
            _ => None,
        };
        min(Some(act.map_or(cap, |a| a.min(cap))), retry)
    }

    /// One input at `now`; the effects to run, in order.
    pub fn step(&mut self, now: u64, input: Input) -> Vec<Effect> {
        let mut fx = Vec::new();
        if self.done() {
            return fx;
        }
        self.now = self.now.max(now);
        self.input(input, &mut fx);
        self.run(&mut fx);
        self.pump(&mut fx);
        self.retry_unsent(&mut fx);
        self.requeue_forgotten(&mut fx);
        self.pump_route(&mut fx);
        if self.phase == Phase::Ending && self.ending_since.is_none() {
            self.ending_since = Some(self.now);
            self.ending_next = self.now.saturating_add(ENDING_MS);
        }
        if self.phase == Phase::Closing
            && self
                .unsent_since
                .is_some_and(|t| self.now >= t.saturating_add(UNSENT_HOLD_MS))
        {
            // IE-059: no leader change came to send them again; RE's owner-loss rule (L18) has
            // dropped the interaction by now (its agent freed or in wrap-up).
            fx.push(Effect::Count("routing_outcome_lost"));
            self.unsent.clear();
            self.unsent_since = None;
            self.unsent_retry_at = 0;
        }
        if self.phase == Phase::Closing
            && self.route_awaiting.is_none()
            && self.route_outbox.is_empty()
            && self.res_deadline().is_none()
            && self.unsent.is_empty()
        {
            self.phase = Phase::Ended;
        }
        fx
    }

    /// IE-059: outcome RPCs RE didn't answer go again when their timer is due (a retry only:
    /// RE's answer settles them; a ended call's hold bounds them).
    fn retry_unsent(&mut self, fx: &mut Vec<Effect>) {
        if self.unsent.is_empty() || self.unsent_retry_at == 0 || self.now < self.unsent_retry_at {
            return;
        }
        for c in std::mem::take(&mut self.unsent) {
            fx.push(Effect::Count("routing_outcome_retried"));
            self.route_outbox.push_back(c);
        }
        self.unsent_retry_at = 0;
        self.unsent_gap = (self.unsent_gap * 2).min(UNSENT_RETRY_MAX_MS);
    }

    /// MF-1b: the reservation of a dial RE forgot has ended without connecting: the caller is
    /// waiting again with no entry in RE, so the dial's `Enqueue` goes again (same seq and times).
    fn requeue_forgotten(&mut self, fx: &mut Vec<Effect>) {
        let live = self.dialing();
        let busy = matches!(self.route_awaiting, Some(RouteKind::Enqueue))
            || self
                .route_outbox
                .iter()
                .any(|c| matches!(c, RouteCmd::Enqueue { .. }));
        let Some(d) = self.dial.as_mut() else {
            return;
        };
        if !d.forgotten || !live || !d.queued() || busy {
            return;
        }
        d.forgotten = false;
        if let Some(c) = self.last_enqueue.clone() {
            d.entry = None;
            fx.push(Effect::Count("routing_reenqueued"));
            self.route_outbox.push_back(c);
        }
    }

    fn queue_route(&mut self, c: RouteCmd) {
        if let RouteCmd::ReportOutcome { reservation_id, .. } = &c {
            self.reported.insert(reservation_id.clone());
        }
        self.route_outbox.push_back(c);
    }

    fn pump_route(&mut self, fx: &mut Vec<Effect>) {
        if self.route_awaiting.is_none()
            && !self.done()
            && let Some(c) = self.route_outbox.pop_front()
        {
            self.route_awaiting = Some(c.kind());
            if matches!(c, RouteCmd::Enqueue { .. }) {
                self.last_enqueue = Some(c.clone());
            }
            self.route_sent = Some(c.clone());
            fx.push(Effect::Route(c));
        }
    }

    /// Reports an outcome for a reservation, at most once per reservation.
    fn report(&mut self, reservation_id: String, outcome: AgentOutcome, detail: &'static str) {
        if !self.reported.contains(&reservation_id) {
            self.queue_route(RouteCmd::ReportOutcome {
                reservation_id,
                outcome,
                detail,
            });
        }
    }

    fn route(&self) -> Option<&Route> {
        self.route.as_ref()
    }

    fn queue(&mut self, c: Cmd) {
        if let Cmd::Play { play_id, .. } = &c {
            if self.open_plays.len() >= MAX_OPEN_PLAYS {
                self.open_plays.pop_front();
            }
            self.open_plays.push_back(play_id.clone());
        }
        self.outbox.push_back(c);
    }

    fn pump(&mut self, fx: &mut Vec<Effect>) {
        if self.awaiting.is_none()
            && !self.done()
            && let Some(c) = self.outbox.pop_front()
        {
            self.awaiting = Some(c.kind());
            fx.push(Effect::Command(c));
        }
    }

    fn end(&mut self, cause: EndCause) {
        if matches!(self.phase, Phase::Ending | Phase::Ended | Phase::Dropped) {
            return;
        }
        // EndSession ends every leg; TC turns digit reporting off with it.
        self.outbox.clear();
        self.collection = None;
        self.activity = Activity::Idle;
        self.summary.ended_by_ie = Some(cause);
        self.queue(Cmd::EndSession(cause));
        self.phase = Phase::Ending;
    }

    fn drop_call(&mut self) {
        self.outbox.clear();
        self.route_outbox.clear();
        self.phase = Phase::Dropped;
    }

    fn progress(&self, fx: &mut Vec<Effect>) {
        if self.claimed {
            fx.push(Effect::Progress(Box::new(self.summary.clone())));
        }
    }

    /// The call leaves the queue (for the CDR), once.
    fn queue_left(&mut self, outcome: QueueOutcome) {
        if matches!(
            self.summary.queue_outcome,
            QueueOutcome::Waiting | QueueOutcome::None
        ) && self.summary.queued_ms.is_some()
        {
            self.summary.queue_outcome = outcome;
            self.summary.dequeued_ms = Some(self.now);
        }
    }

    /// Stops what the caller hears while waiting (before the next verb or an agent).
    fn stop_media(&mut self) {
        if let Some(d) = self.dial.as_mut() {
            match std::mem::replace(&mut d.media, Media::None) {
                Media::Music(p) | Media::Announcing(p) => {
                    self.open_plays.retain(|o| *o != p);
                    self.outbox.push_back(Cmd::StopPlay { play_id: p });
                }
                Media::None => {}
            }
        }
    }

    /// IE-069: the agent joins: every play that may still run on the customer's leg is stopped
    /// (music must never run over the agent), not only the one IE believes plays.
    fn stop_all_plays(&mut self, fx: &mut Vec<Effect>) {
        self.stop_media();
        let open = std::mem::take(&mut self.open_plays);
        if !open.is_empty() {
            fx.push(Effect::Count("plays_stopped_on_attach"));
        }
        for p in open {
            self.outbox.push_back(Cmd::StopPlay { play_id: p });
        }
    }

    /// Hold music, if the dial has some and nothing plays.
    fn start_music(&mut self) {
        let Some(d) = self.dial.as_ref() else {
            return;
        };
        if !d.queued() || d.media != Media::None || self.phase != Phase::Ivr {
            return;
        }
        let Some(prompt_id) = d.music.clone() else {
            return;
        };
        let play_id = self.ids.next_id();
        if let Some(d) = self.dial.as_mut() {
            d.media = Media::Music(play_id.clone());
        }
        self.queue(Cmd::Play {
            play_id,
            prompt_id,
            looped: true,
        });
    }

    /// Announces the latest position if it changed and the gap allows.
    fn maybe_announce(&mut self, fx: &mut Vec<Effect>) {
        let now = self.now;
        let Some(d) = self.dial.as_mut() else {
            return;
        };
        if !d.queued() || !d.announce || matches!(d.media, Media::Announcing(_)) {
            return;
        }
        let Some(pos) = d.position.filter(|p| d.announced != Some(*p) && *p > 0) else {
            return;
        };
        if d.announced_at.is_some_and(|t| now < t + ANNOUNCE_GAP_MS) {
            return;
        }
        if let Media::Music(p) = std::mem::replace(&mut d.media, Media::None) {
            self.outbox.push_back(Cmd::StopPlay { play_id: p });
        }
        let play_id = self.ids.next_id();
        if let Some(d) = self.dial.as_mut() {
            d.media = Media::Announcing(play_id.clone());
            d.announced = Some(pos);
            d.announced_at = Some(now);
        }
        self.queue(Cmd::Play {
            play_id,
            prompt_id: position_prompt(pos),
            looped: false,
        });
        fx.push(Effect::Event(CallEvent::PositionAnnounced(pos)));
    }

    /// Leaves the `dial`: the flow goes on with the next verb.
    fn leave_dial(&mut self) {
        self.stop_media();
        self.dial = None;
        if self.activity == Activity::Dialing {
            self.activity = Activity::Idle;
            self.idx += 1;
        }
    }

    /// The session ended: routing's cleanup for the `dial` (at most once).
    fn caller_gone(&mut self) {
        let reason = if self.summary.ended_by_ie.is_some() {
            DequeueReason::Ended
        } else {
            DequeueReason::Abandoned
        };
        let now = self.now;
        // An Enqueue not sent yet never goes out (no entry, nothing to dequeue).
        self.route_outbox
            .retain(|c| !matches!(c, RouteCmd::Enqueue { .. }));
        let Some(d) = self.dial.as_mut() else {
            return;
        };
        if d.left || d.overflow.is_some() {
            return;
        }
        d.left = true;
        d.media = Media::None;
        let connected = d
            .res
            .as_ref()
            .is_some_and(|r| r.connect == Connect::Recorded);
        if !connected {
            self.queue_left(if reason == DequeueReason::Abandoned {
                QueueOutcome::Abandoned
            } else {
                QueueOutcome::Ended
            });
        }
        let Some(d) = self.dial.as_mut() else {
            return;
        };
        match d.res.as_mut() {
            Some(r) if r.connect == Connect::Recorded => {
                let (agent_id, reservation_id) = (r.agent_id.clone(), r.id.clone());
                self.queue_route(RouteCmd::CallEnded {
                    agent_id,
                    reservation_id,
                    ended_ms: now,
                });
            }
            // CONNECTED is out and unanswered (IE-041): its answer decides.
            Some(r) if r.connect != Connect::NotSent => r.over = Some(reason),
            Some(_) => {
                // The agent's leg is still ringing: TC ends it with the session.
                if let Some(r) = d.res.take() {
                    self.report(r.id, AgentOutcome::CallerGone, "ended");
                }
                self.dequeue(reason);
            }
            // No entry yet: the Enqueue is in flight; its answer sends the Dequeue.
            None => self.dequeue(reason),
        }
    }

    /// Queues the dial's one `Dequeue`, once its entry is known.
    fn dequeue(&mut self, reason: DequeueReason) {
        let Some(d) = self.dial.as_mut() else {
            return;
        };
        if d.dequeued {
            return;
        }
        let Some(entry_id) = d.entry.clone() else {
            return;
        };
        d.dequeued = true;
        self.queue_route(RouteCmd::Dequeue { entry_id, reason });
    }

    fn input(&mut self, input: Input, fx: &mut Vec<Effect>) {
        if self.phase == Phase::Closing {
            // The session is over (its Finish is out): only routing's answers, late RE messages
            // and an unsettled CONNECTED's timers matter; a replayed end, a resync or a command
            // result changes nothing.
            match input {
                Input::Route(r) => self.route_done(r, fx),
                Input::Assigned {
                    reservation_id,
                    entry_id,
                    agent_id,
                    agent_contact,
                    deadline_ms,
                } => self.assigned(
                    reservation_id,
                    entry_id,
                    agent_id,
                    agent_contact,
                    deadline_ms,
                    fx,
                ),
                Input::Tick => self.connect_tick(fx),
                Input::Reattached(sync) => self.reattached(sync, fx),
                _ => {}
            }
            return;
        }
        match input {
            Input::Ended { reason, sip_status } => self.closed(reason, sip_status, fx),
            Input::Reconciled(r) => self.reconciled(r),
            Input::Resync { ended: true } => {
                // The Ended event, with its ack, follows.
                self.outbox.clear();
                self.collection = None;
                self.activity = Activity::Idle;
                self.phase = Phase::Ending;
            }
            Input::Resync { ended: false } => {}
            Input::Unmanaged => self.unmanaged = true,
            Input::Managed => {
                self.unmanaged = false;
                if self.phase == Phase::Ending && self.ending_since.is_some() {
                    // The stream is back: ask TC now (IE-056).
                    self.ending_next = self.now;
                }
            }
            Input::Claimed(epoch) if self.phase == Phase::Claiming => {
                self.claimed = true;
                self.summary.owner_epoch = epoch;
                self.phase = Phase::Answering;
                self.queue(Cmd::Answer);
            }
            Input::ClaimTaken if self.phase == Phase::Claiming => self.drop_call(),
            Input::ClaimUnavailable if self.phase == Phase::Claiming => {
                self.phase = Phase::Dropped;
                fx.push(Effect::Decline { sip_status: 503 });
            }
            Input::Done(r) => self.done_cmd(r, fx),
            Input::PlayFinished { play_id, completed } => {
                self.play_finished(&play_id, completed, fx)
            }
            Input::PlayFailed {
                play_id,
                prompt_unknown,
            } => {
                self.open_plays.retain(|p| *p != play_id);
                let r = if prompt_unknown {
                    CmdResult::PromptUnknown
                } else {
                    CmdResult::Failed
                };
                self.play_failed(&play_id, r, fx);
            }
            Input::Digit {
                collection_id,
                digit,
            } => self.digit(&collection_id, digit, fx),
            Input::Tick => self.tick(fx),
            Input::Claimed(_) | Input::ClaimTaken | Input::ClaimUnavailable => {}
            Input::Route(r) => self.route_done(r, fx),
            Input::Assigned {
                reservation_id,
                entry_id,
                agent_id,
                agent_contact,
                deadline_ms,
            } => self.assigned(
                reservation_id,
                entry_id,
                agent_id,
                agent_contact,
                deadline_ms,
                fx,
            ),
            Input::Revoked { reservation_id } => self.revoked(&reservation_id, fx),
            Input::Overflow { entry_id, action } => self.overflow(&entry_id, action, fx),
            Input::Position { entry_id, position } => {
                if self.dialing()
                    && let Some(d) = self.dial.as_mut()
                    && d.entry.as_deref().is_none_or(|e| e == entry_id)
                {
                    d.position = Some(position);
                    self.maybe_announce(fx);
                }
            }
            Input::AgentLeg { leg_id, leg } => self.agent_leg(&leg_id, leg, fx),
            Input::DtmfAction {
                action,
                result,
                recording,
                muted,
            } => self.dtmf_action(action, result, recording, muted, fx),
            Input::Reattached(sync) => self.reattached(sync, fx),
        }
    }

    /// IE-059: the new leader's view after a leader change. The call's media never change here.
    fn reattached(&mut self, sync: Option<RouteSync>, fx: &mut Vec<Effect>) {
        let Some(sync) = sync else {
            // Unanswered: what is unsent stays for the next leader change (or its hold).
            fx.push(Effect::Count("routing_reattach_failed"));
            return;
        };
        fx.push(Effect::Count("routing_reattached"));
        // Outcome RPCs that found RE unavailable: sent again (the first outcome wins in RE).
        for c in std::mem::take(&mut self.unsent) {
            fx.push(Effect::Count("routing_outcome_resent"));
            self.route_outbox.push_back(c);
        }
        self.unsent_since = None;
        self.unsent_retry_at = 0;
        let now = self.now;
        let enqueue_busy = matches!(self.route_awaiting, Some(RouteKind::Enqueue))
            || self
                .route_outbox
                .iter()
                .any(|c| matches!(c, RouteCmd::Enqueue { .. }));
        let live = self.dialing();
        let Some(d) = self.dial.as_mut() else {
            return;
        };
        // An unsettled CONNECTED goes out again now (IE-041's retry, early).
        if let Some(r) = d.res.as_mut()
            && let Connect::Retry { .. } = r.connect
        {
            r.connect = Connect::Retry { at: now };
        }
        if live && d.res.is_some() && sync.entry_id.is_none() && sync.reservations.is_empty() {
            // MF-1b: RE forgot the call while an agent was being reached: the reservation can't
            // connect (RE answers reservation_unknown); when it ends, the caller is enqueued again.
            d.forgotten = true;
            fx.push(Effect::Count("routing_forgotten_mid_offer"));
            return;
        }
        if !live || !d.queued() {
            return;
        }
        match (&sync.entry_id, d.entry.clone()) {
            // The new leader holds the entry (RE's state survives a leader change): the caller
            // keeps their place. A moved entry is followed.
            (Some(e), Some(mine)) if *e != mine => d.entry = Some(e.clone()),
            (Some(_), _) => {}
            // No entry and no reservation: enqueued again with the same seq and times, so RE
            // puts the caller back where they were (CONVENTIONS "Recovery").
            (None, Some(_)) if sync.reservations.is_empty() && !enqueue_busy => {
                if let Some(c) = self.last_enqueue.clone() {
                    fx.push(Effect::Count("routing_reenqueued"));
                    if let Some(d) = self.dial.as_mut() {
                        d.entry = None;
                    }
                    self.route_outbox.push_back(c);
                }
            }
            _ => {}
        }
    }

    fn play_finished(&mut self, play_id: &str, completed: bool, fx: &mut Vec<Effect>) {
        self.open_plays.retain(|p| p != play_id);
        if matches!(&self.activity, Activity::Playing { play_id: p } if p == play_id) {
            self.play_failures = 0;
            if self.source == Source::Consent && self.phase == Phase::Consent {
                if !completed {
                    // Not heard to its end: no recording; the flow starts.
                    fx.push(Effect::Count("consent_not_completed"));
                    self.close_collection();
                    self.activity = Activity::Idle;
                    self.decide(Decision::Refused, ConsentBasis::NotAnnounced, fx);
                    self.enter_start();
                    return;
                }
                self.consent_heard += 1;
            }
            self.activity = Activity::Idle;
            self.idx += 1;
            return;
        }
        let Some(d) = self.dial.as_mut() else {
            return;
        };
        match &d.media {
            Media::Announcing(p) if p == play_id => {
                d.media = Media::None;
                self.start_music();
                self.maybe_announce(fx);
            }
            // The music ended by itself (a loop never does): no restart, so no loop of plays.
            Media::Music(p) if p == play_id => {
                d.media = Media::None;
                fx.push(Effect::Count("hold_music_ended"));
            }
            _ => {}
        }
    }

    /// Whether the call is live in a `dial` that can still take an agent.
    fn dialing(&self) -> bool {
        self.phase == Phase::Ivr
            && self.activity == Activity::Dialing
            && self
                .dial
                .as_ref()
                .is_some_and(|d| !d.left && d.overflow.is_none())
    }

    fn route_done(&mut self, r: RouteResult, fx: &mut Vec<Effect>) {
        let Some(kind) = self.route_awaiting.take() else {
            return;
        };
        let sent = self.route_sent.take();
        if r == RouteResult::Unavailable
            && matches!(
                kind,
                RouteKind::Dequeue | RouteKind::CallEnded | RouteKind::Outcome(..)
            )
            && !matches!(kind, RouteKind::Outcome(AgentOutcome::Connected, _))
            && let Some(c) = sent
        {
            // IE-059: retried for 15 s already; kept for the next leader change's Reattach.
            fx.push(Effect::Count("routing_outcome_unsent"));
            self.unsent.push(c);
            self.unsent_since.get_or_insert(self.now);
            if self.unsent_retry_at == 0 {
                self.unsent_retry_at = self.now.saturating_add(self.unsent_gap);
            }
        }
        if r == RouteResult::Fenced {
            // A newer owner has the interaction in RE: this replica stops acting on it.
            fx.push(Effect::Count("routing_fenced"));
            self.route_outbox.clear();
            if matches!(self.phase, Phase::Closing) {
                self.phase = Phase::Ended;
            } else {
                self.drop_call();
            }
            return;
        }
        match kind {
            RouteKind::Enqueue => match r {
                RouteResult::Enqueued { entry_id, position } => {
                    let reason = if self.summary.ended_by_ie.is_some() {
                        DequeueReason::Ended
                    } else {
                        DequeueReason::Abandoned
                    };
                    if let Some(d) = self.dial.as_mut() {
                        let e = d.entry.get_or_insert(entry_id).clone();
                        // The caller left while the Enqueue was in flight.
                        if d.left && d.overflow.is_none() {
                            self.dequeue(reason);
                        } else {
                            // The first position comes with the answer: a caller alone in the
                            // queue hears it too (RE sends a PositionUpdate only on a change).
                            if position > 0 && d.position.is_none() {
                                d.position = Some(position);
                            }
                            if self.summary.entry_id != e {
                                self.summary.entry_id = e;
                                self.progress(fx);
                            }
                            self.maybe_announce(fx);
                        }
                    }
                }
                RouteResult::Unavailable => {
                    fx.push(Effect::Count("queue_unavailable"));
                    if self.dialing() {
                        self.queue_left(QueueOutcome::Unavailable);
                        self.leave_dial();
                    }
                }
                _ => {
                    fx.push(Effect::Count("queue_refused"));
                    if self.dialing() {
                        self.queue_left(QueueOutcome::Refused);
                        self.leave_dial();
                    }
                }
            },
            RouteKind::Dequeue => {
                let action = self.dial.as_mut().and_then(|d| d.overflow.take());
                if let Some(action) = action {
                    self.run_overflow(action, fx);
                }
            }
            RouteKind::Outcome(AgentOutcome::Connected, id) => self.connect_answer(&id, r, fx),
            RouteKind::Outcome(..) | RouteKind::CallEnded => {}
        }
    }

    /// The live reservation `id` whose CONNECTED is awaited, if any.
    fn pending_res(&mut self, id: &str) -> Option<&mut Res> {
        self.dial
            .as_mut()
            .and_then(|d| d.res.as_mut())
            .filter(|r| r.id == id && !matches!(r.connect, Connect::NotSent | Connect::Recorded))
    }

    /// IE-041: the answer to `ReportOutcome(CONNECTED)`.
    fn connect_answer(&mut self, id: &str, r: RouteResult, fx: &mut Vec<Effect>) {
        let now = self.now;
        let Some(res) = self.pending_res(id) else {
            return;
        };
        match r {
            RouteResult::Recorded { connected: true } | RouteResult::Done => {
                res.connect = Connect::Recorded;
                let (agent, rid, over) = (res.agent_id.clone(), res.id.clone(), res.over);
                fx.push(Effect::Count("agent_connected"));
                fx.push(Effect::Event(CallEvent::AgentConnected));
                self.summary.agent_id = agent.clone();
                self.summary.reservation_id = rid.clone();
                self.summary.connected_ms = Some(now);
                self.queue_left(QueueOutcome::Connected);
                self.summary.queue_outcome = QueueOutcome::Connected;
                self.progress(fx);
                if over.is_some() {
                    // The conversation is already over: the agent's wrap-up starts.
                    self.queue_route(RouteCmd::CallEnded {
                        agent_id: agent,
                        reservation_id: rid,
                        ended_ms: now,
                    });
                    if self.dialing() {
                        self.leave_dial();
                    }
                }
            }
            RouteResult::Unavailable if now < res.give_up_at => {
                // RE didn't hear: send it again (the same reservation; RE keeps the first).
                let gap = if res.retry_gap == 0 {
                    CONNECT_RETRY_MS
                } else {
                    (res.retry_gap * 2).min(CONNECT_RETRY_MAX_MS)
                };
                res.retry_gap = gap;
                res.connect = Connect::Retry { at: now + gap };
                fx.push(Effect::Count("connect_retry"));
            }
            RouteResult::Unavailable => {
                fx.push(Effect::Count("connect_given_up"));
                self.not_connected(fx);
            }
            // EXPIRED or REVOKED first (L9), or refused: the agent was freed.
            _ => {
                fx.push(Effect::Count("connected_too_late"));
                self.not_connected(fx);
            }
        }
    }

    /// The reservation didn't connect after its leg answered (L9, or IE-041's give-up): RE has
    /// put the entry back (or will). Hang up the agent's leg and wait again; if the conversation
    /// is over, take the entry out.
    fn not_connected(&mut self, fx: &mut Vec<Effect>) {
        let Some(res) = self.dial.as_mut().and_then(|d| d.res.take()) else {
            return;
        };
        self.reported.insert(res.id.clone());
        match res.over {
            Some(reason) => {
                self.dequeue(reason);
                if self.dialing() {
                    self.queue_left(QueueOutcome::Ended);
                    self.leave_dial();
                }
            }
            None => {
                self.queue(Cmd::RemoveLeg { leg_id: res.leg_id });
                self.start_music();
                self.maybe_announce(fx);
            }
        }
    }

    /// IE-041's timers: a `CONNECTED` retry, and its give-up.
    fn connect_tick(&mut self, fx: &mut Vec<Effect>) {
        let now = self.now;
        let Some(res) = self.dial.as_mut().and_then(|d| d.res.as_mut()) else {
            return;
        };
        match res.connect {
            Connect::Retry { .. } | Connect::InFlight if now >= res.give_up_at => {
                if res.connect == Connect::InFlight {
                    // Its answer is still out; it is given up when it comes (Unavailable).
                    return;
                }
                fx.push(Effect::Count("connect_given_up"));
                self.not_connected(fx);
            }
            Connect::Retry { at, .. } if now >= at => {
                res.connect = Connect::InFlight;
                let id = res.id.clone();
                self.queue_route(RouteCmd::ReportOutcome {
                    reservation_id: id,
                    outcome: AgentOutcome::Connected,
                    detail: "",
                });
            }
            _ => {}
        }
    }

    fn assigned(
        &mut self,
        reservation_id: String,
        entry_id: String,
        agent_id: String,
        agent_contact: String,
        deadline_ms: u64,
        fx: &mut Vec<Effect>,
    ) {
        if self.reported.contains(&reservation_id) {
            return;
        }
        let live = self.dialing();
        let now = self.now;
        let record_locked = !self.recording;
        let Some(d) = self
            .dial
            .as_mut()
            .filter(|d| live && d.entry.as_ref().is_none_or(|e| *e == entry_id))
        else {
            // Not waiting for an agent: the caller left (abandon vs assign), or the flow moved on.
            let detail = if matches!(self.phase, Phase::Closing | Phase::Ending) {
                "ended"
            } else {
                "not_queued"
            };
            fx.push(Effect::Count("assigned_not_queued"));
            self.report(reservation_id, AgentOutcome::CallerGone, detail);
            return;
        };
        match &d.res {
            None => {
                d.entry.get_or_insert(entry_id);
                let leg_id = self.ids.next_id();
                let give_up_at = if deadline_ms > 0 {
                    deadline_ms.saturating_add(CONNECT_GIVE_UP_AFTER_DEADLINE_MS)
                } else {
                    now.saturating_add(CONNECT_GIVE_UP_NO_DEADLINE_MS)
                };
                if let Some(d) = self.dial.as_mut() {
                    d.res = Some(Res {
                        id: reservation_id,
                        agent_id,
                        leg_id: leg_id.clone(),
                        leg_deadline: now.saturating_add(AGENT_LEG_ANSWER_MS),
                        answered: false,
                        connect: Connect::NotSent,
                        retry_gap: 0,
                        give_up_at,
                        over: None,
                    });
                }
                self.summary.offers += 1;
                self.legs.insert(leg_id.clone());
                // The caller keeps the music while the agent's leg rings.
                self.queue(Cmd::AddAgentLeg {
                    leg_id,
                    agent_contact,
                    record_locked,
                });
            }
            Some(r) if r.id == reservation_id => {}
            Some(_) => {
                fx.push(Effect::Count("already_assigned"));
                self.report(
                    reservation_id,
                    AgentOutcome::AlreadyAssigned,
                    "already_assigned",
                );
            }
        }
    }

    fn revoked(&mut self, reservation_id: &str, fx: &mut Vec<Effect>) {
        let Some(d) = self.dial.as_mut() else {
            return;
        };
        let ours = d
            .res
            .as_ref()
            .is_some_and(|r| r.id == reservation_id && r.connect != Connect::Recorded);
        if !ours {
            return;
        }
        // RE recorded EXPIRED or REVOKED; the entry is back at its ordering time (L7/L8).
        fx.push(Effect::Count("reservation_revoked"));
        let unsent = d
            .res
            .as_ref()
            .is_some_and(|r| r.connect == Connect::NotSent);
        if unsent {
            if let Some(r) = d.res.take() {
                self.reported.insert(r.id);
                self.queue(Cmd::RemoveLeg { leg_id: r.leg_id });
            }
            self.start_music();
            self.maybe_announce(fx);
        } else {
            self.not_connected(fx);
        }
    }

    fn overflow(&mut self, entry_id: &str, action: OverflowAction, fx: &mut Vec<Effect>) {
        if !self.dialing() {
            return;
        }
        let Some(d) = self.dial.as_mut() else {
            return;
        };
        if d.entry.as_deref() != Some(entry_id) || d.res.is_some() {
            fx.push(Effect::Count("overflow_ignored"));
            return;
        }
        match action {
            OverflowAction::Queue | OverflowAction::CallbackOffer => {
                fx.push(Effect::Count("overflow_kept"));
            }
            action => {
                // L13: Dequeue(OVERFLOW) first, then the action (on its answer). The entry is
                // leaving: the caller hanging up now sends nothing more.
                d.overflow = Some(action);
                d.left = true;
                self.queue_left(QueueOutcome::Overflow);
                self.dequeue(DequeueReason::Overflow);
            }
        }
    }

    fn run_overflow(&mut self, action: OverflowAction, fx: &mut Vec<Effect>) {
        if self.phase != Phase::Ivr {
            return;
        }
        fx.push(Effect::Count("overflow"));
        self.leave_dial();
        match action {
            OverflowAction::Disconnect => self.end(EndCause::Normal),
            OverflowAction::FlowStep(step) => {
                if self
                    .route()
                    .is_some_and(|r| r.flow.steps.contains_key(&step))
                {
                    self.enter_step(step);
                } else {
                    fx.push(Effect::Count("overflow_step_unknown"));
                }
            }
            // Voicemail is step 12: the flow goes on after the dial.
            OverflowAction::Voicemail => fx.push(Effect::Count("voicemail_unsupported")),
            OverflowAction::Queue | OverflowAction::CallbackOffer => {}
        }
    }

    fn agent_leg(&mut self, leg_id: &str, leg: AgentLeg, fx: &mut Vec<Effect>) {
        if !self.dialing() {
            return;
        }
        let now = self.now;
        let recording = self.recording && self.route().is_some_and(|r| r.recording);
        let Some(d) = self.dial.as_mut() else {
            return;
        };
        let Some(r) = d.res.as_mut().filter(|r| r.leg_id == leg_id) else {
            return;
        };
        let id = r.id.clone();
        match leg {
            AgentLeg::Answered if !r.answered => {
                r.answered = true;
                r.connect = Connect::InFlight;
                let leg = r.leg_id.clone();
                // TC bridged the two legs: the waiting media stops (IE-069: every play that may
                // still run); the agent's leg records.
                self.stop_all_plays(fx);
                if recording {
                    let recording_id = self.ids.next_id();
                    self.queue(Cmd::StartRecording {
                        agent_leg: Some(leg),
                        recording_id,
                    });
                }
                self.queue_route(RouteCmd::ReportOutcome {
                    reservation_id: id,
                    outcome: AgentOutcome::Connected,
                    detail: "",
                });
            }
            AgentLeg::Answered => {}
            AgentLeg::NoAnswer | AgentLeg::Failed if !r.answered => {
                // L3/L4: RE puts the entry back at its ordering time; no new Enqueue. TC ended
                // the leg itself.
                d.res = None;
                let (outcome, detail) = if leg == AgentLeg::NoAnswer {
                    (AgentOutcome::AgentNoAnswer, "timeout")
                } else {
                    (AgentOutcome::AgentLegFailed, "leg_failed")
                };
                self.report(id, outcome, detail);
                self.start_music();
                self.maybe_announce(fx);
            }
            // The agent hung up (or the browser went) after answering: the conversation is over.
            AgentLeg::Ended | AgentLeg::NoAnswer | AgentLeg::Failed => match r.connect {
                Connect::Recorded => {
                    let agent = r.agent_id.clone();
                    self.queue_route(RouteCmd::CallEnded {
                        agent_id: agent,
                        reservation_id: id,
                        ended_ms: now,
                    });
                    fx.push(Effect::Count("agent_hung_up"));
                    self.leave_dial();
                }
                // CONNECTED unanswered (IE-041): its answer decides; the flow waits for it.
                _ => r.over = Some(DequeueReason::Ended),
            },
        }
    }

    fn dtmf_action(
        &mut self,
        action: DtmfKind,
        result: DtmfResult,
        recording: RecState,
        muted: bool,
        fx: &mut Vec<Effect>,
    ) {
        if result != DtmfResult::Applied {
            fx.push(Effect::Count(match result {
                DtmfResult::Noop => "dtmf_action_noop",
                DtmfResult::Locked => "dtmf_action_locked",
                _ => "dtmf_action_refused",
            }));
            return;
        }
        match action {
            DtmfKind::RecordingPause | DtmfKind::RecordingResume | DtmfKind::RecordingToggle => {
                match recording {
                    RecState::Paused => {
                        self.summary.recording_pauses += 1;
                        fx.push(Effect::Event(CallEvent::RecordingPaused));
                    }
                    RecState::Active => fx.push(Effect::Event(CallEvent::RecordingResumed)),
                    RecState::Stopped | RecState::Unknown => {
                        fx.push(Effect::Count("dtmf_action_unexpected"));
                    }
                }
            }
            DtmfKind::Mute | DtmfKind::Unmute | DtmfKind::MuteToggle => {
                if muted {
                    self.summary.mutes += 1;
                    fx.push(Effect::Event(CallEvent::Muted));
                } else {
                    fx.push(Effect::Event(CallEvent::Unmuted));
                }
            }
            DtmfKind::Other => fx.push(Effect::Count("dtmf_action_other")),
        }
    }

    /// The session is over: the ENDED record and the CDR (`Finish`) if this replica claimed it.
    fn closed(&mut self, reason: String, sip_status: u32, fx: &mut Vec<Effect>) {
        self.outbox.clear();
        if self.claimed {
            self.phase = Phase::Closing;
            self.summary.ended_ms = self.now;
            self.summary.end_reason = reason;
            self.summary.sip_status = sip_status;
            if let Source::Step(s) = &self.source {
                self.summary.last_step = s.clone();
            }
            self.activity = Activity::Idle;
            self.caller_gone();
            fx.push(Effect::Finish(Box::new(self.summary.clone())));
        } else {
            self.phase = Phase::Dropped;
        }
    }

    /// IE-050/IE-056: `Ending` past its deadline: ask TC again, the wait doubling up to
    /// [`ENDING_MAX_MS`]. The call never closes itself: only TC's end (an event, the tombstone, a
    /// session TC lost) or the node's death (`NodeDead`) closes it. Past [`ENDING_STUCK_MS`] it is
    /// reported stuck, once.
    fn ending_tick(&mut self, fx: &mut Vec<Effect>) {
        let Some(since) = self.ending_since else {
            return;
        };
        if self.now < self.ending_next || self.awaiting.is_some() {
            return;
        }
        if !self.ending_stuck && self.now.saturating_sub(since) >= ENDING_STUCK_MS {
            self.ending_stuck = true;
            fx.push(Effect::Count("ending_stuck"));
        }
        fx.push(Effect::Reconcile);
        self.ending_gap = self.ending_gap.saturating_mul(2).min(ENDING_MAX_MS);
        self.ending_next = self.now.saturating_add(self.ending_gap);
    }

    fn reconciled(&mut self, r: Reconcile) {
        if self.phase != Phase::Ending {
            return;
        }
        match r {
            // Ended: its end follows through the mailbox. Unreachable: a partition, not death;
            // the call and its owner key wait (the next ask, the stream's return, or `NodeDead`).
            Reconcile::Ended | Reconcile::Unreachable => {}
            // Still live at TC: end it again (each ask, so with the same capped backoff).
            Reconcile::Live => {
                if self.awaiting.is_none() && self.outbox.is_empty() {
                    let cause = *self.summary.ended_by_ie.get_or_insert(EndCause::System);
                    self.queue(Cmd::EndSession(cause));
                }
            }
            Reconcile::Fenced => self.drop_call(),
        }
    }

    fn done_cmd(&mut self, r: CmdResult, fx: &mut Vec<Effect>) {
        let Some(kind) = self.awaiting.take() else {
            return;
        };
        match (kind, r) {
            (_, CmdResult::Fenced) => self.drop_call(),
            (_, CmdResult::Gone) => {
                self.outbox.clear();
                self.activity = Activity::Idle;
                self.collection = None;
                self.phase = Phase::Ending;
            }
            (Kind::Answer, CmdResult::Done) => {
                self.summary.answered_ms = Some(self.now);
                // The CDR's row opens: a crash from here on still leaves the call's record,
                // closed by its hangup (DEMO step 5).
                self.progress(fx);
                self.start_consent(fx);
            }
            // IE-068: a failed or missing prompt never ends the call.
            (Kind::Play(p), CmdResult::PromptUnknown | CmdResult::Failed) => {
                self.play_failed(&p, r, fx);
            }
            // The originate started: its LegUpdates follow.
            (Kind::AddLeg(_), CmdResult::Done) => {}
            // The originate didn't start: the leg failed.
            (Kind::AddLeg(leg), _) => {
                fx.push(Effect::Count("agent_leg_refused"));
                self.agent_leg(&leg, AgentLeg::Failed, fx);
            }
            (Kind::StartRecording(agent), CmdResult::Done) => {
                self.summary.recorded_legs += 1;
                fx.push(Effect::Event(CallEvent::RecordingStarted(agent)));
            }
            // Refused (no sink on the node: `invalid_state`; or anything else): reported once,
            // never ends or stalls the call (the coordinator's rule, 2026-10-31).
            (Kind::StartRecording(agent), _) => {
                self.summary.recording_failures += 1;
                fx.push(Effect::Count("recording_refused"));
                fx.push(Effect::Event(CallEvent::RecordingFailed(agent)));
            }
            // A stop, a close or a hang-up that failed changes nothing (the play, the collection
            // or the leg is over, or the session's end takes it).
            (Kind::StopPlay | Kind::DigitsOff | Kind::RemoveLeg, _) => {}
            (Kind::EndSession, _) => {}
            (_, CmdResult::Done) => {}
            (_, CmdResult::Failed | CmdResult::PromptUnknown) => self.end(EndCause::System),
        }
    }

    /// IE-068: a `Play` that failed (`Failed`: parity-ms's prompt wait, TC's timeout) or whose
    /// prompt parity-ms doesn't have. The flow's own prompt is tried once more after a failure,
    /// then skipped; a consent announcement that can't be played means no recording and the
    /// flow's start. Hold music or an announcement that fails never ends a waiting call.
    fn play_failed(&mut self, p: &str, r: CmdResult, fx: &mut Vec<Effect>) {
        if matches!(&self.activity, Activity::Playing { play_id } if play_id == p) {
            if r == CmdResult::Failed && self.play_failures + 1 < PLAY_TRIES {
                self.play_failures += 1;
                fx.push(Effect::Count("prompt_retried"));
                // Idle at the same verb: the runner plays it again (a new play id).
                self.activity = Activity::Idle;
                return;
            }
            self.play_failures = 0;
            fx.push(Effect::Count(if r == CmdResult::PromptUnknown {
                "prompt_unknown"
            } else {
                "prompt_failed_skipped"
            }));
            if self.source == Source::Consent {
                // The caller wasn't told: never recorded. The call goes on to the flow.
                fx.push(Effect::Count("consent_not_announced"));
                self.close_collection();
                self.activity = Activity::Idle;
                self.decide(Decision::Refused, ConsentBasis::NotAnnounced, fx);
                self.enter_start();
            } else {
                self.activity = Activity::Idle;
                self.idx += 1;
            }
            return;
        }
        let name = if r == CmdResult::PromptUnknown {
            "prompt_unknown"
        } else {
            "queue_media_failed"
        };
        if let Some(d) = self.dial.as_mut() {
            match &d.media {
                Media::Announcing(a) if *a == p => {
                    fx.push(Effect::Count(name));
                    d.media = Media::None;
                    self.start_music();
                }
                Media::Music(m) if *m == p => {
                    fx.push(Effect::Count(name));
                    d.media = Media::None;
                }
                _ => fx.push(Effect::Count("stale_play_failed")),
            }
        } else {
            // A play the flow has moved past (a barge-in stopped it): nothing to do.
            fx.push(Effect::Count("stale_play_failed"));
        }
    }

    fn start_consent(&mut self, fx: &mut Vec<Effect>) {
        let mode = self.route().map(|r| r.consent.mode).unwrap_or_default();
        if mode == ConsentMode::Never {
            self.decide(Decision::Refused, ConsentBasis::Never, fx);
            self.enter_start();
        } else {
            self.phase = Phase::Consent;
            self.source = Source::Consent;
            self.idx = 0;
            self.entry_verbs = 0;
        }
    }

    fn decide(&mut self, d: Decision, basis: ConsentBasis, fx: &mut Vec<Effect>) {
        if self.summary.consent.is_some() {
            return;
        }
        // The legal fact: granted only once every announcement prompt played to its end.
        let (d, basis) = if d == Decision::Granted && self.consent_heard < self.consent_prompts() {
            fx.push(Effect::Count("consent_not_completed"));
            (Decision::Refused, ConsentBasis::NotAnnounced)
        } else {
            (d, basis)
        };
        self.summary.consent = Some((d, basis));
        fx.push(Effect::ConsentDecided(d, basis));
        if d == Decision::Granted && self.route().is_some_and(|r| r.consent.record) {
            self.recording = true;
            if self.route().is_some_and(|r| r.recording) {
                // The customer's leg records from here (a null sink until step 9, MS-012).
                let recording_id = self.ids.next_id();
                self.queue(Cmd::StartRecording {
                    agent_leg: None,
                    recording_id,
                });
            } else {
                fx.push(Effect::StartRecording);
            }
        }
    }

    /// How many prompts the consent announcement has (its verbs but the closing gather).
    fn consent_prompts(&self) -> usize {
        self.consent_verbs
            .iter()
            .filter(|v| matches!(v, Verb::Say(_) | Verb::Play(_)))
            .count()
    }

    fn enter_start(&mut self) {
        self.phase = Phase::Ivr;
        let start = self
            .route()
            .map(|r| r.flow.start.clone())
            .unwrap_or_default();
        self.enter_step(start);
    }

    fn enter_step(&mut self, step: String) {
        self.play_failures = 0;
        self.summary.last_step = step.clone();
        self.source = Source::Step(step);
        self.idx = 0;
        self.entry_verbs = 0;
        self.activity = Activity::Idle;
    }

    fn start_no_input(&mut self, fx: &mut Vec<Effect>) {
        fx.push(Effect::Count("no_input_action"));
        self.close_collection();
        self.activity = Activity::Idle;
        self.source = Source::NoInput;
        self.idx = 0;
        self.entry_verbs = 0;
    }

    fn verbs(&self) -> &[Verb] {
        match &self.source {
            Source::Consent => &self.consent_verbs,
            Source::NoInput => self.route().map_or(&[][..], |r| &r.flow.no_input),
            Source::Step(s) => self
                .route()
                .and_then(|r| r.flow.steps.get(s))
                .map_or(&[][..], Vec::as_slice),
        }
    }

    fn close_collection(&mut self) {
        if let Some(c) = self.collection.take() {
            self.queue(Cmd::DigitsOff {
                collection_id: c.id,
            });
        }
    }

    /// Opens a collection for the gather at `gather_idx` unless one is open.
    fn open_collection(&mut self, gather_idx: usize) {
        if self.collection.is_none() {
            let id = self.ids.next_id();
            self.queue(Cmd::DigitsOn {
                collection_id: id.clone(),
                max_duration_s: COLLECTION_MAX_S,
            });
            self.collection = Some(Collection {
                id,
                gather_idx,
                buffer: String::new(),
            });
        }
    }

    /// The gather a run of prompts starting at `i` leads into, if any (barge-in).
    fn gather_after_prompts(&self, i: usize) -> Option<usize> {
        let verbs = self.verbs();
        let mut j = i;
        while matches!(verbs.get(j), Some(Verb::Say(_) | Verb::Play(_))) {
            j += 1;
        }
        matches!(verbs.get(j), Some(Verb::Gather(_))).then_some(j)
    }

    fn goto_allowed(&mut self) -> bool {
        while self
            .gotos
            .front()
            .is_some_and(|t| self.now.saturating_sub(*t) >= 60_000)
        {
            self.gotos.pop_front();
        }
        if self.gotos.len() >= MAX_GOTOS_PER_MINUTE {
            return false;
        }
        self.gotos.push_back(self.now);
        true
    }

    /// Runs verbs until one waits (a prompt, a pause, a gather, a queue) or the call ends.
    fn run(&mut self, fx: &mut Vec<Effect>) {
        loop {
            if !matches!(self.phase, Phase::Consent | Phase::Ivr) || self.activity != Activity::Idle
            {
                return;
            }
            let Some(verb) = self.verbs().get(self.idx).cloned() else {
                // A step's (or no-input's) end hangs up.
                self.end(EndCause::Normal);
                return;
            };
            self.entry_verbs += 1;
            if self.entry_verbs > MAX_VERBS_PER_ENTRY {
                fx.push(Effect::Count("verb_limit"));
                if self.source == Source::NoInput {
                    self.end(EndCause::Normal);
                    return;
                }
                self.start_no_input(fx);
                continue;
            }
            match verb {
                Verb::Say(_) | Verb::Play(_) => {
                    let prompt_id = match &verb {
                        Verb::Say(s) => say_prompt_id(&s.language, &s.voice, &s.text),
                        Verb::Play(p) => p.prompt.clone(),
                        _ => String::new(),
                    };
                    if let Some(g) = self.gather_after_prompts(self.idx) {
                        self.open_collection(g);
                    }
                    let play_id = self.ids.next_id();
                    self.queue(Cmd::Play {
                        play_id: play_id.clone(),
                        prompt_id,
                        looped: false,
                    });
                    self.activity = Activity::Playing { play_id };
                    return;
                }
                Verb::Pause(p) => {
                    self.activity = Activity::Pausing {
                        until: self.now + u64::from(p.seconds) * 1000,
                    };
                    return;
                }
                Verb::Gather(g) => {
                    self.open_collection(self.idx);
                    let buffered = self
                        .collection
                        .as_mut()
                        .map(|c| std::mem::take(&mut c.buffer))
                        .unwrap_or_default();
                    if self.source != Source::Consent {
                        self.summary.gathers += 1;
                    }
                    self.activity = Activity::Gathering {
                        deadline: self.now + u64::from(g.timeout_s) * 1000,
                        spec: g,
                        digits: String::new(),
                    };
                    for d in buffered.chars() {
                        if !matches!(self.activity, Activity::Gathering { .. }) {
                            break;
                        }
                        self.gather_digit(d, fx);
                    }
                    if matches!(self.activity, Activity::Gathering { .. }) {
                        return;
                    }
                }
                Verb::Goto(g) => {
                    if self.goto_allowed() {
                        self.enter_step(g.step);
                    } else {
                        fx.push(Effect::Count("goto_limit"));
                        self.start_no_input(fx);
                    }
                }
                Verb::Hangup(_) => {
                    self.end(EndCause::Normal);
                    return;
                }
                Verb::Dial(d) => {
                    let routing = self.route().is_some_and(|r| r.routing);
                    match d.queue {
                        Some(queue_id) if routing => {
                            self.enqueue_seq += 1;
                            let first = *self.first_queued.get_or_insert(self.now);
                            let (ordering_ms, reason) = if self.enqueue_seq == 1 {
                                (first, EnqueueReason::New)
                            } else {
                                (self.now, EnqueueReason::Transfer)
                            };
                            self.summary.queue_id = queue_id.clone();
                            self.summary.queued_ms.get_or_insert(self.now);
                            self.summary.queue_outcome = QueueOutcome::Waiting;
                            self.summary.dequeued_ms = None;
                            self.queue_route(RouteCmd::Enqueue {
                                enqueue_seq: self.enqueue_seq,
                                queue_id,
                                priority: d.priority,
                                first_queued_ms: first,
                                ordering_ms,
                                reason,
                            });
                            self.dial = Some(Dialing {
                                entry: None,
                                res: None,
                                overflow: None,
                                left: false,
                                dequeued: false,
                                music: d.music.clone(),
                                announce: d.announce_position,
                                media: Media::None,
                                position: None,
                                announced: None,
                                announced_at: None,
                                forgotten: false,
                            });
                            self.activity = Activity::Dialing;
                            self.start_music();
                            self.progress(fx);
                            return;
                        }
                        _ => {
                            fx.push(Effect::Count("verb_unsupported"));
                            self.idx += 1;
                        }
                    }
                }
            }
        }
    }

    fn digit(&mut self, collection_id: &str, d: char, fx: &mut Vec<Effect>) {
        let ours = self
            .collection
            .as_ref()
            .is_some_and(|c| c.id == collection_id);
        if !ours || !matches!(self.phase, Phase::Consent | Phase::Ivr) {
            fx.push(Effect::Count("digit_dropped"));
            return;
        }
        match self.activity.clone() {
            Activity::Gathering { .. } => self.gather_digit(d, fx),
            Activity::Playing { .. } if self.source == Source::Consent => {
                // No barge-in on the consent announcement: it must be heard to its end; the
                // digit waits for the window (the decision).
                if let Some(c) = self.collection.as_mut() {
                    c.buffer.push(d);
                }
            }
            Activity::Playing { play_id } => {
                // Barge-in: stop the prompt; the digit counts toward the gather.
                self.play_failures = 0;
                if let Some(c) = self.collection.as_mut() {
                    c.buffer.push(d);
                    self.idx = c.gather_idx;
                }
                self.queue(Cmd::StopPlay { play_id });
                self.activity = Activity::Idle;
            }
            Activity::Pausing { .. } | Activity::Idle | Activity::Dialing => {
                if let Some(c) = self.collection.as_mut() {
                    c.buffer.push(d);
                }
            }
        }
    }

    fn gather_digit(&mut self, d: char, fx: &mut Vec<Effect>) {
        let Activity::Gathering { spec, digits, .. } = &mut self.activity else {
            return;
        };
        if !spec.finish_on_key.is_empty() && spec.finish_on_key.starts_with(d) {
            let got = std::mem::take(digits);
            self.complete(Some(got), fx);
            return;
        }
        digits.push(d);
        if digits.len() >= spec.num_digits as usize {
            let got = std::mem::take(digits);
            self.complete(Some(got), fx);
        } else if let Activity::Gathering { spec, deadline, .. } = &mut self.activity {
            *deadline = self.now + u64::from(spec.inter_digit_s) * 1000;
        }
    }

    /// A gather ends: `Some(digits)` (maybe empty: only the finish key) or `None` (no input).
    fn complete(&mut self, got: Option<String>, fx: &mut Vec<Effect>) {
        let Activity::Gathering { spec, .. } =
            std::mem::replace(&mut self.activity, Activity::Idle)
        else {
            return;
        };
        self.close_collection();
        if self.source == Source::Consent {
            let mode = self.route().map(|r| r.consent.mode).unwrap_or_default();
            let digit = self
                .route()
                .map(|r| r.consent.digit.clone())
                .unwrap_or_default();
            let pressed = got.as_deref().is_some_and(|g| !g.is_empty() && g == digit);
            let (d, b) = match (mode, pressed) {
                (ConsentMode::OptIn, true) => (Decision::Granted, ConsentBasis::OptedIn),
                (ConsentMode::OptIn, false) => (Decision::Refused, ConsentBasis::NoOptIn),
                (_, true) => (Decision::Refused, ConsentBasis::OptedOut),
                (_, false) => (Decision::Granted, ConsentBasis::NotOptedOut),
            };
            self.decide(d, b, fx);
            self.enter_start();
            return;
        }
        let got = match got {
            Some(g) if !g.is_empty() => g,
            Some(_) | None => {
                self.no_input_run += 1;
                self.summary.no_inputs += 1;
                let to = if self.no_input_run >= MAX_NO_INPUT {
                    "no_input".to_owned()
                } else {
                    spec.default.clone().unwrap_or_default()
                };
                fx.push(Effect::Gathered {
                    to: to.clone(),
                    digits: 0,
                });
                if self.no_input_run >= MAX_NO_INPUT {
                    self.start_no_input(fx);
                } else if !to.is_empty() {
                    self.enter_step(to);
                } else {
                    self.idx += 1;
                }
                return;
            }
        };
        self.no_input_run = 0;
        // The digits pick a route here and go no further.
        let to = spec
            .routes
            .get(&got)
            .cloned()
            .or_else(|| spec.default.clone())
            .unwrap_or_default();
        fx.push(Effect::Gathered {
            to: to.clone(),
            digits: u32::try_from(got.len()).unwrap_or(u32::MAX),
        });
        if to.is_empty() {
            self.idx += 1;
        } else {
            self.enter_step(to);
        }
    }

    fn tick(&mut self, fx: &mut Vec<Effect>) {
        if self.phase == Phase::Ending {
            self.ending_tick(fx);
            return;
        }
        if self.unmanaged {
            return;
        }
        if matches!(self.phase, Phase::Answering | Phase::Consent | Phase::Ivr)
            && self.now >= self.summary.started_ms.saturating_add(MAX_CALL_MS)
        {
            fx.push(Effect::Count("max_call_duration"));
            self.end(EndCause::System);
            return;
        }
        match &self.activity {
            Activity::Pausing { until } if self.now >= *until => {
                self.activity = Activity::Idle;
                self.idx += 1;
            }
            Activity::Gathering {
                deadline, digits, ..
            } if self.now >= *deadline => {
                let got = (!digits.is_empty()).then(|| digits.clone());
                self.complete(got, fx);
            }
            Activity::Dialing => {
                let due = self
                    .dial
                    .as_ref()
                    .and_then(|d| d.res.as_ref())
                    .filter(|r| !r.answered && self.now >= r.leg_deadline)
                    .map(|r| (r.id.clone(), r.leg_id.clone()));
                if let Some((id, leg)) = due {
                    // The leg didn't answer in time: hang it up (TC may still be ringing it).
                    if let Some(d) = self.dial.as_mut() {
                        d.res = None;
                    }
                    self.queue(Cmd::RemoveLeg { leg_id: leg });
                    self.report(id, AgentOutcome::AgentNoAnswer, "timeout");
                    self.start_music();
                }
                self.connect_tick(fx);
                self.maybe_announce(fx);
            }
            _ => {}
        }
    }
}

/// The consent announcement as a synthetic step: its prompts, then a one-digit gather of
/// `window_s` (no routes: the machine reads it as the decision).
fn consent_verbs(c: &Consent) -> Vec<Verb> {
    if c.mode == ConsentMode::Never {
        return Vec::new();
    }
    let mut v: Vec<Verb> = c.announce.clone();
    v.push(Verb::Gather(Gather {
        input: None,
        num_digits: 1,
        finish_on_key: String::new(),
        timeout_s: c.window_s,
        inter_digit_s: 1,
        routes: Default::default(),
        default: None,
    }));
    v
}
