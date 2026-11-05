//! What ie-core sends and receives (ADR 0003 §4.8). No prost message and no SDP appear here. The
//! contract's plain value enums (roles, states, causes) are shared with the wire: they carry no
//! data and no personal data, and an unknown value from a newer TC reads as `Unspecified`.

use ie_obs::pii::mask_phone;
use std::fmt;

pub use parity_proto::telephony::v1::{
    CodecPolicy, DeclineReason, DigitReporting, DigitSource, DtmfActionKind, DtmfActionResult,
    EndCause, EndedBy, Endpoint, ErrorCode, Facet, LegRole, LegState, MediaKind, MonitorMode,
    PlayEndReason, PlayFailReason, RecordingState, RemoteMediaKind, SessionEndReason, SessionState,
};

/// A media node, as named in the configuration and in TC's `node_id`.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeId(pub String);

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// One interaction's session on a node.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SessionKey {
    /// The tenant (a DNS label).
    pub tenant_id: String,
    /// The interaction (a UUID).
    pub interaction_id: String,
}

/// Who acts on a session: the key and the owner's fence (the etcd create revision of
/// `ie/owner/{interaction_id}`). The replica id comes from the client's configuration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Owner {
    /// The session.
    pub key: SessionKey,
    /// The owner's epoch; at least 1.
    pub owner_epoch: u64,
}

/// A phone number from the contract (`PII: phone`): printed only masked.
#[derive(Clone, PartialEq, Eq, Default)]
pub struct Number(String);

impl Number {
    /// Wraps a number as received or to be sent.
    pub fn new(raw: impl Into<String>) -> Self {
        Self(raw.into())
    }
    /// The raw value, for the wire only.
    pub fn expose(&self) -> &str {
        &self.0
    }
    /// An anonymous caller has none.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl fmt::Debug for Number {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Number({})", mask_phone(&self.0))
    }
}

impl fmt::Display for Number {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&mask_phone(&self.0))
    }
}

/// DTMF digits (`PCI: digits`): never printed, logged or stored.
#[derive(Clone, PartialEq, Eq, Default)]
pub struct Digits(String);

impl Digits {
    /// Wraps digits.
    pub fn new(raw: impl Into<String>) -> Self {
        Self(raw.into())
    }
    /// The raw digits, for the flow logic and the wire only.
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Digits {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Digits([redacted])")
    }
}

/// A recording's key (`SECRET: key`): the DEK is in memory only and never printed.
#[derive(Clone, PartialEq, Eq)]
pub struct RecordingKey {
    /// The plain data key.
    pub dek: Vec<u8>,
    /// The data key wrapped with the tenant's public key.
    pub wrapped_dek: Vec<u8>,
    /// The wrapping key's id.
    pub kek_id: String,
}

impl fmt::Debug for RecordingKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RecordingKey {{ kek_id: {:?}, .. }}", self.kek_id)
    }
}

/// Where `AddLeg` dials.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Destination {
    /// An agent's or supervisor's registered contact (a SIP URI).
    Contact(String),
    /// The PSTN through a trunk.
    Pstn {
        /// The number dialled.
        number: Number,
        /// The tenant's trunk.
        trunk_id: String,
    },
}

/// A leg's DTMF action codes (P5).
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct DtmfActions {
    /// A digit sequence (`*` first) and its action.
    pub bindings: Vec<(Digits, DtmfActionKind)>,
    /// Facets a code may not change (consent pins the recording).
    pub locks: Vec<Facet>,
    /// The inter-digit timeout.
    pub inter_digit_timeout_ms: u32,
}

/// `AddLeg`'s parameters.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AddLeg {
    /// A UUID IE made and wrote ahead.
    pub leg_id: String,
    /// The leg's role.
    pub role: LegRole,
    /// Where it goes.
    pub destination: Destination,
    /// The caller id shown to the far end.
    pub caller_id: Number,
    /// Ring time before `NO_ANSWER`.
    pub ring_timeout_s: u32,
    /// A browser (WebRTC) or a SIP phone.
    pub endpoint: Endpoint,
    /// Codecs offered.
    pub codec_policy: CodecPolicy,
    /// Joins muted.
    pub start_muted: bool,
    /// A supervisor's first mode.
    pub monitor_mode: MonitorMode,
    /// Whom a whispering supervisor speaks to.
    pub whisper_to_leg_id: String,
    /// The leg's DTMF actions.
    pub dtmf_actions: Option<DtmfActions>,
}

/// One intent for one session: the contract's per-session commands (ADR 0003 §4.8).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Intent {
    /// 180 Ringing on a parked arrival.
    Ring { leg_id: String },
    /// 200 OK with parity-ms's answer.
    Answer { leg_id: String },
    /// A final failure response (4xx–6xx).
    Reject { leg_id: String, sip_status: u32 },
    /// Originates a leg.
    AddLeg(Box<AddLeg>),
    /// Hangs up one leg.
    RemoveLeg { leg_id: String, cause: EndCause },
    /// Hangs up every leg and deletes the media.
    EndSession { cause: EndCause },
    /// A tenant prompt into one leg.
    Play {
        leg_id: String,
        play_id: String,
        prompt_id: String,
        looped: bool,
    },
    /// Stops a play.
    StopPlay { leg_id: String, play_id: String },
    /// Digit reporting for one collection (T15).
    SetDigitReporting {
        leg_id: String,
        mode: DigitReporting,
        collection_id: String,
        max_duration_s: u32,
    },
    /// RFC 4733 digits toward one leg.
    SendDtmf { leg_id: String, digits: Digits },
    /// Mutes or unmutes what a leg sends.
    SetMute { leg_id: String, muted: bool },
    /// The session's hold.
    SetHold { on: bool, music_prompt_id: String },
    /// A leg hears only music.
    SetIsolation {
        leg_id: String,
        isolated: bool,
        music_prompt_id: String,
    },
    /// A supervisor's mode.
    SetMonitorMode {
        leg_id: String,
        mode: MonitorMode,
        whisper_to_leg_id: String,
    },
    /// A leg's DTMF actions.
    SetDtmfActions {
        leg_id: String,
        actions: DtmfActions,
    },
    /// Records a leg with a supplied key.
    StartRecording {
        leg_id: String,
        recording_id: String,
        key: RecordingKey,
    },
    /// Stops a recording.
    StopRecording { recording_id: String },
}

impl Intent {
    /// The RPC's name, for metrics and logs.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Ring { .. } => "RingLeg",
            Self::Answer { .. } => "AnswerLeg",
            Self::Reject { .. } => "RejectLeg",
            Self::AddLeg(_) => "AddLeg",
            Self::RemoveLeg { .. } => "RemoveLeg",
            Self::EndSession { .. } => "EndSession",
            Self::Play { .. } => "Play",
            Self::StopPlay { .. } => "StopPlay",
            Self::SetDigitReporting { .. } => "SetDigitReporting",
            Self::SendDtmf { .. } => "SendDtmf",
            Self::SetMute { .. } => "SetMute",
            Self::SetHold { .. } => "SetHold",
            Self::SetIsolation { .. } => "SetIsolation",
            Self::SetMonitorMode { .. } => "SetMonitorMode",
            Self::SetDtmfActions { .. } => "SetDtmfActions",
            Self::StartRecording { .. } => "StartRecording",
            Self::StopRecording { .. } => "StopRecording",
        }
    }
}

/// What a successful command answered.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct Reply {
    /// A repeat: the first answer.
    pub existing: bool,
    /// The command changed something.
    pub changed: bool,
    /// The target had already ended (still a success).
    pub already_gone: bool,
    /// `AddLeg`: the leg's state, and its cause if it ended.
    pub leg_state: Option<LegState>,
    /// `AddLeg`: the cause of an ended leg.
    pub cause: Option<EndCause>,
    /// `DeclineArrival`: TC answered the caller 503.
    pub rejected: bool,
}

/// How an `AddLeg` with an unknown result was settled before ie-core may fail over (R1-3).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Reconciled {
    /// TC has no such leg: nothing rings.
    LegAbsent,
    /// The leg existed and was hung up.
    LegRemoved,
    /// The leg had already ended.
    LegEnded,
    /// TC couldn't be asked: the node is unmanaged; ie-core must not fail over.
    Unreconciled,
    /// Not an `AddLeg`: ie-core resyncs.
    NotApplicable,
}

/// The result of an intent (ADR 0003 §4.4).
#[derive(Clone, Debug, PartialEq)]
pub enum Outcome {
    /// Done.
    Done(Reply),
    /// The node's stream is lost: nothing was sent.
    Unmanaged,
    /// A newer owner has the session (`stale_owner`): stop acting on it.
    Fenced,
    /// The client's own field check refused the intent: nothing was sent.
    Refused(crate::validate::Invalid),
    /// A bug or an attack: logged and alerted, never retried.
    Bug(ErrorCode),
    /// TC disagrees with IE's view: here is TC's snapshot.
    Resync(Box<Snapshot>),
    /// TC has no such session on a live, rebuilt node: it is gone (R1-1).
    Lost,
    /// The session ended; its tombstone.
    Ended(Box<Snapshot>),
    /// The arrival isn't offered to this replica any more.
    ArrivalClosed,
    /// A configuration fault (`prompt_unknown`).
    Config(ErrorCode),
    /// No capacity, or the node drains.
    Capacity(ErrorCode),
    /// A read that couldn't be done.
    Unavailable(ErrorCode),
    /// A mutating command whose result is unknown, and how it was settled (R1-3).
    Indeterminate(Reconciled),
    /// TC doesn't know the RPC (an older release).
    Unsupported,
}

/// A parked inbound call offered to this replica. No SDP.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Arrival {
    /// The node it parks on.
    pub node: NodeId,
    /// The session; `tenant_id` is the caller's `X-Tenant-ID` claim, which IE must verify.
    pub key: SessionKey,
    /// The bin it is offered to.
    pub bin: String,
    /// The customer's leg.
    pub leg_id: String,
    /// The SIP Call-ID as received.
    pub sip_call_id: String,
    /// The number dialled.
    pub called_number: Number,
    /// Empty for an anonymous caller.
    pub caller_number: Number,
    /// 1 for the first offer.
    pub attempt: u32,
    /// The bin that redirected it, if any.
    pub redirected_from_bin: String,
    /// TC answers 503 then.
    pub deadline_ms: i64,
    /// The carrier trunk TC matched.
    pub trunk_id: String,
}

/// `DeclineArrival`'s reason (D5 of TC's ADR).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Decline {
    /// Offer it to another bin, once.
    WrongBin { bin: String },
    /// Refuse it with a 4xx–6xx status.
    Refused { sip_status: u32 },
}

/// A leg's media state.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct LegMedia {
    /// Muted.
    pub muted: bool,
    /// Recording state.
    pub recording: Option<RecordingState>,
    /// Monitor mode.
    pub mode: Option<MonitorMode>,
    /// Whom a whisper reaches.
    pub whisper_to_leg_id: String,
}

/// A leg's new state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LegUpdate {
    /// The leg.
    pub leg_id: String,
    /// Its role.
    pub role: LegRole,
    /// Its state.
    pub state: LegState,
    /// Why it ended.
    pub cause: EndCause,
    /// The SIP status that ended it.
    pub sip_status: u32,
    /// Who ended it.
    pub ended_by: EndedBy,
    /// When it answered.
    pub answered_at_ms: i64,
}

/// A recording's result.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordingStopped {
    /// The recording.
    pub recording_id: String,
    /// The leg.
    pub leg_id: String,
    /// Where parity-ms wrote it.
    pub path: String,
    /// Its size.
    pub size_bytes: u64,
    /// Its length.
    pub duration_ms: u32,
    /// Its SHA-256.
    pub sha256: String,
    /// The wrapped key (safe to keep).
    pub wrapped_dek: Vec<u8>,
    /// The wrapping key.
    pub kek_id: String,
    /// Pauses: (start, end, by DTMF).
    pub pauses: Vec<(i64, i64, bool)>,
}

/// Why a session ended, as IE sees it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EndReason {
    /// TC reported it (`SessionEnded`, or an ENDED snapshot).
    Reported(SessionEndReason),
    /// TC, live and rebuilt, has no such session (R1-1): the owner uses `failed_system`.
    SessionUnknown,
    /// The node was declared dead on positive evidence (D4): the owner uses `failed_system`.
    NodeDead,
}

/// What `Telephony::reconcile` found for an ending session (IE-050).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Reconcile {
    /// Over at TC: its end is now in the session's mailbox (from the tombstone, or lost).
    Ended,
    /// TC still has it live.
    Live,
    /// A newer owner has it (the mailbox is closed).
    Fenced,
    /// The node is unmanaged, or TC didn't answer.
    Unreachable,
}

/// What reaches a session's owner, in order.
#[derive(Debug)]
pub enum SessionEvent {
    /// A leg's new state.
    Leg(LegUpdate),
    /// A digit of the open collection.
    Digit {
        leg_id: String,
        digit: Digits,
        collection_id: String,
        source: DigitSource,
    },
    /// A play ended.
    PlayFinished {
        leg_id: String,
        play_id: String,
        reason: PlayEndReason,
        played_ms: u32,
    },
    /// A play started (contracts 0.17.0, `result_events`); `PlayFinished` ends it.
    PlayStarted { leg_id: String, play_id: String },
    /// A play ended without starting (0.17.0, `result_events`); nothing else follows for it.
    PlayFailed {
        leg_id: String,
        play_id: String,
        reason: PlayFailReason,
    },
    /// parity-ms is writing.
    RecordingStarted {
        recording_id: String,
        leg_id: String,
    },
    /// The recording is final.
    RecordingStopped(RecordingStopped),
    /// Media stopped or came back on a leg.
    MediaActivity { leg_id: String, active: bool },
    /// A far end held or resumed.
    RemoteHold { leg_id: String, on: bool },
    /// A far end's re-INVITE changed its media.
    RemoteMediaChanged {
        leg_id: String,
        kind: RemoteMediaKind,
    },
    /// parity-ms acted (or not) on a DTMF code. Never digits.
    DtmfAction {
        leg_id: String,
        role: LegRole,
        action: DtmfActionKind,
        result: DtmfActionResult,
        state: LegMedia,
    },
    /// The session ended. Redeem `ack` once the ENDED record is durable (D5).
    Ended {
        reason: EndReason,
        sip_status: u32,
        ack: crate::ended::EndedAck,
    },
    /// TC's state replaces whatever the owner had (after a gap, an overflow or a TC restart).
    Resync(Box<Snapshot>),
    /// The node's stream is lost: send nothing to RE, run no timer that needs TC (D4).
    Unmanaged,
    /// The stream is back.
    Managed,
    /// An event kind this release doesn't know (acked, otherwise ignored).
    Unknown,
}

/// A leg in a snapshot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LegSnapshot {
    /// The leg.
    pub leg_id: String,
    /// Its role.
    pub role: LegRole,
    /// Its state.
    pub state: LegState,
    /// Its endpoint.
    pub endpoint: Endpoint,
    /// Its media state.
    pub media: LegMedia,
    /// Isolated (a consult).
    pub isolated: bool,
    /// Its digit reporting.
    pub digit_reporting: DigitReporting,
    /// The open collection.
    pub collection_id: String,
    /// The current play.
    pub play_id: String,
    /// Why it ended.
    pub cause: EndCause,
    /// When it ended.
    pub ended_at_ms: i64,
}

/// TC's state of a session; for an ENDED one, its tombstone.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Snapshot {
    /// The session.
    pub key: SessionKey,
    /// TC's fence.
    pub owner_epoch: u64,
    /// The bound replica.
    pub owner_replica_id: String,
    /// Its state.
    pub state: SessionState,
    /// A call, a conference or none yet.
    pub media: MediaKind,
    /// On hold.
    pub on_hold: bool,
    /// Its legs.
    pub legs: Vec<LegSnapshot>,
    /// Its recordings: (id, leg, state).
    pub recordings: Vec<(String, String, RecordingState)>,
    /// The highest `delivery_seq` sent in the snapshot's incarnation.
    pub last_delivery_seq: u64,
    /// Why it ended.
    pub end_reason: SessionEndReason,
    /// When it ended.
    pub ended_at_ms: i64,
}

/// A node's last `NodeStatus`.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct NodeStatus {
    /// Live sessions.
    pub sessions: u32,
    /// parity-ms legs left.
    pub free_legs: u32,
    /// No new sessions.
    pub draining: bool,
    /// Commands get `rebuilding`.
    pub rebuilding: bool,
    /// TC's version.
    pub version: String,
    /// Features beyond the base set.
    pub capabilities: Vec<String>,
}

/// A node's link, as the O1 rule sees it (ADR 0003 §4.5).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Link {
    /// Never connected yet.
    Connecting,
    /// A message within the liveness window.
    Live,
    /// The stream is lost: its sessions are unmanaged.
    Disconnected,
    /// Declared dead on positive evidence: its sessions were ended.
    Dead,
    /// Another process uses this replica id: watching stopped, readiness failed (R1-9).
    Conflict,
}

/// What the client reports outside any session.
#[derive(Debug)]
pub enum ClientEvent {
    /// A parked call offered to this replica, with the mailbox of its session's events.
    Arrived(Box<Arrival>, crate::queue::SessionEvents),
    /// A node's link changed.
    Node { node: NodeId, link: Link },
    /// Events of a session this replica doesn't hold (e.g. after a restart): the ownership module's.
    Orphan { node: NodeId, key: SessionKey },
    /// The end of such a session that had an owner (TC's fence `owner_epoch`): a call this
    /// replica id owned before a restart (its `kill -9`, step 6). Redeem `ack` once the call's
    /// CDR is closed; it acknowledges the end with that fence (no command is sent). `at_ms`:
    /// when TC saw the end.
    OrphanEnded {
        node: NodeId,
        key: SessionKey,
        owner_epoch: u64,
        reason: SessionEndReason,
        sip_status: u32,
        at_ms: i64,
        ack: crate::ended::EndedAck,
    },
}
