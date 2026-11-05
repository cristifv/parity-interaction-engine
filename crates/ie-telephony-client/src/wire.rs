//! The only module that names prost types (ADR 0003 §4.1): domain ↔ `parity.telephony.v1`.
//! Nothing here logs; a prost message is never logged, traced or put into an error (§4.9).

use crate::api::{
    Decline, Destination, DtmfActions, Intent, LegMedia, LegSnapshot, LegUpdate, NodeStatus,
    Number, Owner, RecordingStopped, Reply, SessionEvent, SessionKey, Snapshot,
};
use parity_proto::telephony::v1 as pb;
use pb::watch_response::Event;
use std::time::Duration;
use tonic::metadata::{AsciiMetadataValue, MetadataMap};
use tonic::transport::Channel;

pub(crate) type Client = pb::telephony_service_client::TelephonyServiceClient<Channel>;
pub(crate) use pb::{SessionOwner, WatchResponse};

/// What a Watch message carries, in domain terms.
pub(crate) enum Incoming {
    Arrived(Box<ArrivedData>),
    Event(SessionEvent),
    Ended {
        reason: pb::SessionEndReason,
        sip_status: u32,
    },
    Status(NodeStatus),
    Heartbeat,
    Unknown,
}

/// `Arrived` before the node adds itself and the key.
pub(crate) struct ArrivedData {
    pub bin: String,
    pub leg_id: String,
    pub sip_call_id: String,
    pub called_number: Number,
    pub caller_number: Number,
    pub attempt: u32,
    pub redirected_from_bin: String,
    pub deadline_ms: i64,
    pub trunk_id: String,
}

pub(crate) fn key_of(r: &pb::SessionRef) -> SessionKey {
    SessionKey {
        tenant_id: r.tenant_id.clone(),
        interaction_id: r.interaction_id.clone(),
    }
}

fn media(m: Option<&pb::LegMediaState>) -> LegMedia {
    m.map(|m| LegMedia {
        muted: m.muted,
        recording: Some(m.recording()),
        mode: Some(m.mode()),
        whisper_to_leg_id: m.whisper_to_leg_id.clone(),
    })
    .unwrap_or_default()
}

pub(crate) fn incoming(ev: Option<Event>) -> Incoming {
    let Some(ev) = ev else {
        return Incoming::Unknown;
    };
    match ev {
        Event::Arrived(a) => Incoming::Arrived(Box::new(ArrivedData {
            bin: a.bin,
            leg_id: a.leg_id,
            sip_call_id: a.sip_call_id,
            called_number: Number::new(a.called_number),
            caller_number: Number::new(a.caller_number),
            attempt: a.attempt,
            redirected_from_bin: a.redirected_from_bin,
            deadline_ms: a.deadline_ms,
            trunk_id: a.trunk_id,
        })),
        Event::Leg(l) => Incoming::Event(SessionEvent::Leg(LegUpdate {
            role: l.role(),
            state: l.state(),
            cause: l.cause(),
            ended_by: l.ended_by(),
            leg_id: l.leg_id,
            sip_status: l.sip_status,
            answered_at_ms: l.answered_at_ms,
        })),
        Event::Dtmf(d) => Incoming::Event(SessionEvent::Digit {
            source: d.source(),
            leg_id: d.leg_id,
            digit: crate::api::Digits::new(d.digit),
            collection_id: d.collection_id,
        }),
        Event::PlayFinished(p) => Incoming::Event(SessionEvent::PlayFinished {
            reason: p.reason(),
            leg_id: p.leg_id,
            play_id: p.play_id,
            played_ms: p.played_ms,
        }),
        Event::PlayStarted(p) => Incoming::Event(SessionEvent::PlayStarted {
            leg_id: p.leg_id,
            play_id: p.play_id,
        }),
        Event::PlayFailed(p) => Incoming::Event(SessionEvent::PlayFailed {
            reason: p.reason(),
            leg_id: p.leg_id,
            play_id: p.play_id,
        }),
        Event::RecordingStarted(r) => Incoming::Event(SessionEvent::RecordingStarted {
            recording_id: r.recording_id,
            leg_id: r.leg_id,
        }),
        Event::RecordingStopped(r) => {
            Incoming::Event(SessionEvent::RecordingStopped(RecordingStopped {
                pauses: r
                    .pauses
                    .iter()
                    .map(|p| (p.start_ms, p.end_ms, p.by_dtmf))
                    .collect(),
                recording_id: r.recording_id,
                leg_id: r.leg_id,
                path: r.path,
                size_bytes: r.size_bytes,
                duration_ms: r.duration_ms,
                sha256: r.sha256,
                wrapped_dek: r.wrapped_dek,
                kek_id: r.kek_id,
            }))
        }
        Event::MediaActivity(m) => Incoming::Event(SessionEvent::MediaActivity {
            leg_id: m.leg_id,
            active: m.active,
        }),
        Event::RemoteHold(h) => Incoming::Event(SessionEvent::RemoteHold {
            leg_id: h.leg_id,
            on: h.on,
        }),
        Event::RemoteMediaChanged(c) => Incoming::Event(SessionEvent::RemoteMediaChanged {
            kind: c.kind(),
            leg_id: c.leg_id,
        }),
        Event::DtmfAction(a) => Incoming::Event(SessionEvent::DtmfAction {
            role: a.role(),
            action: a.action(),
            result: a.result(),
            state: media(a.state.as_ref()),
            leg_id: a.leg_id,
        }),
        Event::SessionEnded(e) => Incoming::Ended {
            reason: e.reason(),
            sip_status: e.sip_status,
        },
        Event::NodeStatus(n) => Incoming::Status(NodeStatus {
            sessions: n.sessions,
            free_legs: n.free_legs,
            draining: n.draining,
            rebuilding: n.rebuilding,
            version: n.version,
            capabilities: n.capabilities,
        }),
        Event::Heartbeat(_) => Incoming::Heartbeat,
    }
}

pub(crate) fn snapshot(s: pb::Session) -> Snapshot {
    Snapshot {
        state: s.state(),
        media: s.media(),
        end_reason: s.end_reason(),
        key: SessionKey {
            tenant_id: s.tenant_id,
            interaction_id: s.interaction_id,
        },
        owner_epoch: s.owner_epoch,
        owner_replica_id: s.owner_replica_id,
        on_hold: s.on_hold,
        legs: s
            .legs
            .into_iter()
            .map(|l| LegSnapshot {
                role: l.role(),
                state: l.state(),
                endpoint: l.endpoint(),
                media: media(l.media.as_ref()),
                digit_reporting: l.digit_reporting(),
                cause: l.cause(),
                leg_id: l.leg_id,
                isolated: l.isolated,
                collection_id: l.collection_id,
                play_id: l.play_id,
                ended_at_ms: l.ended_at_ms,
            })
            .collect(),
        recordings: s
            .recordings
            .into_iter()
            .map(|r| {
                let st = r.state();
                (r.recording_id, r.leg_id, st)
            })
            .collect(),
        last_delivery_seq: s.last_delivery_seq,
        ended_at_ms: s.ended_at_ms,
    }
}

pub(crate) fn owner_block(o: &Owner, replica_id: &str, ack: Option<(u64, &str)>) -> SessionOwner {
    SessionOwner {
        tenant_id: o.key.tenant_id.clone(),
        interaction_id: o.key.interaction_id.clone(),
        owner_epoch: o.owner_epoch,
        replica_id: replica_id.to_owned(),
        acked_delivery_seq: ack.map_or(0, |a| a.0),
        acked_incarnation: ack.map_or_else(String::new, |a| a.1.to_owned()),
    }
}

/// Metadata every per-session RPC carries.
#[derive(Clone)]
pub(crate) struct Meta {
    pub tenant_id: String,
    pub test_peer: Option<String>,
    /// `parity-attempt-expires-ms` for this attempt (0.2.3); `None`: send time + the deadline.
    pub expires_ms: Option<i64>,
}

/// Epoch milliseconds now.
pub(crate) fn epoch_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}

fn traceparent() -> String {
    format!(
        "00-{:016x}{:016x}-{:016x}-01",
        fastrand::u64(1..),
        fastrand::u64(..),
        fastrand::u64(1..)
    )
}

pub(crate) fn request<T>(msg: T, meta: &Meta, deadline: Duration) -> tonic::Request<T> {
    let mut r = tonic::Request::new(msg);
    r.set_timeout(deadline);
    put(r.metadata_mut(), "tenant-id", &meta.tenant_id);
    put(r.metadata_mut(), "traceparent", &traceparent());
    // The attempt's absolute expiry (0.2.3): TC refuses it, before any effect, once its clock
    // passes this less its skew budget; gRPC's own deadline is relative to receipt.
    let expires = meta
        .expires_ms
        .unwrap_or_else(|| epoch_ms() + i64::try_from(deadline.as_millis()).unwrap_or(0));
    put(
        r.metadata_mut(),
        "parity-attempt-expires-ms",
        &expires.to_string(),
    );
    if let Some(p) = &meta.test_peer {
        put(r.metadata_mut(), "x-parity-test-peer", p);
    }
    r
}

fn put(m: &mut MetadataMap, k: &'static str, v: &str) {
    if let Ok(v) = AsciiMetadataValue::try_from(v) {
        m.insert(k, v);
    }
}

/// The `parity-error` trailer, if any.
pub(crate) fn parity_error(s: &tonic::Status) -> Option<String> {
    s.metadata()
        .get("parity-error")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned)
}

fn dtmf_actions(a: &DtmfActions) -> pb::DtmfActions {
    pb::DtmfActions {
        bindings: a
            .bindings
            .iter()
            .map(|(seq, kind)| pb::DtmfActionBinding {
                dtmf_sequence: seq.expose().to_owned(),
                action: *kind as i32,
            })
            .collect(),
        locks: a.locks.iter().map(|f| *f as i32).collect(),
        inter_digit_timeout_ms: a.inter_digit_timeout_ms,
    }
}

/// Sends one intent. The owner block and command id are the caller's.
pub(crate) async fn send(
    c: &mut Client,
    meta: &Meta,
    deadline: Duration,
    intent: &Intent,
    owner: SessionOwner,
    command_id: &str,
) -> Result<Reply, tonic::Status> {
    let cid = command_id.to_owned();
    let o = Some(owner);
    macro_rules! rq {
        ($m:expr) => {
            request($m, meta, deadline)
        };
    }
    let done = |existing: bool| Reply {
        existing,
        ..Reply::default()
    };
    Ok(match intent {
        Intent::Ring { leg_id } => done(
            c.ring_leg(rq!(pb::RingLegRequest {
                owner: o,
                command_id: cid,
                leg_id: leg_id.clone()
            }))
            .await?
            .into_inner()
            .existing,
        ),
        Intent::Answer { leg_id } => done(
            c.answer_leg(rq!(pb::AnswerLegRequest {
                owner: o,
                command_id: cid,
                leg_id: leg_id.clone()
            }))
            .await?
            .into_inner()
            .existing,
        ),
        Intent::Reject { leg_id, sip_status } => done(
            c.reject_leg(rq!(pb::RejectLegRequest {
                owner: o,
                command_id: cid,
                leg_id: leg_id.clone(),
                sip_status: *sip_status
            }))
            .await?
            .into_inner()
            .existing,
        ),
        Intent::AddLeg(a) => {
            let destination = Some(match &a.destination {
                Destination::Contact(uri) => {
                    pb::add_leg_request::Destination::AgentContact(uri.clone())
                }
                Destination::Pstn { number, trunk_id } => {
                    pb::add_leg_request::Destination::Pstn(pb::PstnDestination {
                        number: number.expose().to_owned(),
                        trunk_id: trunk_id.clone(),
                    })
                }
            });
            let r = c
                .add_leg(rq!(pb::AddLegRequest {
                    owner: o,
                    command_id: cid,
                    leg_id: a.leg_id.clone(),
                    role: a.role as i32,
                    destination,
                    caller_id: a.caller_id.expose().to_owned(),
                    ring_timeout_s: a.ring_timeout_s,
                    endpoint: a.endpoint as i32,
                    codec_policy: a.codec_policy as i32,
                    start_muted: a.start_muted,
                    monitor_mode: a.monitor_mode as i32,
                    whisper_to_leg_id: a.whisper_to_leg_id.clone(),
                    dtmf_actions: a.dtmf_actions.as_ref().map(dtmf_actions),
                }))
                .await?
                .into_inner();
            Reply {
                existing: r.existing,
                leg_state: Some(r.state()),
                cause: Some(r.cause()),
                ..Reply::default()
            }
        }
        Intent::RemoveLeg { leg_id, cause } => {
            let r = c
                .remove_leg(rq!(pb::RemoveLegRequest {
                    owner: o,
                    command_id: cid,
                    leg_id: leg_id.clone(),
                    cause: *cause as i32
                }))
                .await?
                .into_inner();
            Reply {
                existing: r.existing,
                already_gone: r.already_gone,
                ..Reply::default()
            }
        }
        Intent::EndSession { cause } => {
            let r = c
                .end_session(rq!(pb::EndSessionRequest {
                    owner: o,
                    command_id: cid,
                    cause: *cause as i32
                }))
                .await?
                .into_inner();
            Reply {
                existing: r.existing,
                already_gone: r.already_gone,
                ..Reply::default()
            }
        }
        Intent::Play {
            leg_id,
            play_id,
            prompt_id,
            looped,
        } => done(
            c.play(rq!(pb::PlayRequest {
                owner: o,
                command_id: cid,
                leg_id: leg_id.clone(),
                play_id: play_id.clone(),
                prompt_id: prompt_id.clone(),
                r#loop: *looped,
                // The play saga (contracts 0.17.0, TC ADR 0004): the answer means TC took the
                // play; PlayStarted / PlayFinished / PlayFailed tell the outcome. An older TC
                // ignores it and answers once the play started.
                result_events: true,
            }))
            .await?
            .into_inner()
            .existing,
        ),
        Intent::StopPlay { leg_id, play_id } => {
            let r = c
                .stop_play(rq!(pb::StopPlayRequest {
                    owner: o,
                    command_id: cid,
                    leg_id: leg_id.clone(),
                    play_id: play_id.clone()
                }))
                .await?
                .into_inner();
            Reply {
                existing: r.existing,
                already_gone: r.already_gone,
                ..Reply::default()
            }
        }
        Intent::SetDigitReporting {
            leg_id,
            mode,
            collection_id,
            max_duration_s,
        } => done(
            c.set_digit_reporting(rq!(pb::SetDigitReportingRequest {
                owner: o,
                command_id: cid,
                leg_id: leg_id.clone(),
                mode: *mode as i32,
                collection_id: collection_id.clone(),
                max_duration_s: *max_duration_s,
            }))
            .await?
            .into_inner()
            .existing,
        ),
        Intent::SendDtmf { leg_id, digits } => done(
            c.send_dtmf(rq!(pb::SendDtmfRequest {
                owner: o,
                command_id: cid,
                leg_id: leg_id.clone(),
                digits: digits.expose().to_owned(),
            }))
            .await?
            .into_inner()
            .existing,
        ),
        Intent::SetMute { leg_id, muted } => {
            let r = c
                .set_mute(rq!(pb::SetMuteRequest {
                    owner: o,
                    command_id: cid,
                    leg_id: leg_id.clone(),
                    muted: *muted
                }))
                .await?
                .into_inner();
            Reply {
                existing: r.existing,
                changed: r.changed,
                ..Reply::default()
            }
        }
        Intent::SetHold {
            on,
            music_prompt_id,
        } => {
            let r = c
                .set_hold(rq!(pb::SetHoldRequest {
                    owner: o,
                    command_id: cid,
                    on: *on,
                    music_prompt_id: music_prompt_id.clone()
                }))
                .await?
                .into_inner();
            Reply {
                existing: r.existing,
                changed: r.changed,
                ..Reply::default()
            }
        }
        Intent::SetIsolation {
            leg_id,
            isolated,
            music_prompt_id,
        } => {
            let r = c
                .set_isolation(rq!(pb::SetIsolationRequest {
                    owner: o,
                    command_id: cid,
                    leg_id: leg_id.clone(),
                    isolated: *isolated,
                    music_prompt_id: music_prompt_id.clone(),
                }))
                .await?
                .into_inner();
            Reply {
                existing: r.existing,
                changed: r.changed,
                ..Reply::default()
            }
        }
        Intent::SetMonitorMode {
            leg_id,
            mode,
            whisper_to_leg_id,
        } => {
            let r = c
                .set_monitor_mode(rq!(pb::SetMonitorModeRequest {
                    owner: o,
                    command_id: cid,
                    leg_id: leg_id.clone(),
                    mode: *mode as i32,
                    whisper_to_leg_id: whisper_to_leg_id.clone(),
                }))
                .await?
                .into_inner();
            Reply {
                existing: r.existing,
                changed: r.changed,
                ..Reply::default()
            }
        }
        Intent::SetDtmfActions { leg_id, actions } => done(
            c.set_dtmf_actions(rq!(pb::SetDtmfActionsRequest {
                owner: o,
                command_id: cid,
                leg_id: leg_id.clone(),
                actions: Some(dtmf_actions(actions)),
            }))
            .await?
            .into_inner()
            .existing,
        ),
        Intent::StartRecording {
            leg_id,
            recording_id,
            key,
        } => done(
            c.start_recording(rq!(pb::StartRecordingRequest {
                owner: o,
                command_id: cid,
                leg_id: leg_id.clone(),
                recording_id: recording_id.clone(),
                key: Some(pb::RecordingKey {
                    dek: key.dek.clone(),
                    wrapped_dek: key.wrapped_dek.clone(),
                    kek_id: key.kek_id.clone()
                }),
            }))
            .await?
            .into_inner()
            .existing,
        ),
        Intent::StopRecording { recording_id } => {
            let r = c
                .stop_recording(rq!(pb::StopRecordingRequest {
                    owner: o,
                    command_id: cid,
                    recording_id: recording_id.clone()
                }))
                .await?
                .into_inner();
            Reply {
                existing: r.existing,
                already_gone: r.already_gone,
                ..Reply::default()
            }
        }
    })
}

/// `ClaimSession`: the snapshot, whether it is a repeat, and its incarnation (0.2.2).
pub(crate) async fn claim(
    c: &mut Client,
    meta: &Meta,
    deadline: Duration,
    owner: SessionOwner,
) -> Result<(Snapshot, String), tonic::Status> {
    let r = c
        .claim_session(request(
            pb::ClaimSessionRequest { owner: Some(owner) },
            meta,
            deadline,
        ))
        .await?
        .into_inner();
    let s = r
        .session
        .ok_or_else(|| tonic::Status::internal("ClaimSession without a session"))?;
    Ok((snapshot(s), r.incarnation))
}

/// `GetSession`: the snapshot and its incarnation (0.2.2).
pub(crate) async fn get_session(
    c: &mut Client,
    meta: &Meta,
    deadline: Duration,
    key: &SessionKey,
    replica_id: &str,
) -> Result<(Snapshot, String), tonic::Status> {
    let r = c
        .get_session(request(
            pb::GetSessionRequest {
                tenant_id: key.tenant_id.clone(),
                interaction_id: key.interaction_id.clone(),
                replica_id: replica_id.to_owned(),
            },
            meta,
            deadline,
        ))
        .await?
        .into_inner();
    let s = r
        .session
        .ok_or_else(|| tonic::Status::internal("GetSession without a session"))?;
    Ok((snapshot(s), r.incarnation))
}

/// `DeclineArrival`.
pub(crate) async fn decline(
    c: &mut Client,
    meta: &Meta,
    deadline: Duration,
    key: &SessionKey,
    replica_id: &str,
    leg_id: &str,
    d: &Decline,
) -> Result<Reply, tonic::Status> {
    let (reason, bin, sip_status) = match d {
        Decline::WrongBin { bin } => (pb::DeclineReason::WrongBin, bin.clone(), 0),
        Decline::Refused { sip_status } => (pb::DeclineReason::Refused, String::new(), *sip_status),
    };
    let r = c
        .decline_arrival(request(
            pb::DeclineArrivalRequest {
                tenant_id: key.tenant_id.clone(),
                interaction_id: key.interaction_id.clone(),
                replica_id: replica_id.to_owned(),
                leg_id: leg_id.to_owned(),
                reason: reason as i32,
                bin,
                sip_status,
            },
            meta,
            deadline,
        ))
        .await?
        .into_inner();
    Ok(Reply {
        existing: r.existing,
        rejected: r.rejected,
        ..Reply::default()
    })
}

/// `Acknowledge`: how many TC ignored.
pub(crate) async fn acknowledge(
    c: &mut Client,
    meta: &Meta,
    deadline: Duration,
    acks: Vec<SessionOwner>,
) -> Result<u32, tonic::Status> {
    Ok(
        c.acknowledge(request(pb::AcknowledgeRequest { acks }, meta, deadline))
            .await?
            .into_inner()
            .ignored,
    )
}

/// Opens a replica's stream.
pub(crate) async fn watch(
    c: &mut Client,
    meta: &Meta,
    replica_id: &str,
    bin: &str,
) -> Result<tonic::Streaming<WatchResponse>, tonic::Status> {
    let mut r = tonic::Request::new(pb::WatchRequest {
        replica_id: replica_id.to_owned(),
        bin: bin.to_owned(),
        capabilities: Vec::new(),
    });
    put(r.metadata_mut(), "traceparent", &traceparent());
    if let Some(p) = &meta.test_peer {
        put(r.metadata_mut(), "x-parity-test-peer", p);
    }
    Ok(c.watch(r).await?.into_inner())
}
