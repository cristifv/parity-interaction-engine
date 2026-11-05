//! The client's behaviour against an in-process TC (ADR 0003 §5): liveness and unmanaged sessions,
//! reconnects, gaps and incarnations, superseded streams, ack timing and the `EndedAck` rule, the
//! Dead rule and zombie reconciliation, `Indeterminate` `AddLeg`s, the mailbox's resync
//! coalescing, `session_unknown`, fencing, the trailer classification and the redirect fallback.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod mock;

use ie_telephony_client::{
    ClientConfig, ClientEvent, Decline, EndReason, Intent, Link, ManualClock, NodeConfig,
    NodeEvidence, NodeId, Outcome, Owner, Profile, Reconciled, SessionEvent, SessionEvents,
    SessionKey, SourceKind, StaticEvidence, Telephony, TelephonyClient, Timing, Verdict,
};
use mock::*;
use parity_proto::telephony::v1 as pb;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

struct Env {
    mock: Mock,
    client: TelephonyClient,
    events: mpsc::Receiver<ClientEvent>,
    clock: Arc<ManualClock>,
    evidence: [Arc<StaticEvidence>; 3],
}

fn timing() -> Timing {
    Timing {
        reconnect_min_ms: 20,
        reconnect_max_ms: 100,
        ack_batch_ms: 3_600_000,
        probe_every_ms: 20,
        rebuilding_retry_ms: 600,
        ..Timing::default()
    }
}

async fn env_with(t: Timing) -> Env {
    let mock = Mock::default();
    let uri = mock.serve().await;
    let clock = Arc::new(ManualClock::new());
    let evidence = [
        Arc::new(StaticEvidence::new(SourceKind::Tc)),
        Arc::new(StaticEvidence::new(SourceKind::Dispatcher)),
        Arc::new(StaticEvidence::new(SourceKind::NodeServices)),
    ];
    let cfg = ClientConfig {
        replica_id: "ie-bin-a-1".into(),
        bin: "bin-a".into(),
        profile: Profile::Test,
        nodes: vec![NodeConfig {
            id: NODE.into(),
            grpc: uri,
            management: None,
        }],
        test_peer_header: true,
        timing: t,
    };
    let ev: Vec<Arc<dyn NodeEvidence>> = evidence
        .iter()
        .map(|e| Arc::clone(e) as Arc<dyn NodeEvidence>)
        .collect();
    let started = TelephonyClient::start(cfg, Arc::clone(&clock) as _, ev).unwrap();
    let env = Env {
        mock,
        client: started.client,
        events: started.events,
        clock,
        evidence,
    };
    eventually("the first Watch", || env.mock.watch_calls() >= 1).await;
    env
}

async fn env() -> Env {
    env_with(timing()).await
}

fn node() -> NodeId {
    NodeId(NODE.into())
}

fn owner(iid: &str) -> Owner {
    Owner {
        key: SessionKey {
            tenant_id: T.into(),
            interaction_id: iid.into(),
        },
        owner_epoch: 1042,
    }
}

async fn eventually(what: &str, f: impl Fn() -> bool) {
    for _ in 0..300 {
        if f() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("never: {what}");
}

async fn next(q: &mut SessionEvents) -> SessionEvent {
    tokio::time::timeout(Duration::from_secs(3), q.recv())
        .await
        .expect("an event in time")
        .expect("the mailbox is open")
}

/// Connects, offers one arrival and binds it: the session's mailbox.
async fn arrived_and_bound(e: &mut Env, iid: &str) -> SessionEvents {
    e.mock.send(heartbeat(INC_A)).await;
    e.mock.send(arrived(INC_A, iid, 1)).await;
    let ev = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            match e.events.recv().await {
                Some(ClientEvent::Arrived(_, q)) => break q,
                Some(_) => continue,
                None => panic!("no events"),
            }
        }
    })
    .await
    .expect("an arrival");
    assert!(e.client.bind(&node(), &owner(iid)));
    ev
}

#[tokio::test]
async fn heartbeat_loss_makes_sessions_unmanaged_and_commands_fail_fast() {
    let mut e = env().await;
    let mut q = arrived_and_bound(&mut e, IID).await;
    assert_eq!(e.client.link(&node()), Some(Link::Live));
    e.clock.advance(6_001);
    eventually("Disconnected", || {
        e.client.link(&node()) == Some(Link::Disconnected)
    })
    .await;
    assert!(matches!(next(&mut q).await, SessionEvent::Unmanaged));
    assert_eq!(e.client.unmanaged_calls(&node()), 1);
    let before = e.mock.calls("AnswerLeg").len();
    let o = e
        .client
        .command(&node(), &owner(IID), Intent::Answer { leg_id: LEG.into() })
        .await;
    assert_eq!(o, Outcome::Unmanaged);
    assert_eq!(
        e.mock.calls("AnswerLeg").len(),
        before,
        "nothing sent while unmanaged"
    );
}

#[tokio::test]
async fn reconnects_after_a_stream_error_and_drops_replays() {
    let mut e = env().await;
    let mut q = arrived_and_bound(&mut e, IID).await;
    e.mock
        .send(leg(INC_A, IID, 2, pb::LegState::Answered))
        .await;
    assert!(matches!(next(&mut q).await, SessionEvent::Leg(_)));
    e.mock.end_stream(tonic::Status::unavailable("gone")).await;
    eventually("a second Watch", || e.mock.watch_calls() >= 2).await;
    e.mock.send(heartbeat(INC_A)).await;
    eventually("Live again", || e.client.link(&node()) == Some(Link::Live)).await;
    assert!(matches!(next(&mut q).await, SessionEvent::Unmanaged));
    assert!(matches!(next(&mut q).await, SessionEvent::Managed));
    e.mock
        .send(leg(INC_A, IID, 2, pb::LegState::Answered))
        .await; // a replay
    e.mock.send(leg(INC_A, IID, 3, pb::LegState::Ended)).await;
    match next(&mut q).await {
        SessionEvent::Leg(l) => assert_eq!(l.state, pb::LegState::Ended),
        other => panic!("the replay was delivered again: {other:?}"),
    }
}

#[tokio::test]
async fn a_gap_starts_exactly_one_get_session_and_its_snapshot_replaces_the_queue() {
    let mut e = env().await;
    let mut q = arrived_and_bound(&mut e, IID).await;
    e.mock.session(
        IID,
        Ok((
            session(IID, pb::SessionState::Active, 4, vec![]),
            INC_A.into(),
        )),
    );
    e.mock
        .send(leg(INC_A, IID, 3, pb::LegState::Answered))
        .await; // 2 is missing
    e.mock
        .send(leg(INC_A, IID, 4, pb::LegState::Answered))
        .await;
    match next(&mut q).await {
        SessionEvent::Resync(s) => assert_eq!(s.last_delivery_seq, 4),
        other => panic!("expected a resync, got {other:?}"),
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        e.mock.calls("GetSession").len(),
        1,
        "one resync for one gap"
    );
    e.mock.send(leg(INC_A, IID, 5, pb::LegState::Ended)).await;
    assert!(matches!(next(&mut q).await, SessionEvent::Leg(_)));
}

#[tokio::test]
async fn a_new_incarnation_resyncs_every_session() {
    let mut e = env().await;
    let mut q1 = arrived_and_bound(&mut e, IID).await;
    e.mock.send(arrived(INC_A, IID2, 1)).await;
    let mut q2 = loop {
        if let Some(ClientEvent::Arrived(_, q)) = e.events.recv().await {
            break q;
        }
    };
    assert!(e.client.bind(&node(), &owner(IID2)));
    e.mock.session(
        IID,
        Ok((
            session(IID, pb::SessionState::Active, 2, vec![]),
            INC_B.into(),
        )),
    );
    e.mock.session(
        IID2,
        Ok((
            session(IID2, pb::SessionState::Active, 3, vec![]),
            INC_B.into(),
        )),
    );
    e.mock.send(heartbeat(INC_B)).await;
    assert!(matches!(next(&mut q1).await, SessionEvent::Resync(_)));
    assert!(matches!(next(&mut q2).await, SessionEvent::Resync(_)));
    assert_eq!(e.mock.calls("GetSession").len(), 2);
}

#[tokio::test]
async fn a_snapshot_from_a_newer_incarnation_waits_for_the_stream() {
    let mut e = env().await;
    let mut q = arrived_and_bound(&mut e, IID).await;
    e.mock.session(
        IID,
        Ok((
            session(IID, pb::SessionState::Active, 7, vec![]),
            INC_B.into(),
        )),
    );
    e.mock
        .send(leg(INC_A, IID, 3, pb::LegState::Answered))
        .await; // a gap: resync
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        q.try_recv().is_none(),
        "the snapshot of a newer incarnation isn't trusted yet"
    );
    e.mock.send(heartbeat(INC_B)).await; // the stream reaches it
    match next(&mut q).await {
        SessionEvent::Resync(s) => assert_eq!(s.last_delivery_seq, 7),
        other => panic!("expected the resync, got {other:?}"),
    }
}

#[tokio::test]
async fn an_unexpected_stream_superseded_fails_readiness_without_a_retry_loop() {
    let e = env().await;
    e.mock.send(heartbeat(INC_A)).await;
    eventually("Live", || e.client.link(&node()) == Some(Link::Live)).await;
    assert!(e.client.ready().is_ok());
    e.mock
        .end_stream(status(tonic::Code::Aborted, "stream_superseded"))
        .await;
    eventually("readiness failed", || e.client.ready().is_err()).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(e.mock.watch_calls(), 1, "no reconnect loop");
    assert_eq!(e.client.link(&node()), Some(Link::Conflict));
}

#[tokio::test]
async fn the_ack_rides_on_the_next_command_and_stays_pending_if_it_fails() {
    let mut e = env().await;
    let mut q = arrived_and_bound(&mut e, IID).await;
    e.mock
        .send(leg(INC_A, IID, 2, pb::LegState::Answered))
        .await;
    let _ = next(&mut q).await;
    e.mock.answer(
        "AnswerLeg",
        Answer::Err(status(tonic::Code::InvalidArgument, "invalid_request")),
    );
    let o = e
        .client
        .command(&node(), &owner(IID), Intent::Answer { leg_id: LEG.into() })
        .await;
    assert_eq!(o, Outcome::Bug(pb::ErrorCode::InvalidRequest));
    let o = e
        .client
        .command(&node(), &owner(IID), Intent::Ring { leg_id: LEG.into() })
        .await;
    assert!(matches!(o, Outcome::Done(_)));
    let ring = e.mock.calls("RingLeg");
    assert_eq!(
        ring[0].owner.as_ref().unwrap().acked_delivery_seq,
        2,
        "the failed command's ack is sent again"
    );
    assert_eq!(ring[0].owner.as_ref().unwrap().acked_incarnation, INC_A);
    let o = e
        .client
        .command(&node(), &owner(IID), Intent::Ring { leg_id: LEG.into() })
        .await;
    assert!(matches!(o, Outcome::Done(_)));
    assert_eq!(
        e.mock.calls("RingLeg")[1]
            .owner
            .as_ref()
            .unwrap()
            .acked_delivery_seq,
        0,
        "nothing new to ack"
    );
}

#[tokio::test]
async fn unsent_acks_go_in_a_batched_acknowledge() {
    let mut e = env_with(Timing {
        ack_batch_ms: 20,
        ..timing()
    })
    .await;
    let mut q = arrived_and_bound(&mut e, IID).await;
    e.mock
        .send(leg(INC_A, IID, 2, pb::LegState::Answered))
        .await;
    let _ = next(&mut q).await;
    eventually("an Acknowledge with seq 2", || {
        e.mock
            .acks()
            .iter()
            .any(|a| a.acked_delivery_seq == 2 && a.owner_epoch == 1042)
    })
    .await;
}

#[tokio::test]
async fn session_ended_is_acked_only_once_its_token_is_redeemed() {
    let mut e = env_with(Timing {
        ack_batch_ms: 20,
        ..timing()
    })
    .await;
    let mut q = arrived_and_bound(&mut e, IID).await;
    e.mock.send(ended(INC_A, IID, 2)).await;
    let ack = match next(&mut q).await {
        SessionEvent::Ended {
            reason: EndReason::Reported(_),
            ack,
            ..
        } => ack,
        other => panic!("expected Ended, got {other:?}"),
    };
    e.mock.send(ended(INC_A, IID, 2)).await; // TC's replay
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(q.try_recv().is_none(), "the replay isn't delivered twice");
    assert!(
        e.mock.acks().iter().all(|a| a.acked_delivery_seq < 2),
        "not acked before the token"
    );
    ack.durable();
    eventually("acked after the token", || {
        e.mock.acks().iter().any(|a| a.acked_delivery_seq == 2)
    })
    .await;
}

async fn disconnected(e: &mut Env) {
    e.clock.advance(6_001);
    eventually("Disconnected", || {
        e.client.link(&node()) == Some(Link::Disconnected)
    })
    .await;
}

#[tokio::test]
async fn a_node_is_dead_only_when_every_source_says_so_for_the_whole_window() {
    let mut e = env().await;
    let mut q = arrived_and_bound(&mut e, IID).await;
    disconnected(&mut e).await;
    assert!(matches!(next(&mut q).await, SessionEvent::Unmanaged));
    e.evidence[0].set(Verdict::Dead);
    e.evidence[1].set(Verdict::Dead);
    e.evidence[2].set(Verdict::Unknown);
    for _ in 0..4 {
        e.clock.advance(10_000);
        tokio::time::sleep(Duration::from_millis(60)).await;
    }
    assert_eq!(
        e.client.link(&node()),
        Some(Link::Disconnected),
        "an Unknown source blocks the end"
    );
    e.evidence[2].set(Verdict::Dead);
    tokio::time::sleep(Duration::from_millis(60)).await;
    e.clock.advance(29_000);
    tokio::time::sleep(Duration::from_millis(60)).await;
    assert_eq!(
        e.client.link(&node()),
        Some(Link::Disconnected),
        "29 s isn't 30 s"
    );
    e.clock.advance(2_000);
    eventually("Dead", || e.client.link(&node()) == Some(Link::Dead)).await;
    match next(&mut q).await {
        SessionEvent::Ended {
            reason: EndReason::NodeDead,
            ack,
            ..
        } => assert!(!ack.acknowledges()),
        other => panic!("expected NodeDead, got {other:?}"),
    }
    let closed = tokio::time::timeout(Duration::from_secs(2), q.recv()).await;
    assert!(matches!(closed, Ok(None)), "the mailbox closes");
}

#[tokio::test]
async fn a_node_back_from_dead_has_its_zombies_ended() {
    let mut e = env().await;
    let mut q = arrived_and_bound(&mut e, IID).await;
    disconnected(&mut e).await;
    let _ = next(&mut q).await;
    e.client.declare_node_dead(&node());
    eventually("Dead", || e.client.link(&node()) == Some(Link::Dead)).await;
    // It was a frozen VM: the call is still live on TC.
    e.mock.session(
        IID,
        Ok((
            session(IID, pb::SessionState::Active, 3, vec![]),
            INC_A.into(),
        )),
    );
    // The client reconnected after the liveness timeout; the node now answers on that stream.
    eventually("a new Watch", || e.mock.watch_calls() >= 2).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    e.mock.send(heartbeat(INC_A)).await;
    eventually("EndSession for the zombie", || {
        !e.mock.calls("EndSession").is_empty()
    })
    .await;
    let end = &e.mock.calls("EndSession")[0];
    assert_eq!(
        end.owner.as_ref().unwrap().owner_epoch,
        1042,
        "with the old owner's epoch"
    );
    eventually("counted", || {
        ie_telephony_client::stats::Stats::get(&e.client.stats().zombies_ended) == 1
    })
    .await;
}

fn add_leg() -> Intent {
    Intent::AddLeg(Box::new(ie_telephony_client::AddLeg {
        leg_id: LEG2.into(),
        role: pb::LegRole::Agent,
        destination: ie_telephony_client::Destination::Contact("sip:agent1@bin-a.example".into()),
        caller_id: ie_telephony_client::Number::new("+447700900100"),
        ring_timeout_s: 15,
        endpoint: pb::Endpoint::Webrtc,
        codec_policy: pb::CodecPolicy::G711Only,
        start_muted: false,
        monitor_mode: pb::MonitorMode::Unspecified,
        whisper_to_leg_id: String::new(),
        dtmf_actions: None,
    }))
}

#[tokio::test]
async fn an_add_leg_with_an_unknown_result_is_reconciled_before_failover() {
    let mut e = env().await;
    let _q = arrived_and_bound(&mut e, IID).await;
    for _ in 0..4 {
        e.mock.answer(
            "AddLeg",
            Answer::Err(tonic::Status::unavailable("no trailer")),
        );
    }
    e.mock.session(
        IID,
        Ok((
            session(
                IID,
                pb::SessionState::Active,
                1,
                vec![leg_snapshot(LEG2, pb::LegState::Ringing)],
            ),
            INC_A.into(),
        )),
    );
    let o = e.client.command(&node(), &owner(IID), add_leg()).await;
    assert_eq!(o, Outcome::Indeterminate(Reconciled::LegRemoved));
    let adds = e.mock.calls("AddLeg");
    assert_eq!(adds.len(), 4, "3 retries");
    assert!(
        adds.iter().all(|c| c.command_id == adds[0].command_id),
        "the same command id"
    );
    assert_eq!(
        e.mock.calls("RemoveLeg")[0].detail,
        LEG2,
        "the ringing leg is hung up before ie-core may fail over"
    );
}

#[tokio::test]
async fn an_add_leg_that_never_reached_tc_is_reconciled_as_absent() {
    let mut e = env().await;
    let _q = arrived_and_bound(&mut e, IID).await;
    for _ in 0..4 {
        e.mock.answer(
            "AddLeg",
            Answer::Err(status(tonic::Code::Unavailable, "freeswitch_unavailable")),
        );
    }
    e.mock.session(
        IID,
        Ok((
            session(IID, pb::SessionState::Active, 1, vec![]),
            INC_A.into(),
        )),
    );
    let o = e.client.command(&node(), &owner(IID), add_leg()).await;
    assert_eq!(o, Outcome::Indeterminate(Reconciled::LegAbsent));
    assert!(e.mock.calls("RemoveLeg").is_empty());
}

#[tokio::test]
async fn deadline_exceeded_is_retried_with_the_same_command_id() {
    let mut e = env().await;
    let _q = arrived_and_bound(&mut e, IID).await;
    e.mock
        .answer("AddLeg", Answer::Delay(Duration::from_millis(1_500)));
    let o = e.client.command(&node(), &owner(IID), add_leg()).await;
    assert!(matches!(o, Outcome::Done(_)), "{o:?}");
    let adds = e.mock.calls("AddLeg");
    assert!(adds.len() >= 2);
    assert_eq!(adds[0].command_id, adds[1].command_id);
}

#[tokio::test]
async fn a_full_mailbox_coalesces_into_one_resync_once_drained() {
    let mut e = env_with(Timing {
        mailbox: 4,
        low_water: 2,
        degraded_alert_ms: 10_000,
        ack_batch_ms: 20,
        ..timing()
    })
    .await;
    let mut q = arrived_and_bound(&mut e, IID).await;
    e.mock.session(
        IID,
        Ok((
            session(IID, pb::SessionState::Active, 12, vec![]),
            INC_A.into(),
        )),
    );
    for seq in 2..=12 {
        e.mock
            .send(leg(INC_A, IID, seq, pb::LegState::Answered))
            .await;
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(e.client.degraded_sessions() == 1);
    assert!(
        e.mock.calls("GetSession").is_empty(),
        "no resync while the owner is behind"
    );
    e.clock.advance(10_001);
    eventually("the degraded alert", || {
        ie_telephony_client::stats::Stats::get(&e.client.stats().degraded_alerts) == 1
    })
    .await;
    // The owner drains; below the low-water mark one resync starts, and its snapshot replaces the rest.
    let mut seen_resync = false;
    for _ in 0..8 {
        match tokio::time::timeout(Duration::from_secs(2), q.recv()).await {
            Ok(Some(SessionEvent::Resync(s))) => {
                assert_eq!(s.last_delivery_seq, 12);
                seen_resync = true;
                break;
            }
            Ok(Some(_)) => {}
            _ => break,
        }
    }
    assert!(seen_resync);
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        e.mock.calls("GetSession").len(),
        1,
        "one resync, not one per overflow"
    );
    assert_eq!(e.client.degraded_sessions(), 0);
}

#[tokio::test]
async fn session_unknown_on_a_live_rebuilt_node_ends_the_session() {
    let mut e = env().await;
    let mut q = arrived_and_bound(&mut e, IID).await;
    e.mock.send(node_status(INC_A, false)).await;
    e.mock
        .send(leg(INC_A, IID, 3, pb::LegState::Answered))
        .await; // a gap; TC forgot the session
    match next(&mut q).await {
        SessionEvent::Ended {
            reason: EndReason::SessionUnknown,
            ack,
            ..
        } => assert!(!ack.acknowledges()),
        other => panic!("expected SessionUnknown, got {other:?}"),
    }
}

#[tokio::test]
async fn session_unknown_while_the_node_rebuilds_is_retried_not_ended() {
    let mut e = env().await;
    let mut q = arrived_and_bound(&mut e, IID).await;
    e.mock.send(node_status(INC_A, true)).await;
    e.mock
        .send(leg(INC_A, IID, 3, pb::LegState::Answered))
        .await;
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert!(q.try_recv().is_none(), "not ended while TC rebuilds");
    e.mock.session(
        IID,
        Ok((
            session(IID, pb::SessionState::Active, 3, vec![]),
            INC_A.into(),
        )),
    );
    match next(&mut q).await {
        SessionEvent::Resync(_) => {}
        other => panic!("expected a resync after the rebuild, got {other:?}"),
    }
}

#[tokio::test]
async fn stale_owner_fences_the_session() {
    let mut e = env().await;
    let mut q = arrived_and_bound(&mut e, IID).await;
    e.mock.answer(
        "AnswerLeg",
        Answer::Err(status(tonic::Code::Aborted, "stale_owner")),
    );
    let o = e
        .client
        .command(&node(), &owner(IID), Intent::Answer { leg_id: LEG.into() })
        .await;
    assert_eq!(o, Outcome::Fenced);
    assert_eq!(e.mock.calls("AnswerLeg").len(), 1, "never retried");
    let closed = tokio::time::timeout(Duration::from_secs(2), q.recv()).await;
    assert!(matches!(closed, Ok(None)), "the mailbox closes");
}

#[tokio::test]
async fn the_trailer_decides_and_unimplemented_is_unsupported() {
    let mut e = env().await;
    let _q = arrived_and_bound(&mut e, IID).await;
    e.mock.answer(
        "SetHold",
        Answer::Err(tonic::Status::unimplemented("older TC")),
    );
    let o = e
        .client
        .command(
            &node(),
            &owner(IID),
            Intent::SetHold {
                on: false,
                music_prompt_id: String::new(),
            },
        )
        .await;
    assert_eq!(o, Outcome::Unsupported);
    assert_eq!(e.mock.calls("SetHold").len(), 1);
    e.mock.answer(
        "SetMute",
        Answer::Err(status(tonic::Code::Unavailable, "draining")),
    );
    let o = e
        .client
        .command(
            &node(),
            &owner(IID),
            Intent::SetMute {
                leg_id: LEG.into(),
                muted: true,
            },
        )
        .await;
    assert_eq!(
        o,
        Outcome::Capacity(pb::ErrorCode::Draining),
        "UNAVAILABLE / draining is capacity, not a retry"
    );
    assert_eq!(e.mock.calls("SetMute").len(), 1);
}

#[tokio::test]
async fn a_failed_redirect_falls_back_to_refusing_the_call() {
    let mut e = env().await;
    e.mock.send(heartbeat(INC_A)).await;
    e.mock.send(arrived(INC_A, IID, 1)).await;
    let arrival = loop {
        if let Some(ClientEvent::Arrived(a, _)) = e.events.recv().await {
            break a;
        }
    };
    e.mock.answer(
        "DeclineArrival",
        Answer::Err(status(tonic::Code::InvalidArgument, "invalid_request")),
    );
    let o = e
        .client
        .decline_arrival(
            &arrival,
            Decline::WrongBin {
                bin: "bin-z".into(),
            },
        )
        .await;
    assert!(matches!(o, Outcome::Done(_)), "{o:?}");
    let d = e.mock.calls("DeclineArrival");
    assert_eq!(d.len(), 2);
    assert_eq!(
        d[1].detail,
        format!("{}:503", pb::DeclineReason::Refused as i32)
    );
}

#[tokio::test]
async fn a_digit_of_a_closed_collection_is_not_passed_on() {
    let mut e = env().await;
    let mut q = arrived_and_bound(&mut e, IID).await;
    let c1 = "0192f6a1-9000-7000-8000-000000000001";
    let o = e
        .client
        .command(
            &node(),
            &owner(IID),
            Intent::SetDigitReporting {
                leg_id: LEG.into(),
                mode: pb::DigitReporting::On,
                collection_id: c1.into(),
                max_duration_s: 10,
            },
        )
        .await;
    assert!(matches!(o, Outcome::Done(_)));
    e.mock
        .send(digit(
            INC_A,
            IID,
            2,
            "7",
            "0192f6a1-9000-7000-8000-000000000009",
        ))
        .await;
    e.mock.send(digit(INC_A, IID, 3, "5", c1)).await;
    match next(&mut q).await {
        SessionEvent::Digit {
            collection_id,
            digit,
            ..
        } => {
            assert_eq!(collection_id, c1);
            assert_eq!(digit.expose(), "5");
        }
        other => panic!("{other:?}"),
    }
}

/// Fix 1: a command whose error needs TC's view joins the session's resync in flight; one
/// `GetSession`, and the command gets that snapshot.
#[tokio::test]
async fn a_command_error_resync_joins_the_one_in_flight() {
    let mut e = env().await;
    let mut q = arrived_and_bound(&mut e, IID).await;
    e.mock.get_answer(
        // Slow, but well within GetSession's 300 ms deadline.
        Duration::from_millis(150),
        Ok((
            session(IID, pb::SessionState::Active, 4, vec![]),
            INC_A.into(),
        )),
    );
    e.mock
        .send(leg(INC_A, IID, 3, pb::LegState::Answered))
        .await; // a gap: the worker's resync
    eventually("the worker's GetSession", || {
        e.mock.calls("GetSession").len() == 1
    })
    .await;
    e.mock.answer(
        "AnswerLeg",
        Answer::Err(status(tonic::Code::FailedPrecondition, "invalid_state")),
    );
    let o = e
        .client
        .command(&node(), &owner(IID), Intent::Answer { leg_id: LEG.into() })
        .await;
    match o {
        Outcome::Resync(s) => assert_eq!(s.last_delivery_seq, 4),
        other => panic!(
            "expected the joined resync, got {other:?}; GetSession calls {:?}; AnswerLeg calls {}",
            e.mock.calls("GetSession").len(),
            e.mock.calls("AnswerLeg").len()
        ),
    }
    assert_eq!(
        e.mock.calls("GetSession").len(),
        1,
        "joined, not a second resync"
    );
    assert!(matches!(next(&mut q).await, SessionEvent::Resync(_)));
}

/// Fix 1: two resyncs racing, the older one finishing last, never leave the owner an older view.
#[tokio::test]
async fn two_racing_resyncs_never_leave_an_older_view() {
    let mut e = env().await;
    let mut q = arrived_and_bound(&mut e, IID).await;
    // The first GetSession is slow and older; a second one would be fast and newer.
    e.mock.get_answer(
        Duration::from_millis(150),
        Ok((
            session(IID, pb::SessionState::Active, 3, vec![]),
            INC_A.into(),
        )),
    );
    e.mock.get_answer(
        Duration::ZERO,
        Ok((
            session(IID, pb::SessionState::Active, 5, vec![]),
            INC_A.into(),
        )),
    );
    e.mock
        .send(leg(INC_A, IID, 3, pb::LegState::Answered))
        .await; // a gap
    eventually("the worker's GetSession", || {
        e.mock.calls("GetSession").len() == 1
    })
    .await;
    e.mock.answer(
        "AnswerLeg",
        Answer::Err(status(tonic::Code::FailedPrecondition, "invalid_state")),
    );
    let o = e
        .client
        .command(&node(), &owner(IID), Intent::Answer { leg_id: LEG.into() })
        .await;
    // What the owner saw, in order: the command's snapshot, then the mailbox's.
    let mut seen = Vec::new();
    if let Outcome::Resync(s) = &o {
        seen.push(s.last_delivery_seq);
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    while let Some(ev) = q.try_recv() {
        if let SessionEvent::Resync(s) = ev {
            seen.push(s.last_delivery_seq);
        }
    }
    let served = e.mock.served();
    assert!(!seen.is_empty());
    assert!(
        seen.windows(2).all(|w| w[0] <= w[1]),
        "an older snapshot reached the owner after a newer one: {seen:?} (TC served {served:?})"
    );
    assert_eq!(
        seen.last().copied(),
        served.iter().max().copied(),
        "the owner's final view is the newest snapshot TC served (TC served {served:?})"
    );
}

/// Fix 2: a snapshot of an incarnation the stream has left is asked for again at once, whatever
/// the two incarnations' string order.
#[tokio::test]
async fn a_snapshot_of_a_past_incarnation_is_asked_for_again() {
    let mut e = env().await;
    // The node's first incarnation sorts after its second.
    let inc_z = "0192f700-0000-7000-8000-0000000000ff";
    e.mock.send(heartbeat(inc_z)).await;
    e.mock.send(arrived(inc_z, IID, 1)).await;
    let mut q = loop {
        if let Some(ClientEvent::Arrived(_, q)) = e.events.recv().await {
            break q;
        }
    };
    assert!(e.client.bind(&node(), &owner(IID)));
    e.mock.get_answer(
        Duration::ZERO,
        Ok((
            session(IID, pb::SessionState::Active, 1, vec![]),
            inc_z.into(),
        )),
    );
    e.mock.get_answer(
        Duration::ZERO,
        Ok((
            session(IID, pb::SessionState::Active, 2, vec![]),
            INC_A.into(),
        )),
    );
    e.mock.send(heartbeat(INC_A)).await; // a new incarnation: resync
    match tokio::time::timeout(Duration::from_secs(2), q.recv()).await {
        Ok(Some(SessionEvent::Resync(s))) => assert_eq!(s.last_delivery_seq, 2),
        other => panic!("expected the current incarnation's snapshot within 2 s, got {other:?}"),
    }
}

/// 0.2.3: every attempt carries its own absolute expiry (send time + the RPC's deadline), and a
/// retry gets a fresh one with the same command id.
#[tokio::test]
async fn every_attempt_carries_a_fresh_expiry() {
    let mut e = env().await;
    let _q = arrived_and_bound(&mut e, IID).await;
    e.mock.answer(
        "AddLeg",
        Answer::Err(tonic::Status::unavailable("no trailer")),
    );
    let o = e.client.command(&node(), &owner(IID), add_leg()).await;
    assert!(matches!(o, Outcome::Done(_)), "{o:?}");
    let ex = e.mock.expiries("AddLeg");
    assert_eq!(ex.len(), 2);
    for (expires, arrived) in &ex {
        let expires = expires.expect("the expiry header");
        let left = expires - arrived;
        assert!(
            (800..=1_000).contains(&left),
            "AddLeg's 1 s deadline, from the send: {left} ms left on arrival"
        );
    }
    assert!(ex[1].0 > ex[0].0, "a fresh expiry for the retry");
    let adds = e.mock.calls("AddLeg");
    assert_eq!(
        adds[0].command_id, adds[1].command_id,
        "the same command id"
    );
}

/// 0.2.3: `ATTEMPT_EXPIRED` (DEADLINE_EXCEEDED / attempt_expired) means this attempt did
/// nothing: it is retried like any attempt, with a fresh expiry and the same command id.
#[tokio::test]
async fn an_expired_attempt_is_retried_with_a_fresh_expiry() {
    let mut e = env().await;
    let _q = arrived_and_bound(&mut e, IID).await;
    e.mock.answer(
        "AddLeg",
        Answer::Err(status(tonic::Code::DeadlineExceeded, "attempt_expired")),
    );
    let o = e.client.command(&node(), &owner(IID), add_leg()).await;
    assert!(matches!(o, Outcome::Done(_)), "{o:?}");
    let adds = e.mock.calls("AddLeg");
    assert_eq!(adds.len(), 2);
    assert_eq!(adds[0].command_id, adds[1].command_id);
    let ex = e.mock.expiries("AddLeg");
    assert!(ex[1].0 > ex[0].0);
}

/// 0.2.3: "leg absent" is trusted only once every attempt's expiry plus TC's 250 ms skew budget
/// has passed: the reconciling GetSession comes no earlier.
#[tokio::test]
async fn leg_absent_is_read_only_after_every_attempt_has_expired() {
    let mut e = env().await;
    let _q = arrived_and_bound(&mut e, IID).await;
    for _ in 0..4 {
        e.mock.answer(
            "AddLeg",
            Answer::Err(status(tonic::Code::Unavailable, "freeswitch_unavailable")),
        );
    }
    e.mock.session(
        IID,
        Ok((
            session(IID, pb::SessionState::Active, 1, vec![]),
            INC_A.into(),
        )),
    );
    let o = e.client.command(&node(), &owner(IID), add_leg()).await;
    assert_eq!(o, Outcome::Indeterminate(Reconciled::LegAbsent));
    let last_expiry = e
        .mock
        .expiries("AddLeg")
        .iter()
        .filter_map(|x| x.0)
        .max()
        .unwrap();
    let get = e.mock.expiries("GetSession");
    assert!(
        get[0].1 >= last_expiry + 250,
        "GetSession at {} before the last expiry {} + 250 ms",
        get[0].1,
        last_expiry
    );
}

/// 0.2.3: a node whose heartbeats show its clock off by more than 100 ms raises an alert.
#[tokio::test]
async fn a_clock_offset_above_100_ms_raises_an_alert() {
    let e = env().await;
    let mut hb = heartbeat(INC_A);
    hb.at_ms = now_ms() + 500;
    e.mock.send(hb).await;
    eventually("the skew alert", || {
        ie_telephony_client::stats::Stats::get(&e.client.stats().clock_skew_alerts) == 1
    })
    .await;
}
