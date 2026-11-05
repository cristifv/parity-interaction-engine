//! Soak-blockers round 1, against the in-process TC: a `session_ended` answer closes a held
//! session from its tombstone when its end never reached the owner (IE-049); `reconcile` asks TC
//! about an ending session (IE-050); while arrivals aren't taken, an offered arrival is neither
//! handed out nor acked, and an offer once they are taken again is a new arrival (IE-055).
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod mock;

use ie_telephony_client::{
    ClientConfig, ClientEvent, Intent, ManualClock, NodeConfig, NodeEvidence, NodeId, Outcome,
    Owner, Profile, Reconcile, SessionEvent, SessionEvents, SessionKey, Telephony, TelephonyClient,
    Timing, stats::Stats,
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
}

async fn env() -> Env {
    let mock = Mock::default();
    let uri = mock.serve().await;
    let clock = Arc::new(ManualClock::new());
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
        timing: Timing {
            reconnect_min_ms: 20,
            reconnect_max_ms: 100,
            ack_batch_ms: 10,
            ..Timing::default()
        },
    };
    let started = TelephonyClient::start(
        cfg,
        Arc::clone(&clock) as _,
        Vec::<Arc<dyn NodeEvidence>>::new(),
    )
    .unwrap();
    let e = Env {
        mock,
        client: started.client,
        events: started.events,
        clock,
    };
    eventually("the first Watch", || e.mock.watch_calls() >= 1).await;
    e.mock.send(heartbeat(INC_A)).await;
    e
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
    for _ in 0..500 {
        if f() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("never: {what}");
}

async fn arrival(e: &mut Env) -> SessionEvents {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match e.events.recv().await {
                Some(ClientEvent::Arrived(_, q)) => break q,
                Some(_) => continue,
                None => panic!("no events"),
            }
        }
    })
    .await
    .expect("an arrival")
}

async fn next(q: &mut SessionEvents) -> SessionEvent {
    tokio::time::timeout(Duration::from_secs(5), q.recv())
        .await
        .expect("an event in time")
        .expect("the mailbox is open")
}

fn acked(e: &Env, iid: &str, seq: u64) -> bool {
    e.mock
        .acks()
        .iter()
        .any(|a| a.interaction_id == iid && a.acked_delivery_seq == seq && a.owner_epoch == 1042)
}

/// IE-049: TC ended the session with no owner and sent its single end elsewhere; this replica
/// claimed it in etcd and its first command gets `session_ended`. The client closes it from the
/// tombstone: Resync, then Ended with a token whose redemption acks the end.
#[tokio::test]
async fn a_session_ended_answer_closes_a_held_session_from_its_tombstone() {
    let mut e = env().await;
    e.mock.send(arrived(INC_A, IID, 1)).await;
    let mut q = arrival(&mut e).await;
    assert!(e.client.bind(&node(), &owner(IID)));
    e.mock.answer(
        "AnswerLeg",
        Answer::Err(status(tonic::Code::FailedPrecondition, "session_ended")),
    );
    e.mock.session(
        IID,
        Ok((
            session(IID, pb::SessionState::Ended, 3, vec![]),
            INC_A.into(),
        )),
    );
    let o = e
        .client
        .command(&node(), &owner(IID), Intent::Answer { leg_id: LEG.into() })
        .await;
    assert!(matches!(o, Outcome::Ended(_)), "{o:?}");
    assert!(matches!(next(&mut q).await, SessionEvent::Resync(s) if s.last_delivery_seq == 3));
    let ack = match next(&mut q).await {
        SessionEvent::Ended { ack, .. } => ack,
        other => panic!("expected Ended, got {other:?}"),
    };
    assert!(ack.acknowledges());
    assert!(!acked(&e, IID, 3), "not before the ENDED record");
    ack.durable();
    drop(q);
    eventually("the tombstone's end acked", || acked(&e, IID, 3)).await;
    eventually("forgotten", || e.client.held().sessions == 0).await;
}

/// IE-049: a session whose end already reached its owner gets no second end.
#[tokio::test]
async fn a_session_ended_answer_after_the_end_came_adds_nothing() {
    let mut e = env().await;
    e.mock.send(arrived(INC_A, IID, 1)).await;
    let mut q = arrival(&mut e).await;
    assert!(e.client.bind(&node(), &owner(IID)));
    e.mock.send(ended(INC_A, IID, 2)).await;
    assert!(matches!(next(&mut q).await, SessionEvent::Ended { .. }));
    e.mock.answer(
        "AnswerLeg",
        Answer::Err(status(tonic::Code::FailedPrecondition, "session_ended")),
    );
    e.mock.session(
        IID,
        Ok((
            session(IID, pb::SessionState::Ended, 2, vec![]),
            INC_A.into(),
        )),
    );
    let o = e
        .client
        .command(&node(), &owner(IID), Intent::Answer { leg_id: LEG.into() })
        .await;
    assert!(matches!(o, Outcome::Ended(_)));
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(q.try_recv().is_none(), "no second end");
}

/// IE-050: `reconcile` asks TC. ENDED: the end goes to the mailbox from the tombstone. Live: said
/// so. Unmanaged node: unreachable, at once.
#[tokio::test]
async fn reconcile_closes_from_the_tombstone_reports_live_and_fails_fast_when_unmanaged() {
    let mut e = env().await;
    e.mock.send(arrived(INC_A, IID, 1)).await;
    let mut q = arrival(&mut e).await;
    assert!(e.client.bind(&node(), &owner(IID)));
    e.mock.session(
        IID,
        Ok((
            session(IID, pb::SessionState::Active, 1, vec![]),
            INC_A.into(),
        )),
    );
    assert_eq!(
        e.client.reconcile(&node(), &owner(IID)).await,
        Reconcile::Live
    );
    e.mock.session(
        IID,
        Ok((
            session(IID, pb::SessionState::Ended, 4, vec![]),
            INC_A.into(),
        )),
    );
    assert_eq!(
        e.client.reconcile(&node(), &owner(IID)).await,
        Reconcile::Ended
    );
    // The Live answer's resync-free read left nothing queued; the ENDED one: Resync, then Ended.
    assert!(matches!(next(&mut q).await, SessionEvent::Resync(_)));
    assert!(matches!(next(&mut q).await, SessionEvent::Ended { .. }));
    // The partition: the stream goes silent.
    e.clock.advance(6_001);
    eventually("unmanaged", || {
        e.client.link(&node()) == Some(ie_telephony_client::Link::Disconnected)
    })
    .await;
    let t = std::time::Instant::now();
    assert_eq!(
        e.client.reconcile(&node(), &owner(IID2)).await,
        Reconcile::Unreachable
    );
    assert!(t.elapsed() < Duration::from_millis(100));
}

/// IE-055: while arrivals aren't taken, an offered one is neither handed out nor acked (TC offers
/// it to another replica after 1 s), and its end is no orphan; offered again once they are taken,
/// it is a new arrival, acked as usual.
#[tokio::test]
async fn an_arrival_offered_while_none_are_taken_is_left_unacked_and_taken_when_offered_again() {
    let mut e = env().await;
    e.client.accept_arrivals(false);
    e.mock.send(arrived(INC_A, IID, 1)).await;
    e.mock.send(arrived(INC_A, IID, 1)).await; // TC's 2 s redelivery
    tokio::time::sleep(Duration::from_millis(150)).await;
    while let Ok(ev) = e.events.try_recv() {
        assert!(
            !matches!(ev, ClientEvent::Arrived(..)),
            "nothing handed out"
        );
    }
    assert!(
        e.mock.acks().iter().all(|a| a.interaction_id != IID),
        "not acked"
    );
    assert_eq!(e.client.held().sessions, 0);
    assert_eq!(Stats::get(&e.client.stats().arrivals_not_taken), 1);
    // Taken again: TC's next offer of it (a redelivery or a later attempt) is a new arrival.
    e.client.accept_arrivals(true);
    e.mock.send(arrived(INC_A, IID, 1)).await;
    let _q = arrival(&mut e).await;
    eventually("the arrival acked", || {
        e.mock
            .acks()
            .iter()
            .any(|a| a.interaction_id == IID && a.acked_delivery_seq == 1 && a.owner_epoch == 0)
    })
    .await;
    // Another one, not taken, whose ownerless end then comes: no orphan.
    e.client.accept_arrivals(false);
    e.mock.send(arrived(INC_A, IID2, 1)).await;
    e.mock.send(ended(INC_A, IID2, 2)).await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    while let Ok(ev) = e.events.try_recv() {
        assert!(
            !matches!(ev, ClientEvent::Orphan { .. } | ClientEvent::Arrived(..)),
            "{ev:?}"
        );
    }
}
