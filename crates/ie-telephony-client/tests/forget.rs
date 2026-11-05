//! IE-022: the client forgets a session once nothing is left to do for it (its end acked; a
//! declined, abandoned or orphan session over), keeps its key a short while to drop late
//! duplicates, and never walks every session on a timer. Against the in-process TC.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod mock;

use ie_telephony_client::{
    Arrival, ClientConfig, ClientEvent, Decline, Held, ManualClock, NodeConfig, NodeEvidence,
    NodeId, Outcome, Owner, Profile, SessionEvent, SessionEvents, SessionKey, Telephony,
    TelephonyClient, Timing, stats::Stats,
};
use mock::*;
use parity_proto::telephony::v1 as pb;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

struct Env {
    mock: Mock,
    client: TelephonyClient,
    events: mpsc::Receiver<ClientEvent>,
    clock: Arc<ManualClock>,
}

fn timing() -> Timing {
    Timing {
        // The manual clock moves only when a test says so; no liveness drop when it jumps.
        liveness_ms: 3_600_000,
        reconnect_min_ms: 20,
        reconnect_max_ms: 100,
        ack_batch_ms: 10,
        ..Timing::default()
    }
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
        timing: timing(),
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

fn iid(i: usize) -> String {
    format!("0192f6a1-7c3e-7b21-9a4d-{i:012x}")
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

/// The next arrival and its mailbox (other client events are skipped).
async fn arrival(e: &mut Env) -> (Box<Arrival>, SessionEvents) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match e.events.recv().await {
                Some(ClientEvent::Arrived(a, q)) => break (a, q),
                Some(_) => continue,
                None => panic!("no events"),
            }
        }
    })
    .await
    .expect("an arrival")
}

async fn ended_ack(q: &mut SessionEvents) -> ie_telephony_client::EndedAck {
    loop {
        match tokio::time::timeout(Duration::from_secs(5), q.recv()).await {
            Ok(Some(SessionEvent::Ended { ack, .. })) => return ack,
            Ok(Some(_)) => continue,
            other => panic!("expected Ended, got {other:?}"),
        }
    }
}

/// No `Orphan` among the client events that came so far.
fn no_orphan(e: &mut Env) {
    while let Ok(ev) = e.events.try_recv() {
        assert!(
            !matches!(ev, ClientEvent::Orphan { .. }),
            "an orphan: {ev:?}"
        );
    }
}

fn held(e: &Env) -> Held {
    e.client.held()
}

/// 100,000 calls (arrive, claim, end, ENDED record, ack): what the client holds stays at the
/// calls in flight plus a capped set of keys; per-call delivery time doesn't grow with the calls
/// already seen. Before IE-022 every one of them stayed, and every 50 ms tick walked them all.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_hundred_thousand_calls_leave_only_a_capped_set_of_keys() {
    let mut e = env().await;
    const CALLS: usize = 100_000;
    const CHUNK: usize = 500;
    let mut chunk_ms: Vec<f64> = Vec::new();
    let mut max_held = 0usize;
    for c in 0..CALLS / CHUNK {
        let t0 = Instant::now();
        for i in c * CHUNK..(c + 1) * CHUNK {
            e.mock.send(arrived(INC_A, &iid(i), 1)).await;
        }
        let mut mailboxes: HashMap<String, SessionEvents> = HashMap::new();
        while mailboxes.len() < CHUNK {
            let (a, q) = arrival(&mut e).await;
            let id = a.key.interaction_id.clone();
            assert!(e.client.bind(&node(), &owner(&id)));
            mailboxes.insert(id, q);
        }
        for id in mailboxes.keys() {
            e.mock.send(ended(INC_A, id, 2)).await;
        }
        for (_, mut q) in mailboxes.drain() {
            ended_ack(&mut q).await.durable();
            // The call task ends: its mailbox goes.
        }
        chunk_ms.push(t0.elapsed().as_secs_f64() * 1000.0);
        max_held = max_held.max(held(&e).sessions);
        eventually("the chunk's ends acked and forgotten", || {
            held(&e).sessions == 0
        })
        .await;
    }
    let h = held(&e);
    assert_eq!(h.sessions, 0);
    assert_eq!(h.acks_queued, 0);
    assert_eq!(h.forgotten, Timing::default().forgotten_max, "capped");
    assert!(max_held <= CHUNK, "at most the calls in flight: {max_held}");
    assert_eq!(
        Stats::get(&e.client.stats().sessions_forgotten),
        CALLS as u64
    );
    // Every end was acked once, with its owner's epoch.
    let acks = e.mock.acks();
    assert!(acks.iter().filter(|a| a.acked_delivery_seq == 2).count() >= CALLS);
    // Delivery time per chunk: the last tenth no slower than the first tenth (generous: a
    // shared host), where before IE-022 each tick's walk grew with every call ever seen.
    let tenth = chunk_ms.len() / 10;
    let median = |v: &[f64]| {
        let mut v = v.to_vec();
        v.sort_by(f64::total_cmp);
        v[v.len() / 2]
    };
    let (first, last) = (
        median(&chunk_ms[..tenth]),
        median(&chunk_ms[chunk_ms.len() - tenth..]),
    );
    let rss_kib = std::fs::read_to_string("/proc/self/statm")
        .ok()
        .and_then(|s| s.split_whitespace().nth(1)?.parse::<u64>().ok())
        .map_or(0, |pages| pages * 4);
    println!(
        "chunk of {CHUNK} calls: first tenth median {first:.1} ms, last tenth {last:.1} ms; held max {max_held}; test process RSS {rss_kib} KiB"
    );
    assert!(
        last <= first * 3.0 + 50.0,
        "delivery slowed: {first:.1} ms -> {last:.1} ms"
    );
    // The keys go with their window.
    e.clock.advance(Timing::default().forget_after_ms + 1);
    eventually("forgotten keys expired", || held(&e).forgotten == 0).await;
}

/// IE-016: the end of an arrival this replica declined is swallowed, not an orphan.
#[tokio::test]
async fn a_declined_arrivals_late_end_is_dropped_not_an_orphan() {
    let mut e = env().await;
    e.mock.send(arrived(INC_A, IID, 1)).await;
    let (a, q) = arrival(&mut e).await;
    let o = e
        .client
        .decline_arrival(&a, Decline::Refused { sip_status: 404 })
        .await;
    assert!(matches!(o, Outcome::Done(_)), "{o:?}");
    drop(q);
    e.mock.send(ended(INC_A, IID, 2)).await;
    e.mock.send(ended(INC_A, IID, 2)).await; // TC's replay
    e.mock.send(heartbeat(INC_A)).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    no_orphan(&mut e);
    assert_eq!(
        held(&e),
        Held {
            sessions: 0,
            forgotten: 1,
            acks_queued: 0
        }
    );
}

/// An arrival whose task ends without a claim, and an orphan whose end comes, are forgotten.
#[tokio::test]
async fn an_abandoned_arrival_and_an_ended_orphan_are_forgotten() {
    let mut e = env().await;
    e.mock.send(arrived(INC_A, IID, 1)).await;
    let (_, q) = arrival(&mut e).await;
    assert_eq!(held(&e).sessions, 1);
    drop(q);
    eventually("the abandoned arrival forgotten", || held(&e).sessions == 0).await;
    // Events of a session this replica doesn't hold: an orphan, held until its end.
    e.mock
        .send(leg(INC_A, IID2, 5, pb::LegState::Answered))
        .await;
    eventually("the orphan", || held(&e).sessions == 1).await;
    e.mock.send(ended(INC_A, IID2, 6)).await;
    eventually("the orphan forgotten at its end", || held(&e).sessions == 0).await;
    assert_eq!(held(&e).forgotten, 2);
    // A replay of either is dropped, and a command for a forgotten session makes no entry.
    e.mock.send(ended(INC_A, IID2, 6)).await;
    e.mock.send(arrived(INC_A, IID, 1)).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(held(&e).sessions, 0);
    let o = e
        .client
        .command(
            &node(),
            &owner(IID2),
            ie_telephony_client::Intent::Ring { leg_id: LEG.into() },
        )
        .await;
    assert_eq!(o, Outcome::Lost);
    assert_eq!(held(&e).sessions, 0);
}

/// An owned session is held until TC took the ack of its redeemed end, even past its owner's
/// end; a replay of that end on a new incarnation (TC lost the ack) is acked again, no orphan.
#[tokio::test]
async fn an_owned_session_is_held_until_its_end_is_acked_and_a_replayed_end_is_acked_again() {
    let mut e = env().await;
    e.mock.send(arrived(INC_A, IID, 1)).await;
    let (_, mut q) = arrival(&mut e).await;
    assert!(e.client.bind(&node(), &owner(IID)));
    e.mock.send(ended(INC_A, IID, 2)).await;
    let ack = ended_ack(&mut q).await;
    // Unredeemed and abandoned (the ENDED record failed): nothing to ack; it waits for nothing.
    for _ in 0..50 {
        e.mock.answer(
            "Acknowledge",
            Answer::Err(status(tonic::Code::Unavailable, "internal")),
        );
    }
    ack.durable();
    drop(q);
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(
        held(&e).sessions,
        1,
        "held while its redeemed end's ack fails"
    );
    // TC takes acks again.
    e.mock.0.lock().unwrap().answers.clear();
    eventually("acked and forgotten", || held(&e).sessions == 0).await;
    assert!(
        e.mock
            .acks()
            .iter()
            .any(|a| a.interaction_id == IID && a.acked_delivery_seq == 2 && a.owner_epoch == 1042)
    );
    // TC restarted and lost the ack: its rebuild replays the end on the new incarnation.
    e.mock.send(heartbeat(INC_B)).await;
    e.mock.send(ended(INC_B, IID, 1)).await;
    eventually("the replayed end acked again", || {
        e.mock.acks().iter().any(|a| {
            a.interaction_id == IID
                && a.acked_incarnation == INC_B
                && a.acked_delivery_seq == 1
                && a.owner_epoch == 1042
        })
    })
    .await;
    assert_eq!(Stats::get(&e.client.stats().ends_acked_again), 1);
    no_orphan(&mut e);
    assert_eq!(held(&e).sessions, 0);
}

/// An owned session whose owner gave up without an ENDED record is forgotten at its end, unacked
/// (TC keeps the tombstone for an adopter, step 8).
#[tokio::test]
async fn an_owned_session_abandoned_unredeemed_is_forgotten_at_its_end_unacked() {
    let mut e = env().await;
    e.mock.send(arrived(INC_A, IID, 1)).await;
    let (_, q) = arrival(&mut e).await;
    assert!(e.client.bind(&node(), &owner(IID)));
    drop(q);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(held(&e).sessions, 1, "live: still held");
    e.mock.send(ended(INC_A, IID, 2)).await;
    eventually("forgotten at its end", || held(&e).sessions == 0).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        e.mock
            .acks()
            .iter()
            .all(|a| a.interaction_id != IID || a.acked_delivery_seq < 2),
        "an unredeemed end is never acked"
    );
}

/// Step 6 (`kill -9` of IE mid-call, then a restart under the same replica id): the end of a
/// session this process never held but whose owner was this replica id (TC's fence > 0) is
/// handed over as `OrphanEnded`, held until its owner redeems it (its CDR closed), acked with
/// TC's fence and this replica id, then forgotten. An unowned orphan's end is not.
#[tokio::test]
async fn an_owned_orphans_end_is_handed_over_and_acked_with_tcs_fence_once_redeemed() {
    let mut e = env().await;
    let mut m = leg(INC_A, IID2, 5, pb::LegState::Answered);
    m.session.as_mut().unwrap().owner_epoch = 1042;
    e.mock.send(m).await;
    let mut end = ended(INC_A, IID2, 6);
    end.session.as_mut().unwrap().owner_epoch = 1042;
    e.mock.send(end).await;
    let (key, epoch, ack) = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match e.events.recv().await {
                Some(ClientEvent::OrphanEnded {
                    key,
                    owner_epoch,
                    ack,
                    ..
                }) => break (key, owner_epoch, ack),
                Some(_) => continue,
                None => panic!("no events"),
            }
        }
    })
    .await
    .expect("an OrphanEnded");
    assert_eq!((key.interaction_id.as_str(), epoch), (IID2, 1042));
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(held(&e).sessions, 1, "held until its owner redeems the end");
    assert!(
        !e.mock
            .acks()
            .iter()
            .any(|a| a.interaction_id == IID2 && a.acked_delivery_seq >= 6),
        "not acked before the CDR is closed"
    );
    ack.durable();
    eventually("the end acked with TC's fence", || {
        e.mock.acks().iter().any(|a| {
            a.interaction_id == IID2
                && a.owner_epoch == 1042
                && a.acked_delivery_seq == 6
                && a.replica_id == "ie-bin-a-1"
        })
    })
    .await;
    eventually("forgotten once acked", || held(&e).sessions == 0).await;
    // An unowned orphan's end (fence 0) is no OrphanEnded.
    e.mock.send(ended(INC_A, IID, 3)).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    while let Ok(ev) = e.events.try_recv() {
        assert!(!matches!(ev, ClientEvent::OrphanEnded { .. }), "{ev:?}");
    }
}

/// CONVENTIONS (0.10.3): IE sends an agent leg's DTMF codes only once the node's `NodeStatus`
/// lists `dtmf_actions`.
#[tokio::test]
async fn a_capability_counts_once_the_nodes_status_lists_it() {
    let e = env().await;
    assert!(!e.client.has_capability(&node(), "dtmf_actions"));
    let mut m = node_status(INC_A, false);
    if let Some(pb::watch_response::Event::NodeStatus(s)) = m.event.as_mut() {
        s.capabilities = vec!["dtmf_actions".into(), "recording_null_sink".into()];
    }
    e.mock.send(m).await;
    eventually("the capability", || {
        e.client.has_capability(&node(), "dtmf_actions")
    })
    .await;
    assert!(e.client.has_capability(&node(), "recording_null_sink"));
    assert!(!e.client.has_capability(&node(), "opus"));
    assert!(
        !e.client
            .has_capability(&NodeId("media-9".into()), "dtmf_actions")
    );
}
