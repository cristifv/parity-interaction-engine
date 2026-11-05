//! The routing client against an in-process double (no routing engine exists yet): fencing,
//! tenant metadata, retries, the Watch checks, acks after processing, reconnects, supersession,
//! and messages after a call is released.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod double;

use double::{Double, Seen, error};
use ie_routing_client::{
    ClientEvent, Config, DequeueReason, Enqueue, EnqueueReason, Link, Outcome, Owner, Recorded,
    RouteError, RoutingClient, RoutingEvent,
};
use parity_proto::routing::v1 as pb;
use std::time::Duration;
use tokio::sync::mpsc;
use tonic::Code;

const I: &str = "0192f6a1-7c3e-7b21-9a4d-5e6f70819203";
const I2: &str = "0192f6a1-7c3e-7b21-9a4d-5e6f70819204";
const E: &str = "0192f6a1-9000-7aaa-8bbb-000000000001";
const R: &str = "0192f6a1-9100-7ccc-8ddd-000000000001";

fn owner(tenant: &str, iid: &str, epoch: u64) -> Owner {
    Owner {
        tenant_id: tenant.into(),
        interaction_id: iid.into(),
        owner_epoch: epoch,
    }
}

fn enqueue() -> Enqueue {
    Enqueue {
        enqueue_seq: 1,
        queue_id: "sales".into(),
        priority: 0,
        first_queued_at_ms: 1_792_800_005_000,
        ordering_at_ms: 1_792_800_005_000,
        reason: EnqueueReason::New,
        language: "en-GB".into(),
    }
}

fn entry() -> pb::EnqueueResponse {
    pb::EnqueueResponse {
        entry: Some(pb::QueueEntry {
            entry_id: E.into(),
            ..Default::default()
        }),
        existing: false,
    }
}

async fn start() -> (Double, RoutingClient, mpsc::Receiver<ClientEvent>) {
    let d = Double::default();
    let uri = d.serve().await;
    let mut cfg = Config::new(&uri, "ie-bin-a-1", "bin-a");
    cfg.reconnect_min = Duration::from_millis(20);
    cfg.reconnect_max = Duration::from_millis(50);
    let s = RoutingClient::start(cfg).unwrap();
    // The stream opens; the double sends a heartbeat first, as RE does.
    wait(|| d.streams() == 1).await;
    d.push(heartbeat(7)).await;
    wait(|| s.client.link() == Link::Live).await;
    (d, s.client, s.events)
}

async fn wait(mut f: impl FnMut() -> bool) {
    for _ in 0..200 {
        if f() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("timed out");
}

fn heartbeat(leader: u64) -> pb::WatchResponse {
    pb::WatchResponse {
        leader_epoch: leader,
        delivery_seq: 0,
        event: Some(pb::watch_response::Event::Heartbeat(pb::Heartbeat {
            at_ms: 1,
        })),
    }
}

fn assigned(tenant: &str, iid: &str, epoch: u64, leader: u64, seq: u64) -> pb::WatchResponse {
    pb::WatchResponse {
        leader_epoch: leader,
        delivery_seq: seq,
        event: Some(pb::watch_response::Event::Assigned(pb::Assigned {
            target: Some(pb::EventTarget {
                tenant_id: tenant.into(),
                interaction_id: iid.into(),
                owner_epoch: epoch,
            }),
            reservation_id: R.into(),
            entry_id: E.into(),
            agent_id: "agent-a1".into(),
            agent_contact: "sip:agent-a1@acme.agents.example".into(),
            deadline_ms: 1_792_800_060_000,
            consult: false,
        })),
    }
}

async fn next(rx: &mut mpsc::Receiver<ie_routing_client::Delivery>) -> ie_routing_client::Delivery {
    tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("a delivery")
        .expect("the mailbox is open")
}

#[tokio::test]
async fn an_assigned_reaches_the_bound_call_and_is_acknowledged_after_processing() {
    ie_obs::testing::capture_to_file("routing_client");
    let (d, c, _ev) = start().await;
    let o = owner("acme", I, 1042);
    let mut mb = c.bind(&o);
    d.push(assigned("acme", I, 1042, 7, 1)).await;
    let got = next(&mut mb).await;
    assert_eq!(got.delivery_seq, 1);
    assert!(
        matches!(got.event, RoutingEvent::Assigned { ref agent_id, .. } if agent_id == "agent-a1")
    );
    // Not acknowledged until the call says its machine took it.
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(!d.seen().iter().any(|s| matches!(s, Seen::Acknowledge(_))));
    c.ack(&o, 1);
    wait(|| {
        d.seen().iter().any(|s| {
            matches!(s, Seen::Acknowledge(a)
        if a.acks.len() == 1 && a.acks[0].acked_delivery_seq == 1
            && a.acks[0].tenant_id == "acme" && a.acks[0].owner_epoch == 1042
            && a.acks[0].replica_id == "ie-bin-a-1")
        })
    })
    .await;
    // The interaction's next request carries it too, with the owner's tenant as tenant-id.
    c.dequeue(&o, E, DequeueReason::Abandoned).await.unwrap();
    let dq = d
        .seen()
        .into_iter()
        .find_map(|s| match s {
            Seen::Dequeue(t, r) => Some((t, r)),
            _ => None,
        })
        .unwrap();
    assert_eq!(dq.0.as_deref(), Some("acme"));
    let ow = dq.1.owner.unwrap();
    assert_eq!(
        (
            ow.owner_epoch,
            ow.acked_delivery_seq,
            ow.replica_id.as_str()
        ),
        (1042, 1, "ie-bin-a-1")
    );
    assert_eq!(dq.1.reason, pb::DequeueReason::Abandoned as i32);
}

#[tokio::test]
async fn another_tenants_older_epochs_and_duplicates_are_dropped() {
    ie_obs::testing::capture_to_file("routing_client");
    let (d, c, _ev) = start().await;
    let o = owner("acme", I, 1042);
    let mut mb = c.bind(&o);
    // (A higher leader epoch is a leader change: IE-059's test, below.)
    d.push(assigned("acme", I, 1042, 6, 1)).await; // an older leader
    d.push(assigned("globex", I, 1042, 7, 1)).await; // another tenant's
    d.push(assigned("acme", I, 1041, 7, 1)).await; // an older owner's
    d.push(assigned("acme", I, 1042, 7, 1)).await; // the one
    d.push(assigned("acme", I, 1042, 7, 1)).await; // redelivered before the ack
    let got = next(&mut mb).await;
    assert_eq!(got.delivery_seq, 1);
    wait(|| c.dropped().duplicate == 1).await;
    let dropped = c.dropped();
    assert_eq!((dropped.tenant_mismatch, dropped.stale), (1, 2));
    assert!(mb.try_recv().is_err(), "only one message reached the call");
}

#[tokio::test]
async fn requests_retry_unavailable_with_the_same_body_and_classify_refusals() {
    ie_obs::testing::capture_to_file("routing_client");
    let (d, c, _ev) = start().await;
    let o = owner("acme", I, 1042);
    d.answer_enqueue(Err(tonic::Status::unavailable("down")));
    d.answer_enqueue(Err(error(Code::Unavailable, "not_leader")));
    d.answer_enqueue(Ok(entry()));
    let (id, existing, _position) = c.enqueue(&o, &enqueue()).await.unwrap();
    assert_eq!((id.as_str(), existing), (E, false));
    let tries: Vec<_> = d
        .seen()
        .into_iter()
        .filter_map(|s| match s {
            Seen::Enqueue(t, r) => Some((t, r)),
            _ => None,
        })
        .collect();
    assert_eq!(tries.len(), 3);
    for (t, r) in &tries {
        assert_eq!(t.as_deref(), Some("acme"));
        assert_eq!(r.enqueue_seq, 1);
        assert_eq!(r.first_queued_at_ms, 1_792_800_005_000);
        assert_eq!(r.ordering_at_ms, 1_792_800_005_000);
        assert_eq!(r.reason, pb::EnqueueReason::New as i32);
    }

    d.answer_enqueue(Err(error(Code::Aborted, "stale_owner")));
    assert_eq!(c.enqueue(&o, &enqueue()).await, Err(RouteError::Fenced));
    d.answer_enqueue(Err(error(Code::FailedPrecondition, "queue_unknown")));
    assert_eq!(
        c.enqueue(&o, &enqueue()).await,
        Err(RouteError::Refused("queue_unknown".into()))
    );
    d.answer_enqueue(Err(error(Code::InvalidArgument, "tenant_mismatch")));
    assert_eq!(
        c.enqueue(&o, &enqueue()).await,
        Err(RouteError::Bug("tenant_mismatch".into()))
    );
    let enqueues = d
        .seen()
        .iter()
        .filter(|s| matches!(s, Seen::Enqueue(..)))
        .count();
    assert_eq!(enqueues, 6, "refusals are not retried");

    // An entry RE no longer knows is gone: Dequeue succeeds.
    d.answer_dequeue(Err(error(Code::NotFound, "entry_unknown")));
    assert_eq!(c.dequeue(&o, E, DequeueReason::Abandoned).await, Ok(true));
    // The first outcome wins: RE answers what it recorded.
    d.answer_outcome(Ok(pb::ReportOutcomeResponse {
        existing: true,
        recorded: pb::Outcome::Expired as i32,
    }));
    assert_eq!(
        c.report_outcome(&o, R, Outcome::Connected, "").await,
        Ok(Recorded::Other)
    );
    assert_eq!(
        c.report_outcome(&o, R, Outcome::CallerGone, "ended").await,
        Ok(Recorded::Other)
    );
    c.call_ended(&o, "agent-a1", R, 1_792_800_090_000)
        .await
        .unwrap();
}

#[tokio::test]
async fn unavailable_ends_after_the_tries_or_the_budget() {
    ie_obs::testing::capture_to_file("routing_client");
    let d = Double::default();
    let uri = d.serve().await;
    let mut cfg = Config::new(&uri, "ie-bin-a-1", "bin-a");
    cfg.enqueue_budget = Duration::from_millis(400);
    cfg.outcome_budget = Duration::from_millis(400);
    let c = RoutingClient::start(cfg).unwrap().client;
    let o = owner("acme", I, 1042);
    // Nothing scripted: the double answers UNAVAILABLE every time.
    let t = std::time::Instant::now();
    assert!(matches!(
        c.enqueue(&o, &enqueue()).await,
        Err(RouteError::Unavailable(_))
    ));
    assert!(t.elapsed() >= Duration::from_millis(400));
    assert!(
        d.seen()
            .iter()
            .filter(|s| matches!(s, Seen::Enqueue(..)))
            .count()
            > 3
    );
    // Outcome RPCs: retried for their budget (15 s by default: IE-059, across a leader change).
    for _ in 0..100 {
        d.answer_outcome(Err(tonic::Status::unavailable("down")));
    }
    let t = std::time::Instant::now();
    assert!(matches!(
        c.report_outcome(&o, R, Outcome::CallerGone, "ended").await,
        Err(RouteError::Unavailable(_))
    ));
    assert!(t.elapsed() >= Duration::from_millis(400));
    assert!(
        d.seen()
            .iter()
            .filter(|s| matches!(s, Seen::ReportOutcome(..)))
            .count()
            > 3
    );
    // Reattach (an adoption's): 3 tries.
    let before = d.seen().len();
    let (_mb, r) = c.reattach(&owner("acme", I, 1043)).await;
    assert!(r.is_err());
    assert_eq!(d.seen().len() - before, 3);
}

#[tokio::test]
async fn a_reconnect_replays_without_duplicates_and_superseded_stops() {
    ie_obs::testing::capture_to_file("routing_client");
    let (d, c, _ev) = start().await;
    let o = owner("acme", I, 1042);
    let mut mb = c.bind(&o);
    d.push(assigned("acme", I, 1042, 7, 1)).await;
    assert_eq!(next(&mut mb).await.delivery_seq, 1);
    c.ack(&o, 1);
    d.end(tonic::Status::unavailable("restart")).await;
    wait(|| d.streams() == 2).await;
    d.push(heartbeat(7)).await;
    // RE replays what it holds: seq 1 (processed) and a new seq 2.
    d.push(assigned("acme", I, 1042, 7, 1)).await;
    let mut revoke = assigned("acme", I, 1042, 7, 2);
    revoke.event = Some(pb::watch_response::Event::Revoke(pb::Revoke {
        target: Some(pb::EventTarget {
            tenant_id: "acme".into(),
            interaction_id: I.into(),
            owner_epoch: 1042,
        }),
        reservation_id: R.into(),
        reason: "expired".into(),
    }));
    d.push(revoke).await;
    let got = next(&mut mb).await;
    assert_eq!(got.delivery_seq, 2);
    assert!(matches!(got.event, RoutingEvent::Revoke { .. }));
    // A second process with this replica id: the stream stops for good.
    d.end(error(Code::Aborted, "stream_superseded")).await;
    wait(|| c.link() == Link::Superseded).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(d.streams(), 2, "no reconnect after stream_superseded");
}

#[tokio::test]
async fn after_release_messages_go_to_the_dispatcher_and_strangers_are_orphans() {
    ie_obs::testing::capture_to_file("routing_client");
    let (d, c, mut ev) = start().await;
    let o = owner("acme", I, 1042);
    let _mb = c.bind(&o);
    c.release(&o);
    d.push(assigned("acme", I, 1042, 7, 1)).await;
    let e = tokio::time::timeout(Duration::from_secs(2), ev.recv())
        .await
        .unwrap()
        .unwrap();
    let ClientEvent::AfterEnd {
        owner: got,
        delivery,
    } = e
    else {
        panic!("{e:?}")
    };
    assert_eq!(got, o);
    assert_eq!(delivery.delivery_seq, 1);
    c.ack_released(&o, 1);
    wait(|| d.seen().iter().any(|s| matches!(s, Seen::Acknowledge(a) if a.acks.iter().any(|k| k.interaction_id == I && k.acked_delivery_seq == 1)))).await;
    // Another tenant naming the released interaction: dropped, not handed over.
    d.push(assigned("globex", I, 1042, 7, 2)).await;
    // An interaction this replica never held: one Orphan, however often RE redelivers.
    d.push(assigned("acme", I2, 1052, 7, 1)).await;
    d.push(assigned("acme", I2, 1052, 7, 1)).await;
    let e = tokio::time::timeout(Duration::from_secs(2), ev.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        e,
        ClientEvent::Orphan {
            tenant_id: "acme".into(),
            interaction_id: I2.into()
        }
    );
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(ev.try_recv().is_err());
    assert_eq!(c.dropped().tenant_mismatch, 1);
}

fn revoke(iid: &str, epoch: u64, leader: u64, seq: u64) -> pb::WatchResponse {
    pb::WatchResponse {
        leader_epoch: leader,
        delivery_seq: seq,
        event: Some(pb::watch_response::Event::Revoke(pb::Revoke {
            target: Some(pb::EventTarget {
                tenant_id: "acme".into(),
                interaction_id: iid.into(),
                owner_epoch: epoch,
            }),
            reservation_id: R.into(),
            reason: "expired".into(),
        })),
    }
}

/// CR-053, IE's part: a message past the next one expected is a gap (held back, never acked
/// past), so the missing one, redelivered by RE in order, still reaches the call.
#[tokio::test]
async fn a_message_past_a_gap_is_held_back_until_the_missing_one_comes() {
    ie_obs::testing::capture_to_file("routing_client");
    let (d, c, _ev) = start().await;
    let o = owner("acme", I, 1042);
    let mut mb = c.bind(&o);
    d.push(assigned("acme", I, 1042, 7, 1)).await;
    assert_eq!(next(&mut mb).await.delivery_seq, 1);
    c.ack(&o, 1);
    // seq 2 was lost on the way (a full mailbox, an inverted replay): 3 must not pass.
    d.push(revoke(I, 1042, 7, 3)).await;
    wait(|| c.dropped().gap == 1).await;
    assert!(mb.try_recv().is_err(), "nothing past the gap");
    // RE redelivers in order: 2, then 3.
    d.push(assigned("acme", I, 1042, 7, 2)).await;
    d.push(revoke(I, 1042, 7, 3)).await;
    assert_eq!(next(&mut mb).await.delivery_seq, 2);
    assert_eq!(next(&mut mb).await.delivery_seq, 3);
    c.ack(&o, 3);
    wait(|| {
        d.seen().iter().any(|s| {
            matches!(s, Seen::Acknowledge(a) if a.acks.iter().any(|k| k.interaction_id == I && k.acked_delivery_seq == 3))
        })
    })
    .await;
    // A fresh binding takes the first message's seq as its base (RE's restart bases are
    // (N - 1) x 2^32).
    let o2 = owner("acme", I2, 1052);
    let mut mb2 = c.bind(&o2);
    d.push(assigned("acme", I2, 1052, 7, (1 << 32) + 1)).await;
    assert_eq!(next(&mut mb2).await.delivery_seq, (1 << 32) + 1);
}

/// IE-059 (CONVENTIONS "A leader change"): the leader steps down mid-call. The client follows
/// `parity-leader` to the new leader for its stream and its requests, a `ReportOutcome` caught by
/// the change is retried there, the call's binding (and its mailbox) survives, the bound calls
/// hear of the change, replays are dropped by `delivery_seq`, and nothing stops.
#[tokio::test]
async fn a_leader_change_is_followed_without_losing_the_call_or_an_outcome() {
    ie_obs::testing::capture_to_file("routing_client");
    let (a, b) = Double::pair();
    let ua = a.serve().await;
    let ub = b.serve().await;
    b.set_standby(ua.trim_start_matches("http://"));
    let mut cfg = Config::with_endpoints(&[ua.clone(), ub.clone()], "ie-bin-a-1", "bin-a");
    cfg.reconnect_min = Duration::from_millis(20);
    cfg.reconnect_max = Duration::from_millis(50);
    cfg.resync_spread = Duration::from_millis(10);
    let c = RoutingClient::start(cfg).unwrap().client;
    wait(|| a.streams() == 1).await;
    a.push(heartbeat(7)).await;
    wait(|| c.link() == Link::Live).await;
    let o = owner("acme", I, 1042);
    let mut leader = c.subscribe_leader();
    let mut mb = c.bind(&o);
    a.push(assigned("acme", I, 1042, 7, 1)).await;
    assert_eq!(next(&mut mb).await.delivery_seq, 1);
    c.ack(&o, 1);

    // The failover: A steps down naming B; B serves the same state under leader epoch 8.
    b.set_leader();
    a.step_down(ub.trim_start_matches("http://")).await;
    // An outcome sent now reaches B (A answers not_leader, the client follows B).
    let recorded = c
        .report_outcome(&o, R, Outcome::Connected, "")
        .await
        .unwrap();
    assert_eq!(recorded, Recorded::Connected);
    assert!(
        b.served() >= 1,
        "the outcome was answered by the new leader"
    );
    assert_eq!(c.current_endpoint(), ub.trim_start_matches("http://"));
    // The stream reconnects to B, whose first message carries the new epoch.
    wait(|| a.streams() == 2).await;
    a.push(heartbeat(8)).await;
    tokio::time::timeout(Duration::from_secs(2), leader.changed())
        .await
        .expect("the leader change is announced")
        .unwrap();
    assert_eq!(*leader.borrow(), 8);
    assert_eq!(c.leader_changes(), 1);
    // The replay: seq 1 again (processed, dropped) and a new seq 2 (delivered), both at epoch 8.
    a.push(assigned("acme", I, 1042, 8, 1)).await;
    let mut revoke = assigned("acme", I, 1042, 8, 2);
    revoke.event = Some(pb::watch_response::Event::Revoke(pb::Revoke {
        target: Some(pb::EventTarget {
            tenant_id: "acme".into(),
            interaction_id: I.into(),
            owner_epoch: 1042,
        }),
        reservation_id: R.into(),
        reason: "expired".into(),
    }));
    a.push(revoke).await;
    let got = next(&mut mb).await;
    assert_eq!(got.delivery_seq, 2, "the processed replay was dropped");
    assert!(c.dropped().duplicate >= 1);
    // The call re-attaches with its unchanged owner; its mailbox stays bound.
    a.answer_reattach(
        Vec::new(),
        Ok(pb::ReattachResponse {
            entry: Some(pb::QueueEntry {
                entry_id: E.into(),
                ..Default::default()
            }),
            reservations: Vec::new(),
            last_delivery_seq: 2,
        }),
    );
    let r = c.resync(&o).await.unwrap();
    assert_eq!(r.entry_id.as_deref(), Some(E), "the entry kept its place");
    let req = a
        .seen()
        .into_iter()
        .find_map(|s| match s {
            Seen::Reattach(_, r) => Some(r),
            _ => None,
        })
        .unwrap();
    assert_eq!(
        req.owner.unwrap().owner_epoch,
        1042,
        "the owner epoch is unchanged"
    );
    a.push(assigned("acme", I, 1042, 8, 3)).await;
    assert_eq!(next(&mut mb).await.delivery_seq, 3, "the binding survived");
    assert_eq!(c.link(), Link::Live, "never stopped");
}

/// MF-1a: a routing engine restarted without its state (HA off) numbers afresh from its
/// incarnation's base (2^32 for the second). After the call's Reattach shows no entry and no
/// reservation at that base, the binding is re-based: RE's first new message is delivered, not
/// held back as a gap for ever.
#[tokio::test]
async fn a_stateless_restart_rebases_the_delivery_sequence() {
    ie_obs::testing::capture_to_file("routing_client");
    let (d, c, _ev) = start().await;
    let o = owner("acme", I, 1042);
    let mut mb = c.bind(&o);
    d.push(assigned("acme", I, 1042, 7, 1)).await;
    assert_eq!(next(&mut mb).await.delivery_seq, 1);
    c.ack(&o, 1);
    // RE restarts alone: a new stream, leader epoch 8, and it has forgotten the call.
    d.end(tonic::Status::unavailable("restart")).await;
    wait(|| d.streams() == 2).await;
    d.push(heartbeat(8)).await;
    d.answer_reattach(
        Vec::new(),
        Ok(pb::ReattachResponse {
            entry: None,
            reservations: Vec::new(),
            last_delivery_seq: 1 << 32,
        }),
    );
    let r = c.resync(&o).await.unwrap();
    assert!(r.entry_id.is_none());
    // IE enqueues the caller again; RE's first message about the new entry is 2^32 + 1.
    d.push(assigned("acme", I, 1042, 8, (1 << 32) + 1)).await;
    assert_eq!(
        next(&mut mb).await.delivery_seq,
        (1 << 32) + 1,
        "delivered, not held back as a gap"
    );
    assert_eq!(c.dropped().gap, 0);
}

/// A leader without a stream to name: a standby says `not_leader` with no `parity-leader`, and
/// the client moves to the bin's next instance.
#[tokio::test]
async fn not_leader_without_a_name_tries_the_next_instance() {
    ie_obs::testing::capture_to_file("routing_client");
    let (a, b) = Double::pair();
    let ua = a.serve().await;
    let ub = b.serve().await;
    a.set_standby("");
    let c = RoutingClient::start(Config::with_endpoints(
        &[ua, ub.clone()],
        "ie-bin-a-1",
        "bin-a",
    ))
    .unwrap()
    .client;
    let o = owner("acme", I, 1042);
    c.call_ended(&o, "agent-a1", R, 1_792_800_090_000)
        .await
        .unwrap();
    assert_eq!(c.current_endpoint(), ub.trim_start_matches("http://"));
}

/// The interaction is bound at the new epoch before `Reattach` goes out: the replay RE sends at
/// the epoch raise (re-stamped with the new fence, before its answer) reaches the new mailbox.
#[tokio::test]
async fn reattach_binds_the_new_epoch_before_the_request() {
    ie_obs::testing::capture_to_file("routing_client");
    let (d, c, _ev) = start().await;
    let old = owner("acme", I, 1042);
    let _old_mb = c.bind(&old);
    let new = owner("acme", I, 1077);
    d.answer_reattach(
        vec![assigned("acme", I, 1077, 7, 1)],
        Ok(pb::ReattachResponse {
            entry: None,
            reservations: vec![pb::Reservation {
                reservation_id: R.into(),
                agent_id: "agent-a1".into(),
                agent_contact: "sip:agent-a1@acme.agents.example".into(),
                deadline_ms: 1_792_800_060_000,
                state: pb::ReservationState::Assigned as i32,
                consult: false,
            }],
            last_delivery_seq: 1,
        }),
    );
    let (mut mb, r) = c.reattach(&new).await;
    let r = r.unwrap();
    assert_eq!(r.reservations.len(), 1);
    assert!(!r.reservations[0].connected);
    let got = next(&mut mb).await;
    assert_eq!(got.delivery_seq, 1);
    assert!(matches!(got.event, RoutingEvent::Assigned { .. }));
    assert_eq!(
        c.dropped().stale,
        0,
        "the replay wasn't taken for another owner's"
    );
    let req = d
        .seen()
        .into_iter()
        .find_map(|s| match s {
            Seen::Reattach(t, r) => Some((t, r)),
            _ => None,
        })
        .unwrap();
    assert_eq!(req.0.as_deref(), Some("acme"));
    assert_eq!(req.1.owner.unwrap().owner_epoch, 1077);
}
