//! G5 from IE's side: the requests IE's client writes are byte-for-byte the ones the executable
//! routing.v1 scripts carry (parity-contracts), so routing-engine is tested with what IE sends.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod double;

use double::{Double, Seen};
use ie_routing_client::{
    Config, DequeueReason, Enqueue, EnqueueReason, Outcome, Owner, RoutingClient,
};
use parity_proto::routing::v1 as pb;
use prost::Message;
use prost_reflect::{DescriptorPool, DynamicMessage};
use serde_json::Value;
use std::path::PathBuf;
use std::time::Duration;

fn contracts() -> PathBuf {
    std::env::var_os("PARITY_CONTRACTS_DIR").map_or_else(
        || PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../parity-contracts"),
        PathBuf::from,
    )
}

/// The body of the script's `n`th call of `rpc`, as the generated type.
fn script_call<T: Message + Default>(script: &str, rpc: &str, n: usize) -> T {
    let path = contracts().join("fixtures/routing.v1/valid").join(script);
    let f: Value = serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
    let step = f["steps"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|s| {
            s["method"]
                .as_str()
                .is_some_and(|m| m.ends_with(&format!("/{rpc}")))
        })
        .nth(n)
        .unwrap_or_else(|| panic!("{script}: no call {n} of {rpc}"));
    assert_eq!(
        step["metadata"]["tenant-id"],
        step["body"]["owner"]["tenant_id"]
    );
    let pool = DescriptorPool::decode(parity_proto::FILE_DESCRIPTOR_SET).unwrap();
    let desc = pool
        .get_message_by_name(step["message"].as_str().unwrap())
        .unwrap();
    let dm = DynamicMessage::deserialize(desc, step["body"].clone()).unwrap();
    T::decode(dm.encode_to_vec().as_slice()).unwrap()
}

#[tokio::test]
async fn ie_writes_the_requests_of_the_routing_race_scripts() {
    let d = Double::default();
    let uri = d.serve().await;
    let c = RoutingClient::start(Config::new(&uri, "ie-bin-a-1", "bin-a"))
        .unwrap()
        .client;
    let o = Owner {
        tenant_id: "acme".into(),
        interaction_id: "0192f6a1-7c3e-7b21-9a4d-5e6f70819203".into(),
        owner_epoch: 1042,
    };
    let entry = "0192f6a1-9000-7aaa-8bbb-000000000001";
    let reservation = "0192f6a1-9100-7ccc-8ddd-000000000001";

    // race-offer-timeout-vs-accept: the caller's first Enqueue.
    d.answer_enqueue(Ok(pb::EnqueueResponse {
        entry: Some(pb::QueueEntry {
            entry_id: entry.into(),
            ..Default::default()
        }),
        existing: false,
    }));
    let e = Enqueue {
        enqueue_seq: 1,
        queue_id: "sales".into(),
        priority: 0,
        first_queued_at_ms: 1_792_800_000_000,
        ordering_at_ms: 1_792_800_000_000,
        reason: EnqueueReason::New,
        language: "en-GB".into(),
    };
    c.enqueue(&o, &e).await.unwrap();

    // race-abandon-vs-assign: Dequeue before the Assigned is processed, then CALLER_GONE.
    let mut mb = c.bind(&o);
    d.push(pb::WatchResponse {
        leader_epoch: 7,
        delivery_seq: 1,
        event: Some(pb::watch_response::Event::Assigned(pb::Assigned {
            target: Some(pb::EventTarget {
                tenant_id: "acme".into(),
                interaction_id: o.interaction_id.clone(),
                owner_epoch: 1042,
            }),
            reservation_id: reservation.into(),
            entry_id: entry.into(),
            ..Default::default()
        })),
    })
    .await;
    tokio::time::timeout(Duration::from_secs(2), mb.recv())
        .await
        .unwrap()
        .unwrap();
    c.dequeue(&o, entry, DequeueReason::Abandoned)
        .await
        .unwrap();
    c.ack(&o, 1);
    c.report_outcome(&o, reservation, Outcome::CallerGone, "ended")
        .await
        .unwrap();

    let seen = d.seen();
    let sent_enqueue = seen.iter().find_map(|s| match s {
        Seen::Enqueue(Some(t), r) if t == "acme" => Some(r.clone()),
        _ => None,
    });
    let sent_dequeue = seen.iter().find_map(|s| match s {
        Seen::Dequeue(Some(t), r) if t == "acme" => Some(r.clone()),
        _ => None,
    });
    let sent_outcome = seen.iter().find_map(|s| match s {
        Seen::ReportOutcome(Some(t), r) if t == "acme" => Some(r.clone()),
        _ => None,
    });
    assert_eq!(
        sent_enqueue.unwrap(),
        script_call::<pb::EnqueueRequest>("race-offer-timeout-vs-accept.json", "Enqueue", 0)
    );
    assert_eq!(
        sent_dequeue.unwrap(),
        script_call::<pb::DequeueRequest>("race-abandon-vs-assign.json", "Dequeue", 0)
    );
    assert_eq!(
        sent_outcome.unwrap(),
        script_call::<pb::ReportOutcomeRequest>("race-abandon-vs-assign.json", "ReportOutcome", 0)
    );
}
