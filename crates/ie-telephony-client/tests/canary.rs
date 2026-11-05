//! ADR 0003 §4.9: no phone number and no digit reaches a log line, whatever the client does with
//! them (an arrival's numbers, a collected digit, an AddLeg's caller id, an error path).
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod mock;

use ie_obs::testing::{assert_no_canary, capture_logs};
use ie_telephony_client::{
    ClientConfig, ClientEvent, Intent, ManualClock, NodeConfig, NodeId, Owner, Profile, SessionKey,
    Telephony, TelephonyClient, Timing,
};
use mock::*;
use parity_proto::telephony::v1 as pb;
use std::sync::Arc;
use std::time::Duration;

const CANARIES: [&str; 4] = ["7700900123", "7700900100", "900123", "4111111111111111"];

#[test]
fn no_number_or_digit_reaches_the_logs() {
    let lines = capture_logs(|| {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(2)
            .build()
            .unwrap();
        rt.block_on(async {
            let mock = Mock::default();
            let uri = mock.serve().await;
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
                    ack_batch_ms: 20,
                    reconnect_min_ms: 20,
                    reconnect_max_ms: 50,
                    ..Timing::default()
                },
            };
            let mut s = TelephonyClient::start(cfg, Arc::new(ManualClock::new()), vec![]).unwrap();
            for _ in 0..100 {
                if mock.watch_calls() > 0 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            mock.send(heartbeat(INC_A)).await;
            mock.send(arrived(INC_A, IID, 1)).await;
            let (arrival, mut q) = loop {
                if let Some(ClientEvent::Arrived(a, q)) = s.events.recv().await {
                    break (a, q);
                }
            };
            tracing::info!(arrival = ?arrival, "the arrival as a test would log it");
            let node = NodeId(NODE.into());
            let owner = Owner {
                key: SessionKey {
                    tenant_id: T.into(),
                    interaction_id: IID.into(),
                },
                owner_epoch: 1042,
            };
            s.client.bind(&node, &owner);
            let c = "0192f6a1-9000-7000-8000-000000000001";
            s.client
                .command(
                    &node,
                    &owner,
                    Intent::SetDigitReporting {
                        leg_id: LEG.into(),
                        mode: pb::DigitReporting::On,
                        collection_id: c.into(),
                        max_duration_s: 30,
                    },
                )
                .await;
            mock.send(digit(INC_A, IID, 2, "4111111111111111", c)).await;
            let ev = tokio::time::timeout(Duration::from_secs(2), q.recv())
                .await
                .unwrap()
                .unwrap();
            tracing::info!(event = ?ev, "the digit event as a test would log it");
            mock.answer(
                "SendDtmf",
                Answer::Err(status(tonic::Code::InvalidArgument, "invalid_request")),
            );
            s.client
                .command(
                    &node,
                    &owner,
                    Intent::SendDtmf {
                        leg_id: LEG.into(),
                        digits: ie_telephony_client::Digits::new("4111111111111111"),
                    },
                )
                .await;
            s.client
                .command(
                    &node,
                    &owner,
                    Intent::SendDtmf {
                        leg_id: LEG.into(),
                        digits: ie_telephony_client::Digits::new("12x4111111111111111"),
                    },
                )
                .await;
            tokio::time::sleep(Duration::from_millis(100)).await;
            s.client.shutdown();
        });
    });
    assert!(!lines.is_empty(), "the client logged nothing at all");
    assert_no_canary(&lines, &CANARIES).unwrap();
}
