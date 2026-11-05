//! Soak-blockers rounds 1-2, the whole call path: the call tasks (`Calls`) with the real
//! telephony client against an in-process TC, a real etcd and a real Postgres (containers on
//! random loopback ports, removed at the end). A call closes (the ENDED record, the ack when TC's
//! end is known, the owner key released, the CDR row) only on TC's end or the node's death.
//! - IE-049: an etcd-claimed arrival whose ownerless end TC sent elsewhere: its first command gets
//!   `session_ended`, and it closes from the tombstone.
//! - IE-050/IE-056, the partition: its node goes unmanaged mid-call, a command and `EndSession`
//!   fail, `Ending` waits. Healed: TC is asked and the call closes from the tombstone. Never
//!   healed: it keeps waiting, owner key held, no record or CDR, until the node is declared dead
//!   on positive evidence (`NodeDead`). TC answering "live": `EndSession` again at each ask, the
//!   call never closes itself.
//! - IE-055: with no etcd lease the client takes no arrival.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

#[path = "../../ie-telephony-client/tests/mock/mod.rs"]
mod mock;

use ie_obs::Metrics;
use ie_obs::testing::identity;
use ie_telephony_client::{
    ClientConfig, MemoryEndedLog, NodeConfig, NodeEvidence, Profile, SourceKind, StaticEvidence,
    TelephonyClient, Timing, Verdict,
};
use interaction_engine::calls::owner::{self, Etcd, Lease, Registration};
use interaction_engine::calls::routes::Routes;
use interaction_engine::calls::run::{CallMetrics, Calls};
use interaction_engine::cdr::{self, PgTarget};
use interaction_engine::tenants::NumberMap;
use mock::*;
use parity_proto::telephony::v1 as pb;
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant};

const ETCD: &str = "gcr.io/etcd-development/etcd@sha256:7c6c239825d00e3f6328a69caafd54be92063acf0c2ce78b8394699f52b75dc3";
const POSTGRES: &str =
    "postgres@sha256:b0f9560a2de083e2cc7382e75f808c7381a32852a7ec49117deedb300e552b24";
const NUMBER: &str = "+15550100100";
const IID3: &str = "0192f6a1-7c3e-7b21-9a4d-5e6f70819205";
const IID5: &str = "0192f6a1-7c3e-7b21-9a4d-5e6f70819207";

/// One number of acme's, a flow that answers and then plays (so a command follows the answer).
const FLOWS: &str = r#"{
  "schema_version": 1,
  "flows": {
    "acme-main": {
      "tenant_id": "acme",
      "start": "menu",
      "steps": {
        "menu": [
          {"say": {"text": "Welcome to Acme.", "language": "en-GB"}},
          {"pause": {"seconds": 60}},
          {"hangup": {}}
        ]
      }
    }
  },
  "numbers": {"+15550100100": {"flow": "acme-main"}},
  "consent": {}
}"#;

struct Container(String);
impl Drop for Container {
    fn drop(&mut self) {
        let _ = Command::new("docker").args(["rm", "-f", &self.0]).output();
    }
}

fn docker(args: &[&str]) -> String {
    let out = Command::new("docker").args(args).output().expect("docker");
    assert!(
        out.status.success(),
        "docker {args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).trim().to_owned()
}

fn run(image_args: &[&str], port: &str) -> (Container, u16) {
    let mut args = vec![
        "run",
        "-d",
        "--rm",
        "--label",
        "parity.test=parity-interaction-engine",
        "--security-opt",
        "label=disable",
    ];
    args.extend_from_slice(image_args);
    let c = Container(docker(&args));
    let p: u16 = docker(&["port", &c.0, port])
        .lines()
        .next()
        .and_then(|l| l.rsplit(':').next())
        .and_then(|p| p.parse().ok())
        .expect("a mapped port");
    (c, p)
}

async fn eventually_async<F, Fut>(what: &str, secs: u64, f: F)
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let end = Instant::now() + Duration::from_secs(secs);
    while !f().await {
        assert!(Instant::now() < end, "never within {secs} s: {what}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// An arrival on acme's number (the mock's own helper dials an unmapped one).
fn arrival(iid: &str) -> pb::WatchResponse {
    let mut m = arrived(INC_A, iid, 1);
    if let Some(pb::watch_response::Event::Arrived(a)) = m.event.as_mut() {
        a.called_number = NUMBER.into();
    }
    m
}

fn map() -> NumberMap {
    let dir = std::env::var("PARITY_CONTRACTS_DIR").unwrap_or_else(|_| {
        concat!(env!("CARGO_MANIFEST_DIR"), "/../../../parity-contracts").into()
    });
    let text = std::fs::read_to_string(format!("{dir}/tenants/test-tenants.v1.json")).unwrap();
    NumberMap::parse(&text, true).unwrap()
}

struct Shared {
    etcd: Etcd,
    lease: Lease,
    pg: PgTarget,
    metrics: Arc<Metrics>,
}

/// One replica's call path against its own in-process TC.
struct Replica {
    mock: Mock,
    client: TelephonyClient,
    clock: Arc<ie_telephony_client::ManualClock>,
    calls: Arc<Calls<TelephonyClient>>,
}

async fn replica(s: &Shared, replica_id: &str) -> Replica {
    replica_with(s, replica_id, Vec::new()).await
}

async fn replica_with(
    s: &Shared,
    replica_id: &str,
    evidence: Vec<Arc<dyn NodeEvidence>>,
) -> Replica {
    let mock = Mock::default();
    let uri = mock.serve().await;
    let clock = Arc::new(ie_telephony_client::ManualClock::new());
    let cfg = ClientConfig {
        replica_id: replica_id.into(),
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
            ack_batch_ms: 20,
            probe_every_ms: 20,
            ..Timing::default()
        },
    };
    let started = TelephonyClient::start(cfg, Arc::clone(&clock) as _, evidence).unwrap();
    let calls = Arc::new(Calls {
        client: started.client.clone(),
        etcd: s.etcd.clone(),
        lease: s.lease.clone(),
        replica_id: replica_id.into(),
        bin: "bin-a".into(),
        routes: Routes::build(map(), FLOWS).unwrap(),
        cdr: cdr::spawn(s.pg.clone(), 64, s.metrics.meter()),
        ended_log: MemoryEndedLog::new(Profile::Test).unwrap(),
        metrics: CallMetrics::new(s.metrics.meter()),
        routing: None,
        agents: Default::default(),
        recording: Default::default(),
    });
    tokio::spawn(Arc::clone(&calls).dispatch(started.events));
    let r = Replica {
        mock,
        client: started.client,
        clock,
        calls,
    };
    let end = Instant::now() + Duration::from_secs(5);
    while r.mock.watch_calls() < 1 {
        assert!(Instant::now() < end, "no Watch");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    r.mock.send(heartbeat(INC_A)).await;
    r
}

impl Replica {
    fn acked(&self, iid: &str) -> Option<u64> {
        self.mock
            .acks()
            .iter()
            .filter(|a| a.interaction_id == iid && a.owner_epoch > 0)
            .map(|a| a.acked_delivery_seq)
            .max()
    }
    fn recorded(&self, iid: &str) -> bool {
        self.calls
            .ended_log
            .entries()
            .iter()
            .any(|(k, _)| k.interaction_id == iid)
    }
}

async fn cdr_end_reason(pg: &PgTarget, iid: &str) -> Option<String> {
    let c = cdr::connect(pg).await.ok()?;
    // The id is one of this test's constants (a UUID): a literal, as the column is a uuid.
    c.query_opt(
        &format!("SELECT end_reason FROM cdr WHERE interaction_id = '{iid}'::uuid"),
        &[],
    )
    .await
    .ok()
    .flatten()
    // Step 6: the row opens at answer with no end; only a closed row has an end reason.
    .and_then(|r| r.get::<_, Option<String>>(0))
}

async fn owner_key_gone(etcd: &Etcd, iid: &str) -> bool {
    matches!(etcd.get(&owner::owner_key(iid)).await, Ok(None))
}

async fn owner_key_held(etcd: &Etcd, iid: &str) -> bool {
    matches!(etcd.get(&owner::owner_key(iid)).await, Ok(Some(_)))
}

/// The partition: the node's stream goes silent while `AnswerLeg` is in flight, so the next
/// command and `EndSession` fail as unmanaged and the call waits in `Ending`.
async fn partition_mid_call(r: &Replica, iid: &str, etcd: &Etcd) {
    r.mock
        .answer("AnswerLeg", Answer::Delay(Duration::from_millis(600)));
    r.mock.send(arrival(iid)).await;
    eventually_async("the owner key claimed", 10, || owner_key_held(etcd, iid)).await;
    eventually_async("AnswerLeg sent", 10, || async {
        !r.mock.calls("AnswerLeg").is_empty()
    })
    .await;
    r.clock.advance(6_001);
    eventually_async("unmanaged", 5, || async {
        r.client.link(&ie_telephony_client::NodeId(NODE.into()))
            == Some(ie_telephony_client::Link::Disconnected)
    })
    .await;
    // AnswerLeg completes; Play and EndSession then fail fast (unmanaged), never reaching TC.
    tokio::time::sleep(Duration::from_millis(1_000)).await;
    assert!(r.mock.calls("Play").is_empty() && r.mock.calls("EndSession").is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_call_closes_only_on_tcs_end_or_its_nodes_death() {
    ie_obs::testing::capture_to_file("call_end");
    let password = uuid::Uuid::now_v7().simple().to_string();
    let (_pgc, pg_port) = run(
        &[
            "-e",
            &format!("POSTGRES_PASSWORD={password}"),
            "-e",
            "POSTGRES_USER=parity_ie",
            "-e",
            "POSTGRES_DB=parity_interaction_engine",
            "-p",
            "127.0.0.1::5432",
            POSTGRES,
        ],
        "5432/tcp",
    );
    let (_ec, etcd_port) = run(
        &[
            "--cap-drop",
            "ALL",
            "--tmpfs",
            "/tmp:mode=1777",
            "-p",
            "127.0.0.1::2379",
            ETCD,
            "etcd",
            "--name",
            "t",
            "--data-dir",
            "/tmp/etcd",
            "--listen-client-urls",
            "http://0.0.0.0:2379",
            "--advertise-client-urls",
            "http://127.0.0.1:2379",
        ],
        "2379/tcp",
    );
    let pg = PgTarget {
        host: "127.0.0.1".into(),
        port: pg_port,
        user: "parity_ie".into(),
        dbname: "parity_interaction_engine".into(),
        password,
        migrate: true,
    };
    let etcd = Etcd::connect(
        &format!("http://127.0.0.1:{etcd_port}"),
        Duration::from_millis(1_000),
    )
    .unwrap();
    eventually_async("etcd", 30, || async { etcd.get("probe").await.is_ok() }).await;
    eventually_async("postgres", 60, || async {
        match cdr::connect(&pg).await {
            Ok(c) => c.simple_query("SELECT 1").await.is_ok(),
            Err(_) => false,
        }
    })
    .await;
    let reg = Registration {
        bin: "bin-a".into(),
        replica_id: "ie-bin-a-test".into(),
        value: b"{}".to_vec(),
        ttl_s: 10,
    };
    let (_stop_tx, mut stop) = tokio::sync::watch::channel(false);
    let first = owner::register(&etcd, &reg, |_| {}, &mut stop)
        .await
        .unwrap();
    let lease = Lease::new(first);
    tokio::spawn(owner::maintain(
        etcd.clone(),
        reg,
        lease.clone(),
        stop.clone(),
    ));
    let s = Shared {
        etcd: etcd.clone(),
        lease: lease.clone(),
        pg: pg.clone(),
        metrics: Arc::new(Metrics::new(&identity()).unwrap()),
    };

    // IE-049: claimed in etcd, TC's fence still 0; TC ended it and sent its end elsewhere.
    let a = replica(&s, "ie-bin-a-1").await;
    a.mock.answer(
        "AnswerLeg",
        Answer::Err(status(tonic::Code::FailedPrecondition, "session_ended")),
    );
    a.mock.session(
        IID,
        Ok((
            session(IID, pb::SessionState::Ended, 3, vec![]),
            INC_A.into(),
        )),
    );
    a.mock.send(arrival(IID)).await;

    // IE-050, healed and never healed, at the same time; c can be shown dead (three sources).
    let b = replica(&s, "ie-bin-a-2").await;
    let sources: Vec<Arc<StaticEvidence>> = [
        SourceKind::Tc,
        SourceKind::Dispatcher,
        SourceKind::NodeServices,
    ]
    .into_iter()
    .map(|k| Arc::new(StaticEvidence::new(k)))
    .collect();
    let c = replica_with(
        &s,
        "ie-bin-a-3",
        sources
            .iter()
            .map(|e| Arc::clone(e) as Arc<dyn NodeEvidence>)
            .collect(),
    )
    .await;
    // IE-056: TC says the session is live and refuses every EndSession. The call ends because its
    // AnswerLeg fails (IE-068: a failed Play no longer ends a call; it is retried, then skipped).
    let d = replica(&s, "ie-bin-a-4").await;
    for _ in 0..4 {
        d.mock.answer(
            "AnswerLeg",
            Answer::Err(status(tonic::Code::Unavailable, "internal")),
        );
    }
    for _ in 0..400 {
        d.mock.answer(
            "EndSession",
            Answer::Err(status(tonic::Code::Unavailable, "freeswitch_unavailable")),
        );
    }
    d.mock.session(
        IID5,
        Ok((
            session(IID5, pb::SessionState::Active, 2, vec![]),
            INC_A.into(),
        )),
    );
    d.mock.send(arrival(IID5)).await;
    tokio::join!(
        partition_mid_call(&b, IID2, &etcd),
        partition_mid_call(&c, IID3, &etcd)
    );
    let ending_from = Instant::now();
    // b's partition heals: the stream is back, and TC has the session's tombstone.
    b.mock.session(
        IID2,
        Ok((
            session(IID2, pb::SessionState::Ended, 5, vec![]),
            INC_A.into(),
        )),
    );
    eventually_async("b reconnected", 5, || async { b.mock.watch_calls() >= 2 }).await;
    b.mock.send(heartbeat(INC_A)).await;

    // IE-049's call: ENDED record, the tombstone's end acked, owner key released, CDR.
    eventually_async("a's end acked", 15, || async { a.acked(IID) == Some(3) }).await;
    assert!(a.recorded(IID));
    eventually_async("a's owner key released", 10, || owner_key_gone(&etcd, IID)).await;
    eventually_async("a's CDR", 30, || async {
        cdr_end_reason(&pg, IID).await.is_some()
    })
    .await;

    // IE-050 healed: closed at the first deadline (10 s) from the tombstone, acked.
    eventually_async("b's end acked", 25, || async { b.acked(IID2) == Some(5) }).await;
    let healed = ending_from.elapsed();
    assert!(b.recorded(IID2));
    eventually_async("b's owner key released", 10, || owner_key_gone(&etcd, IID2)).await;
    eventually_async("b's CDR", 30, || async {
        cdr_end_reason(&pg, IID2).await.is_some()
    })
    .await;

    // IE-056, never healed: past several asks it still waits: owner key held, no record, no CDR,
    // and TC isn't asked while the node is unmanaged.
    let wait = Duration::from_secs(35).saturating_sub(ending_from.elapsed());
    tokio::time::sleep(wait).await;
    assert!(
        !c.recorded(IID3),
        "a partition is not death: no ENDED record"
    );
    assert!(owner_key_held(&etcd, IID3).await, "the owner key is kept");
    assert_eq!(cdr_end_reason(&pg, IID3).await, None, "no CDR");
    assert!(
        c.mock.calls("GetSession").is_empty(),
        "unmanaged: TC never asked"
    );
    // d, live at TC and refusing EndSession: re-sent at each ask (10 s, then 20 s), never closed.
    let ends = d.mock.calls("EndSession").len();
    assert!(
        ends >= 12,
        "the end and two re-sends, 4 attempts each: {ends}"
    );
    assert!(d.mock.calls("GetSession").len() >= 2);
    assert!(!d.recorded(IID5) && owner_key_held(&etcd, IID5).await);
    assert_eq!(cdr_end_reason(&pg, IID5).await, None);
    // c's node is declared dead on positive evidence (every source, for the whole window): the
    // client ends its sessions as NodeDead and the call closes: record, key released, CDR, no ack.
    for e in &sources {
        e.set(Verdict::Dead);
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    c.clock.advance(30_001);
    eventually_async("c closed as NodeDead", 10, || async { c.recorded(IID3) }).await;
    let dead_after = ending_from.elapsed();
    assert!(
        c.acked(IID3) <= Some(1),
        "nothing acked past the arrival without TC's end"
    );
    eventually_async("c's owner key released", 10, || owner_key_gone(&etcd, IID3)).await;
    eventually_async("c's CDR", 30, || async {
        cdr_end_reason(&pg, IID3).await.as_deref() == Some("NodeDead")
    })
    .await;

    // IE-055: with no lease, no arrival is taken (TC offers it elsewhere after 1 s).
    // (A lease of its own, lost and never granted again: the shared one is kept alive.)
    let gone = Lease::new(1);
    tokio::spawn(interaction_engine::calls::follow_lease(
        gone.clone(),
        a.client.clone(),
        stop.clone(),
    ));
    gone.lost(1);
    eventually_async("arrivals not taken", 5, || async {
        !a.client.accepting_arrivals()
    })
    .await;
    let iid4 = "0192f6a1-7c3e-7b21-9a4d-5e6f70819206";
    a.mock.send(arrival(iid4)).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        a.mock.acks().iter().all(|x| x.interaction_id != iid4),
        "not acked"
    );
    assert!(a.mock.calls("DeclineArrival").is_empty(), "not declined");
    println!(
        "IE-049 closed from the tombstone; IE-050 healed closed {:.1} s into Ending; unhealed waited (key held) until declared dead at {:.1} s; live-at-TC sent EndSession {ends} times, still waiting",
        healed.as_secs_f64(),
        dead_after.as_secs_f64()
    );
}
