//! The CDR writer against a real Postgres (a container on a random loopback port, removed at the
//! end): one row per call even when the writer sees it twice; row-level security keeps a tenant's
//! reader from another tenant's rows. Step 6: snapshots of the live call, the CDR row once at the
//! end; a restarted IE writes an orphan's CDR from its latest snapshot and TC's end (DEMO step 5).
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use interaction_engine::cdr::{self, CdrEnd, CdrOp, CdrRecord, OrphanRow, PgTarget};
use std::process::Command;
use std::time::{Duration, Instant};

/// postgres:17-alpine, pinned by digest (the image the demo runs too).
const IMAGE: &str =
    "postgres@sha256:b0f9560a2de083e2cc7382e75f808c7381a32852a7ec49117deedb300e552b24";

struct Pg {
    id: String,
    target: PgTarget,
}

impl Drop for Pg {
    fn drop(&mut self) {
        let _ = Command::new("docker").args(["rm", "-f", &self.id]).output();
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

async fn postgres() -> Pg {
    // A throwaway password for a container that lives as long as this test.
    let password = uuid::Uuid::now_v7().simple().to_string();
    let id = docker(&[
        "run",
        "-d",
        "--rm",
        "--label",
        "parity.test=parity-interaction-engine",
        "--security-opt",
        "label=disable",
        "-e",
        &format!("POSTGRES_PASSWORD={password}"),
        "-e",
        "POSTGRES_USER=parity_ie",
        "-e",
        "POSTGRES_DB=parity_interaction_engine",
        "-p",
        "127.0.0.1::5432",
        IMAGE,
    ]);
    let port: u16 = docker(&["port", &id, "5432/tcp"])
        .lines()
        .next()
        .and_then(|l| l.rsplit(':').next())
        .and_then(|p| p.parse().ok())
        .expect("a mapped port");
    let pg = Pg {
        id,
        target: PgTarget {
            host: "127.0.0.1".into(),
            port,
            user: "parity_ie".into(),
            dbname: "parity_interaction_engine".into(),
            password,
            migrate: true,
        },
    };
    let until = Instant::now() + Duration::from_secs(60);
    loop {
        if let Ok(c) = cdr::connect(&pg.target).await
            && c.simple_query("SELECT 1").await.is_ok()
        {
            return pg;
        }
        assert!(
            Instant::now() < until,
            "Postgres never accepted a connection"
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

fn record(tenant: &str, interaction: &str) -> CdrRecord {
    CdrRecord {
        tenant_id: tenant.into(),
        interaction_id: interaction.into(),
        node_id: "media-1".into(),
        direction: "inbound",
        called_number: "+15550100100".into(),
        caller_number: "+447700900123".into(),
        flow: "acme-main".into(),
        last_step: "sales".into(),
        consent: "refused",
        consent_basis: "never",
        started_at_ms: 1_793_125_000_000,
        answered_at_ms: Some(1_793_125_000_120),
        end: Some(CdrEnd {
            ended_at_ms: 1_793_125_009_000,
            end_reason: "EndedByOwner".into(),
            ended_by: "ie",
            sip_status: 0,
        }),
        owner_epoch: 3,
        gathers: 1,
        no_inputs: 0,
        queue_id: String::new(),
        entry_id: String::new(),
        queued_at_ms: None,
        dequeued_at_ms: None,
        queue_outcome: "",
        offers: 0,
        agent_id: String::new(),
        reservation_id: String::new(),
        connected_at_ms: None,
        recorded_legs: 0,
        recording_failures: 0,
        recording_pauses: 0,
        mutes: 0,
        parent_interaction_id: String::new(),
    }
}

/// An open row of a queued call (no end), waiting or connected.
fn queued(tenant: &str, interaction: &str, connected: bool) -> CdrRecord {
    let mut r = record(tenant, interaction);
    r.end = None;
    r.queue_id = "sales".into();
    r.entry_id = "0192f6a1-9000-7aaa-8bbb-000000000001".into();
    r.queued_at_ms = Some(1_793_125_002_000);
    r.queue_outcome = "waiting";
    if connected {
        r.queue_outcome = "connected";
        r.dequeued_at_ms = Some(1_793_125_007_000);
        r.connected_at_ms = Some(1_793_125_007_000);
        r.offers = 2;
        r.agent_id = "agent-a1".into();
        r.reservation_id = "0192f6a1-9100-7ccc-8ddd-000000000001".into();
        r.recorded_legs = 2;
    }
    r
}

async fn count(pg: &Pg, sql: &str) -> i64 {
    let c = cdr::connect(&pg.target).await.unwrap();
    c.query_one(sql, &[]).await.unwrap().get(0)
}

/// As `count`, 0 while the table isn't there yet (the writer migrates on its first record).
async fn count_soon(pg: &Pg, sql: &str) -> i64 {
    let Ok(c) = cdr::connect(&pg.target).await else {
        return 0;
    };
    c.query_one(sql, &[]).await.map_or(0, |r| r.get(0))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_row_per_call_even_when_seen_twice_and_tenants_are_isolated() {
    ie_obs::testing::capture_to_file("cdr");
    let pg = postgres().await;
    let a = "01a17e9b-0000-7000-8000-000000000001";
    let b = "01a17e9b-0000-7000-8000-000000000002";

    // The writer task: the same record twice, then another tenant's.
    let sink = cdr::spawn(pg.target.clone(), 16, &opentelemetry::global::meter("test"));
    sink.submit(CdrOp::Close(Box::new(record("acme", a))));
    sink.submit(CdrOp::Close(Box::new(record("acme", a))));
    sink.submit(CdrOp::Close(Box::new(record("globex", b))));
    let until = Instant::now() + Duration::from_secs(30);
    // The writer keeps order: once the globex row is there, the duplicate has been handled.
    while count_soon(
        &pg,
        &format!("SELECT count(*) FROM cdr WHERE interaction_id = '{b}'"),
    )
    .await
        == 0
    {
        assert!(Instant::now() < until, "the writer never wrote");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(
        count(
            &pg,
            &format!("SELECT count(*) FROM cdr WHERE interaction_id = '{a}'")
        )
        .await,
        1
    );

    // Directly: a second insert of the same call writes nothing.
    let mut c = cdr::connect(&pg.target).await.unwrap();
    assert!(!cdr::insert(&mut c, &record("acme", a)).await.unwrap());
    assert_eq!(count(&pg, "SELECT count(*) FROM cdr").await, 2);

    // Row-level security: a non-owner reader sees only the tenant it sets, and nothing unset.
    c.batch_execute(
        "CREATE ROLE cdr_reader LOGIN PASSWORD 'reader-only-in-this-test'; GRANT SELECT ON cdr TO cdr_reader",
    )
    .await
    .unwrap();
    let reader = PgTarget {
        user: "cdr_reader".into(),
        password: "reader-only-in-this-test".into(),
        migrate: false,
        ..pg.target.clone()
    };
    let r = cdr::connect(&reader).await.unwrap();
    let n: i64 = r
        .query_one("SELECT count(*) FROM cdr", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(n, 0, "no tenant set: no rows");
    r.execute("SELECT set_config('app.tenant_id', 'globex', false)", &[])
        .await
        .unwrap();
    let rows = r.query("SELECT tenant_id FROM cdr", &[]).await.unwrap();
    let tenants: Vec<String> = rows.iter().map(|row| row.get(0)).collect();
    assert_eq!(
        tenants,
        vec!["globex".to_owned()],
        "globex sees only its own row"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshots_then_one_row_and_a_restarted_ie_writes_an_orphans_cdr() {
    ie_obs::testing::capture_to_file("cdr");
    let pg = postgres().await;
    let mut c = cdr::connect(&pg.target).await.unwrap();
    cdr::migrate(&c).await.unwrap();
    cdr::migrate(&c).await.unwrap(); // repeatable
    let a = "01a17e9b-0000-7000-8000-000000000011";
    // Snapshots while the call lives (a repeat is one row), then the CDR row once.
    assert!(
        cdr::progress(&mut c, &queued("acme", a, false), 1)
            .await
            .unwrap()
    );
    assert!(
        !cdr::progress(&mut c, &queued("acme", a, false), 1)
            .await
            .unwrap()
    );
    assert!(
        cdr::progress(&mut c, &queued("acme", a, true), 2)
            .await
            .unwrap()
    );
    let mut closed = queued("acme", a, true);
    closed.end = record("acme", a).end;
    closed.recording_pauses = 1;
    assert!(cdr::insert(&mut c, &closed).await.unwrap());
    assert!(
        !cdr::insert(&mut c, &closed).await.unwrap(),
        "one row per call"
    );
    // Its CDR is there: an orphan close (a late restart) writes nothing.
    let end = CdrEnd {
        ended_at_ms: 1_793_125_060_000,
        end_reason: "AllLegsGone".into(),
        ended_by: "remote",
        sip_status: 0,
    };
    assert_eq!(
        cdr::close_orphan(&mut c, "acme", a, &end).await.unwrap(),
        None
    );
    let row = c
        .query_one(
            "SELECT count(*) OVER (), queue_outcome, wait_ms, agent_id, offers, recording_pauses, \
             closed_by, duration_ms FROM cdr WHERE interaction_id = $1::text::uuid",
            &[&a],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, i64>(0), 1);
    assert_eq!(row.get::<_, String>(1), "connected");
    assert_eq!(row.get::<_, i64>(2), 5_000);
    assert_eq!(row.get::<_, String>(3), "agent-a1");
    assert_eq!(row.get::<_, i32>(4), 2);
    assert_eq!(row.get::<_, i32>(5), 1);
    assert_eq!(row.get::<_, String>(6), "owner");
    assert_eq!(row.get::<_, i64>(7), 9_000);

    // IE killed mid-call: the restarted IE writes the CDR of a connected call from its latest
    // snapshot and TC's end, and learns whom routing-engine must free.
    let b = "01a17e9b-0000-7000-8000-000000000012";
    assert!(
        cdr::progress(&mut c, &queued("acme", b, false), 1)
            .await
            .unwrap()
    );
    assert!(
        cdr::progress(&mut c, &queued("acme", b, true), 2)
            .await
            .unwrap()
    );
    // Another tenant naming it writes nothing.
    assert_eq!(
        cdr::close_orphan(&mut c, "globex", b, &end).await.unwrap(),
        None
    );
    let got = cdr::close_orphan(&mut c, "acme", b, &end).await.unwrap();
    assert_eq!(
        got,
        Some(OrphanRow {
            owner_epoch: 3,
            connected: Some((
                "agent-a1".into(),
                "0192f6a1-9100-7ccc-8ddd-000000000001".into()
            )),
            entry_id: None,
        })
    );
    assert_eq!(
        cdr::close_orphan(&mut c, "acme", b, &end).await.unwrap(),
        None,
        "once"
    );
    let row = c
        .query_one(
            "SELECT closed_by, duration_ms, end_reason, agent_id FROM cdr WHERE interaction_id = $1::text::uuid",
            &[&b],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, String>(0), "restart");
    assert_eq!(row.get::<_, i64>(1), 60_000);
    assert_eq!(row.get::<_, String>(2), "AllLegsGone");
    assert_eq!(row.get::<_, String>(3), "agent-a1");
    // Snapshots, and so a CDR written from one, hold numbers only masked.
    let row = c
        .query_one(
            "SELECT caller_number, called_number FROM cdr WHERE interaction_id = $1::text::uuid",
            &[&b],
        )
        .await
        .unwrap();
    assert_eq!(
        (row.get::<_, String>(0), row.get::<_, String>(1)),
        ("***123".to_owned(), "***100".to_owned())
    );
    let n: i64 = c
        .query_one(
            "SELECT count(*) FROM cdr_progress WHERE caller_number LIKE '+%' OR called_number LIKE '+%'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(n, 0, "no snapshot holds a whole number");
    // The same interaction id under another tenant writes no second CDR.
    let mut other = closed.clone();
    other.tenant_id = "globex".into();
    other.interaction_id = a.into();
    assert!(
        !cdr::insert(&mut c, &other).await.unwrap(),
        "one CDR per interaction id"
    );
    // A caller still waiting: abandoned, and its entry is named for a Dequeue.
    let w = "01a17e9b-0000-7000-8000-000000000013";
    assert!(
        cdr::progress(&mut c, &queued("acme", w, false), 1)
            .await
            .unwrap()
    );
    let got = cdr::close_orphan(&mut c, "acme", w, &end)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got.connected, None);
    assert_eq!(
        got.entry_id.as_deref(),
        Some("0192f6a1-9000-7aaa-8bbb-000000000001")
    );
    let row = c
        .query_one(
            "SELECT queue_outcome, wait_ms FROM cdr WHERE interaction_id = $1::text::uuid",
            &[&w],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, String>(0), "abandoned");
    assert_eq!(row.get::<_, i64>(1), 58_000);
    // No snapshot at all: nothing.
    let x = "01a17e9b-0000-7000-8000-000000000014";
    assert_eq!(
        cdr::close_orphan(&mut c, "acme", x, &end).await.unwrap(),
        None
    );
}

/// As deploy runs it (PL-051): migrations applied by an owner role, IE writing as a login role
/// that may only SELECT and INSERT and doesn't bypass row-level security. Every step-6 write is
/// an insert, so all of them work there.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_write_works_for_an_insert_only_role_under_row_level_security() {
    ie_obs::testing::capture_to_file("cdr");
    let pg = postgres().await;
    let su = cdr::connect(&pg.target).await.unwrap();
    su.batch_execute(
        "CREATE ROLE ie_owner NOLOGIN NOBYPASSRLS; \
         CREATE ROLE ie_login LOGIN NOBYPASSRLS PASSWORD 'login-only-in-this-test';",
    )
    .await
    .unwrap();
    su.simple_query("CREATE DATABASE ie_p2 OWNER ie_owner")
        .await
        .unwrap();
    let owner_db = PgTarget {
        dbname: "ie_p2".into(),
        ..pg.target.clone()
    };
    let o = cdr::connect(&owner_db).await.unwrap();
    o.batch_execute(
        "REVOKE ALL ON SCHEMA public FROM PUBLIC; GRANT USAGE, CREATE ON SCHEMA public TO ie_owner; \
         GRANT USAGE ON SCHEMA public TO ie_login; \
         ALTER DEFAULT PRIVILEGES FOR ROLE ie_owner IN SCHEMA public GRANT SELECT, INSERT ON TABLES TO ie_login; \
         SET ROLE ie_owner;",
    )
    .await
    .unwrap();
    cdr::migrate(&o).await.unwrap();
    let login = PgTarget {
        user: "ie_login".into(),
        password: "login-only-in-this-test".into(),
        dbname: "ie_p2".into(),
        migrate: false,
        ..pg.target.clone()
    };
    let mut c = cdr::connect(&login).await.unwrap();
    let a = "01a17e9b-0000-7000-8000-000000000021";
    assert!(
        cdr::progress(&mut c, &queued("acme", a, true), 1)
            .await
            .unwrap()
    );
    let end = CdrEnd {
        ended_at_ms: 1_793_125_060_000,
        end_reason: "AllLegsGone".into(),
        ended_by: "remote",
        sip_status: 0,
    };
    assert!(
        cdr::close_orphan(&mut c, "acme", a, &end)
            .await
            .unwrap()
            .is_some()
    );
    let b = "01a17e9b-0000-7000-8000-000000000022";
    assert!(
        cdr::progress(&mut c, &queued("acme", b, false), 1)
            .await
            .unwrap()
    );
    let mut closed = queued("acme", b, false);
    closed.end = record("acme", b).end;
    assert!(cdr::insert(&mut c, &closed).await.unwrap());
    // globex's writer sees and closes none of acme's (row-level security on both tables).
    assert_eq!(
        cdr::close_orphan(&mut c, "globex", b, &end).await.unwrap(),
        None
    );
    let n: i64 = c
        .query_one("SELECT count(*) FROM cdr", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(n, 0, "no tenant set outside a write: no rows");
}
