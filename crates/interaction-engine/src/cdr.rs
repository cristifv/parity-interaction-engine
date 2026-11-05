//! The CDR writer (PLAN steps 4 and 6): one row per interaction in Postgres, off the call path. A
//! call task hands its writes to a bounded queue; one writer task applies them in order. Every
//! write is an insert (IE's role may only SELECT and INSERT, deploy PL-051).
//!
//! Step 6: while the call lives, IE writes **snapshots** of what it knows ([`CdrOp::Progress`]:
//! at answer and at the queue's moments) to `cdr_progress`; at the session's end it writes the
//! CDR row ([`CdrOp::Close`], `ON CONFLICT DO NOTHING`, so a record seen twice is one row). A call
//! whose IE died mid-call keeps its snapshots; the IE restarted under the same replica id writes
//! its CDR from the latest one and TC's end ([`CdrOp::CloseOrphan`], DEMO step 5). Each write runs
//! in a transaction that sets `app.tenant_id` for the tables' row-level security.
//!
//! STEP-4 MINIMAL: the queue is in memory. Step 9 replaces it with IE's consumer of
//! `interaction.events` (repeat-safe from Kafka offsets); until then a queue full during a long
//! Postgres outage loses writes (counted, logged: IE-032).

use crate::backoff::Backoff;
use opentelemetry::metrics::{Counter, Meter};
use std::fmt;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio_postgres::NoTls;

/// The schema, applied in order at start in the dev, demo and test profiles.
pub const MIGRATIONS: [(&str, &str); 3] = [
    (
        "0001_cdr.sql",
        include_str!("../../../migrations/0001_cdr.sql"),
    ),
    (
        "0002_cdr_queue.sql",
        include_str!("../../../migrations/0002_cdr_queue.sql"),
    ),
    (
        "0003_cdr_parent.sql",
        include_str!("../../../migrations/0003_cdr_parent.sql"),
    ),
];

/// How the call ended.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CdrEnd {
    pub ended_at_ms: i64,
    pub end_reason: String,
    pub ended_by: &'static str,
    pub sip_status: i32,
}

/// One CDR. Numbers are personal data: its `Debug` masks them. No digit is ever in it.
#[derive(Clone, PartialEq, Eq)]
pub struct CdrRecord {
    pub tenant_id: String,
    pub interaction_id: String,
    pub node_id: String,
    pub direction: &'static str,
    pub called_number: String,
    pub caller_number: String,
    pub flow: String,
    pub last_step: String,
    /// `granted`, `refused` or `undecided`.
    pub consent: &'static str,
    pub consent_basis: &'static str,
    pub started_at_ms: i64,
    pub answered_at_ms: Option<i64>,
    /// None: a live call's snapshot.
    pub end: Option<CdrEnd>,
    pub owner_epoch: i64,
    pub gathers: i32,
    pub no_inputs: i32,
    /// IE-046 (empty or None: no queue).
    pub queue_id: String,
    pub entry_id: String,
    pub queued_at_ms: Option<i64>,
    pub dequeued_at_ms: Option<i64>,
    pub queue_outcome: &'static str,
    pub offers: i32,
    pub agent_id: String,
    pub reservation_id: String,
    pub connected_at_ms: Option<i64>,
    pub recorded_legs: i32,
    pub recording_failures: i32,
    pub recording_pauses: i32,
    pub mutes: i32,
    /// The xsid of the call that caused this one (a consult, a callback); empty: none.
    pub parent_interaction_id: String,
}

impl fmt::Debug for CdrRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CdrRecord")
            .field("tenant_id", &self.tenant_id)
            .field("interaction_id", &self.interaction_id)
            .field(
                "called_number",
                &ie_obs::pii::mask_phone(&self.called_number),
            )
            .field(
                "caller_number",
                &ie_obs::pii::mask_phone(&self.caller_number),
            )
            .field("flow", &self.flow)
            .field("consent", &self.consent)
            .field("end", &self.end)
            .field("queue_outcome", &self.queue_outcome)
            .finish_non_exhaustive()
    }
}

/// What the restarted IE learns from writing an orphan's CDR: what to tell routing-engine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrphanRow {
    pub owner_epoch: i64,
    /// The agent who connected, and the reservation (`CallEnded`), if any.
    pub connected: Option<(String, String)>,
    /// The entry still waiting (`Dequeue(ABANDONED)`), if no agent connected.
    pub entry_id: Option<String>,
}

/// One write.
#[derive(Debug)]
pub enum CdrOp {
    /// A snapshot of the live call (`seq`: the call's own counter, from 1).
    Progress(Box<CdrRecord>, i32),
    /// The CDR row (its `end` set).
    Close(Box<CdrRecord>),
    /// IE restarted under the same replica id: the CDR of a call its previous process owned,
    /// from the latest snapshot and TC's end. `done` gets the row's routing facts (None: no
    /// snapshot, or the CDR was written already).
    CloseOrphan {
        tenant_id: String,
        interaction_id: String,
        end: CdrEnd,
        done: oneshot::Sender<Option<OrphanRow>>,
    },
}

impl CdrOp {
    fn ids(&self) -> (&str, &str) {
        match self {
            Self::Progress(r, _) | Self::Close(r) => (&r.tenant_id, &r.interaction_id),
            Self::CloseOrphan {
                tenant_id,
                interaction_id,
                ..
            } => (tenant_id, interaction_id),
        }
    }
}

/// Where Postgres is.
#[derive(Debug, Clone)]
pub struct PgTarget {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub dbname: String,
    /// Read from a file; never logged.
    pub password: String,
    pub migrate: bool,
}

/// The writer's counters.
#[derive(Clone)]
struct CdrMetrics {
    written: Counter<u64>,
    duplicates: Counter<u64>,
    dropped: Counter<u64>,
    failures: Counter<u64>,
}

/// The CDR database from the configuration (the password read from its file).
pub fn target(cfg: &crate::config::Config) -> Result<PgTarget, String> {
    let c = cfg.cdr.as_ref().ok_or("no [cdr]")?;
    let password = std::fs::read_to_string(&c.password_file)
        .map_err(|e| format!("cdr.password_file: {e}"))?
        .trim_end_matches(['\n', '\r'])
        .to_owned();
    let migrate = c.migrate
        && cfg
            .telephony
            .as_ref()
            .is_some_and(|t| t.profile != ie_telephony_client::Profile::Production);
    Ok(PgTarget {
        host: c.host.clone(),
        port: c.port,
        user: c.user.clone(),
        dbname: c.dbname.clone(),
        password,
        migrate,
    })
}

/// The call tasks' end of the queue.
#[derive(Clone)]
pub struct CdrSink {
    tx: mpsc::Sender<CdrOp>,
    dropped: Counter<u64>,
}

impl CdrSink {
    /// Hands a write to the writer without waiting; a full queue drops it (counted, logged).
    pub fn submit(&self, op: CdrOp) {
        if let Err(e) = self.tx.try_send(op) {
            self.dropped.add(1, &[]);
            let op = match e {
                mpsc::error::TrySendError::Full(o) | mpsc::error::TrySendError::Closed(o) => o,
            };
            let (tenant, iid) = op.ids();
            tracing::error!(event = "cdr.write_lost",
                tenant_id = %tenant,
                interaction_id = %iid,
                "the CDR queue is full or closed: this CDR write is lost (IE-032, step 9)"
            );
        }
    }

    /// Writes an orphan's CDR from its latest snapshot and returns its routing facts once written
    /// (None: no snapshot, already written, or the write was lost).
    pub async fn close_orphan(
        &self,
        tenant_id: String,
        interaction_id: String,
        end: CdrEnd,
    ) -> Option<OrphanRow> {
        let (done, rx) = oneshot::channel();
        self.submit(CdrOp::CloseOrphan {
            tenant_id,
            interaction_id,
            end,
            done,
        });
        rx.await.ok().flatten()
    }
}

/// Starts the writer task; returns the sink.
pub fn spawn(target: PgTarget, capacity: usize, meter: &Meter) -> CdrSink {
    let (tx, rx) = mpsc::channel(capacity);
    let m = CdrMetrics {
        written: meter
            .u64_counter("interaction_engine_cdr_written_total")
            .with_description("CDR writes applied (CDR rows and live-call snapshots)")
            .build(),
        duplicates: meter
            .u64_counter("interaction_engine_cdr_duplicates_total")
            .with_description("CDR writes seen again (already there: nothing written)")
            .build(),
        dropped: meter
            .u64_counter("interaction_engine_cdr_dropped_total")
            .with_description("CDR writes lost: the queue was full or closed")
            .build(),
        failures: meter
            .u64_counter("interaction_engine_cdr_write_failures_total")
            .with_description("CDR write attempts that failed (retried)")
            .build(),
    };
    let sink = CdrSink {
        tx,
        dropped: m.dropped.clone(),
    };
    tokio::spawn(writer(target, rx, m));
    sink
}

/// Connects.
pub async fn connect(t: &PgTarget) -> Result<tokio_postgres::Client, tokio_postgres::Error> {
    let mut cfg = tokio_postgres::Config::new();
    cfg.host(&t.host)
        .port(t.port)
        .user(&t.user)
        .dbname(&t.dbname)
        .password(&t.password)
        .application_name("parity-interaction-engine")
        .connect_timeout(Duration::from_secs(3));
    let (client, conn) = cfg.connect(NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            tracing::warn!(event = "dependency.unavailable", dependency = "postgres", error = %describe(&e), "the CDR database connection ended");
        }
    });
    Ok(client)
}

/// Applies the migrations in order (each repeatable).
pub async fn migrate(client: &tokio_postgres::Client) -> Result<(), tokio_postgres::Error> {
    for (_, sql) in MIGRATIONS {
        client.batch_execute(sql).await?;
    }
    Ok(())
}

fn secs(v: i64) -> f64 {
    v as f64 / 1000.0
}

/// The snapshot columns, shared by `cdr_progress` and `cdr` ($1..$28 in [`snapshot_params`]).
const SNAPSHOT: &str = "tenant_id, interaction_id, node_id, direction, called_number, caller_number, \
     flow, last_step, consent, consent_basis, started_at, answered_at, owner_epoch, gathers, \
     no_inputs, queue_id, entry_id, queued_at, dequeued_at, queue_outcome, offers, agent_id, \
     reservation_id, connected_at, recorded_legs, recording_failures, recording_pauses, mutes, \
     parent_interaction_id";

const SNAPSHOT_VALUES: &str = "$1, CAST($2::text AS uuid), $3, $4, $5, $6, $7, $8, $9, $10, \
     to_timestamp($11), to_timestamp($12), $13, $14, $15, NULLIF($16, ''), NULLIF($17, ''), \
     to_timestamp($18), to_timestamp($19), NULLIF($20, ''), $21, NULLIF($22, ''), NULLIF($23, ''), \
     to_timestamp($24), $25, $26, $27, $28, CAST(NULLIF($29, '') AS uuid)";

fn secs_opt(v: Option<i64>) -> Option<f64> {
    v.map(secs)
}

/// The snapshot's parameters, in [`SNAPSHOT`]'s order.
struct Snap {
    started: f64,
    answered: Option<f64>,
    queued: Option<f64>,
    dequeued: Option<f64>,
    connected: Option<f64>,
}

impl Snap {
    fn of(r: &CdrRecord) -> Self {
        Self {
            started: secs(r.started_at_ms),
            answered: secs_opt(r.answered_at_ms),
            queued: secs_opt(r.queued_at_ms),
            dequeued: secs_opt(r.dequeued_at_ms),
            connected: secs_opt(r.connected_at_ms),
        }
    }
}

fn snapshot_params<'a>(
    r: &'a CdrRecord,
    t: &'a Snap,
) -> Vec<&'a (dyn tokio_postgres::types::ToSql + Sync)> {
    vec![
        &r.tenant_id,
        &r.interaction_id,
        &r.node_id,
        &r.direction,
        &r.called_number,
        &r.caller_number,
        &r.flow,
        &r.last_step,
        &r.consent,
        &r.consent_basis,
        &t.started,
        &t.answered,
        &r.owner_epoch,
        &r.gathers,
        &r.no_inputs,
        &r.queue_id,
        &r.entry_id,
        &t.queued,
        &t.dequeued,
        &r.queue_outcome,
        &r.offers,
        &r.agent_id,
        &r.reservation_id,
        &t.connected,
        &r.recorded_legs,
        &r.recording_failures,
        &r.recording_pauses,
        &r.mutes,
        &r.parent_interaction_id,
    ]
}

async fn with_tenant<'c>(
    client: &'c mut tokio_postgres::Client,
    tenant: &str,
) -> Result<tokio_postgres::Transaction<'c>, tokio_postgres::Error> {
    let tx = client.transaction().await?;
    tx.execute("SELECT set_config('app.tenant_id', $1, true)", &[&tenant])
        .await?;
    Ok(tx)
}

/// A snapshot of the live call; `Ok(false)`: that one was written already. Its numbers are kept
/// only as the masking rules print them (last 3 digits): snapshots are minimal.
pub async fn progress(
    client: &mut tokio_postgres::Client,
    r: &CdrRecord,
    seq: i32,
) -> Result<bool, tokio_postgres::Error> {
    let masked = CdrRecord {
        called_number: ie_obs::pii::mask_phone(&r.called_number),
        caller_number: ie_obs::pii::mask_phone(&r.caller_number),
        ..r.clone()
    };
    let r = &masked;
    let tx = with_tenant(client, &r.tenant_id).await?;
    let sql = format!(
        "INSERT INTO cdr_progress ({SNAPSHOT}, progress_seq) VALUES ({SNAPSHOT_VALUES}, $30) \
         ON CONFLICT DO NOTHING"
    );
    let t = Snap::of(r);
    let mut p = snapshot_params(r, &t);
    p.push(&seq);
    let n = tx.execute(sql.as_str(), &p).await?;
    tx.commit().await?;
    Ok(n == 1)
}

/// The CDR row; `Ok(true)` if a row was written, `Ok(false)` if it existed (or `end` is None).
pub async fn insert(
    client: &mut tokio_postgres::Client,
    r: &CdrRecord,
) -> Result<bool, tokio_postgres::Error> {
    let Some(end) = r.end.as_ref() else {
        return Ok(false);
    };
    let tx = with_tenant(client, &r.tenant_id).await?;
    let sql = format!(
        "INSERT INTO cdr ({SNAPSHOT}, ended_at, duration_ms, end_reason, ended_by, sip_status, \
         wait_ms, closed_by) VALUES ({SNAPSHOT_VALUES}, to_timestamp($30), $31, $32, $33, $34, $35, \
         'owner') ON CONFLICT DO NOTHING"
    );
    let t = Snap::of(r);
    let ended = secs(end.ended_at_ms);
    let duration = (end.ended_at_ms - r.started_at_ms).max(0);
    let wait = match (r.queued_at_ms, r.dequeued_at_ms) {
        (Some(q), Some(d)) => Some((d - q).max(0)),
        _ => None,
    };
    let mut p = snapshot_params(r, &t);
    p.extend([
        &ended as &(dyn tokio_postgres::types::ToSql + Sync),
        &duration,
        &end.end_reason,
        &end.ended_by,
        &end.sip_status,
        &wait,
    ]);
    let n = tx.execute(sql.as_str(), &p).await?;
    tx.commit().await?;
    Ok(n == 1)
}

/// IE restarted: the CDR of a call its previous process owned, from the latest snapshot and TC's
/// end (a queue still waiting becomes abandoned). Returns what routing-engine must hear; None: no
/// snapshot (the call never got that far, or it is another tenant's: row-level security), or the
/// CDR is there already.
pub async fn close_orphan(
    client: &mut tokio_postgres::Client,
    tenant_id: &str,
    interaction_id: &str,
    end: &CdrEnd,
) -> Result<Option<OrphanRow>, tokio_postgres::Error> {
    let tx = with_tenant(client, tenant_id).await?;
    let w = "CASE WHEN queue_outcome = 'waiting' THEN";
    let sql = format!(
        "INSERT INTO cdr ({SNAPSHOT}, ended_at, duration_ms, end_reason, ended_by, sip_status, \
         wait_ms, closed_by) \
         SELECT tenant_id, interaction_id, node_id, direction, called_number, caller_number, flow, \
           last_step, consent, consent_basis, started_at, answered_at, owner_epoch, gathers, \
           no_inputs, queue_id, entry_id, queued_at, \
           {w} to_timestamp($3) ELSE dequeued_at END, \
           {w} 'abandoned' ELSE queue_outcome END, \
           offers, agent_id, reservation_id, connected_at, recorded_legs, recording_failures, \
           recording_pauses, mutes, parent_interaction_id, to_timestamp($3), \
           GREATEST(0, (EXTRACT(EPOCH FROM (to_timestamp($3) - started_at)) * 1000)::bigint), \
           $4, $5, $6, \
           GREATEST(0, (EXTRACT(EPOCH FROM (({w} to_timestamp($3) ELSE dequeued_at END) - queued_at)) * 1000)::bigint), \
           'restart' \
         FROM cdr_progress WHERE tenant_id = $1 AND interaction_id = CAST($2::text AS uuid) \
         ORDER BY progress_seq DESC LIMIT 1 \
         ON CONFLICT DO NOTHING \
         RETURNING owner_epoch, agent_id, reservation_id, connected_at IS NOT NULL, entry_id, queue_outcome"
    );
    let row = tx
        .query_opt(
            sql.as_str(),
            &[
                &tenant_id as &(dyn tokio_postgres::types::ToSql + Sync),
                &interaction_id,
                &secs(end.ended_at_ms),
                &end.end_reason,
                &end.ended_by,
                &end.sip_status,
            ],
        )
        .await?;
    tx.commit().await?;
    Ok(row.map(|row| {
        let agent: Option<String> = row.get(1);
        let reservation: Option<String> = row.get(2);
        let connected: bool = row.get(3);
        let entry: Option<String> = row.get(4);
        let outcome: Option<String> = row.get(5);
        OrphanRow {
            owner_epoch: row.get(0),
            connected: match (connected, agent, reservation) {
                (true, Some(a), Some(r)) => Some((a, r)),
                _ => None,
            },
            entry_id: entry.filter(|_| !connected && outcome.as_deref() == Some("abandoned")),
        }
    }))
}

/// An error for a log line: a server error by its SQLSTATE only (its detail can quote the row,
/// numbers included); a client or connection error as it is.
pub fn describe(e: &tokio_postgres::Error) -> String {
    match e.as_db_error() {
        Some(db) => format!("sqlstate {}", db.code().code()),
        None => e.to_string(),
    }
}

/// A server refusal that retrying can't fix (a data, integrity, syntax or permission error).
fn poison(e: &tokio_postgres::Error) -> bool {
    e.as_db_error().is_some_and(|db| {
        ["22", "23", "42"]
            .iter()
            .any(|c| db.code().code().starts_with(c))
    })
}

/// What a write did.
enum Applied {
    Written,
    Unchanged,
    Orphan(Option<OrphanRow>),
}

async fn apply(
    c: &mut tokio_postgres::Client,
    op: &CdrOp,
) -> Result<Applied, tokio_postgres::Error> {
    let written = |w: bool| {
        if w {
            Applied::Written
        } else {
            Applied::Unchanged
        }
    };
    match op {
        CdrOp::Progress(r, seq) => Ok(written(progress(c, r, *seq).await?)),
        CdrOp::Close(r) => Ok(written(insert(c, r).await?)),
        CdrOp::CloseOrphan {
            tenant_id,
            interaction_id,
            end,
            ..
        } => Ok(Applied::Orphan(
            close_orphan(c, tenant_id, interaction_id, end).await?,
        )),
    }
}

async fn writer(t: PgTarget, mut rx: mpsc::Receiver<CdrOp>, m: CdrMetrics) {
    let mut client: Option<tokio_postgres::Client> = None;
    let mut migrated = !t.migrate;
    let mut backoff = Backoff::new(Duration::from_millis(200), Duration::from_secs(10));
    while let Some(op) = rx.recv().await {
        let (tenant, iid) = {
            let (a, b) = op.ids();
            (a.to_owned(), b.to_owned())
        };
        let kind = match &op {
            CdrOp::Progress(..) => "progress",
            CdrOp::Close(_) => "close",
            CdrOp::CloseOrphan { .. } => "close_orphan",
        };
        // Retried until written: the queue holds the rest meanwhile.
        let result = loop {
            if client
                .as_ref()
                .is_none_or(tokio_postgres::Client::is_closed)
            {
                match connect(&t).await {
                    Ok(c) => client = Some(c),
                    Err(e) => {
                        m.failures.add(1, &[]);
                        tracing::warn!(event = "dependency.unavailable", dependency = "postgres", error = %describe(&e), "can't reach the CDR database; retrying");
                        tokio::time::sleep(backoff.next_delay()).await;
                        continue;
                    }
                }
            }
            let Some(c) = client.as_mut() else {
                continue;
            };
            if !migrated {
                match migrate(c).await {
                    Ok(()) => {
                        migrated = true;
                        tracing::info!(event = "cdr.schema_applied", "CDR schema applied");
                    }
                    Err(e) => {
                        m.failures.add(1, &[]);
                        tracing::warn!(event = "cdr.migration_failed",  error = %describe(&e), "the CDR migration failed; retrying");
                        client = None;
                        tokio::time::sleep(backoff.next_delay()).await;
                        continue;
                    }
                }
            }
            match apply(c, &op).await {
                Ok(a) => break Some(a),
                Err(e) if poison(&e) => {
                    m.dropped.add(1, &[]);
                    tracing::error!(event = "cdr.write_lost",  tenant_id = %tenant, interaction_id = %iid, write = kind, error = %describe(&e), "Postgres refused this CDR write; dropped (not retried)");
                    break None;
                }
                Err(e) => {
                    m.failures.add(1, &[]);
                    tracing::warn!(event = "cdr.write_retried",  tenant_id = %tenant, interaction_id = %iid, write = kind, error = %describe(&e), "a CDR write failed; retrying");
                    client = None;
                    tokio::time::sleep(backoff.next_delay()).await;
                }
            }
        };
        match result {
            Some(Applied::Written) => {
                m.written.add(1, &[]);
                tracing::info!(event = "cdr.written", outcome = "ok", tenant_id = %tenant, interaction_id = %iid, write = kind, "CDR written");
            }
            Some(Applied::Unchanged) => {
                m.duplicates.add(1, &[]);
                tracing::info!(event = "cdr.written", outcome = "ok", existing = true, tenant_id = %tenant, interaction_id = %iid, write = kind, "CDR write seen before: unchanged");
            }
            Some(Applied::Orphan(row)) => {
                if row.is_some() {
                    m.written.add(1, &[]);
                } else {
                    m.duplicates.add(1, &[]);
                }
                tracing::info!(event = "cdr.written", outcome = "ok", tenant_id = %tenant, interaction_id = %iid, closed = row.is_some(), "CDR of a call owned before the restart: closed from TC's end");
                if let CdrOp::CloseOrphan { done, .. } = op {
                    let _ = done.send(row);
                }
            }
            None => {}
        }
        backoff = Backoff::new(Duration::from_millis(200), Duration::from_secs(10));
    }
}
