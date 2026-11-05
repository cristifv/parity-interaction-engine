//! Wire conformance (ADR 0003 §5, D7): every `telephony.v1` script of parity-contracts, played from
//! IE's side through the generated Rust client against fake-telephony-controller (TC's jar, spawned
//! on free ports). Matching follows TC's own runner: bodies parsed strictly into the generated
//! types and compared as messages; session messages in order per (stream, session); node-level
//! ones where a step expects one; a second Watch for a replica ends the older one
//! (ABORTED / `stream_superseded`); at the end nothing is left unmatched and the fake reports no
//! invariant violation.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::print_stdout
)]

mod fake;

use parity_proto::telephony::v1 as pb;
use prost_reflect::{DescriptorPool, DynamicMessage};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tonic::metadata::AsciiMetadataValue;

type Client = pb::telephony_service_client::TelephonyServiceClient<tonic::transport::Channel>;
const WAIT: Duration = Duration::from_secs(3);

fn contracts() -> PathBuf {
    std::env::var_os("PARITY_CONTRACTS_DIR").map_or_else(
        || PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../parity-contracts"),
        PathBuf::from,
    )
}

fn scripts() -> Vec<PathBuf> {
    let dir = contracts().join("fixtures/telephony.v1");
    let mut out = Vec::new();
    for sub in ["valid", "invalid"] {
        for e in std::fs::read_dir(dir.join(sub)).unwrap() {
            let p = e.unwrap().path();
            if p.extension().is_some_and(|x| x == "json") {
                out.push(p);
            }
        }
    }
    out.sort();
    out
}

fn pool() -> DescriptorPool {
    DescriptorPool::decode(parity_proto::FILE_DESCRIPTOR_SET).unwrap()
}

fn typed<T: prost::Message + Default>(
    pool: &DescriptorPool,
    name: &str,
    body: &Value,
) -> Result<T, String> {
    let desc = pool
        .get_message_by_name(&format!("parity.telephony.v1.{name}"))
        .ok_or(format!("no message {name}"))?;
    let m = DynamicMessage::deserialize(desc, body.clone()).map_err(|e| format!("{name}: {e}"))?;
    m.transcode_to::<T>().map_err(|e| format!("{name}: {e}"))
}

fn code_name(c: tonic::Code) -> &'static str {
    use tonic::Code::*;
    match c {
        Ok => "OK",
        Cancelled => "CANCELLED",
        Unknown => "UNKNOWN",
        InvalidArgument => "INVALID_ARGUMENT",
        DeadlineExceeded => "DEADLINE_EXCEEDED",
        NotFound => "NOT_FOUND",
        AlreadyExists => "ALREADY_EXISTS",
        PermissionDenied => "PERMISSION_DENIED",
        ResourceExhausted => "RESOURCE_EXHAUSTED",
        FailedPrecondition => "FAILED_PRECONDITION",
        Aborted => "ABORTED",
        OutOfRange => "OUT_OF_RANGE",
        Unimplemented => "UNIMPLEMENTED",
        Internal => "INTERNAL",
        Unavailable => "UNAVAILABLE",
        DataLoss => "DATA_LOSS",
        Unauthenticated => "UNAUTHENTICATED",
    }
}

fn trailer(s: &tonic::Status) -> Option<String> {
    s.metadata()
        .get("parity-error")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned)
}

/// One Watch stream as the runner sees it.
#[derive(Default)]
struct StreamState {
    replica_id: String,
    first: Option<pb::WatchResponse>,
    closed: Option<(String, Option<String>)>,
    node: VecDeque<pb::WatchResponse>,
    sessions: HashMap<String, VecDeque<pb::WatchResponse>>,
}

struct Runner<'a> {
    pool: DescriptorPool,
    fake: &'a fake::Fake,
    client: Client,
    streams: HashMap<String, (Arc<Mutex<StreamState>>, tokio::task::JoinHandle<()>)>,
    declared: HashMap<String, (String, String)>,
    peers: HashMap<String, String>,
    delta: Option<i64>,
}

impl Runner<'_> {
    fn map_epochs(&self, v: &Value) -> Value {
        let mut v = v.clone();
        fn walk(v: &mut Value, d: i64) {
            match v {
                Value::Object(m) => {
                    for (k, x) in m.iter_mut() {
                        if k == "owner_epoch" {
                            let n: i64 = x
                                .as_str()
                                .and_then(|s| s.parse().ok())
                                .or_else(|| x.as_i64())
                                .unwrap_or(0);
                            if n != 0 {
                                *x = Value::String((n + d).to_string());
                            }
                        } else {
                            walk(x, d);
                        }
                    }
                }
                Value::Array(a) => a.iter_mut().for_each(|x| walk(x, d)),
                _ => {}
            }
        }
        if let Some(d) = self.delta {
            walk(&mut v, d);
        }
        v
    }

    fn metadata<T>(&self, step: &Value, label: &str, msg: T) -> tonic::Request<T> {
        let mut r = tonic::Request::new(msg);
        r.set_timeout(Duration::from_secs(5));
        if let Some(m) = step.get("metadata").and_then(Value::as_object) {
            for (k, v) in m {
                let key = tonic::metadata::AsciiMetadataKey::from_bytes(k.as_bytes()).unwrap();
                r.metadata_mut().insert(
                    key,
                    AsciiMetadataValue::try_from(v.as_str().unwrap()).unwrap(),
                );
            }
        }
        let peer = match step.get("peer").and_then(Value::as_str) {
            Some(p) => self.peers[p].clone(),
            None => format!("parity-interaction-engine.{}", self.declared[label].1),
        };
        r.metadata_mut().insert(
            "x-parity-test-peer",
            AsciiMetadataValue::try_from(peer).unwrap(),
        );
        r
    }

    async fn run(&mut self, file: &Path) -> Result<(), String> {
        let script: Value = serde_json::from_str(&std::fs::read_to_string(file).unwrap()).unwrap();
        for (k, v) in script
            .get("streams")
            .and_then(Value::as_object)
            .into_iter()
            .flatten()
        {
            self.declared.insert(
                k.clone(),
                (
                    v["replica_id"].as_str().unwrap().into(),
                    v["bin"].as_str().unwrap().into(),
                ),
            );
        }
        for (k, v) in script
            .get("peers")
            .and_then(Value::as_object)
            .into_iter()
            .flatten()
        {
            self.peers.insert(k.clone(), v.as_str().unwrap().into());
        }
        let r = self.fake.reset(&script["given"]).await;
        for (s, real) in r
            .get("epochs")
            .and_then(Value::as_object)
            .into_iter()
            .flatten()
        {
            let s: i64 = s.parse().unwrap();
            let real: i64 = real
                .as_str()
                .map_or_else(|| real.as_i64().unwrap(), |x| x.parse().unwrap());
            self.delta = Some(real - s);
        }
        for (i, step) in script["steps"].as_array().unwrap().iter().enumerate() {
            let op = step["op"].as_str().unwrap();
            let what = step
                .get("method")
                .or_else(|| step.get("what"))
                .and_then(Value::as_str)
                .unwrap_or("");
            self.step(step)
                .await
                .map_err(|e| format!("step {i} ({op} {what}): {e}"))?;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
        for (label, (s, _)) in &self.streams {
            let s = s.lock().unwrap();
            for (key, q) in &s.sessions {
                if let Some(m) = q.front() {
                    return Err(format!(
                        "stream {label} has an unmatched message for {key}: {m:?}"
                    ));
                }
            }
        }
        let v = self.fake.invariants().await;
        if !v.is_empty() {
            return Err(format!("invariants: {v:?}"));
        }
        Ok(())
    }

    async fn step(&mut self, step: &Value) -> Result<(), String> {
        match step["op"].as_str().unwrap() {
            "advance" => {
                let ms = step["ms"]
                    .as_str()
                    .map_or_else(|| step["ms"].as_u64().unwrap(), |s| s.parse().unwrap());
                self.fake.advance(ms).await;
                Ok(())
            }
            "inject" => {
                self.inject(
                    step["what"].as_str().unwrap(),
                    &self.map_epochs(&step["args"]),
                )
                .await
            }
            "call" => self.call(step).await,
            "deliver" => self.deliver(step).await,
            other => Err(format!("unknown op {other}")),
        }
    }

    async fn inject(&mut self, what: &str, args: &Value) -> Result<(), String> {
        let mut a = args.clone();
        match what {
            "stream_drop" => {
                let label = a["stream"].as_str().unwrap().to_owned();
                let st = Arc::clone(&self.streams[&label].0);
                let replica = st.lock().unwrap().replica_id.clone();
                a["replica_id"] = Value::String(replica);
                self.fake.inject(what, &a).await;
                expect_closed(&st, "UNAVAILABLE", None).await
            }
            "owner_claim" => {
                // The script's epoch; the writer decides the real one.
                let mapped: i64 = a["owner_epoch"].as_str().unwrap().parse().unwrap();
                let script_epoch = mapped - self.delta.unwrap_or(0);
                a["owner_epoch"] = Value::String(script_epoch.to_string());
                let r = self.fake.inject(what, &a).await;
                if let Some(real) = r.get("revision").and_then(|v| {
                    v.as_str()
                        .map(|s| s.parse::<i64>().unwrap())
                        .or_else(|| v.as_i64())
                }) && real != 0
                {
                    self.delta = Some(real - script_epoch);
                }
                Ok(())
            }
            "tc_restart" | "tc_crash_mid_fence_raise" => {
                let open: Vec<_> = self
                    .streams
                    .values()
                    .filter(|(s, _)| s.lock().unwrap().closed.is_none())
                    .map(|(s, _)| Arc::clone(s))
                    .collect();
                self.fake.inject(what, &a).await;
                for s in open {
                    expect_closed(&s, "UNAVAILABLE", None).await?;
                }
                Ok(())
            }
            _ => {
                self.fake.inject(what, &a).await;
                Ok(())
            }
        }
    }

    async fn call(&mut self, step: &Value) -> Result<(), String> {
        let method = step["method"]
            .as_str()
            .unwrap()
            .rsplit('/')
            .next()
            .unwrap()
            .to_owned();
        let label = step["stream"].as_str().unwrap().to_owned();
        let body = self.map_epochs(&step["body"]);
        let expect = self.map_epochs(&step["expect"]);
        if method == "Watch" {
            return self.watch(step, &label, &body, &expect).await;
        }
        macro_rules! unary {
            ($m:ident, $Req:ident, $Resp:ident) => {{
                let req: pb::$Req = typed(&self.pool, stringify!($Req), &body)?;
                let r = self.metadata(step, &label, req);
                match self.client.clone().$m(r).await {
                    Ok(resp) => {
                        let got = resp.into_inner();
                        if expect.get("status").is_some() {
                            return Err(format!(
                                "expected {} / {}, got {got:?}",
                                expect["status"], expect["parity_error"]
                            ));
                        }
                        let want: pb::$Resp =
                            typed(&self.pool, stringify!($Resp), &expect["body"])?;
                        if want != got {
                            return Err(format!(
                                "answer differs:\n  want {want:?}\n  got  {got:?}"
                            ));
                        }
                        Ok(())
                    }
                    Err(s) => {
                        let (code, t) = (code_name(s.code()), trailer(&s));
                        if expect.get("status").is_none() {
                            return Err(format!(
                                "expected {}, got {code} / {t:?}",
                                expect["message"]
                            ));
                        }
                        if expect["status"].as_str() != Some(code)
                            || expect["parity_error"].as_str() != t.as_deref()
                        {
                            return Err(format!(
                                "expected {} / {}, got {code} / {t:?}",
                                expect["status"], expect["parity_error"]
                            ));
                        }
                        Ok(())
                    }
                }
            }};
        }
        match method.as_str() {
            "Acknowledge" => unary!(acknowledge, AcknowledgeRequest, AcknowledgeResponse),
            "ClaimSession" => unary!(claim_session, ClaimSessionRequest, ClaimSessionResponse),
            "GetSession" => unary!(get_session, GetSessionRequest, GetSessionResponse),
            "DeclineArrival" => unary!(
                decline_arrival,
                DeclineArrivalRequest,
                DeclineArrivalResponse
            ),
            "RingLeg" => unary!(ring_leg, RingLegRequest, RingLegResponse),
            "AnswerLeg" => unary!(answer_leg, AnswerLegRequest, AnswerLegResponse),
            "RejectLeg" => unary!(reject_leg, RejectLegRequest, RejectLegResponse),
            "AddLeg" => unary!(add_leg, AddLegRequest, AddLegResponse),
            "RemoveLeg" => unary!(remove_leg, RemoveLegRequest, RemoveLegResponse),
            "EndSession" => unary!(end_session, EndSessionRequest, EndSessionResponse),
            "Play" => unary!(play, PlayRequest, PlayResponse),
            "StopPlay" => unary!(stop_play, StopPlayRequest, StopPlayResponse),
            "SetDigitReporting" => unary!(
                set_digit_reporting,
                SetDigitReportingRequest,
                SetDigitReportingResponse
            ),
            "SendDtmf" => unary!(send_dtmf, SendDtmfRequest, SendDtmfResponse),
            "SetMute" => unary!(set_mute, SetMuteRequest, SetMuteResponse),
            "SetHold" => unary!(set_hold, SetHoldRequest, SetHoldResponse),
            "SetIsolation" => unary!(set_isolation, SetIsolationRequest, SetIsolationResponse),
            "SetMonitorMode" => unary!(
                set_monitor_mode,
                SetMonitorModeRequest,
                SetMonitorModeResponse
            ),
            "SetDtmfActions" => unary!(
                set_dtmf_actions,
                SetDtmfActionsRequest,
                SetDtmfActionsResponse
            ),
            "StartRecording" => unary!(
                start_recording,
                StartRecordingRequest,
                StartRecordingResponse
            ),
            "StopRecording" => unary!(stop_recording, StopRecordingRequest, StopRecordingResponse),
            other => Err(format!("no method {other}")),
        }
    }

    async fn watch(
        &mut self,
        step: &Value,
        label: &str,
        body: &Value,
        expect: &Value,
    ) -> Result<(), String> {
        let req: pb::WatchRequest = typed(&self.pool, "WatchRequest", body)?;
        let older = self
            .streams
            .values()
            .find(|(s, _)| {
                let s = s.lock().unwrap();
                s.replica_id == req.replica_id && s.closed.is_none()
            })
            .map(|(s, _)| Arc::clone(s));
        let state = Arc::new(Mutex::new(StreamState {
            replica_id: req.replica_id.clone(),
            ..StreamState::default()
        }));
        let r = self.metadata(step, label, req);
        let mut c = self.client.clone();
        let st = Arc::clone(&state);
        let task = tokio::spawn(async move {
            let mut stream = match c.watch(r).await {
                Ok(s) => s.into_inner(),
                Err(e) => {
                    st.lock().unwrap().closed = Some((code_name(e.code()).into(), trailer(&e)));
                    return;
                }
            };
            loop {
                match stream.message().await {
                    Ok(Some(m)) => {
                        let mut s = st.lock().unwrap();
                        if s.first.is_none() {
                            s.first = Some(m);
                        } else if let Some(sess) = &m.session {
                            let key = format!("{}/{}", sess.tenant_id, sess.interaction_id);
                            s.sessions.entry(key).or_default().push_back(m);
                        } else {
                            s.node.push_back(m);
                        }
                    }
                    Ok(None) => {
                        st.lock().unwrap().closed = Some(("OK".into(), None));
                        return;
                    }
                    Err(e) => {
                        st.lock().unwrap().closed = Some((code_name(e.code()).into(), trailer(&e)));
                        return;
                    }
                }
            }
        });
        if let Some((_, old)) = self
            .streams
            .insert(label.to_owned(), (Arc::clone(&state), task))
        {
            old.abort();
        }
        if expect.get("status").is_some() {
            return expect_closed(
                &state,
                expect["status"].as_str().unwrap(),
                expect["parity_error"].as_str(),
            )
            .await;
        }
        let until = tokio::time::Instant::now() + WAIT;
        let first = loop {
            if let Some(f) = state.lock().unwrap().first.clone() {
                break f;
            }
            if let Some(c) = state.lock().unwrap().closed.clone() {
                return Err(format!("Watch failed: {c:?}"));
            }
            if tokio::time::Instant::now() > until {
                return Err("Watch: no first message within 3 s".into());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        let want: pb::WatchResponse = typed(&self.pool, "WatchResponse", &expect["body"])?;
        if want != first {
            return Err(format!(
                "Watch's first message differs:\n  want {want:?}\n  got  {first:?}"
            ));
        }
        if let Some(old) = older {
            expect_closed(&old, "ABORTED", Some("stream_superseded")).await?;
        }
        Ok(())
    }

    async fn deliver(&mut self, step: &Value) -> Result<(), String> {
        let label = step["stream"].as_str().unwrap();
        let st = Arc::clone(
            &self
                .streams
                .get(label)
                .ok_or(format!("no stream {label}"))?
                .0,
        );
        let want: pb::WatchResponse =
            typed(&self.pool, "WatchResponse", &self.map_epochs(&step["body"]))?;
        let until = tokio::time::Instant::now() + WAIT;
        loop {
            {
                let mut s = st.lock().unwrap();
                match &want.session {
                    None => {
                        while let Some(m) = s.node.pop_front() {
                            if m == want {
                                return Ok(());
                            }
                        }
                    }
                    Some(sess) => {
                        let key = format!("{}/{}", sess.tenant_id, sess.interaction_id);
                        if let Some(got) = s.sessions.get_mut(&key).and_then(VecDeque::pop_front) {
                            return if got == want {
                                Ok(())
                            } else {
                                Err(format!(
                                    "delivered message differs:\n  want {want:?}\n  got  {got:?}"
                                ))
                            };
                        }
                    }
                }
            }
            if tokio::time::Instant::now() > until {
                return Err(format!("nothing delivered; wanted {want:?}"));
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

async fn expect_closed(
    st: &Arc<Mutex<StreamState>>,
    code: &str,
    error: Option<&str>,
) -> Result<(), String> {
    let until = tokio::time::Instant::now() + WAIT;
    loop {
        let closed = st.lock().unwrap().closed.clone();
        if let Some((c, t)) = closed {
            return if c == code && (error.is_none() || t.as_deref() == error) {
                Ok(())
            } else {
                Err(format!(
                    "stream ended {c} / {t:?}, expected {code} / {error:?}"
                ))
            };
        }
        if tokio::time::Instant::now() > until {
            return Err(format!("stream should have ended with {code}"));
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_telephony_script_passes_from_ies_side_against_the_fake() {
    let fake = fake::Fake::start().await;
    let channel = tonic::transport::Endpoint::from_shared(fake.grpc_uri())
        .unwrap()
        .connect()
        .await
        .unwrap();
    let all = scripts();
    assert!(
        all.len() == 64,
        "{} telephony.v1 scripts in {}, expected exactly 64 (0.17.0): a new script means an IE change",
        all.len(),
        contracts().display()
    );
    let mut failures = BTreeMap::new();
    for file in &all {
        let mut runner = Runner {
            pool: pool(),
            fake: &fake,
            client: Client::new(channel.clone()),
            streams: HashMap::new(),
            declared: HashMap::new(),
            peers: HashMap::new(),
            delta: None,
        };
        let r = runner.run(file).await;
        for (_, (_, t)) in runner.streams.drain() {
            t.abort();
        }
        if let Err(e) = r {
            failures.insert(file.file_name().unwrap().to_string_lossy().into_owned(), e);
        }
    }
    println!(
        "{} telephony.v1 scripts played, {} failed",
        all.len(),
        failures.len()
    );
    assert!(
        failures.is_empty(),
        "{} of {} scripts failed:\n{}",
        failures.len(),
        all.len(),
        failures
            .iter()
            .map(|(k, v)| format!("- {k}: {v}"))
            .collect::<Vec<_>>()
            .join("\n")
    );
}
