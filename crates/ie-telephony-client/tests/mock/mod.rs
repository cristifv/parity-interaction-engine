//! An in-process TC (parity-proto's `server` feature) for the client's behaviour tests: the test
//! pushes Watch messages, ends streams with any status, and scripts each RPC's answers.
#![allow(dead_code, clippy::unwrap_used, clippy::expect_used)]

use parity_proto::telephony::v1 as pb;
use pb::telephony_service_server::{TelephonyService, TelephonyServiceServer};
use pb::watch_response::Event;
use std::collections::{HashMap, VecDeque};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_stream::Stream;
use tonic::{Request, Response, Status};

pub const T: &str = "acme";
pub const IID: &str = "0192f6a1-7c3e-7b21-9a4d-5e6f70819203";
pub const IID2: &str = "0192f6a1-7c3e-7b21-9a4d-5e6f70819204";
pub const LEG: &str = "0192f6a1-8000-7000-8000-000000000001";
pub const LEG2: &str = "0192f6a1-8000-7000-8000-000000000002";
pub const INC_A: &str = "0192f700-0000-7000-8000-000000000001";
pub const INC_B: &str = "0192f700-0000-7000-8000-000000000002";
pub const NODE: &str = "media-1";

/// What a scripted RPC answers.
pub enum Answer {
    Ok,
    Err(Status),
    /// Waits, then answers OK (to exceed a deadline).
    Delay(Duration),
}

/// Epoch milliseconds now (heartbeats carry TC's clock; here the same host's).
pub fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as i64)
}

pub fn status(code: tonic::Code, trailer: &'static str) -> Status {
    let mut s = Status::new(code, "test");
    s.metadata_mut().insert(
        "parity-error",
        tonic::metadata::MetadataValue::from_static(trailer),
    );
    s
}

/// One received unary call: the method, the owner block, the command id, and a short summary.
#[derive(Clone, Debug)]
pub struct Call {
    pub method: &'static str,
    pub owner: Option<pb::SessionOwner>,
    pub command_id: String,
    pub detail: String,
}

/// A GetSession / ClaimSession answer: the session and its incarnation, or an error.
pub type SessionAnswer = Result<(pb::Session, String), Status>;

#[derive(Default)]
pub struct State {
    pub streams: Vec<mpsc::Sender<Result<pb::WatchResponse, Status>>>,
    pub watch_calls: usize,
    pub calls: Vec<Call>,
    /// Per call: the method, its `parity-attempt-expires-ms`, and when it arrived (epoch ms).
    pub expiries: Vec<(&'static str, Option<i64>, i64)>,
    pub acks: Vec<pb::SessionOwner>,
    pub answers: HashMap<&'static str, VecDeque<Answer>>,
    /// GetSession / ClaimSession: by interaction id, the session, its incarnation, or an error.
    pub sessions: HashMap<String, Result<(pb::Session, String), Status>>,
    /// GetSession answers in call order, each after a delay; `sessions` once they run out.
    pub get_answers: VecDeque<(Duration, SessionAnswer)>,
    /// The `last_delivery_seq` of every snapshot GetSession answered, in answer order.
    pub served: Vec<u64>,
}

#[derive(Clone, Default)]
pub struct Mock(pub Arc<Mutex<State>>);

impl Mock {
    pub fn answer(&self, method: &'static str, a: Answer) {
        self.0
            .lock()
            .unwrap()
            .answers
            .entry(method)
            .or_default()
            .push_back(a);
    }
    /// The next GetSession answers this, after `delay`.
    pub fn get_answer(&self, delay: Duration, s: SessionAnswer) {
        self.0.lock().unwrap().get_answers.push_back((delay, s));
    }
    pub fn session(&self, iid: &str, s: Result<(pb::Session, String), Status>) {
        self.0.lock().unwrap().sessions.insert(iid.to_owned(), s);
    }
    pub async fn send(&self, m: pb::WatchResponse) {
        let tx = self.0.lock().unwrap().streams.last().cloned();
        if let Some(tx) = tx {
            let _ = tx.send(Ok(m)).await;
        }
    }
    /// Ends the current stream with a status.
    pub async fn end_stream(&self, s: Status) {
        let tx = self.0.lock().unwrap().streams.pop();
        if let Some(tx) = tx {
            let _ = tx.send(Err(s)).await;
        }
    }
    pub fn calls(&self, method: &str) -> Vec<Call> {
        self.0
            .lock()
            .unwrap()
            .calls
            .iter()
            .filter(|c| c.method == method)
            .cloned()
            .collect()
    }
    pub fn served(&self) -> Vec<u64> {
        self.0.lock().unwrap().served.clone()
    }
    pub fn watch_calls(&self) -> usize {
        self.0.lock().unwrap().watch_calls
    }
    /// (expiry header, arrival) of each call of `method`, in order.
    pub fn expiries(&self, method: &str) -> Vec<(Option<i64>, i64)> {
        self.0
            .lock()
            .unwrap()
            .expiries
            .iter()
            .filter(|e| e.0 == method)
            .map(|e| (e.1, e.2))
            .collect()
    }

    fn note<T>(&self, method: &'static str, r: &Request<T>) {
        let expires = r
            .metadata()
            .get("parity-attempt-expires-ms")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse().ok());
        self.0
            .lock()
            .unwrap()
            .expiries
            .push((method, expires, now_ms()));
    }

    pub fn acks(&self) -> Vec<pb::SessionOwner> {
        self.0.lock().unwrap().acks.clone()
    }

    fn record(
        &self,
        method: &'static str,
        owner: Option<pb::SessionOwner>,
        command_id: &str,
        detail: String,
    ) {
        let mut st = self.0.lock().unwrap();
        if let Some(o) = &owner
            && o.acked_delivery_seq > 0
        {
            st.acks.push(o.clone());
        }
        st.calls.push(Call {
            method,
            owner,
            command_id: command_id.to_owned(),
            detail,
        });
    }

    async fn scripted(&self, method: &'static str) -> Result<(), Status> {
        let a = self
            .0
            .lock()
            .unwrap()
            .answers
            .get_mut(method)
            .and_then(VecDeque::pop_front);
        match a {
            None | Some(Answer::Ok) => Ok(()),
            Some(Answer::Err(s)) => Err(s),
            Some(Answer::Delay(d)) => {
                tokio::time::sleep(d).await;
                Ok(())
            }
        }
    }

    fn snapshot(&self, iid: &str) -> Result<(pb::Session, String), Status> {
        self.0
            .lock()
            .unwrap()
            .sessions
            .get(iid)
            .cloned()
            .unwrap_or_else(|| Err(status(tonic::Code::NotFound, "session_unknown")))
    }

    /// Serves on a free port; the client's URI.
    pub async fn serve(&self) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let svc = TelephonyServiceServer::new(self.clone());
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(svc)
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        format!("http://{addr}")
    }
}

pub fn session(
    iid: &str,
    state: pb::SessionState,
    last: u64,
    legs: Vec<pb::LegSnapshot>,
) -> pb::Session {
    pb::Session {
        tenant_id: T.into(),
        interaction_id: iid.into(),
        owner_epoch: 1042,
        owner_replica_id: "ie-bin-a-1".into(),
        state: state as i32,
        legs,
        last_delivery_seq: last,
        ..Default::default()
    }
}

pub fn leg_snapshot(leg: &str, state: pb::LegState) -> pb::LegSnapshot {
    pb::LegSnapshot {
        leg_id: leg.into(),
        role: pb::LegRole::Agent as i32,
        state: state as i32,
        ..Default::default()
    }
}

fn envelope(inc: &str, iid: Option<&str>, seq: u64, ev: Event) -> pb::WatchResponse {
    pb::WatchResponse {
        incarnation: inc.into(),
        node_id: NODE.into(),
        delivery_seq: seq,
        session: iid.map(|i| pb::SessionRef {
            tenant_id: T.into(),
            interaction_id: i.into(),
            owner_epoch: 0,
        }),
        at_ms: now_ms(),
        event: Some(ev),
    }
}

pub fn heartbeat(inc: &str) -> pb::WatchResponse {
    envelope(inc, None, 0, Event::Heartbeat(pb::Heartbeat {}))
}

pub fn node_status(inc: &str, rebuilding: bool) -> pb::WatchResponse {
    envelope(
        inc,
        None,
        0,
        Event::NodeStatus(pb::NodeStatus {
            sessions: 1,
            free_legs: 100,
            rebuilding,
            ..Default::default()
        }),
    )
}

pub fn arrived(inc: &str, iid: &str, seq: u64) -> pb::WatchResponse {
    envelope(
        inc,
        Some(iid),
        seq,
        Event::Arrived(pb::Arrived {
            bin: "bin-a".into(),
            leg_id: LEG.into(),
            sip_call_id: "a84b4c76e66710@carrier.example".into(),
            called_number: "+447700900100".into(),
            caller_number: "+447700900123".into(),
            attempt: 1,
            deadline_ms: 1_792_800_005_000,
            trunk_id: "carrier-a".into(),
            ..Default::default()
        }),
    )
}

pub fn leg(inc: &str, iid: &str, seq: u64, state: pb::LegState) -> pb::WatchResponse {
    envelope(
        inc,
        Some(iid),
        seq,
        Event::Leg(pb::LegUpdate {
            leg_id: LEG.into(),
            role: pb::LegRole::Customer as i32,
            state: state as i32,
            ..Default::default()
        }),
    )
}

pub fn digit(inc: &str, iid: &str, seq: u64, digit: &str, collection: &str) -> pb::WatchResponse {
    envelope(
        inc,
        Some(iid),
        seq,
        Event::Dtmf(pb::DtmfReceived {
            leg_id: LEG.into(),
            digit: digit.into(),
            collection_id: collection.into(),
            duration_ms: 100,
            source: 1,
        }),
    )
}

pub fn ended(inc: &str, iid: &str, seq: u64) -> pb::WatchResponse {
    envelope(
        inc,
        Some(iid),
        seq,
        Event::SessionEnded(pb::SessionEnded {
            reason: pb::SessionEndReason::AllLegsGone as i32,
            sip_status: 0,
        }),
    )
}

type WatchStream = Pin<Box<dyn Stream<Item = Result<pb::WatchResponse, Status>> + Send>>;

macro_rules! unary {
    ($self:ident, $req:ident, $name:literal, $Resp:ident, $detail:expr) => {{
        $self.note($name, &$req);
        let r = $req.into_inner();
        $self.record($name, r.owner.clone(), &r.command_id, $detail(&r));
        $self.scripted($name).await?;
        Ok(Response::new(pb::$Resp::default()))
    }};
}

#[tonic::async_trait]
impl TelephonyService for Mock {
    type WatchStream = WatchStream;

    async fn watch(
        &self,
        _r: Request<pb::WatchRequest>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        let (tx, rx) = mpsc::channel(1024);
        {
            let mut st = self.0.lock().unwrap();
            st.watch_calls += 1;
            st.streams.push(tx);
        }
        Ok(Response::new(Box::pin(
            tokio_stream::wrappers::ReceiverStream::new(rx),
        )))
    }

    async fn acknowledge(
        &self,
        r: Request<pb::AcknowledgeRequest>,
    ) -> Result<Response<pb::AcknowledgeResponse>, Status> {
        let r = r.into_inner();
        // A scripted error: TC took none of these acks.
        self.scripted("Acknowledge").await?;
        let mut st = self.0.lock().unwrap();
        st.acks.extend(r.acks.iter().cloned());
        st.calls.push(Call {
            method: "Acknowledge",
            owner: None,
            command_id: String::new(),
            detail: format!("{}", r.acks.len()),
        });
        Ok(Response::new(pb::AcknowledgeResponse::default()))
    }

    async fn claim_session(
        &self,
        r: Request<pb::ClaimSessionRequest>,
    ) -> Result<Response<pb::ClaimSessionResponse>, Status> {
        let r = r.into_inner();
        let iid = r
            .owner
            .as_ref()
            .map(|o| o.interaction_id.clone())
            .unwrap_or_default();
        self.record("ClaimSession", r.owner, "", String::new());
        self.scripted("ClaimSession").await?;
        let (s, inc) = self.snapshot(&iid)?;
        Ok(Response::new(pb::ClaimSessionResponse {
            session: Some(s),
            existing: false,
            incarnation: inc,
        }))
    }

    async fn get_session(
        &self,
        r: Request<pb::GetSessionRequest>,
    ) -> Result<Response<pb::GetSessionResponse>, Status> {
        self.note("GetSession", &r);
        let r = r.into_inner();
        self.record("GetSession", None, "", r.interaction_id.clone());
        self.scripted("GetSession").await?;
        let next = self.0.lock().unwrap().get_answers.pop_front();
        let (s, inc) = match next {
            Some((delay, answer)) => {
                tokio::time::sleep(delay).await;
                answer?
            }
            None => self.snapshot(&r.interaction_id)?,
        };
        self.0.lock().unwrap().served.push(s.last_delivery_seq);
        Ok(Response::new(pb::GetSessionResponse {
            session: Some(s),
            incarnation: inc,
        }))
    }

    async fn decline_arrival(
        &self,
        r: Request<pb::DeclineArrivalRequest>,
    ) -> Result<Response<pb::DeclineArrivalResponse>, Status> {
        let r = r.into_inner();
        self.record(
            "DeclineArrival",
            None,
            "",
            format!("{}:{}", r.reason, r.sip_status),
        );
        self.scripted("DeclineArrival").await?;
        Ok(Response::new(pb::DeclineArrivalResponse::default()))
    }

    async fn ring_leg(
        &self,
        r: Request<pb::RingLegRequest>,
    ) -> Result<Response<pb::RingLegResponse>, Status> {
        unary!(
            self,
            r,
            "RingLeg",
            RingLegResponse,
            |r: &pb::RingLegRequest| r.leg_id.clone()
        )
    }
    async fn answer_leg(
        &self,
        r: Request<pb::AnswerLegRequest>,
    ) -> Result<Response<pb::AnswerLegResponse>, Status> {
        unary!(
            self,
            r,
            "AnswerLeg",
            AnswerLegResponse,
            |r: &pb::AnswerLegRequest| r.leg_id.clone()
        )
    }
    async fn reject_leg(
        &self,
        r: Request<pb::RejectLegRequest>,
    ) -> Result<Response<pb::RejectLegResponse>, Status> {
        unary!(
            self,
            r,
            "RejectLeg",
            RejectLegResponse,
            |r: &pb::RejectLegRequest| r.leg_id.clone()
        )
    }
    async fn add_leg(
        &self,
        r: Request<pb::AddLegRequest>,
    ) -> Result<Response<pb::AddLegResponse>, Status> {
        self.note("AddLeg", &r);
        let r = r.into_inner();
        self.record("AddLeg", r.owner.clone(), &r.command_id, r.leg_id.clone());
        self.scripted("AddLeg").await?;
        Ok(Response::new(pb::AddLegResponse {
            existing: false,
            state: pb::LegState::Originating as i32,
            cause: 0,
        }))
    }
    async fn remove_leg(
        &self,
        r: Request<pb::RemoveLegRequest>,
    ) -> Result<Response<pb::RemoveLegResponse>, Status> {
        unary!(
            self,
            r,
            "RemoveLeg",
            RemoveLegResponse,
            |r: &pb::RemoveLegRequest| r.leg_id.clone()
        )
    }
    async fn end_session(
        &self,
        r: Request<pb::EndSessionRequest>,
    ) -> Result<Response<pb::EndSessionResponse>, Status> {
        unary!(
            self,
            r,
            "EndSession",
            EndSessionResponse,
            |r: &pb::EndSessionRequest| r.cause.to_string()
        )
    }
    async fn play(
        &self,
        r: Request<pb::PlayRequest>,
    ) -> Result<Response<pb::PlayResponse>, Status> {
        unary!(self, r, "Play", PlayResponse, |r: &pb::PlayRequest| r
            .play_id
            .clone())
    }
    async fn stop_play(
        &self,
        r: Request<pb::StopPlayRequest>,
    ) -> Result<Response<pb::StopPlayResponse>, Status> {
        unary!(
            self,
            r,
            "StopPlay",
            StopPlayResponse,
            |r: &pb::StopPlayRequest| r.play_id.clone()
        )
    }
    async fn set_digit_reporting(
        &self,
        r: Request<pb::SetDigitReportingRequest>,
    ) -> Result<Response<pb::SetDigitReportingResponse>, Status> {
        unary!(
            self,
            r,
            "SetDigitReporting",
            SetDigitReportingResponse,
            |r: &pb::SetDigitReportingRequest| r.collection_id.clone()
        )
    }
    async fn send_dtmf(
        &self,
        r: Request<pb::SendDtmfRequest>,
    ) -> Result<Response<pb::SendDtmfResponse>, Status> {
        unary!(
            self,
            r,
            "SendDtmf",
            SendDtmfResponse,
            |_r: &pb::SendDtmfRequest| String::new()
        )
    }
    async fn set_mute(
        &self,
        r: Request<pb::SetMuteRequest>,
    ) -> Result<Response<pb::SetMuteResponse>, Status> {
        unary!(
            self,
            r,
            "SetMute",
            SetMuteResponse,
            |r: &pb::SetMuteRequest| r.leg_id.clone()
        )
    }
    async fn set_hold(
        &self,
        r: Request<pb::SetHoldRequest>,
    ) -> Result<Response<pb::SetHoldResponse>, Status> {
        unary!(
            self,
            r,
            "SetHold",
            SetHoldResponse,
            |r: &pb::SetHoldRequest| r.on.to_string()
        )
    }
    async fn set_isolation(
        &self,
        r: Request<pb::SetIsolationRequest>,
    ) -> Result<Response<pb::SetIsolationResponse>, Status> {
        unary!(
            self,
            r,
            "SetIsolation",
            SetIsolationResponse,
            |r: &pb::SetIsolationRequest| r.leg_id.clone()
        )
    }
    async fn set_monitor_mode(
        &self,
        r: Request<pb::SetMonitorModeRequest>,
    ) -> Result<Response<pb::SetMonitorModeResponse>, Status> {
        unary!(
            self,
            r,
            "SetMonitorMode",
            SetMonitorModeResponse,
            |r: &pb::SetMonitorModeRequest| r.leg_id.clone()
        )
    }
    async fn set_dtmf_actions(
        &self,
        r: Request<pb::SetDtmfActionsRequest>,
    ) -> Result<Response<pb::SetDtmfActionsResponse>, Status> {
        unary!(
            self,
            r,
            "SetDtmfActions",
            SetDtmfActionsResponse,
            |r: &pb::SetDtmfActionsRequest| r.leg_id.clone()
        )
    }
    async fn start_recording(
        &self,
        r: Request<pb::StartRecordingRequest>,
    ) -> Result<Response<pb::StartRecordingResponse>, Status> {
        unary!(
            self,
            r,
            "StartRecording",
            StartRecordingResponse,
            |r: &pb::StartRecordingRequest| r.recording_id.clone()
        )
    }
    async fn stop_recording(
        &self,
        r: Request<pb::StopRecordingRequest>,
    ) -> Result<Response<pb::StopRecordingResponse>, Status> {
        unary!(
            self,
            r,
            "StopRecording",
            StopRecordingResponse,
            |r: &pb::StopRecordingRequest| r.recording_id.clone()
        )
    }
}
