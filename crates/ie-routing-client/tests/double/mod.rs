//! An in-process test double of `RoutingService` for the client's tests: it records every
//! request with its `tenant-id`, answers from a script the test sets, and streams what the test
//! pushes. It routes nothing (no queues, no agents): test code only, never a routing engine.

#![allow(clippy::unwrap_used, clippy::expect_used, dead_code)]

use parity_proto::routing::v1 as pb;
use pb::routing_service_server::{RoutingService, RoutingServiceServer};
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use tokio_stream::Stream;
use tonic::{Request, Response, Status};

/// A request as the double saw it.
#[derive(Debug, Clone)]
pub enum Seen {
    Enqueue(Option<String>, pb::EnqueueRequest),
    Dequeue(Option<String>, pb::DequeueRequest),
    ReportOutcome(Option<String>, pb::ReportOutcomeRequest),
    CallEnded(Option<String>, pb::CallEndedRequest),
    Reattach(Option<String>, pb::ReattachRequest),
    Acknowledge(pb::AcknowledgeRequest),
    Watch(pb::WatchRequest),
}

type Answer<T> = Result<T, Status>;
type WatchTx = mpsc::Sender<Result<pb::WatchResponse, Status>>;

#[derive(Default)]
struct Inner {
    seen: Vec<Seen>,
    enqueue: VecDeque<Answer<pb::EnqueueResponse>>,
    dequeue: VecDeque<Answer<pb::DequeueResponse>>,
    outcome: VecDeque<Answer<pb::ReportOutcomeResponse>>,
    reattach: VecDeque<(Vec<pb::WatchResponse>, Answer<pb::ReattachResponse>)>,
    streams: Vec<WatchTx>,
}

#[derive(Clone, Default)]
pub struct Double {
    inner: Arc<Mutex<Inner>>,
    /// IE-059: `Some(leader)` while this instance is a standby: every RPC and `Watch` answers
    /// `UNAVAILABLE` / `not_leader` with `parity-leader` = the leader (empty: none named).
    standby: Arc<Mutex<Option<String>>>,
    /// Requests this instance itself answered (a pair shares `seen`).
    served: Arc<Mutex<usize>>,
}

/// A `Status` with the `parity-error` trailer.
pub fn error(code: tonic::Code, parity_error: &str) -> Status {
    let mut s = Status::new(code, parity_error.to_owned());
    s.metadata_mut()
        .insert("parity-error", parity_error.parse().unwrap());
    s
}

fn tenant<T>(r: &Request<T>) -> Option<String> {
    r.metadata()
        .get("tenant-id")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned)
}

impl Double {
    /// A leader and a standby of one bin sharing one state (RE's `re.state`): what the test
    /// scripts or pushes is the bin's, whichever instance serves it.
    pub fn pair() -> (Self, Self) {
        let a = Self::default();
        let b = Self {
            inner: Arc::clone(&a.inner),
            standby: Arc::default(),
            served: Arc::default(),
        };
        (a, b)
    }

    /// This instance turns standby, naming `leader` (`host:port`; empty: none).
    pub fn set_standby(&self, leader: &str) {
        *self.standby.lock().unwrap() = Some(leader.to_owned());
    }

    pub fn set_leader(&self) {
        *self.standby.lock().unwrap() = None;
    }

    /// Requests (and Watch opens) this instance served as the leader.
    pub fn served(&self) -> usize {
        *self.served.lock().unwrap()
    }

    /// `Err(not_leader)` while a standby; counts the request otherwise.
    fn gate(&self) -> Result<(), Status> {
        if let Some(leader) = self.standby.lock().unwrap().clone() {
            let mut s = error(tonic::Code::Unavailable, "not_leader");
            if !leader.is_empty() {
                s.metadata_mut()
                    .insert("parity-leader", leader.parse().unwrap());
            }
            return Err(s);
        }
        *self.served.lock().unwrap() += 1;
        Ok(())
    }

    /// Ends every stream with `not_leader` naming `leader` (the old leader stepping down).
    pub async fn step_down(&self, leader: &str) {
        self.set_standby(leader);
        let txs: Vec<WatchTx> = self.inner.lock().unwrap().streams.clone();
        let mut s = error(tonic::Code::Unavailable, "not_leader");
        s.metadata_mut()
            .insert("parity-leader", leader.parse().unwrap());
        for tx in txs {
            let _ = tx.send(Err(s.clone())).await;
        }
    }

    pub async fn serve(&self) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let svc = RoutingServiceServer::new(self.clone());
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(svc)
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        format!("http://{addr}")
    }

    pub fn seen(&self) -> Vec<Seen> {
        self.inner.lock().unwrap().seen.clone()
    }

    pub fn answer_enqueue(&self, a: Answer<pb::EnqueueResponse>) {
        self.inner.lock().unwrap().enqueue.push_back(a);
    }

    pub fn answer_dequeue(&self, a: Answer<pb::DequeueResponse>) {
        self.inner.lock().unwrap().dequeue.push_back(a);
    }

    pub fn answer_outcome(&self, a: Answer<pb::ReportOutcomeResponse>) {
        self.inner.lock().unwrap().outcome.push_back(a);
    }

    /// The next `Reattach`: `replay` goes out on the newest stream before the answer (as RE
    /// replays at the epoch raise).
    pub fn answer_reattach(&self, replay: Vec<pb::WatchResponse>, a: Answer<pb::ReattachResponse>) {
        self.inner.lock().unwrap().reattach.push_back((replay, a));
    }

    /// Watch streams opened so far.
    pub fn streams(&self) -> usize {
        self.inner.lock().unwrap().streams.len()
    }

    /// Sends on the newest stream.
    pub async fn push(&self, m: pb::WatchResponse) {
        let tx = self.inner.lock().unwrap().streams.last().cloned().unwrap();
        tx.send(Ok(m)).await.unwrap();
    }

    /// Ends the newest stream with this status.
    pub async fn end(&self, s: Status) {
        let tx = self.inner.lock().unwrap().streams.last().cloned().unwrap();
        tx.send(Err(s)).await.unwrap();
    }

    fn record(&self, s: Seen) {
        self.inner.lock().unwrap().seen.push(s);
    }
}

#[tonic::async_trait]
impl RoutingService for Double {
    async fn enqueue(
        &self,
        r: Request<pb::EnqueueRequest>,
    ) -> Result<Response<pb::EnqueueResponse>, Status> {
        self.gate()?;
        self.record(Seen::Enqueue(tenant(&r), r.get_ref().clone()));
        let a = self.inner.lock().unwrap().enqueue.pop_front();
        a.unwrap_or_else(|| Err(Status::unavailable("no answer scripted")))
            .map(Response::new)
    }

    async fn dequeue(
        &self,
        r: Request<pb::DequeueRequest>,
    ) -> Result<Response<pb::DequeueResponse>, Status> {
        self.gate()?;
        self.record(Seen::Dequeue(tenant(&r), r.get_ref().clone()));
        let a = self.inner.lock().unwrap().dequeue.pop_front();
        a.unwrap_or_else(|| Ok(pb::DequeueResponse::default()))
            .map(Response::new)
    }

    async fn convert_to_callback(
        &self,
        _: Request<pb::ConvertToCallbackRequest>,
    ) -> Result<Response<pb::ConvertToCallbackResponse>, Status> {
        Err(Status::unimplemented("not in the double"))
    }

    async fn create_voicemail_task(
        &self,
        _: Request<pb::CreateVoicemailTaskRequest>,
    ) -> Result<Response<pb::CreateVoicemailTaskResponse>, Status> {
        Err(Status::unimplemented("not in the double"))
    }

    async fn report_outcome(
        &self,
        r: Request<pb::ReportOutcomeRequest>,
    ) -> Result<Response<pb::ReportOutcomeResponse>, Status> {
        self.gate()?;
        self.record(Seen::ReportOutcome(tenant(&r), r.get_ref().clone()));
        let a = self.inner.lock().unwrap().outcome.pop_front();
        let outcome = r.get_ref().outcome;
        a.unwrap_or_else(|| {
            Ok(pb::ReportOutcomeResponse {
                existing: false,
                recorded: outcome,
            })
        })
        .map(Response::new)
    }

    async fn call_ended(
        &self,
        r: Request<pb::CallEndedRequest>,
    ) -> Result<Response<pb::CallEndedResponse>, Status> {
        self.gate()?;
        self.record(Seen::CallEnded(tenant(&r), r.get_ref().clone()));
        Ok(Response::new(pb::CallEndedResponse::default()))
    }

    async fn reattach(
        &self,
        r: Request<pb::ReattachRequest>,
    ) -> Result<Response<pb::ReattachResponse>, Status> {
        self.gate()?;
        self.record(Seen::Reattach(tenant(&r), r.get_ref().clone()));
        let next = self.inner.lock().unwrap().reattach.pop_front();
        let Some((replay, a)) = next else {
            return Err(Status::unavailable("no answer scripted"));
        };
        let tx = self.inner.lock().unwrap().streams.last().cloned();
        if let Some(tx) = tx {
            for m in replay {
                tx.send(Ok(m)).await.unwrap();
            }
        }
        a.map(Response::new)
    }

    async fn acknowledge(
        &self,
        r: Request<pb::AcknowledgeRequest>,
    ) -> Result<Response<pb::AcknowledgeResponse>, Status> {
        self.gate()?;
        self.record(Seen::Acknowledge(r.into_inner()));
        Ok(Response::new(pb::AcknowledgeResponse::default()))
    }

    // parity-contracts 0.14.0 (campaign lending, IE step 13a): not used by these tests yet.
    async fn lend_agents(
        &self,
        _r: Request<pb::LendAgentsRequest>,
    ) -> Result<Response<pb::LendAgentsResponse>, Status> {
        Err(Status::unimplemented("not in the double"))
    }

    async fn return_agents(
        &self,
        _r: Request<pb::ReturnAgentsRequest>,
    ) -> Result<Response<pb::ReturnAgentsResponse>, Status> {
        Err(Status::unimplemented("not in the double"))
    }

    async fn reserve_agent(
        &self,
        _r: Request<pb::ReserveAgentRequest>,
    ) -> Result<Response<pb::ReserveAgentResponse>, Status> {
        Err(Status::unimplemented("not in the double"))
    }

    type WatchStream = Pin<Box<dyn Stream<Item = Result<pb::WatchResponse, Status>> + Send>>;

    async fn watch(
        &self,
        r: Request<pb::WatchRequest>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        self.gate()?;
        self.record(Seen::Watch(r.into_inner()));
        let (tx, rx) = mpsc::channel(64);
        self.inner.lock().unwrap().streams.push(tx);
        Ok(Response::new(Box::pin(
            tokio_stream::wrappers::ReceiverStream::new(rx),
        )))
    }
}
