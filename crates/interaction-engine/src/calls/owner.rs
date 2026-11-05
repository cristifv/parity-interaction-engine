//! STEP-3 MINIMAL: IE's etcd ownership, pulled forward from step 7 (PLAN "etcd keys and leases",
//! ADR 0003 §6). What is here:
//! - one lease per process (`lease_ttl_s`), kept alive every third of its TTL;
//! - `ie/replicas/{bin}/{replica_id}` on it: TC refuses a `Watch` from a replica that isn't
//!   registered;
//! - `ie/owner/{interaction_id}` created only if absent, on the lease, with the value TC's
//!   `EtcdOwnerDirectory` reads; its create revision is the owner epoch TC verifies;
//! - the owner key deleted when the call ends, only if it is still this claim's.
//!
//! - the lease granted again once it is lost (IE-023): when etcd says it has expired, or when no
//!   keepalive has worked for a whole TTL, there is no lease ([`Lease::current`] is `None`: the
//!   readiness check is not ready and new arrivals are declined 503 at once); a new lease is granted
//!   with backoff, the replica key written on it, and new claims use it. Calls already claimed keep
//!   going: TC checks etcd only for an epoch above its fence.
//!
//! What is not (step 7): stepping down 1 s before the lease could expire, watches, the partition
//! handover, adoption (step 8). Owner keys on a lost lease are gone with it and not written again.
//!
//! etcd's gRPC API through tonic, with hand-written prost messages for the five RPCs used (only
//! the fields IE reads or writes; prost skips the rest), so no etcd client crate is added.

use serde::{Deserialize, Serialize};
use std::time::Duration;
use tonic::codegen::http::uri::PathAndQuery;
use tonic::transport::{Channel, Endpoint};

/// etcd's messages (`etcdserverpb`, `mvccpb`), by tag; field names as in etcd's `rpc.proto`.
mod pb {
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct ResponseHeader {
        #[prost(int64, tag = "3")]
        pub revision: i64,
    }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct LeaseGrantRequest {
        #[prost(int64, tag = "1")]
        pub ttl: i64,
        #[prost(int64, tag = "2")]
        pub id: i64,
    }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct LeaseGrantResponse {
        #[prost(message, optional, tag = "1")]
        pub header: Option<ResponseHeader>,
        #[prost(int64, tag = "2")]
        pub id: i64,
        #[prost(int64, tag = "3")]
        pub ttl: i64,
        #[prost(string, tag = "4")]
        pub error: String,
    }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct LeaseKeepAliveRequest {
        #[prost(int64, tag = "1")]
        pub id: i64,
    }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct LeaseKeepAliveResponse {
        #[prost(int64, tag = "2")]
        pub id: i64,
        #[prost(int64, tag = "3")]
        pub ttl: i64,
    }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct KeyValue {
        #[prost(bytes = "vec", tag = "1")]
        pub key: Vec<u8>,
        #[prost(int64, tag = "2")]
        pub create_revision: i64,
        #[prost(bytes = "vec", tag = "5")]
        pub value: Vec<u8>,
        #[prost(int64, tag = "6")]
        pub lease: i64,
    }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct PutRequest {
        #[prost(bytes = "vec", tag = "1")]
        pub key: Vec<u8>,
        #[prost(bytes = "vec", tag = "2")]
        pub value: Vec<u8>,
        #[prost(int64, tag = "3")]
        pub lease: i64,
    }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct PutResponse {
        #[prost(message, optional, tag = "1")]
        pub header: Option<ResponseHeader>,
    }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct RangeRequest {
        #[prost(bytes = "vec", tag = "1")]
        pub key: Vec<u8>,
    }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct RangeResponse {
        #[prost(message, repeated, tag = "2")]
        pub kvs: Vec<KeyValue>,
    }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct DeleteRangeRequest {
        #[prost(bytes = "vec", tag = "1")]
        pub key: Vec<u8>,
    }
    /// `result` EQUAL is 0; `target` CREATE is 1.
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct Compare {
        #[prost(int32, tag = "1")]
        pub result: i32,
        #[prost(int32, tag = "2")]
        pub target: i32,
        #[prost(bytes = "vec", tag = "3")]
        pub key: Vec<u8>,
        #[prost(oneof = "compare::TargetUnion", tags = "5, 7")]
        pub target_union: Option<compare::TargetUnion>,
    }
    pub mod compare {
        #[derive(Clone, PartialEq, prost::Oneof)]
        pub enum TargetUnion {
            /// Written even when 0 (a oneof member always is): "the key doesn't exist".
            #[prost(int64, tag = "5")]
            CreateRevision(i64),
            /// The key's exact value (`target` VALUE).
            #[prost(bytes = "vec", tag = "7")]
            Value(Vec<u8>),
        }
    }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct RequestOp {
        #[prost(oneof = "request_op::Request", tags = "1, 2, 3")]
        pub request: Option<request_op::Request>,
    }
    pub mod request_op {
        #[derive(Clone, PartialEq, prost::Oneof)]
        pub enum Request {
            #[prost(message, tag = "1")]
            Range(super::RangeRequest),
            #[prost(message, tag = "2")]
            Put(super::PutRequest),
            #[prost(message, tag = "3")]
            DeleteRange(super::DeleteRangeRequest),
        }
    }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct ResponseOp {
        #[prost(oneof = "response_op::Response", tags = "1")]
        pub response: Option<response_op::Response>,
    }
    pub mod response_op {
        #[derive(Clone, PartialEq, prost::Oneof)]
        pub enum Response {
            #[prost(message, tag = "1")]
            Range(super::RangeResponse),
        }
    }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct TxnRequest {
        #[prost(message, repeated, tag = "1")]
        pub compare: Vec<Compare>,
        #[prost(message, repeated, tag = "2")]
        pub success: Vec<RequestOp>,
        #[prost(message, repeated, tag = "3")]
        pub failure: Vec<RequestOp>,
    }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct TxnResponse {
        #[prost(message, optional, tag = "1")]
        pub header: Option<ResponseHeader>,
        #[prost(bool, tag = "2")]
        pub succeeded: bool,
        #[prost(message, repeated, tag = "3")]
        pub responses: Vec<ResponseOp>,
    }
}

const COMPARE_CREATE: i32 = 1;
const COMPARE_VALUE: i32 = 3;

/// Why etcd couldn't be used.
#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum EtcdError {
    /// Not reachable, timed out, or an error status.
    #[error("etcd unavailable: {0}")]
    Unavailable(String),
    /// The lease written with is gone (expired, or revoked): IE-023.
    #[error("etcd: the lease is gone")]
    LeaseNotFound,
    /// An answer IE can't use.
    #[error("etcd answered unexpectedly: {0}")]
    Unexpected(String),
}

/// `ie/owner/{interaction_id}`'s value: what TC's `EtcdOwnerDirectory` checks. Its JSON is
/// written in field order, so the same record always gives the same bytes (`release` compares them).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OwnerRecord {
    pub tenant_id: String,
    pub bin: String,
    pub replica_id: String,
    pub node_id: String,
}

/// A claim's result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Claim {
    /// Created now: the owner epoch.
    Claimed(u64),
    /// Already this replica's (a repeat): its epoch.
    Ours(u64),
    /// Another replica owns it.
    Taken,
}

/// The owner key of an interaction.
pub fn owner_key(interaction_id: &str) -> String {
    format!("ie/owner/{interaction_id}")
}

/// The replica key TC checks when a Watch stream opens.
pub fn replica_key(bin: &str, replica_id: &str) -> String {
    format!("ie/replicas/{bin}/{replica_id}")
}

/// A lazily connected etcd client (one HTTP/2 connection).
#[derive(Clone)]
pub struct Etcd {
    channel: Channel,
    timeout: Duration,
}

fn status(e: tonic::Status) -> EtcdError {
    // etcd's ErrGRPCLeaseNotFound: NOT_FOUND, "etcdserver: requested lease not found".
    if e.code() == tonic::Code::NotFound && e.message().contains("lease not found") {
        return EtcdError::LeaseNotFound;
    }
    EtcdError::Unavailable(format!("{:?}: {}", e.code(), e.message()))
}

/// A key as etcd holds it (tests and diagnostics).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stored {
    pub value: Vec<u8>,
    pub create_revision: i64,
    /// The lease it is on (0: none).
    pub lease: i64,
}

impl Etcd {
    /// `endpoint`: `http://host:port` of etcd's client port.
    pub fn connect(endpoint: &str, timeout: Duration) -> Result<Self, EtcdError> {
        let channel = Endpoint::from_shared(endpoint.to_owned())
            .map_err(|e| EtcdError::Unexpected(e.to_string()))?
            .connect_timeout(timeout)
            .tcp_keepalive(Some(Duration::from_secs(10)))
            .connect_lazy();
        Ok(Self { channel, timeout })
    }

    async fn unary<Q, R>(&self, path: &'static str, req: Q) -> Result<R, EtcdError>
    where
        Q: prost::Message + Send + Sync + 'static,
        R: prost::Message + Default + Send + Sync + 'static,
    {
        let mut grpc = tonic::client::Grpc::new(self.channel.clone());
        tokio::time::timeout(self.timeout, grpc.ready())
            .await
            .map_err(|_| EtcdError::Unavailable("connect timed out".into()))?
            .map_err(|e| EtcdError::Unavailable(e.to_string()))?;
        let mut request = tonic::Request::new(req);
        request.set_timeout(self.timeout);
        let codec = tonic_prost::ProstCodec::<Q, R>::default();
        grpc.unary(request, PathAndQuery::from_static(path), codec)
            .await
            .map(tonic::Response::into_inner)
            .map_err(status)
    }

    /// A lease of `ttl_s` seconds.
    pub async fn grant(&self, ttl_s: i64) -> Result<i64, EtcdError> {
        let r: pb::LeaseGrantResponse = self
            .unary(
                "/etcdserverpb.Lease/LeaseGrant",
                pb::LeaseGrantRequest { ttl: ttl_s, id: 0 },
            )
            .await?;
        if !r.error.is_empty() || r.id == 0 {
            return Err(EtcdError::Unexpected(format!("lease grant: {}", r.error)));
        }
        Ok(r.id)
    }

    /// One keepalive; the lease's remaining TTL (0: it has expired).
    pub async fn keep_alive(&self, lease: i64) -> Result<i64, EtcdError> {
        let mut grpc = tonic::client::Grpc::new(self.channel.clone());
        tokio::time::timeout(self.timeout, grpc.ready())
            .await
            .map_err(|_| EtcdError::Unavailable("connect timed out".into()))?
            .map_err(|e| EtcdError::Unavailable(e.to_string()))?;
        // The RPC is a bidirectional stream; one request, then the end of the stream, gets one answer.
        let mut request = tonic::Request::new(tokio_stream::iter([pb::LeaseKeepAliveRequest {
            id: lease,
        }]));
        request.set_timeout(self.timeout);
        let codec =
            tonic_prost::ProstCodec::<pb::LeaseKeepAliveRequest, pb::LeaseKeepAliveResponse>::default();
        let mut stream = grpc
            .streaming(
                request,
                PathAndQuery::from_static("/etcdserverpb.Lease/LeaseKeepAlive"),
                codec,
            )
            .await
            .map_err(status)?
            .into_inner();
        match tokio::time::timeout(self.timeout, stream.message()).await {
            Ok(Ok(Some(r))) => Ok(r.ttl),
            Ok(Ok(None)) => Err(EtcdError::Unexpected("keepalive: no answer".into())),
            Ok(Err(e)) => Err(status(e)),
            Err(_) => Err(EtcdError::Unavailable("keepalive timed out".into())),
        }
    }

    /// Writes `key` on `lease`.
    pub async fn put(&self, key: &str, value: Vec<u8>, lease: i64) -> Result<(), EtcdError> {
        let _: pb::PutResponse = self
            .unary(
                "/etcdserverpb.KV/Put",
                pb::PutRequest {
                    key: key.as_bytes().to_vec(),
                    value,
                    lease,
                },
            )
            .await?;
        Ok(())
    }

    /// Reads one key.
    pub async fn get(&self, key: &str) -> Result<Option<Stored>, EtcdError> {
        let r: pb::RangeResponse = self
            .unary(
                "/etcdserverpb.KV/Range",
                pb::RangeRequest {
                    key: key.as_bytes().to_vec(),
                },
            )
            .await?;
        Ok(r.kvs.into_iter().next().map(|kv| Stored {
            value: kv.value,
            create_revision: kv.create_revision,
            lease: kv.lease,
        }))
    }

    /// Creates `ie/owner/{interaction_id}` on `lease` if it doesn't exist (one transaction).
    pub async fn claim(
        &self,
        interaction_id: &str,
        record: &OwnerRecord,
        lease: i64,
    ) -> Result<Claim, EtcdError> {
        let key = owner_key(interaction_id).into_bytes();
        let value = owner_value(record)?;
        let txn = pb::TxnRequest {
            compare: vec![pb::Compare {
                result: 0,
                target: COMPARE_CREATE,
                key: key.clone(),
                target_union: Some(pb::compare::TargetUnion::CreateRevision(0)),
            }],
            success: vec![pb::RequestOp {
                request: Some(pb::request_op::Request::Put(pb::PutRequest {
                    key: key.clone(),
                    value,
                    lease,
                })),
            }],
            failure: vec![pb::RequestOp {
                request: Some(pb::request_op::Request::Range(pb::RangeRequest { key })),
            }],
        };
        let r: pb::TxnResponse = self.unary("/etcdserverpb.KV/Txn", txn).await?;
        decide_claim(&r, record)
    }

    /// Deletes the owner key only if `epoch` is still its create revision **and** its value is
    /// exactly what this replica wrote for `record` (one transaction): never another tenant's,
    /// node's or replica's key, whatever the interaction id.
    pub async fn release(
        &self,
        interaction_id: &str,
        epoch: u64,
        record: &OwnerRecord,
    ) -> Result<bool, EtcdError> {
        let key = owner_key(interaction_id).into_bytes();
        let txn = pb::TxnRequest {
            compare: release_guard(&key, epoch, owner_value(record)?),
            success: vec![pb::RequestOp {
                request: Some(pb::request_op::Request::DeleteRange(
                    pb::DeleteRangeRequest { key },
                )),
            }],
            failure: Vec::new(),
        };
        let r: pb::TxnResponse = self.unary("/etcdserverpb.KV/Txn", txn).await?;
        Ok(r.succeeded)
    }
}

/// The bytes written to the owner key.
fn owner_value(record: &OwnerRecord) -> Result<Vec<u8>, EtcdError> {
    serde_json::to_vec(record).map_err(|e| EtcdError::Unexpected(e.to_string()))
}

/// `release`'s compares: the claim's create revision and the exact value written.
fn release_guard(key: &[u8], epoch: u64, value: Vec<u8>) -> Vec<pb::Compare> {
    vec![
        pb::Compare {
            result: 0,
            target: COMPARE_CREATE,
            key: key.to_vec(),
            target_union: Some(pb::compare::TargetUnion::CreateRevision(
                i64::try_from(epoch).unwrap_or(i64::MAX),
            )),
        },
        pb::Compare {
            result: 0,
            target: COMPARE_VALUE,
            key: key.to_vec(),
            target_union: Some(pb::compare::TargetUnion::Value(value)),
        },
    ]
}

/// The claim's outcome from the transaction's answer: created (the epoch is the transaction's
/// revision, which is the key's create revision), or the existing key's owner. An existing key is
/// `Ours` only if its record equals this arrival's in every field (tenant, bin, replica, node): an
/// arrival reusing another tenant's or node's interaction id must never bind that call's epoch.
fn decide_claim(r: &pb::TxnResponse, record: &OwnerRecord) -> Result<Claim, EtcdError> {
    if r.succeeded {
        let rev = r.header.as_ref().map_or(0, |h| h.revision);
        return u64::try_from(rev)
            .ok()
            .filter(|&e| e > 0)
            .map(Claim::Claimed)
            .ok_or_else(|| EtcdError::Unexpected("no revision".into()));
    }
    let Some(kv) = r.responses.iter().find_map(|op| match &op.response {
        Some(pb::response_op::Response::Range(rr)) => rr.kvs.first(),
        None => None,
    }) else {
        // Deleted between the compare and the read: someone else's claim just ended. Not ours.
        return Ok(Claim::Taken);
    };
    let stored = serde_json::from_slice::<OwnerRecord>(&kv.value).ok();
    match (stored, u64::try_from(kv.create_revision)) {
        (Some(s), Ok(e)) if s == *record && e > 0 => Ok(Claim::Ours(e)),
        _ => Ok(Claim::Taken),
    }
}

/// This replica's etcd registration: the replica key and the lease it is written on.
#[derive(Debug, Clone)]
pub struct Registration {
    pub bin: String,
    pub replica_id: String,
    /// The replica key's value (what TC reads).
    pub value: Vec<u8>,
    pub ttl_s: i64,
}

/// STEP-3 MINIMAL (IE-023): the lease claims are written on, `None` while there is none (lost,
/// and not yet granted again). Cheap to clone; clones share it.
#[derive(Clone, Debug)]
pub struct Lease {
    tx: std::sync::Arc<tokio::sync::watch::Sender<Option<i64>>>,
    regrants: std::sync::Arc<std::sync::atomic::AtomicU64>,
}

impl Lease {
    /// A live lease, just granted.
    pub fn new(lease: i64) -> Self {
        Self {
            tx: std::sync::Arc::new(tokio::sync::watch::channel(Some(lease)).0),
            regrants: std::sync::Arc::default(),
        }
    }

    /// The live lease, if there is one.
    pub fn current(&self) -> Option<i64> {
        *self.tx.borrow()
    }

    /// Its changes (IE-055: the telephony client takes arrivals only while there is one).
    pub fn subscribe(&self) -> tokio::sync::watch::Receiver<Option<i64>> {
        self.tx.subscribe()
    }

    /// etcd refused `lease` (not found): it is gone. A newer one is left alone.
    pub fn lost(&self, lease: i64) {
        self.tx.send_if_modified(|c| {
            let gone = *c == Some(lease);
            if gone {
                *c = None;
            }
            gone
        });
    }

    /// How many times a lost lease was replaced since the process started.
    pub fn regrants(&self) -> u64 {
        self.regrants.load(std::sync::atomic::Ordering::Relaxed)
    }

    fn set(&self, lease: i64) {
        self.tx.send_replace(Some(lease));
    }
}

/// Grants a lease and writes the replica key on it; one attempt.
pub async fn register_once(etcd: &Etcd, reg: &Registration) -> Result<i64, EtcdError> {
    let lease = etcd.grant(reg.ttl_s).await?;
    etcd.put(
        &replica_key(&reg.bin, &reg.replica_id),
        reg.value.clone(),
        lease,
    )
    .await?;
    Ok(lease)
}

/// Registers, retrying with backoff (200 ms doubling to 5 s, full jitter) until it works; `None`
/// if `stop` turned true first. `failed` sees each failure.
pub async fn register(
    etcd: &Etcd,
    reg: &Registration,
    mut failed: impl FnMut(&EtcdError),
    stop: &mut tokio::sync::watch::Receiver<bool>,
) -> Option<i64> {
    let mut backoff =
        crate::backoff::Backoff::new(Duration::from_millis(200), Duration::from_secs(5));
    loop {
        if *stop.borrow() {
            return None;
        }
        match register_once(etcd, reg).await {
            Ok(l) => return Some(l),
            Err(e) => {
                failed(&e);
                tokio::select! {
                    () = tokio::time::sleep(backoff.next_delay()) => {}
                    _ = stop.changed() => return None,
                }
            }
        }
    }
}

/// Keeps the lease alive every third of its TTL until `stop` turns true (IE-023). The lease is
/// lost when etcd says it has expired (TTL 0: expired or not found), when a claim saw etcd refuse
/// it ([`Lease::lost`]), or when no keepalive has worked for a whole TTL (etcd has dropped it by
/// then). It is then granted again with backoff, with the replica key, and claims resume on it.
/// STEP-3 MINIMAL: no step-down before expiry (step 7).
pub async fn maintain(
    etcd: Etcd,
    reg: Registration,
    lease: Lease,
    mut stop: tokio::sync::watch::Receiver<bool>,
) {
    let ttl = Duration::from_secs(u64::try_from(reg.ttl_s.max(3)).unwrap_or(3));
    let every = ttl / 3;
    let mut watch = lease.tx.subscribe();
    let mut last_ok = std::time::Instant::now();
    loop {
        if let Some(id) = lease.current() {
            tokio::select! {
                () = tokio::time::sleep(every) => {}
                _ = watch.changed() => {}
                _ = stop.changed() => return,
            }
            if *stop.borrow() {
                return;
            }
            if lease.current() == Some(id) {
                match etcd.keep_alive(id).await {
                    Ok(left) if left > 0 => last_ok = std::time::Instant::now(),
                    Ok(_) => {
                        tracing::error!(
                            event = "owner.lease_lost",
                            lease = id,
                            "the etcd lease expired: new calls are declined until a new one is granted"
                        );
                        lease.lost(id);
                    }
                    Err(e) if last_ok.elapsed() >= ttl => {
                        tracing::error!(event = "owner.lease_lost",  lease = id, error = %e, "no etcd keepalive worked for a whole TTL: the lease is presumed gone; new calls are declined until a new one is granted");
                        lease.lost(id);
                    }
                    Err(e) => {
                        tracing::warn!(event = "etcd.keepalive_failed",  lease = id, error = %e, "an etcd keepalive failed")
                    }
                }
            }
            continue;
        }
        let Some(new) = register(
            &etcd,
            &reg,
            |e| tracing::warn!(event = "dependency.unavailable", dependency = "etcd", error = %e, "granting a new etcd lease failed; retrying"),
            &mut stop,
        )
        .await
        else {
            return;
        };
        lease
            .regrants
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        lease.set(new);
        let _ = watch.borrow_and_update();
        last_ok = std::time::Instant::now();
        tracing::info!(
            event = "owner.lease_restored",
            lease = new,
            "a new etcd lease is live, with the replica key: claims resume (STEP-3 MINIMAL)"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;

    fn ours() -> OwnerRecord {
        OwnerRecord {
            tenant_id: "acme".into(),
            bin: "bin-a".into(),
            replica_id: "ie-1".into(),
            node_id: "media-1".into(),
        }
    }

    fn existing(value: &[u8], rev: i64) -> pb::TxnResponse {
        pb::TxnResponse {
            header: Some(pb::ResponseHeader { revision: 99 }),
            succeeded: false,
            responses: vec![pb::ResponseOp {
                response: Some(pb::response_op::Response::Range(pb::RangeResponse {
                    kvs: vec![pb::KeyValue {
                        key: b"ie/owner/x".to_vec(),
                        create_revision: rev,
                        value: value.to_vec(),
                        lease: 0,
                    }],
                })),
            }],
        }
    }

    fn with(f: impl FnOnce(&mut OwnerRecord)) -> Vec<u8> {
        let mut r = ours();
        f(&mut r);
        owner_value(&r).unwrap()
    }

    #[test]
    fn a_created_key_takes_the_transaction_revision_as_its_epoch() {
        let r = pb::TxnResponse {
            header: Some(pb::ResponseHeader { revision: 42 }),
            succeeded: true,
            responses: Vec::new(),
        };
        assert_eq!(decide_claim(&r, &ours()), Ok(Claim::Claimed(42)));
    }

    #[test]
    fn an_exact_match_of_tenant_bin_replica_and_node_is_ours() {
        let r = existing(&owner_value(&ours()).unwrap(), 7);
        assert_eq!(decide_claim(&r, &ours()), Ok(Claim::Ours(7)));
    }

    #[test]
    fn another_tenant_s_record_on_this_replica_is_taken() {
        // An arrival reusing another tenant's live interaction id (X-Interaction-ID) must not bind it.
        let r = existing(&with(|o| o.tenant_id = "globex".into()), 7);
        assert_eq!(decide_claim(&r, &ours()), Ok(Claim::Taken));
    }

    #[test]
    fn the_same_tenant_on_another_node_or_replica_is_taken() {
        for v in [
            with(|o| o.node_id = "media-2".into()),
            with(|o| o.replica_id = "ie-2".into()),
            with(|o| o.bin = "bin-b".into()),
            br#"{"replica_id":"ie-1"}"#.to_vec(),
            b"not json".to_vec(),
        ] {
            assert_eq!(decide_claim(&existing(&v, 7), &ours()), Ok(Claim::Taken));
        }
        let gone = pb::TxnResponse {
            succeeded: false,
            ..Default::default()
        };
        assert_eq!(decide_claim(&gone, &ours()), Ok(Claim::Taken));
    }

    #[test]
    fn release_compares_the_create_revision_and_the_exact_value_written() {
        let value = owner_value(&ours()).unwrap();
        let guard = release_guard(b"ie/owner/x", 7, value.clone());
        assert_eq!(guard.len(), 2);
        assert_eq!(
            (guard[0].target, guard[0].target_union.clone()),
            (
                COMPARE_CREATE,
                Some(pb::compare::TargetUnion::CreateRevision(7))
            )
        );
        assert_eq!(
            (guard[1].target, guard[1].target_union.clone()),
            (COMPARE_VALUE, Some(pb::compare::TargetUnion::Value(value)))
        );
        // The value is deterministic: the claim and the release write and compare the same bytes.
        assert_eq!(owner_value(&ours()).unwrap(), owner_value(&ours()).unwrap());
    }

    #[test]
    fn the_absent_key_compare_writes_create_revision_zero() {
        // etcd reads a missing target_union as "no compare value"; 0 must be on the wire.
        let c = pb::Compare {
            result: 0,
            target: COMPARE_CREATE,
            key: b"k".to_vec(),
            target_union: Some(pb::compare::TargetUnion::CreateRevision(0)),
        };
        let bytes = c.encode_to_vec();
        // field 5, varint (0x28), value 0
        assert!(bytes.windows(2).any(|w| w == [0x28, 0x00]), "{bytes:?}");
    }

    #[test]
    fn a_lost_lease_is_none_until_replaced_and_a_newer_one_is_left_alone() {
        let l = Lease::new(7);
        l.lost(6);
        assert_eq!(
            l.current(),
            Some(7),
            "an older lease's loss changes nothing"
        );
        l.lost(7);
        assert_eq!(l.current(), None);
        l.set(8);
        l.lost(7);
        assert_eq!(l.current(), Some(8));
    }

    #[test]
    fn etcd_s_lease_not_found_is_told_apart() {
        let e = status(tonic::Status::not_found(
            "etcdserver: requested lease not found",
        ));
        assert_eq!(e, EtcdError::LeaseNotFound);
        assert!(matches!(
            status(tonic::Status::unavailable("no leader")),
            EtcdError::Unavailable(_)
        ));
    }

    #[test]
    fn the_owner_value_is_what_tc_reads() {
        let v = serde_json::to_value(OwnerRecord {
            tenant_id: "acme".into(),
            bin: "bin-a".into(),
            replica_id: "ie-1".into(),
            node_id: "media-1".into(),
        })
        .unwrap_or_default();
        for k in ["tenant_id", "bin", "replica_id", "node_id"] {
            assert!(v.get(k).is_some(), "{k}");
        }
    }
}
