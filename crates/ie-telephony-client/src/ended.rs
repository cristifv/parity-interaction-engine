//! Ending a session (ADR 0003 §4.7, D5): `SessionEnded` is acked only once its owner has made the
//! ENDED record durable, which it signals by redeeming the [`EndedAck`] token.

use crate::api::{EndReason, NodeId, SessionKey};
use crate::config::Profile;
use std::fmt;
use std::future::Future;
use std::sync::Mutex;
use tokio::sync::mpsc::UnboundedSender;

/// What a redeemed token tells the client.
#[derive(Debug, Clone)]
pub(crate) struct Redeem {
    pub node: NodeId,
    pub key: SessionKey,
    pub incarnation: String,
    pub seq: u64,
}

/// What owners tell the client about their sessions, in the order they did it (one channel, so a
/// redeemed end is always seen before the mailbox's drop that follows it, IE-022).
pub(crate) enum Control {
    /// An end made durable.
    Redeem(Redeem),
    /// The owner dropped the session's mailbox (the queue identifies which one it held).
    Abandoned(NodeId, SessionKey, std::sync::Weak<crate::queue::Queue>),
}

/// Redeem once the ENDED record is durable. Dropping it unredeemed leaves the `SessionEnded`
/// unacked: TC replays it and keeps the tombstone (X19).
pub struct EndedAck(Option<(UnboundedSender<Control>, Redeem)>);

impl EndedAck {
    pub(crate) fn new(tx: UnboundedSender<Control>, r: Redeem) -> Self {
        Self(Some((tx, r)))
    }
    /// A token with nothing to acknowledge (an end TC never reported: `SessionUnknown`, `NodeDead`).
    pub fn nothing() -> Self {
        Self(None)
    }
    /// Whether redeeming it acknowledges a `delivery_seq`.
    pub fn acknowledges(&self) -> bool {
        self.0.is_some()
    }
    /// The ENDED record is durable: the client may acknowledge.
    pub fn durable(self) {
        if let Some((tx, r)) = self.0 {
            // The client gone means there is nothing left to acknowledge to.
            let _ = tx.send(Control::Redeem(r));
        }
    }
}

impl fmt::Debug for EndedAck {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "EndedAck {{ acknowledges: {} }}", self.acknowledges())
    }
}

/// Where an owner makes an ENDED record durable.
pub trait EndedLog: Send + Sync {
    /// Durable when the future completes with `Ok`.
    fn record(
        &self,
        key: &SessionKey,
        reason: EndReason,
    ) -> impl Future<Output = Result<(), EndedLogError>> + Send;
}

/// The record couldn't be written; the token stays unredeemed.
#[derive(Debug, thiserror::Error)]
#[error("the ENDED record could not be written: {0}")]
pub struct EndedLogError(pub String);

/// `MemoryEndedLog` outside the dev, demo and test profiles (R1-7).
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[error(
    "MemoryEndedLog is not durable: refused in the {0:?} profile (the ie.calls checkpoint comes with step 8)"
)]
pub struct NotDurable(pub Profile);

/// The stand-in until step 8's `ie.calls` checkpoint: an in-memory list of the latest
/// [`MemoryEndedLog::KEEP`] ends (IE-048: bounded, so a soak doesn't grow it). Not durable;
/// refused in production. Before step 8 an IE crash loses every call's state anyway, so the only
/// extra risk is X19 (an ack before the ENDED record).
#[derive(Debug, Default)]
pub struct MemoryEndedLog(Mutex<std::collections::VecDeque<(SessionKey, EndReason)>>);

impl MemoryEndedLog {
    /// How many of the latest ends it keeps.
    pub const KEEP: usize = 10_000;

    /// Refuses outside the dev, demo and test profiles.
    pub fn new(profile: Profile) -> Result<Self, NotDurable> {
        match profile {
            Profile::Dev | Profile::Demo | Profile::Test => {
                tracing::warn!(
                    event = "telephony.acks_not_durable",
                    ?profile,
                    "SessionEnded acks are not durable until step 8 (MemoryEndedLog)"
                );
                Ok(Self::default())
            }
            Profile::Production => Err(NotDurable(profile)),
        }
    }
    /// What was recorded (the latest [`Self::KEEP`]).
    pub fn entries(&self) -> Vec<(SessionKey, EndReason)> {
        self.0
            .lock()
            .map(|v| v.iter().cloned().collect())
            .unwrap_or_default()
    }
}

impl EndedLog for MemoryEndedLog {
    fn record(
        &self,
        key: &SessionKey,
        reason: EndReason,
    ) -> impl Future<Output = Result<(), EndedLogError>> + Send {
        let r = self
            .0
            .lock()
            .map(|mut v| {
                if v.len() >= Self::KEEP {
                    v.pop_front();
                }
                v.push_back((key.clone(), reason));
            })
            .map_err(|_| EndedLogError("poisoned".into()));
        async move { r }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_ended_log_refuses_production() {
        assert_eq!(
            MemoryEndedLog::new(Profile::Production).err(),
            Some(NotDurable(Profile::Production))
        );
        for p in [Profile::Dev, Profile::Demo, Profile::Test] {
            assert!(MemoryEndedLog::new(p).is_ok(), "{p:?}");
        }
    }

    /// IE-048: it keeps the latest ends only.
    #[tokio::test]
    async fn memory_ended_log_keeps_the_latest_only() {
        let log = MemoryEndedLog::default();
        for i in 0..MemoryEndedLog::KEEP + 5 {
            let key = SessionKey {
                tenant_id: "acme".into(),
                interaction_id: format!("{i}"),
            };
            log.record(&key, EndReason::NodeDead).await.unwrap();
        }
        let e = log.entries();
        assert_eq!(e.len(), MemoryEndedLog::KEEP);
        assert_eq!(e[0].0.interaction_id, "5");
    }
}
