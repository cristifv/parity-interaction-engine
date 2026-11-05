//! interaction-engine's client of parity-telephony-controller (`parity.telephony.v1`), ADR 0003:
//! IE's only path to a call. Arrivals in, intents out, events back; no SDP, ESL or parity-ms code.
//!
//! - [`TelephonyClient`] implements [`Telephony`], the interface ie-core uses (§4.8).
//! - Delivery, acks and resyncs are the client's (D3); lost contact makes sessions unmanaged and
//!   ends them only on positive evidence (D4); `SessionEnded` is acked through [`EndedAck`] (D5).

pub mod api;
pub mod client;
pub mod clock;
mod commands;
pub mod config;
mod delivery;
pub mod ended;
pub mod evidence;
mod forgotten;
mod queue;
pub mod stats;
pub mod validate;
mod wire;

pub use api::*;
pub use client::{Held, NotReady, Started, Telephony, TelephonyClient};
pub use clock::{Clock, ManualClock, SystemClock};
pub use config::{ClientConfig, NodeConfig, Profile, Timing};
pub use ended::{EndedAck, EndedLog, MemoryEndedLog};
pub use evidence::{NodeEvidence, SourceKind, StaticEvidence, TcProbe, Verdict};
pub use queue::SessionEvents;
