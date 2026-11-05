//! interaction-engine's client of routing-engine (`parity.routing.v1`, CONVENTIONS "The routing
//! contract"), PLAN step 5a. There is no routing engine yet (parity-cc-routing comes first, the
//! user's decision of 2026-10-28): this crate is tested against in-process doubles only.
//!
//! - Requests ([`RoutingClient::enqueue`], [`RoutingClient::dequeue`],
//!   [`RoutingClient::report_outcome`], [`RoutingClient::call_ended`]) carry the owner's tenant
//!   (`tenant-id`), its fence (`owner_epoch`) and the interaction's `acked_delivery_seq`; they are
//!   retried on `UNAVAILABLE`/`DEADLINE_EXCEEDED` and classified by the `parity-error` trailer.
//! - One `Watch` stream per replica: messages are checked (leader epoch, tenant, owner epoch,
//!   `delivery_seq`) and handed to the bound call's mailbox; the call acknowledges each after
//!   its machine took it ([`RoutingClient::ack`]), in batches.
//! - Messages for an interaction released less than [`Config::ended_ttl`] ago go to the
//!   dispatcher ([`ClientEvent::AfterEnd`]): an `Assigned` for a gone caller is answered
//!   `CALLER_GONE` there.

mod client;
mod config;
mod types;

pub use client::{Dropped, RoutingClient, Started};
pub use config::{Config, ConfigError};
pub use types::*;
