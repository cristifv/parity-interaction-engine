//! The client's configuration (ADR 0003 §4.2): the replica, its bin, the static node list, and the
//! timings of TC's ADR (§4.2 deadlines, §4.4 delivery, O2's heartbeat).

use serde::Deserialize;
use std::time::Duration;

/// Where IE runs; decides what may be used (R1-7, the test-peer header).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Profile {
    /// A real zone.
    Production,
    /// A developer's machine.
    Dev,
    /// DEMO.md's setup.
    Demo,
    /// Automated tests.
    Test,
}

/// One media node's TC.
#[derive(Clone, Debug, Deserialize)]
pub struct NodeConfig {
    /// The node id TC reports.
    pub id: String,
    /// `telephony.v1`, e.g. `http://10.0.3.11:9700`.
    pub grpc: String,
    /// TC's management port (`/livez`), `host:port`, for the evidence probe.
    #[serde(default)]
    pub management: Option<String>,
}

/// Timings, all with TC's ADR defaults.
#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct Timing {
    /// A stream with no message for this long is dead (3 missed 2 s heartbeats, O2).
    pub liveness_ms: u64,
    /// Reconnect backoff bounds, full jitter.
    pub reconnect_min_ms: u64,
    /// Reconnect backoff bounds, full jitter.
    pub reconnect_max_ms: u64,
    /// The batched `Acknowledge` period.
    pub ack_batch_ms: u64,
    /// A session's mailbox.
    pub mailbox: usize,
    /// A degraded session's resync starts below this.
    pub low_water: usize,
    /// A session degraded this long raises the alert.
    pub degraded_alert_ms: u64,
    /// `GetSession`s per second, per replica, across all nodes.
    pub resyncs_per_s: u32,
    /// Every evidence source dead for this long ends a node's sessions.
    pub dead_after_ms: u64,
    /// The evidence sources' period.
    pub probe_every_ms: u64,
    /// Unary deadlines (§4.2): commands, `ClaimSession`, `AddLeg`, `EndSession`, `RemoveLeg`.
    pub command_deadline_ms: u64,
    /// `ClaimSession`.
    pub claim_deadline_ms: u64,
    /// `AddLeg` and `EndSession`.
    pub long_deadline_ms: u64,
    /// `RemoveLeg`.
    pub remove_deadline_ms: u64,
    /// How long `rebuilding` and `owner_check_unavailable` are retried.
    pub rebuilding_retry_ms: u64,
    /// A session the client forgot (its end acked, or never owned here and over) is remembered by
    /// key this long, so a late duplicate of its events is dropped, not taken for an orphan (IE-022).
    pub forget_after_ms: u64,
    /// At most this many forgotten keys per node; past it the oldest go early (IE-022).
    pub forgotten_max: usize,
}

impl Default for Timing {
    fn default() -> Self {
        Self {
            liveness_ms: 6_000,
            reconnect_min_ms: 100,
            reconnect_max_ms: 5_000,
            ack_batch_ms: 50,
            mailbox: 64,
            low_water: 16,
            degraded_alert_ms: 10_000,
            resyncs_per_s: 500,
            dead_after_ms: 30_000,
            probe_every_ms: 2_000,
            command_deadline_ms: 300,
            claim_deadline_ms: 500,
            long_deadline_ms: 1_000,
            remove_deadline_ms: 500,
            rebuilding_retry_ms: 10_000,
            forget_after_ms: 60_000,
            forgotten_max: 50_000,
        }
    }
}

impl Timing {
    pub(crate) fn ms(v: u64) -> Duration {
        Duration::from_millis(v)
    }
}

/// The client's configuration.
#[derive(Clone, Debug, Deserialize)]
pub struct ClientConfig {
    /// This replica (registered under `ie/replicas/{bin}/`).
    pub replica_id: String,
    /// Its bin.
    pub bin: String,
    /// The profile.
    pub profile: Profile,
    /// The media nodes (static in v1).
    pub nodes: Vec<NodeConfig>,
    /// Sends `x-parity-test-peer` until TC has mTLS (TC step 8). Never in production.
    #[serde(default)]
    pub test_peer_header: bool,
    /// Timings.
    #[serde(default)]
    pub timing: Timing,
}

/// Why a configuration is refused.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ConfigError {
    /// A field's value.
    #[error("telephony.{0}: {1}")]
    Field(&'static str, String),
}

impl ClientConfig {
    /// Checks the rules a running client relies on.
    pub fn validate(&self) -> Result<(), ConfigError> {
        let field = |f: &'static str, m: &str| Err(ConfigError::Field(f, m.to_owned()));
        if !crate::validate::is_command_id(&self.replica_id) || self.replica_id.len() > 100 {
            return field("replica_id", "1-100 of A-Za-z0-9._:-");
        }
        if !crate::validate::is_dns_label(&self.bin) {
            return field("bin", "a DNS label");
        }
        if self.nodes.is_empty() {
            return field("nodes", "at least one media node");
        }
        let mut ids: Vec<&str> = self.nodes.iter().map(|n| n.id.as_str()).collect();
        ids.sort_unstable();
        ids.dedup();
        if ids.len() != self.nodes.len() {
            return field("nodes", "node ids must be unique");
        }
        if self
            .nodes
            .iter()
            .any(|n| !n.grpc.starts_with("http://") && !n.grpc.starts_with("https://"))
        {
            return field("nodes.grpc", "an http:// or https:// URI");
        }
        if self.test_peer_header && self.profile == Profile::Production {
            return field("test_peer_header", "never in the production profile");
        }
        let t = &self.timing;
        if t.liveness_ms < 2_000 || t.mailbox < 4 || t.low_water == 0 || t.low_water >= t.mailbox {
            return field(
                "timing",
                "liveness >= 2000 ms, mailbox >= 4, 0 < low_water < mailbox",
            );
        }
        if t.resyncs_per_s == 0
            || t.reconnect_min_ms == 0
            || t.reconnect_min_ms > t.reconnect_max_ms
        {
            return field(
                "timing",
                "resyncs_per_s > 0, 0 < reconnect_min <= reconnect_max",
            );
        }
        // IE-052: the window must outlast a few of TC's 2 s redeliveries.
        if t.forgotten_max == 0 || !(10_000..=3_600_000).contains(&t.forget_after_ms) {
            return field(
                "timing",
                "forgotten_max > 0, 10 s <= forget_after_ms <= 1 h",
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> ClientConfig {
        ClientConfig {
            replica_id: "ie-bin-a-1".into(),
            bin: "bin-a".into(),
            profile: Profile::Production,
            nodes: vec![NodeConfig {
                id: "media-1".into(),
                grpc: "http://127.0.0.1:9700".into(),
                management: None,
            }],
            test_peer_header: false,
            timing: Timing::default(),
        }
    }

    #[test]
    fn the_test_peer_header_is_refused_in_production() {
        let mut c = cfg();
        assert_eq!(c.validate(), Ok(()));
        c.test_peer_header = true;
        assert!(c.validate().is_err());
        c.profile = Profile::Dev;
        assert_eq!(c.validate(), Ok(()));
    }

    /// IE-052: no duplicate window (or one shorter than a few redeliveries) is refused.
    #[test]
    fn a_forget_window_under_10_s_is_refused() {
        let mut c = cfg();
        c.timing.forget_after_ms = 0;
        assert!(c.validate().is_err());
        c.timing.forget_after_ms = 9_999;
        assert!(c.validate().is_err());
        c.timing.forget_after_ms = 10_000;
        assert_eq!(c.validate(), Ok(()));
    }
}
