//! The client's settings (CONVENTIONS "Timeouts and retries" for routing).

use std::time::Duration;

/// The most routing-engine instances one bin may list (and leaders learned from `parity-leader`).
pub const MAX_ENDPOINTS: usize = 16;

/// The client's settings; every duration has the contract's default.
#[derive(Debug, Clone)]
pub struct Config {
    /// The bin's routing-engine instances, `http://host:port` each (IE-059: its leader and
    /// standbys; one is enough without HA). Requests and the Watch stream go to the leader:
    /// a standby's `not_leader` names it in `parity-leader`, else the next instance is tried.
    pub endpoints: Vec<String>,
    /// This IE replica (the Watch stream's id and every request's `replica_id`).
    pub replica_id: String,
    /// Its deployment bin.
    pub bin: String,
    /// Per try (300 ms).
    pub rpc_timeout: Duration,
    /// Tries for a retryable failure (3).
    pub tries: u32,
    /// How long a queued caller's `Enqueue` is retried while RE is unreachable (60 s).
    pub enqueue_budget: Duration,
    /// IE-059 (CR-150): how long `ReportOutcome`, `CallEnded`, `Dequeue` and a leader change's
    /// `Reattach` are retried, so that they span a leader change (5-6 s failover): 15 s.
    pub outcome_budget: Duration,
    /// IE-059: after a leader change each call re-attaches after a random wait up to this, so
    /// that thousands of calls don't hit the new leader in the same millisecond (1 s).
    pub resync_spread: Duration,
    /// IE-059: at most this many leader-change `Reattach`es in flight at once (32).
    pub resync_concurrency: usize,
    /// No message (heartbeats come every 5 s) for this long: reconnect (15 s, 3 missed).
    pub heartbeat_timeout: Duration,
    /// Acks wait at most this long before an `Acknowledge` (50 ms).
    pub ack_flush: Duration,
    /// Released interactions are remembered this long (10 min): late messages go to the
    /// dispatcher instead of being orphans.
    pub ended_ttl: Duration,
    /// Backoff of the Watch reconnects (100 ms to 5 s, full jitter).
    pub reconnect_min: Duration,
    pub reconnect_max: Duration,
}

/// A setting out of range.
#[derive(Debug, thiserror::Error)]
#[error("routing: {0}")]
pub struct ConfigError(pub String);

impl Config {
    /// The contract's defaults for one endpoint and this replica.
    pub fn new(endpoint: &str, replica_id: &str, bin: &str) -> Self {
        Self::with_endpoints(&[endpoint.to_owned()], replica_id, bin)
    }

    /// The contract's defaults for the bin's instances (leader and standbys, any order).
    pub fn with_endpoints(endpoints: &[String], replica_id: &str, bin: &str) -> Self {
        Self {
            endpoints: endpoints.to_vec(),
            replica_id: replica_id.to_owned(),
            bin: bin.to_owned(),
            rpc_timeout: Duration::from_millis(300),
            tries: 3,
            enqueue_budget: Duration::from_secs(60),
            outcome_budget: Duration::from_secs(15),
            resync_spread: Duration::from_secs(1),
            resync_concurrency: 32,
            heartbeat_timeout: Duration::from_secs(15),
            ack_flush: Duration::from_millis(50),
            ended_ttl: Duration::from_secs(600),
            reconnect_min: Duration::from_millis(100),
            reconnect_max: Duration::from_secs(5),
        }
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        let bad = |m: &str| Err(ConfigError(m.to_owned()));
        if self.endpoints.is_empty() || self.endpoints.len() > MAX_ENDPOINTS {
            return bad("1 to 16 endpoints");
        }
        if self
            .endpoints
            .iter()
            .any(|e| !e.starts_with("http://") || authority(e).is_none())
        {
            // tonic is built without TLS until mTLS (IE-031's note applies here too).
            return bad("every endpoint must be an http://host:port URI until mTLS");
        }
        if self.replica_id.is_empty() || self.bin.is_empty() {
            return bad("replica_id and bin are required");
        }
        if !(1..=10).contains(&self.tries)
            || self.rpc_timeout.is_zero()
            || self.rpc_timeout > Duration::from_secs(5)
        {
            return bad("tries 1-10 and rpc_timeout up to 5 s");
        }
        if self.enqueue_budget > Duration::from_secs(300) || self.ack_flush.is_zero() {
            return bad("enqueue_budget up to 300 s and a non-zero ack_flush");
        }
        // CONVENTIONS "Timeouts and retries" (0.15.1): at least 15 s across a leader change (the
        // default; IE's configuration doesn't change it, tests shorten it).
        if self.outcome_budget.is_zero() || self.outcome_budget > Duration::from_secs(60) {
            return bad("outcome_budget up to 60 s (15 s spans a leader change)");
        }
        if self.resync_spread > Duration::from_secs(10)
            || !(1..=1024).contains(&self.resync_concurrency)
        {
            return bad("resync_spread up to 10 s and resync_concurrency 1-1024");
        }
        Ok(())
    }
}

/// `host:port` of an `http://host:port[/]` URI (what `parity-leader` names).
pub(crate) fn authority(uri: &str) -> Option<&str> {
    let a = uri.strip_prefix("http://")?.trim_end_matches('/');
    valid_authority(a).then_some(a)
}

/// A `host:port` as RE advertises it: an ASCII host (name or IPv4) and a port, no path.
pub(crate) fn valid_authority(a: &str) -> bool {
    let Some((host, port)) = a.rsplit_once(':') else {
        return false;
    };
    !host.is_empty()
        && host.len() <= 253
        && host
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'.' || b == b'-' || b == b'_')
        && port.parse::<u16>().is_ok_and(|p| p > 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoints_and_budgets_are_checked() {
        assert!(
            Config::new("http://re:9800", "ie-1", "bin-a")
                .validate()
                .is_ok()
        );
        assert!(
            Config::new("https://re:9800", "ie-1", "bin-a")
                .validate()
                .is_err()
        );
        assert!(
            Config::new("http://re", "ie-1", "bin-a")
                .validate()
                .is_err()
        );
        assert!(
            Config::with_endpoints(&[], "ie-1", "bin-a")
                .validate()
                .is_err()
        );
        let c = Config::new("http://re:9800", "ie-1", "bin-a");
        assert!(
            c.outcome_budget >= Duration::from_secs(15),
            "the contract's floor is 15 s"
        );
        assert_eq!(
            authority("http://re-1.bin-a:9800/"),
            Some("re-1.bin-a:9800")
        );
        assert!(!valid_authority("re-1:9800/x"));
        assert!(!valid_authority("http://re-1:9800"));
        assert!(valid_authority("10.0.0.7:9800"));
    }
}
