//! The client's counters (ADR 0003 §4.10). Gauges per node are computed from the nodes' state when
//! read; no tenant or interaction labels.

use std::sync::atomic::{AtomicU64, Ordering};

/// Monotonic counters.
#[derive(Debug, Default)]
pub struct Stats {
    /// Events delivered to owners.
    pub events: AtomicU64,
    /// Replays dropped.
    pub replays_dropped: AtomicU64,
    /// Events held back (a gap, or a resync due).
    pub held: AtomicU64,
    /// `GetSession` resyncs started.
    pub resyncs: AtomicU64,
    /// Resyncs that failed and will be retried.
    pub resync_failures: AtomicU64,
    /// Mailbox overflows.
    pub mailbox_overflows: AtomicU64,
    /// Sessions degraded for longer than the alert threshold.
    pub degraded_alerts: AtomicU64,
    /// Watch reconnects.
    pub reconnects: AtomicU64,
    /// Another process used this replica id (R1-9).
    pub replica_conflicts: AtomicU64,
    /// Sessions TC refused because a newer owner holds them.
    pub fence_refusals: AtomicU64,
    /// Sessions ended because TC (live, rebuilt) no longer knew them (R1-1).
    pub sessions_lost: AtomicU64,
    /// Nodes declared dead.
    pub nodes_dead: AtomicU64,
    /// Sessions still live on a node that came back from `Dead`, ended (R1-2).
    pub zombies_ended: AtomicU64,
    /// `AddLeg`s whose result was unknown (R1-3).
    pub indeterminate: AtomicU64,
    /// Event kinds this release doesn't know.
    pub unknown_events: AtomicU64,
    /// A node's clock offset passed 100 ms (from heartbeats' `at_ms`, 0.2.3).
    pub clock_skew_alerts: AtomicU64,
    /// Sessions the client stopped holding once nothing was left to do for them (IE-022).
    pub sessions_forgotten: AtomicU64,
    /// A forgotten session's `SessionEnded` replayed (a TC restart lost the ack): acked again.
    pub ends_acked_again: AtomicU64,
    /// Arrivals left unacked while no arrival was taken (IE-055: no etcd lease).
    pub arrivals_not_taken: AtomicU64,
}

impl Stats {
    pub(crate) fn inc(c: &AtomicU64) {
        c.fetch_add(1, Ordering::Relaxed);
    }
    /// A counter's value.
    pub fn get(c: &AtomicU64) -> u64 {
        c.load(Ordering::Relaxed)
    }
}
