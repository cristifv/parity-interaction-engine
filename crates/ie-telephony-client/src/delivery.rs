//! One session's delivery state in one TC incarnation (ADR 0003 §4.3, D3): which `delivery_seq`
//! the owner has, what may be acknowledged, and when a resync must replace events. Pure; the node
//! task calls it under its lock.
//!
//! Rules:
//! - `seq <= processed`: a replay, dropped (counted by the caller); its ack follows `ackable`.
//! - `seq == processed + 1` with no resync wanted: delivered.
//! - anything else, or while a resync is wanted or running: held back, not delivered and not
//!   acked (TC replays unacknowledged events every 2 s), and a resync is wanted.
//! - A resync's snapshot sets `processed` to its `last_delivery_seq`: the state it carries already
//!   reflects every event up to there.
//! - A `SessionEnded` (or an ENDED snapshot) caps the ack below itself until its token is redeemed;
//!   its replays are dropped and not acked (R1-4).

/// Where a session's resync stands.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Resync {
    /// None needed.
    Idle,
    /// Needed; starts when the mailbox is below its low-water mark.
    Wanted,
    /// A `GetSession` is in flight (never two).
    InFlight,
}

/// What to do with an offered event.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Offer {
    /// Hand it to the owner, then call [`Delivery::delivered`].
    Deliver,
    /// Already processed: drop it.
    Replay,
    /// Out of order or a resync is due: drop it unacked.
    Hold,
}

#[derive(Clone, Debug)]
pub(crate) struct Delivery {
    processed: u64,
    acked: u64,
    ended_at: Option<u64>,
    redeemed: bool,
    resync: Resync,
}

impl Delivery {
    /// A session seen for the first time at `seq`: everything before it is not this owner's.
    pub fn starting_before(seq: u64) -> Self {
        Self {
            processed: seq.saturating_sub(1),
            acked: 0,
            ended_at: None,
            redeemed: false,
            resync: Resync::Idle,
        }
    }

    /// A session whose state comes from a snapshot first (a claim, a new incarnation).
    pub fn awaiting_snapshot() -> Self {
        Self {
            processed: 0,
            acked: 0,
            ended_at: None,
            redeemed: false,
            resync: Resync::Wanted,
        }
    }

    pub fn processed(&self) -> u64 {
        self.processed
    }

    pub fn resync(&self) -> Resync {
        self.resync
    }

    pub fn offer(&mut self, seq: u64) -> Offer {
        if seq <= self.processed {
            Offer::Replay
        } else if seq == self.processed + 1 && self.resync == Resync::Idle {
            Offer::Deliver
        } else {
            if self.resync == Resync::Idle {
                self.resync = Resync::Wanted;
            }
            Offer::Hold
        }
    }

    /// The owner has `seq` (only after [`Offer::Deliver`]).
    pub fn delivered(&mut self, seq: u64, session_ended: bool) {
        debug_assert_eq!(seq, self.processed + 1);
        self.processed = seq;
        if session_ended && self.ended_at.is_none() {
            self.ended_at = Some(seq);
            self.redeemed = false;
        }
    }

    /// The mailbox overflowed: resync once it drains (R1-4).
    pub fn want_resync(&mut self) {
        if self.resync == Resync::Idle {
            self.resync = Resync::Wanted;
        }
    }

    /// Starts the one resync, if one is wanted.
    pub fn start_resync(&mut self) -> bool {
        if self.resync == Resync::Wanted {
            self.resync = Resync::InFlight;
            true
        } else {
            false
        }
    }

    /// The resync failed for now: want it again.
    pub fn resync_failed(&mut self) {
        if self.resync == Resync::InFlight {
            self.resync = Resync::Wanted;
        }
    }

    /// The snapshot replaced the owner's view. `ended`: it is a tombstone. A snapshot older than
    /// what the owner already has is ignored (false): `processed` only moves forward (fix 1).
    pub fn resynced(&mut self, last_delivery_seq: u64, ended: bool) -> bool {
        if last_delivery_seq < self.processed {
            return false;
        }
        self.processed = last_delivery_seq;
        self.resync = Resync::Idle;
        if ended {
            self.ended_at = Some(last_delivery_seq.max(1));
            self.redeemed = false;
        } else {
            self.ended_at = None;
        }
        true
    }

    /// The owner made the ENDED record durable.
    pub fn redeem(&mut self, seq: u64) {
        if self.ended_at == Some(seq) {
            self.redeemed = true;
        }
    }

    /// The highest `delivery_seq` that may be acknowledged.
    pub fn ackable(&self) -> u64 {
        match self.ended_at {
            Some(e) if !self.redeemed => self.processed.min(e.saturating_sub(1)),
            _ => self.processed,
        }
    }

    /// An ack to send, if any is new.
    pub fn pending_ack(&self) -> Option<u64> {
        let a = self.ackable();
        (a > self.acked).then_some(a)
    }

    /// TC took an ack up to `seq` (the command or `Acknowledge` carrying it succeeded).
    pub fn ack_confirmed(&mut self, seq: u64) {
        self.acked = self.acked.max(seq);
    }

    /// The session's end (a `SessionEnded` or an ENDED snapshot) has reached the owner.
    pub fn ended(&self) -> bool {
        self.ended_at.is_some()
    }

    /// The end is redeemed: its ack may go (and must, before the session is forgotten).
    pub fn end_redeemed(&self) -> bool {
        self.ended_at.is_some() && self.redeemed
    }

    /// The end is redeemed and TC took its ack (IE-022: nothing of the session is left to do).
    pub fn end_acked(&self) -> bool {
        matches!(self.ended_at, Some(e) if self.redeemed && self.acked >= e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn a_replayed_session_ended_is_dropped_and_not_acked_until_redeemed() {
        let mut d = Delivery::starting_before(1);
        assert_eq!(d.offer(1), Offer::Deliver);
        d.delivered(1, false);
        assert_eq!(d.offer(2), Offer::Deliver);
        d.delivered(2, true);
        assert_eq!(d.pending_ack(), Some(1), "the SessionEnded itself waits");
        assert_eq!(d.offer(2), Offer::Replay);
        assert_eq!(d.pending_ack(), Some(1));
        d.redeem(2);
        assert_eq!(d.pending_ack(), Some(2));
        assert!(d.ended() && !d.end_acked(), "redeemed, not yet taken by TC");
        d.ack_confirmed(1);
        assert!(!d.end_acked());
        d.ack_confirmed(2);
        assert!(d.end_acked());
    }

    #[test]
    fn a_gap_holds_everything_until_the_snapshot() {
        let mut d = Delivery::starting_before(1);
        assert_eq!(d.offer(1), Offer::Deliver);
        d.delivered(1, false);
        assert_eq!(d.offer(3), Offer::Hold);
        assert_eq!(
            d.offer(2),
            Offer::Hold,
            "even the missing one, once a resync is due"
        );
        assert!(d.start_resync());
        assert!(!d.start_resync(), "never two in flight");
        assert!(d.resynced(3, false));
        assert_eq!(d.offer(3), Offer::Replay);
        assert_eq!(d.offer(4), Offer::Deliver);
    }

    /// Fix 1: an older snapshot landing after a newer one never moves `processed` back.
    #[test]
    fn an_older_snapshot_never_moves_processed_back() {
        let mut d = Delivery::starting_before(1);
        assert!(d.resynced(5, false));
        assert!(!d.resynced(3, false), "older: ignored");
        assert_eq!(d.offer(4), Offer::Replay);
        assert_eq!(d.offer(6), Offer::Deliver);
    }

    #[derive(Debug, Clone)]
    enum Op {
        Offer(u64, bool),
        Overflow,
        StartResync,
        Resynced(u64, bool),
        ResyncFailed,
        Redeem,
        Confirm,
    }

    fn op() -> impl Strategy<Value = Op> {
        prop_oneof![
            6 => (1u64..30, proptest::bool::weighted(0.1)).prop_map(|(s, e)| Op::Offer(s, e)),
            1 => Just(Op::Overflow),
            1 => Just(Op::StartResync),
            1 => (0u64..30, proptest::bool::weighted(0.2)).prop_map(|(s, e)| Op::Resynced(s, e)),
            1 => Just(Op::ResyncFailed),
            1 => Just(Op::Redeem),
            2 => Just(Op::Confirm),
        ]
    }

    proptest! {
        /// Over any stream of duplicates, replays, gaps, overflows and resyncs: events reach the
        /// owner in order, one at a time, never while a resync is due; an ack never passes what
        /// the owner has; an unredeemed end is never acked.
        #[test]
        fn delivery_invariants(ops in proptest::collection::vec(op(), 1..200)) {
            let mut d = Delivery::starting_before(1);
            let mut last_delivered: u64 = 0;
            let mut ended_unredeemed: Option<u64> = None;
            for o in ops {
                match o {
                    Op::Offer(seq, ended) => match d.offer(seq) {
                        Offer::Deliver => {
                            prop_assert_eq!(seq, d.processed() + 1);
                            prop_assert_eq!(d.resync(), Resync::Idle);
                            prop_assert!(seq > last_delivered || last_delivered == 0 || seq == d.processed() + 1);
                            d.delivered(seq, ended);
                            last_delivered = seq;
                            if ended && ended_unredeemed.is_none() && d.ackable() < seq {
                                ended_unredeemed = Some(seq);
                            }
                        }
                        Offer::Replay => prop_assert!(seq <= d.processed()),
                        Offer::Hold => prop_assert!(seq > d.processed()),
                    },
                    Op::Overflow => d.want_resync(),
                    Op::StartResync => { d.start_resync(); }
                    Op::Resynced(last, ended) => {
                        if d.resync() == Resync::InFlight {
                            let before = d.processed();
                            if d.resynced(last, ended) {
                                last_delivered = last;
                                ended_unredeemed = if ended { Some(last.max(1)) } else { None };
                            } else {
                                prop_assert_eq!(d.processed(), before, "an older snapshot is ignored");
                            }
                        }
                    }
                    Op::ResyncFailed => d.resync_failed(),
                    Op::Redeem => {
                        if let Some(e) = ended_unredeemed.take() { d.redeem(e); }
                    }
                    Op::Confirm => {
                        if let Some(a) = d.pending_ack() { d.ack_confirmed(a); }
                    }
                }
                prop_assert!(d.ackable() <= d.processed());
                if let Some(e) = ended_unredeemed {
                    prop_assert!(d.ackable() < e, "an unredeemed end is never ackable");
                    if let Some(a) = d.pending_ack() { prop_assert!(a < e); }
                }
            }
        }
    }
}
