//! Sessions a node's state no longer holds (IE-022): their keys only, for a short window, so a
//! late duplicate of their events is dropped instead of starting an orphan. Bounded by time (one
//! window for every key, so expiry is first in, first out) and by count (past the cap the oldest
//! go early). Pure; the node's lock guards it.

use crate::api::SessionKey;
use std::collections::{HashMap, VecDeque};

/// What is kept of a forgotten session.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Gone {
    /// When it is dropped (the client's clock, ms).
    pub expires_ms: u64,
    /// The epoch this replica acked its end with; 0 if it never owned it here (no ack to repeat).
    pub owner_epoch: u64,
    /// An arrival left unacked while this replica took no arrivals (IE-055): offered again once
    /// it takes them, it is a new arrival, not a duplicate.
    pub retake: bool,
}

#[derive(Debug, Default)]
pub(crate) struct Forgotten {
    keys: HashMap<SessionKey, Gone>,
    /// In expiry order. A key forgotten again has a newer entry; the older one is skipped.
    order: VecDeque<(u64, SessionKey)>,
}

impl Forgotten {
    /// Forgets `key` for `window_ms`; past `cap` keys the oldest go now.
    pub fn insert(
        &mut self,
        key: SessionKey,
        owner_epoch: u64,
        retake: bool,
        now_ms: u64,
        window_ms: u64,
        cap: usize,
    ) {
        let expires_ms = now_ms.saturating_add(window_ms);
        self.keys.insert(
            key.clone(),
            Gone {
                expires_ms,
                owner_epoch,
                retake,
            },
        );
        self.order.push_back((expires_ms, key));
        while self.keys.len() > cap.max(1) {
            if !self.pop_front() {
                break;
            }
        }
    }

    /// Drops every key whose window is over.
    pub fn expire(&mut self, now_ms: u64) {
        while self.order.front().is_some_and(|(at, _)| *at <= now_ms) {
            self.pop_front();
        }
    }

    pub fn get(&self, key: &SessionKey) -> Option<Gone> {
        self.keys.get(key).copied()
    }

    /// The session is held again (an explicit claim): it is no longer forgotten.
    pub fn remove(&mut self, key: &SessionKey) {
        self.keys.remove(key);
    }

    pub fn len(&self) -> usize {
        self.keys.len()
    }

    /// Pops the oldest entry; false once there is none.
    fn pop_front(&mut self) -> bool {
        let Some((at, key)) = self.order.pop_front() else {
            return false;
        };
        if self.keys.get(&key).is_some_and(|g| g.expires_ms == at) {
            self.keys.remove(&key);
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn k(i: usize) -> SessionKey {
        SessionKey {
            tenant_id: "acme".into(),
            interaction_id: format!("0192f6a1-7c3e-7b21-9a4d-{i:012}"),
        }
    }

    #[test]
    fn a_key_lasts_its_window_then_goes() {
        let mut f = Forgotten::default();
        f.insert(k(1), 7, false, 1_000, 60_000, 10);
        assert_eq!(
            f.get(&k(1)),
            Some(Gone {
                expires_ms: 61_000,
                owner_epoch: 7,
                retake: false
            })
        );
        f.expire(60_999);
        assert!(f.get(&k(1)).is_some());
        f.expire(61_000);
        assert!(f.get(&k(1)).is_none());
        assert_eq!(f.len(), 0);
    }

    #[test]
    fn past_the_cap_the_oldest_go_first() {
        let mut f = Forgotten::default();
        for i in 0..5 {
            f.insert(k(i), 0, false, i as u64, 60_000, 3);
        }
        assert_eq!(f.len(), 3);
        assert!(f.get(&k(0)).is_none() && f.get(&k(1)).is_none());
        assert!(f.get(&k(2)).is_some() && f.get(&k(4)).is_some());
    }

    #[test]
    fn a_key_forgotten_again_keeps_its_newer_window() {
        let mut f = Forgotten::default();
        f.insert(k(1), 0, false, 0, 100, 10);
        f.insert(k(1), 9, false, 50, 100, 10);
        f.expire(100);
        assert_eq!(
            f.get(&k(1)).map(|g| g.owner_epoch),
            Some(9),
            "the stale entry is skipped"
        );
        f.expire(150);
        assert!(f.get(&k(1)).is_none());
        assert_eq!(f.order.len(), 0);
    }

    #[test]
    fn a_hundred_thousand_keys_stay_within_the_cap_and_drain_by_time() {
        let mut f = Forgotten::default();
        for i in 0..100_000 {
            f.insert(k(i), 1, false, i as u64, 60_000, 50_000);
            assert!(f.len() <= 50_000);
        }
        assert_eq!(f.len(), 50_000);
        f.expire(100_000 + 60_000);
        assert_eq!(f.len(), 0);
        assert!(f.order.is_empty());
    }
}
