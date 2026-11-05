//! A session's mailbox (ADR 0003 §4.3, R1-4): bounded for events, never for the control events
//! that tell the owner what happened to the session; a resync's snapshot replaces what is queued.
//! Its owner dropping it tells the client (IE-022): nobody will read or redeem its events again.

use crate::api::SessionEvent;
use std::collections::VecDeque;
use std::sync::Mutex;
use tokio::sync::Notify;

pub(crate) struct Queue {
    st: Mutex<QueueSt>,
    notify: Notify,
    cap: usize,
    low_water: usize,
    on_low: Box<dyn Fn() + Send + Sync>,
    on_abandon: Box<dyn Fn() + Send + Sync>,
}

struct QueueSt {
    events: VecDeque<SessionEvent>,
    closed: bool,
}

impl Queue {
    pub fn new(
        cap: usize,
        low_water: usize,
        on_low: Box<dyn Fn() + Send + Sync>,
        on_abandon: Box<dyn Fn() + Send + Sync>,
    ) -> Self {
        Self {
            st: Mutex::new(QueueSt {
                events: VecDeque::new(),
                closed: false,
            }),
            notify: Notify::new(),
            cap,
            low_water,
            on_low,
            on_abandon,
        }
    }

    /// An event; false if the mailbox is full (it is then not queued).
    pub fn push(&self, ev: SessionEvent) -> bool {
        let Ok(mut st) = self.st.lock() else {
            return false;
        };
        if st.closed || st.events.len() >= self.cap {
            return false;
        }
        st.events.push_back(ev);
        drop(st);
        self.notify.notify_one();
        true
    }

    /// A control event: always queued.
    pub fn force(&self, ev: SessionEvent) {
        if let Ok(mut st) = self.st.lock()
            && !st.closed
        {
            st.events.push_back(ev);
        }
        self.notify.notify_one();
    }

    /// A snapshot replaces the events still queued (R1-4); the control events that tell the owner
    /// about the node (`Unmanaged`, `Managed`) stay, in order, before it (fix 3).
    pub fn replace_with(&self, evs: Vec<SessionEvent>) {
        if let Ok(mut st) = self.st.lock()
            && !st.closed
        {
            st.events
                .retain(|e| matches!(e, SessionEvent::Unmanaged | SessionEvent::Managed));
            st.events.extend(evs);
        }
        self.notify.notify_one();
    }

    /// No more events: `recv` drains, then ends.
    pub fn close(&self) {
        if let Ok(mut st) = self.st.lock() {
            st.closed = true;
        }
        self.notify.notify_one();
    }

    pub fn below_low_water(&self) -> bool {
        self.st
            .lock()
            .map(|st| st.events.len() < self.low_water)
            .unwrap_or(true)
    }

    fn pop(&self) -> (Option<SessionEvent>, bool, bool) {
        let Ok(mut st) = self.st.lock() else {
            return (None, true, false);
        };
        let ev = st.events.pop_front();
        let low = st.events.len() < self.low_water;
        (ev, st.closed, low)
    }
}

/// The owner's end of a session's mailbox.
pub struct SessionEvents(pub(crate) std::sync::Arc<Queue>);

impl SessionEvents {
    /// The next event; `None` once the session is over for this owner (fenced, dead, ended).
    pub async fn recv(&mut self) -> Option<SessionEvent> {
        loop {
            let (ev, closed, low) = self.0.pop();
            if let Some(ev) = ev {
                if low {
                    (self.0.on_low)();
                }
                return Some(ev);
            }
            if closed {
                return None;
            }
            self.0.notify.notified().await;
        }
    }

    /// The next event if one is queued.
    pub fn try_recv(&mut self) -> Option<SessionEvent> {
        let (ev, _, low) = self.0.pop();
        if ev.is_some() && low {
            (self.0.on_low)();
        }
        ev
    }
}

impl Drop for SessionEvents {
    /// The owner is done with the session (IE-022): the client may forget it.
    fn drop(&mut self) {
        (self.0.on_abandon)();
    }
}

impl std::fmt::Debug for SessionEvents {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SessionEvents { .. }")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Fix 3: a resync's snapshot keeps the node's control events queued before it.
    #[test]
    fn a_snapshot_keeps_the_control_events() {
        let q = std::sync::Arc::new(Queue::new(8, 2, Box::new(|| {}), Box::new(|| {})));
        q.force(SessionEvent::Unmanaged);
        assert!(q.push(SessionEvent::Unknown));
        q.force(SessionEvent::Managed);
        q.replace_with(vec![SessionEvent::Unknown, SessionEvent::Unknown]);
        let mut r = SessionEvents(q);
        assert!(matches!(r.try_recv(), Some(SessionEvent::Unmanaged)));
        assert!(matches!(r.try_recv(), Some(SessionEvent::Managed)));
        assert!(matches!(r.try_recv(), Some(SessionEvent::Unknown)));
        assert!(matches!(r.try_recv(), Some(SessionEvent::Unknown)));
        assert!(r.try_recv().is_none(), "the replaced event is gone");
    }
}
