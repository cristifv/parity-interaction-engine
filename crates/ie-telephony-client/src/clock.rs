//! The clock the liveness timer and the evidence window read (R1-8): real time in production, a
//! manual clock against fake-telephony-controller, whose own clock moves only when told.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// Milliseconds on some monotonic scale.
pub trait Clock: Send + Sync + 'static {
    /// Now, in milliseconds.
    fn now_ms(&self) -> u64;
}

/// Monotonic real time.
#[derive(Debug)]
pub struct SystemClock(Instant);

impl SystemClock {
    /// Starts at 0.
    pub fn new() -> Self {
        Self(Instant::now())
    }
}

impl Default for SystemClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for SystemClock {
    fn now_ms(&self) -> u64 {
        u64::try_from(self.0.elapsed().as_millis()).unwrap_or(u64::MAX)
    }
}

/// A clock that moves only through [`ManualClock::advance`].
#[derive(Debug, Default)]
pub struct ManualClock(AtomicU64);

impl ManualClock {
    /// Starts at 0.
    pub fn new() -> Self {
        Self::default()
    }
    /// Moves time forward.
    pub fn advance(&self, ms: u64) {
        self.0.fetch_add(ms, Ordering::SeqCst);
    }
}

impl Clock for ManualClock {
    fn now_ms(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}
