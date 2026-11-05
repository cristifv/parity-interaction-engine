//! Exponential backoff with full jitter. Moves to a shared crate when `ie-esl` needs it
//! (step 2).

use std::time::Duration;

/// Delays that double from `base` up to `cap`; each attempt sleeps a random time in
/// `[0, current]` (full jitter).
#[derive(Debug, Clone)]
pub struct Backoff {
    current: Duration,
    cap: Duration,
}

impl Backoff {
    pub fn new(base: Duration, cap: Duration) -> Self {
        Self { current: base, cap }
    }

    /// The next delay to sleep.
    pub fn next_delay(&mut self) -> Duration {
        let ceiling = self.current;
        self.current = (self.current * 2).min(self.cap);
        let millis = u64::try_from(ceiling.as_millis()).unwrap_or(u64::MAX);
        Duration::from_millis(fastrand::u64(0..=millis))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delays_stay_under_a_doubling_ceiling_up_to_the_cap() {
        let mut b = Backoff::new(Duration::from_millis(100), Duration::from_millis(400));
        for ceiling in [100, 200, 400, 400, 400] {
            assert!(b.next_delay() <= Duration::from_millis(ceiling));
        }
    }
}
