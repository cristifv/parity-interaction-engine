//! interaction-engine's observability: JSON logs with PII masking, and metrics.
//! The shared logging library of ARCHITECTURE.md §5.2; every later IE crate depends on it.

pub mod logging;
pub mod metrics;
pub mod pii;
#[cfg(feature = "testing")]
pub mod testing;

pub use logging::{Identity, LogConfig, LogGuard};
pub use metrics::Metrics;
pub use pii::{Masked, Phone};
