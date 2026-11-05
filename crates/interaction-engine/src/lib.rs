//! parity-interaction-engine, step 0: the process shape every later step builds on
//! (ADR 0001 §5–§7): configuration, the readiness gate and healthy file, the §7.3 drain,
//! JSON logs with masking, and metrics. Step 3 adds the call path through the telephony
//! controller (`calls`): DEMO level 0, one prompt per call.

pub mod backoff;
pub mod calls;
pub mod cdr;
pub mod config;
pub mod files;
pub mod lifecycle;
pub mod management;
pub mod readiness;
pub mod tenants;
