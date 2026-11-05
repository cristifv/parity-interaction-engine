//! interaction-engine's interaction core (PLAN step 4; `ie-core` and `ie-verbs` of PLAN's module
//! list, one crate for now): the static flow file, the voice state machine and its verb runner.
//! Pure: no I/O, no clock, no randomness (ids come from an [`machine::Ids`] source).

pub mod flow;
pub mod machine;
pub mod prompt;
pub mod xsid;

pub use flow::{Consent, ConsentMode, Flow, FlowError, FlowFile, Verb};
pub use machine::{
    AgentLeg, AgentOutcome, Call, CallEvent, Cmd, CmdResult, ConsentBasis, Decision, DequeueReason,
    DtmfKind, DtmfResult, Effect, EndCause, EnqueueReason, Ids, Input, OverflowAction, Phase,
    QueueOutcome, QueueState, RecState, Reconcile, Route, RouteCmd, RouteResult, RouteSync,
    Summary, position_prompt,
};
pub use prompt::say_prompt_id;
pub use xsid::{CallIds, Origin, XsidError};
