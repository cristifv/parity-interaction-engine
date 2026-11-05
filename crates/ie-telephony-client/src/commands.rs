//! Errors and retries (ADR 0003 §4.4, R1-3, R1-5): the `parity-error` trailer decides, the gRPC
//! status only when there is none.

use crate::api::ErrorCode;
use crate::wire;
use std::time::Duration;

/// What to do about a failed call.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Class {
    /// Retry with the same `command_id`, up to 3 times.
    Retry(ErrorCode),
    /// Retry for up to `rebuilding_retry_ms` (`rebuilding`, `owner_check_unavailable`).
    RetryLong(ErrorCode),
    /// `stale_owner`.
    Fenced,
    /// Log, alert, never retry.
    Bug(ErrorCode),
    /// TC disagrees with IE's view: resync.
    Resync(ErrorCode),
    /// `session_ended`.
    Ended,
    /// `arrival_closed`.
    ArrivalClosed,
    /// `prompt_unknown`.
    Config(ErrorCode),
    /// `no_capacity`, `draining`.
    Capacity(ErrorCode),
    /// `UNIMPLEMENTED` without a trailer: an older TC.
    Unsupported,
    /// `stream_superseded`.
    Superseded,
}

pub(crate) fn classify(s: &tonic::Status) -> Class {
    if let Some(t) = wire::parity_error(s) {
        // A trailer this release doesn't know (a newer TC) falls back to the status (fix 5).
        let Some(code) =
            ErrorCode::from_str_name(&format!("ERROR_CODE_{}", t.to_ascii_uppercase()))
        else {
            return by_status(s);
        };
        return match code {
            ErrorCode::StaleOwner => Class::Fenced,
            ErrorCode::OwnerUnverified
            | ErrorCode::TenantMismatch
            | ErrorCode::InvalidRequest
            | ErrorCode::IdentityMismatch
            | ErrorCode::LegConflict
            | ErrorCode::CommandConflict
            | ErrorCode::Unspecified => Class::Bug(code),
            ErrorCode::OwnerCheckUnavailable | ErrorCode::Rebuilding => Class::RetryLong(code),
            ErrorCode::SessionUnknown
            | ErrorCode::LegUnknown
            | ErrorCode::WrongNode
            | ErrorCode::InvalidState => Class::Resync(code),
            ErrorCode::SessionEnded => Class::Ended,
            ErrorCode::ArrivalClosed => Class::ArrivalClosed,
            ErrorCode::PromptUnknown => Class::Config(code),
            ErrorCode::NoCapacity | ErrorCode::Draining => Class::Capacity(code),
            ErrorCode::FreeswitchUnavailable
            | ErrorCode::ParityMsUnavailable
            | ErrorCode::AttemptExpired
            | ErrorCode::SessionBusy
            | ErrorCode::Internal => Class::Retry(code),
            ErrorCode::StreamSuperseded => Class::Superseded,
        };
    }
    by_status(s)
}

fn by_status(s: &tonic::Status) -> Class {
    match s.code() {
        tonic::Code::Unavailable
        | tonic::Code::DeadlineExceeded
        | tonic::Code::Cancelled
        | tonic::Code::Unknown => Class::Retry(ErrorCode::Unspecified),
        tonic::Code::Unimplemented => Class::Unsupported,
        _ => Class::Bug(ErrorCode::Unspecified),
    }
}

/// A client-side deadline that ran out: the result is unknown, so retry with the same id.
pub(crate) fn timed_out() -> Class {
    Class::Retry(ErrorCode::Unspecified)
}

/// Full jitter: a random delay in `[0, min(cap, base * 2^attempt)]`.
pub(crate) fn backoff(attempt: u32, base_ms: u64, cap_ms: u64) -> Duration {
    let ceiling = base_ms
        .saturating_mul(1u64 << attempt.min(16))
        .min(cap_ms)
        .max(1);
    Duration::from_millis(fastrand::u64(0..=ceiling))
}

/// `<replica_id>.<process start ms>:<UUIDv7>`, within `^[A-Za-z0-9._:-]{1,200}$`.
pub(crate) fn command_id(replica_id: &str, start_ms: u128) -> String {
    format!("{replica_id}.{start_ms}:{}", uuid::Uuid::now_v7())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::metadata::MetadataValue;

    fn status(code: tonic::Code, trailer: Option<&'static str>) -> tonic::Status {
        let mut s = tonic::Status::new(code, "x");
        if let Some(t) = trailer {
            s.metadata_mut()
                .insert("parity-error", MetadataValue::from_static(t));
        }
        s
    }

    /// R1-5: the trailer decides, whatever the status says.
    #[test]
    fn the_trailer_decides_not_the_status() {
        assert_eq!(
            classify(&status(tonic::Code::Unavailable, Some("invalid_state"))),
            Class::Resync(ErrorCode::InvalidState)
        );
        assert_eq!(
            classify(&status(tonic::Code::Aborted, Some("stale_owner"))),
            Class::Fenced
        );
        assert_eq!(
            classify(&status(tonic::Code::Unavailable, Some("draining"))),
            Class::Capacity(ErrorCode::Draining)
        );
        assert_eq!(
            classify(&status(tonic::Code::Unavailable, Some("rebuilding"))),
            Class::RetryLong(ErrorCode::Rebuilding)
        );
        assert_eq!(
            classify(&status(
                tonic::Code::Unavailable,
                Some("owner_check_unavailable")
            )),
            Class::RetryLong(ErrorCode::OwnerCheckUnavailable)
        );
        for t in [
            "freeswitch_unavailable",
            "parity_ms_unavailable",
            "internal",
        ] {
            assert!(
                matches!(
                    classify(&status(tonic::Code::Unavailable, Some(t))),
                    Class::Retry(_)
                ),
                "{t}"
            );
        }
        assert_eq!(
            classify(&status(tonic::Code::Unimplemented, None)),
            Class::Unsupported
        );
        assert_eq!(
            classify(&status(tonic::Code::DeadlineExceeded, None)),
            Class::Retry(ErrorCode::Unspecified)
        );
        assert_eq!(
            classify(&status(tonic::Code::InvalidArgument, Some("no_such_code"))),
            Class::Bug(ErrorCode::Unspecified)
        );
    }

    /// Fix 5: a trailer from a newer TC that this release doesn't know falls back to the status.
    #[test]
    fn an_unknown_trailer_falls_back_to_the_status() {
        assert_eq!(
            classify(&status(tonic::Code::Unavailable, Some("some_future_code"))),
            Class::Retry(ErrorCode::Unspecified)
        );
        assert_eq!(
            classify(&status(
                tonic::Code::Unimplemented,
                Some("some_future_code")
            )),
            Class::Unsupported
        );
    }

    #[test]
    fn command_ids_fit_the_contract() {
        assert!(crate::validate::is_command_id(&command_id(
            "ie-bin-a-1",
            1_792_800_000_000
        )));
    }
}
