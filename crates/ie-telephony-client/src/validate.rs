//! The contract's field rules (TC ADR 0001 §4.2), checked before anything is sent: a request a
//! writer-invalid script describes is never built (ADR 0003 §5). TC checks them again.

use crate::api::{Decline, Destination, DigitReporting, Intent, Owner};

/// Which rule an intent breaks; the names follow the writer-invalid scripts' `violates`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum Invalid {
    /// `owner_epoch` must be at least 1.
    #[error("bad_epoch")]
    BadEpoch,
    /// The tenant isn't a DNS label.
    #[error("bad_tenant")]
    BadTenant,
    /// An id that must be a lowercase UUID isn't.
    #[error("bad_uuid")]
    BadUuid,
    /// A `command_id` outside `^[A-Za-z0-9._:-]{1,200}$`.
    #[error("bad_command_id")]
    BadCommandId,
    /// A SIP status outside 400–699 where a failure is required.
    #[error("bad_sip_status")]
    BadSipStatus,
    /// A digit outside `0-9*#A-D`.
    #[error("bad_digit")]
    BadDigit,
    /// A DTMF action code that doesn't start with `*`.
    #[error("bad_dtmf_sequence")]
    BadDtmfSequence,
    /// A prompt id with a path separator, or empty.
    #[error("bad_prompt_id")]
    BadPromptId,
    /// A value out of its range (`max_duration_s`, a missing enum, an empty contact).
    #[error("out_of_range")]
    OutOfRange,
}

/// A lowercase canonical UUID.
pub fn is_uuid(s: &str) -> bool {
    let b = s.as_bytes();
    b.len() == 36
        && b.iter().enumerate().all(|(i, &c)| match i {
            8 | 13 | 18 | 23 => c == b'-',
            _ => c.is_ascii_digit() || (b'a'..=b'f').contains(&c),
        })
}

/// A DNS label: 1–63 of `a-z0-9-`, no leading or trailing `-`.
pub fn is_dns_label(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 63
        && !s.starts_with('-')
        && !s.ends_with('-')
        && s.bytes()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'-')
}

/// `^[A-Za-z0-9._:-]{1,200}$`
pub fn is_command_id(s: &str) -> bool {
    (1..=200).contains(&s.len())
        && s.bytes()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'.' | b'_' | b':' | b'-'))
}

fn uuid(s: &str) -> Result<(), Invalid> {
    if is_uuid(s) {
        Ok(())
    } else {
        Err(Invalid::BadUuid)
    }
}

fn failure_status(code: u32) -> Result<(), Invalid> {
    if (400..=699).contains(&code) {
        Ok(())
    } else {
        Err(Invalid::BadSipStatus)
    }
}

fn prompt(id: &str) -> Result<(), Invalid> {
    if id.is_empty() || id.len() > 200 || id.contains('/') || id.contains('\\') || id.contains("..")
    {
        Err(Invalid::BadPromptId)
    } else {
        Ok(())
    }
}

fn digits(d: &str) -> Result<(), Invalid> {
    if !d.is_empty()
        && d.bytes()
            .all(|c| c.is_ascii_digit() || matches!(c, b'*' | b'#' | b'A'..=b'D'))
    {
        Ok(())
    } else {
        Err(Invalid::BadDigit)
    }
}

/// The owner block's rules.
pub fn owner(o: &Owner) -> Result<(), Invalid> {
    if o.owner_epoch == 0 {
        return Err(Invalid::BadEpoch);
    }
    if !is_dns_label(&o.key.tenant_id) {
        return Err(Invalid::BadTenant);
    }
    uuid(&o.key.interaction_id)
}

/// An intent's rules, the owner's included.
pub fn intent(o: &Owner, i: &Intent) -> Result<(), Invalid> {
    owner(o)?;
    match i {
        Intent::Ring { leg_id } | Intent::Answer { leg_id } => uuid(leg_id),
        Intent::Reject { leg_id, sip_status } => {
            uuid(leg_id)?;
            failure_status(*sip_status)
        }
        Intent::AddLeg(a) => {
            uuid(&a.leg_id)?;
            match &a.destination {
                Destination::Contact(c) if c.is_empty() => return Err(Invalid::OutOfRange),
                Destination::Pstn { number, trunk_id }
                    if number.is_empty() || trunk_id.is_empty() =>
                {
                    return Err(Invalid::OutOfRange);
                }
                _ => {}
            }
            if !a.whisper_to_leg_id.is_empty() {
                uuid(&a.whisper_to_leg_id)?;
            }
            if let Some(actions) = &a.dtmf_actions {
                dtmf_actions(actions)?;
            }
            Ok(())
        }
        Intent::RemoveLeg { leg_id, .. } => uuid(leg_id),
        Intent::EndSession { .. } => Ok(()),
        Intent::Play {
            leg_id,
            play_id,
            prompt_id,
            ..
        } => {
            uuid(leg_id)?;
            uuid(play_id)?;
            prompt(prompt_id)
        }
        Intent::StopPlay { leg_id, play_id } => {
            uuid(leg_id)?;
            uuid(play_id)
        }
        Intent::SetDigitReporting {
            leg_id,
            mode,
            collection_id,
            max_duration_s,
        } => {
            uuid(leg_id)?;
            match mode {
                DigitReporting::Off => Ok(()),
                DigitReporting::On | DigitReporting::Masked => {
                    uuid(collection_id)?;
                    if (1..=300).contains(max_duration_s) {
                        Ok(())
                    } else {
                        Err(Invalid::OutOfRange)
                    }
                }
                DigitReporting::Unspecified => Err(Invalid::OutOfRange),
            }
        }
        Intent::SendDtmf { leg_id, digits: d } => {
            uuid(leg_id)?;
            digits(d.expose())
        }
        Intent::SetMute { leg_id, .. } | Intent::SetIsolation { leg_id, .. } => uuid(leg_id),
        Intent::SetHold {
            music_prompt_id,
            on,
        } => {
            if *on {
                prompt(music_prompt_id)
            } else {
                Ok(())
            }
        }
        Intent::SetMonitorMode {
            leg_id,
            whisper_to_leg_id,
            ..
        } => {
            uuid(leg_id)?;
            if whisper_to_leg_id.is_empty() {
                Ok(())
            } else {
                uuid(whisper_to_leg_id)
            }
        }
        Intent::SetDtmfActions { leg_id, actions } => {
            uuid(leg_id)?;
            dtmf_actions(actions)
        }
        Intent::StartRecording {
            leg_id,
            recording_id,
            key,
        } => {
            uuid(leg_id)?;
            uuid(recording_id)?;
            if key.dek.is_empty() || key.kek_id.is_empty() {
                Err(Invalid::OutOfRange)
            } else {
                Ok(())
            }
        }
        Intent::StopRecording { recording_id } => uuid(recording_id),
    }
}

fn dtmf_actions(a: &crate::api::DtmfActions) -> Result<(), Invalid> {
    for (seq, _) in &a.bindings {
        let s = seq.expose();
        if !s.starts_with('*') {
            return Err(Invalid::BadDtmfSequence);
        }
        digits(s)?;
    }
    Ok(())
}

/// `DeclineArrival`'s rules.
pub fn decline(
    tenant_id: &str,
    interaction_id: &str,
    leg_id: &str,
    d: &Decline,
) -> Result<(), Invalid> {
    if !is_dns_label(tenant_id) {
        return Err(Invalid::BadTenant);
    }
    uuid(interaction_id)?;
    uuid(leg_id)?;
    match d {
        Decline::WrongBin { bin } if bin.is_empty() => Err(Invalid::OutOfRange),
        Decline::WrongBin { .. } => Ok(()),
        Decline::Refused { sip_status } => failure_status(*sip_status),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::{Digits, DtmfActionKind, DtmfActions, SessionKey};

    const IID: &str = "0192f6a1-7c3e-7b21-9a4d-5e6f70819203";
    const LEG: &str = "0192f6a1-8000-7000-8000-000000000001";

    fn own(epoch: u64, tenant: &str) -> Owner {
        Owner {
            key: SessionKey {
                tenant_id: tenant.into(),
                interaction_id: IID.into(),
            },
            owner_epoch: epoch,
        }
    }

    /// Each writer-invalid script of parity-contracts (fixtures/telephony.v1/invalid) is a request
    /// the client refuses to build. `missing_owner` and `tenant_mismatch` can't be expressed at
    /// all: the owner block is mandatory and the tenant metadata is always taken from it.
    #[test]
    fn writer_invalid_requests_are_refused() {
        let ok = own(1042, "acme");
        assert_eq!(
            intent(&own(0, "acme"), &Intent::Answer { leg_id: LEG.into() }),
            Err(Invalid::BadEpoch),
            "epoch-zero"
        );
        assert_eq!(
            intent(
                &ok,
                &Intent::Answer {
                    leg_id: "leg-1".into()
                }
            ),
            Err(Invalid::BadUuid),
            "leg-id-not-uuid"
        );
        assert_eq!(
            intent(
                &ok,
                &Intent::Reject {
                    leg_id: LEG.into(),
                    sip_status: 200
                }
            ),
            Err(Invalid::BadSipStatus),
            "reject-with-200"
        );
        assert!(!is_command_id("ie-bin-a-1 0001"), "bad-command-id");
        assert!(is_command_id(
            "ie-bin-a-1.1792800000000:0192f6a1-8000-7000-8000-000000000001"
        ));
        assert_eq!(
            intent(&own(1, "Acme Corp"), &Intent::Answer { leg_id: LEG.into() }),
            Err(Invalid::BadTenant)
        );
        assert_eq!(
            intent(
                &ok,
                &Intent::SendDtmf {
                    leg_id: LEG.into(),
                    digits: Digits::new("12x")
                }
            ),
            Err(Invalid::BadDigit),
            "a digit outside the set"
        );
        let actions = DtmfActions {
            bindings: vec![(Digits::new("12"), DtmfActionKind::Mute)],
            ..Default::default()
        };
        assert_eq!(
            intent(
                &ok,
                &Intent::SetDtmfActions {
                    leg_id: LEG.into(),
                    actions
                }
            ),
            Err(Invalid::BadDtmfSequence),
            "a dtmf_sequence without *"
        );
        assert_eq!(
            intent(
                &ok,
                &Intent::Play {
                    leg_id: LEG.into(),
                    play_id: LEG.into(),
                    prompt_id: "../etc/x".into(),
                    looped: false
                }
            ),
            Err(Invalid::BadPromptId),
            "a prompt_id with /"
        );
        assert_eq!(intent(&ok, &Intent::Answer { leg_id: LEG.into() }), Ok(()));
        assert_eq!(
            decline("acme", IID, LEG, &Decline::Refused { sip_status: 200 }),
            Err(Invalid::BadSipStatus)
        );
    }

    #[test]
    fn uuids_are_lowercase_canonical() {
        assert!(is_uuid(LEG));
        assert!(!is_uuid(&LEG.to_uppercase()));
        assert!(!is_uuid("0192f6a18000700080000000000000001"));
    }
}
