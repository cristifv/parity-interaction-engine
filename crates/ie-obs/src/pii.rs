//! Personal data in logs (ARCHITECTURE.md §5.2; ADR 0001 §7.2).
//!
//! - [`mask_phone`]: a number keeps only its last 3 digits.
//! - [`Phone`]: a number that can only be printed masked.
//! - [`classify`]: the field policy the JSON formatter applies to every field name, after
//!   [`normalize`] (camelCase split, lowercase, `-` → `_`, `variable_` stripped), whatever the
//!   value's type.
//! - [`Masked`]: logs a structured value with the policy applied at every depth.

use serde::Serialize;
use serde_json::Value;
use std::fmt;

/// What the formatter does with a field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldClass {
    /// Masked to its last 3 digits.
    Phone,
    /// Replaced by `[redacted]`.
    Secret,
    /// Written as is.
    Plain,
}

const REDACTED: &str = "[redacted]";

const PHONE_NAMES: [&str; 33] = [
    "from",
    "to",
    "number",
    "phone",
    "caller",
    "callee",
    "caller_id",
    "callback_number",
    "destination",
    "ani",
    "dnis",
    "msisdn",
    "e164",
    "contact",
    "uri",
    "pai",
    "p_asserted_identity",
    "rpid",
    "remote_party_id",
    "diversion",
    "history_info",
    "user",
    // freeswitch ESL names that carry numbers (F-C). Step 2 replaces this list, for ESL
    // events, with an allowlist of headers that may be logged.
    "channel_name",
    "caller_channel_name",
    "caller_username",
    "rdnis",
    "dialed_extension",
    "originate_string",
    "dial_string",
    "sip_from_display",
    "caller_caller_id_name",
    "sip_destination_url",
    "sip_call_id",
];
const SECRET_NAMES: [&str; 14] = [
    "digits",
    "dtmf",
    "pin",
    "password",
    "secret",
    "token",
    "authorization",
    "api_key",
    "dek",
    "wrapped_dek",
    "sdp",
    "sip_body",
    "passwd",
    "credentials",
];
/// Any name with one of these `_`-separated tokens is a secret (F-A: DTMF digits can be a
/// card number, PCI DSS): `DTMF-Digit` → `dtmf_digit`, `variable_dtmf_digits`, `digit`.
const SECRET_TOKENS: [&str; 3] = ["dtmf", "digit", "digits"];
/// Names a rule would catch that are known not to hold personal data.
const EXEMPT: [&str; 2] = ["reply_to", "num_digits"];

/// camelCase split into words (H1: `callerNumber` → `caller_number`, `DTMFDigit` →
/// `dtmf_digit`, `sipCallId` → `sip_call_id`), then lowercase, `-` → `_`, and a leading
/// `variable_` (freeswitch channel variables) stripped. snake_case and kebab-case names are
/// unchanged by the split: a boundary is only an uppercase letter after a lowercase letter or
/// a digit, or the last capital of an acronym followed by a lowercase letter.
pub fn normalize(name: &str) -> String {
    let chars: Vec<char> = name.chars().collect();
    let mut out = String::with_capacity(name.len() + 4);
    for (i, &c) in chars.iter().enumerate() {
        if c == '-' {
            out.push('_');
            continue;
        }
        if c.is_ascii_uppercase() && i > 0 {
            let prev = chars[i - 1];
            let next_lower = chars.get(i + 1).is_some_and(char::is_ascii_lowercase);
            if prev.is_ascii_lowercase()
                || prev.is_ascii_digit()
                || (prev.is_ascii_uppercase() && next_lower)
            {
                out.push('_');
            }
        }
        out.push(c.to_ascii_lowercase());
    }
    match out.strip_prefix("variable_") {
        Some(rest) => rest.to_owned(),
        None => out,
    }
}

/// `name` equals `listed`, or ends with `_` + `listed`.
fn matches(name: &str, listed: &str) -> bool {
    name == listed
        || (name.len() > listed.len()
            && name.ends_with(listed)
            && name.as_bytes()[name.len() - listed.len() - 1] == b'_')
}

/// The policy for a field name (normalized first).
pub fn classify(name: &str) -> FieldClass {
    let n = normalize(name);
    if EXEMPT.contains(&n.as_str()) {
        FieldClass::Plain
    } else if n.split('_').any(|t| SECRET_TOKENS.contains(&t))
        || SECRET_NAMES.iter().any(|s| matches(&n, s))
        || ["_secret", "_token", "_password"]
            .iter()
            .any(|s| n.ends_with(s))
    {
        FieldClass::Secret
    } else if PHONE_NAMES.iter().any(|p| matches(&n, p))
        || ["_number", "_phone", "_user"]
            .iter()
            .any(|s| n.ends_with(s))
    {
        FieldClass::Phone
    } else {
        FieldClass::Plain
    }
}

/// Keeps the last 3 ASCII digits behind `***` when there are at least 4; otherwise `***`.
/// The length is hidden, so a masked number reveals neither its country nor its type.
pub fn mask_phone(raw: &str) -> String {
    let digits: Vec<char> = raw.chars().filter(char::is_ascii_digit).collect();
    // Already masked (a `Phone` printed, by Display or Debug, into a phone-named field): keep
    // its last 3 digits rather than masking them again.
    if let Some(at) = raw.find("***") {
        let tail: String = raw[at + 3..]
            .chars()
            .take_while(char::is_ascii_digit)
            .collect();
        if !tail.is_empty() && tail.len() <= 3 && tail.len() == digits.len() {
            return format!("***{tail}");
        }
    }
    if digits.len() >= 4 {
        let tail: String = digits[digits.len() - 3..].iter().collect();
        format!("***{tail}")
    } else {
        "***".to_owned()
    }
}

/// Applies the policy to a value logged under `name`.
pub fn mask_field(name: &str, value: Value) -> Value {
    match classify(name) {
        FieldClass::Plain => mask_nested(value),
        FieldClass::Secret => Value::String(REDACTED.to_owned()),
        FieldClass::Phone => mask_all(value),
    }
}

/// Recurse into objects, applying the policy to every key.
fn mask_nested(value: Value) -> Value {
    match value {
        Value::Object(map) => Value::Object(
            map.into_iter()
                .map(|(k, v)| {
                    let masked = mask_field(&k, v);
                    (k, masked)
                })
                .collect(),
        ),
        Value::Array(items) => Value::Array(items.into_iter().map(mask_nested).collect()),
        other => other,
    }
}

/// Everything under a phone key is masked, scalars and nested values alike.
fn mask_all(value: Value) -> Value {
    match value {
        Value::String(s) => Value::String(mask_phone(&s)),
        Value::Number(n) => Value::String(mask_phone(&n.to_string())),
        Value::Array(items) => Value::Array(items.into_iter().map(mask_all).collect()),
        Value::Object(map) => {
            Value::Object(map.into_iter().map(|(k, v)| (k, mask_all(v))).collect())
        }
        other => other,
    }
}

/// A structured value (a header map, an ESL event, a command body) logged with the policy
/// applied to every key at every depth: `info!(headers = %Masked(&headers), "esl event")`.
pub struct Masked<'a, T: Serialize + ?Sized>(pub &'a T);

impl<T: Serialize + ?Sized> fmt::Display for Masked<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match serde_json::to_value(self.0) {
            Ok(v) => write!(f, "{}", mask_nested(v)),
            Err(_) => f.write_str(REDACTED),
        }
    }
}

/// Why a string isn't an E.164 number.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[error("not an E.164 number")]
pub struct NotE164;

/// An E.164 phone number. `Display` and `Debug` print only the masked form, there is no
/// `Serialize`, and the raw value comes only from [`Phone::expose`].
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Phone(String);

impl Phone {
    /// `^\+[1-9][0-9]{6,14}$`
    pub fn parse_e164(s: &str) -> Result<Self, NotE164> {
        let digits = s.strip_prefix('+').ok_or(NotE164)?;
        let ok = (7..=15).contains(&digits.len())
            && digits.bytes().all(|b| b.is_ascii_digit())
            && !digits.starts_with('0');
        if ok {
            Ok(Self(s.to_owned()))
        } else {
            Err(NotE164)
        }
    }

    /// The raw number, for the places that must send it (a SIP request, a callback).
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Phone {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&mask_phone(&self.0))
    }
}

impl fmt::Debug for Phone {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Phone({})", mask_phone(&self.0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn masking_keeps_only_the_last_three_digits() {
        for (input, out) in [
            ("+447700900123", "***123"),
            ("+44 7700 900123", "***123"),
            ("sip:+447700900123@carrier.example;user=phone", "***123"),
            ("07700900123", "***123"),
            ("1234", "***234"),
            ("***123", "***123"),
            ("Phone(***123)", "***123"),
            ("123", "***"),
            ("", "***"),
            ("anonymous", "***"),
            ("٠١٢٣٤٥٦٧٨٩", "***"),
        ] {
            assert_eq!(mask_phone(input), out, "{input}");
        }
        assert_eq!(mask_phone(&"9".repeat(10_000)), "***999");
    }

    #[test]
    fn camel_case_names_are_split_into_words() {
        for (input, out) in [
            ("callerNumber", "caller_number"),
            ("dtmfDigit", "dtmf_digit"),
            ("DTMFDigit", "dtmf_digit"),
            ("sipCallId", "sip_call_id"),
            ("CallerIDNumber", "caller_id_number"),
            ("e164Number", "e164_number"),
            ("wrappedDek", "wrapped_dek"),
            // Unchanged: snake_case, kebab-case, acronyms on their own, the variable_ prefix.
            ("Caller-Caller-ID-Number", "caller_caller_id_number"),
            (
                "variable_sip_P-Asserted-Identity",
                "sip_p_asserted_identity",
            ),
            ("Caller_ANI", "caller_ani"),
            ("Unique-ID", "unique_id"),
            ("DTMF", "dtmf"),
            ("e164", "e164"),
            ("tenant_id", "tenant_id"),
        ] {
            assert_eq!(normalize(input), out, "{input}");
        }
    }

    #[test]
    fn camel_case_names_get_their_policy() {
        for name in [
            "callerNumber",
            "sipCallId",
            "callbackNumber",
            "fromUser",
            "remotePartyId",
        ] {
            assert_eq!(classify(name), FieldClass::Phone, "{name}");
        }
        for name in [
            "dtmfDigit",
            "DTMFDigit",
            "dtmfDigits",
            "wrappedDek",
            "eslPassword",
            "apiKey",
        ] {
            assert_eq!(classify(name), FieldClass::Secret, "{name}");
        }
        for name in ["tenantId", "interactionId", "numDigits", "replyTo", "legId"] {
            assert_eq!(classify(name), FieldClass::Plain, "{name}");
        }
    }

    #[test]
    fn names_are_normalized_before_the_policy() {
        assert_eq!(
            normalize("Caller-Caller-ID-Number"),
            "caller_caller_id_number"
        );
        assert_eq!(normalize("variable_sip_from_user"), "sip_from_user");
        for name in [
            "to",
            "from",
            "sip_from",
            "Caller-Caller-ID-Number",
            "variable_sip_from_user",
            "variable_sip_P-Asserted-Identity",
            "Diversion",
            "Remote-Party-ID",
            "callback_number",
            "agent_contact",
        ] {
            assert_eq!(classify(name), FieldClass::Phone, "{name}");
        }
        for name in [
            "digits",
            "dtmf",
            "sdp",
            "wrapped_dek",
            "x_api_key",
            "esl_password",
            "refresh_token",
        ] {
            assert_eq!(classify(name), FieldClass::Secret, "{name}");
        }
        for name in [
            "reply_to",
            "num_digits",
            "tenant_id",
            "interaction_id",
            "owner_epoch",
            "state",
            "message",
        ] {
            assert_eq!(classify(name), FieldClass::Plain, "{name}");
        }
    }

    #[test]
    fn structured_values_are_masked_at_every_depth() {
        let v = json!({"headers": {"From": "<sip:+447700900123@x>", "list": [{"to": 447700900123_u64}], "DTMF": "1234"}, "tenant_id": "acme"});
        assert_eq!(
            Masked(&v).to_string(),
            r#"{"headers":{"From":"***123","list":[{"to":"***123"}],"DTMF":"[redacted]"},"tenant_id":"acme"}"#
        );
    }

    #[test]
    fn a_phone_prints_masked_and_parses_only_e164() {
        let p = Phone::parse_e164("+447700900123").unwrap();
        assert_eq!(p.to_string(), "***123");
        assert_eq!(format!("{p:?}"), "Phone(***123)");
        assert_eq!(p.expose(), "+447700900123");
        for bad in ["447700900123", "+0447700900", "+44 7700", "+12345"] {
            assert_eq!(Phone::parse_e164(bad), Err(NotE164), "{bad}");
        }
    }
}
