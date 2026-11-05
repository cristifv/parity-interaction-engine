//! S0-AC4: a phone number in a log line shows only its last 3 digits, through the production
//! subscriber (ie_obs::testing), whatever the value's type, nesting or field-name style.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use ie_obs::testing::{assert_no_canary, capture_logs};
use ie_obs::{Masked, Phone};
use serde_json::{Value, json};
use std::collections::BTreeMap;

/// Longer runs of the test number's digits than masking allows.
const CANARIES: [&str; 4] = ["7700900123", "447700900", "0900123", "900123"];

fn parse(lines: &[String]) -> Vec<Value> {
    lines
        .iter()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect()
}

#[test]
fn numbers_are_masked_in_every_shape() {
    let phone = Phone::parse_e164("+447700900123").unwrap();
    let mut esl = BTreeMap::new();
    esl.insert("Caller-Caller-ID-Number", "+447700900123");
    esl.insert("variable_sip_from_user", "447700900123");
    esl.insert("Unique-ID", "6f1c2a9e-0000-4000-8000-000000000001");
    let nested = json!({"headers": {"From": "<sip:+447700900123@carrier.example>", "list": [{"to": 447700900123_u64}]}});
    let lines = capture_logs(|| {
        tracing::info!(to = "+447700900123", "a string field");
        tracing::info!(to = 447700900123_u64, "an integer field");
        tracing::info!(caller = %phone, "a Phone as a field");
        tracing::info!("a Phone in the message: {}", phone);
        tracing::info!(callback_number = ?phone, "a Phone through Debug");
        let span = tracing::info_span!("call", from = "+447700900123", tenant_id = "acme");
        let _g = span.enter();
        tracing::warn!(esl = %Masked(&esl), "an ESL-style map");
        tracing::warn!(payload = %Masked(&nested), "a nested value");
        tracing::info!(
            digits = "1234",
            reply_to = "replies.parity-voice-api",
            "secrets and exempt names"
        );
        tracing::info!(Caller_ANI = "+447700900123", "an ESL-style field name");
    });
    assert_eq!(lines.len(), 9, "{lines:#?}");
    assert_no_canary(&lines, &CANARIES).unwrap();
    let v = parse(&lines);
    assert_eq!(v[0]["attrs"]["to"], "***123");
    assert_eq!(v[1]["attrs"]["to"], "***123");
    assert_eq!(v[2]["attrs"]["caller"], "***123");
    assert_eq!(v[3]["msg"], "a Phone in the message: ***123");
    assert_eq!(v[4]["attrs"]["callback_number"], "***123");
    // span fields on the child's lines
    assert_eq!(v[5]["attrs"]["from"], "***123");
    assert_eq!(v[5]["tenant_id"], "acme");
    let esl: Value = serde_json::from_str(v[5]["attrs"]["esl"].as_str().unwrap()).unwrap();
    assert_eq!(esl["Caller-Caller-ID-Number"], "***123");
    assert_eq!(esl["variable_sip_from_user"], "***123");
    assert_eq!(esl["Unique-ID"], "6f1c2a9e-0000-4000-8000-000000000001");
    let nested: Value = serde_json::from_str(v[6]["attrs"]["payload"].as_str().unwrap()).unwrap();
    assert_eq!(nested["headers"]["From"], "***123");
    assert_eq!(nested["headers"]["list"][0]["to"], "***123");
    assert_eq!(v[7]["attrs"]["digits"], "[redacted]");
    assert_eq!(v[7]["attrs"]["reply_to"], "replies.parity-voice-api");
    assert_eq!(v[8]["attrs"]["caller_ani"], "***123");
}

#[test]
fn every_line_has_the_envelope_and_user_fields_cannot_overwrite_it() {
    let lines = capture_logs(|| tracing::error!(level = "spoofed", service = "x", "hello"));
    assert_no_canary(&lines, &CANARIES).unwrap();
    let v = parse(&lines);
    assert_eq!(v[0]["level"], "error");
    assert_eq!(v[0]["service"], "parity-interaction-engine");
    assert_eq!(v[0]["attrs"]["level"], "spoofed");
    assert_eq!(v[0]["attrs"]["service"], "x");
    assert_eq!(
        v[0]["event"], "log.external",
        "a record without an event (not IE's code here)"
    );
    assert_eq!(v[0]["seq"], 1);
    assert_eq!(v[0]["msg"], "hello");
    assert!(v[0]["ts"].as_str().unwrap().ends_with('Z'));
}

#[test]
fn the_canary_check_catches_a_leak() {
    let lines = capture_logs(|| tracing::info!(note = "+447700900123", "an unlisted field name"));
    assert!(
        assert_no_canary(&lines, &CANARIES).is_err(),
        "unlisted names aren't masked; the canary must say so"
    );
}

/// Logs an ESL-style event through `Masked` and the production subscriber; returns the
/// masked map as JSON.
fn esl_line(pairs: &[(&str, &str)]) -> (Vec<String>, Value) {
    let event: BTreeMap<&str, &str> = pairs.iter().copied().collect();
    let lines = capture_logs(|| tracing::info!(esl = %Masked(&event), "esl event"));
    let v = parse(&lines);
    let esl = serde_json::from_str(v[0]["attrs"]["esl"].as_str().unwrap()).unwrap();
    (lines, esl)
}

#[test]
fn dtmf_digits_are_redacted_in_esl_events_and_fields() {
    let (lines, esl) = esl_line(&[
        ("Event-Name", "DTMF"),
        ("DTMF-Digit", "5"),
        ("DTMF-Duration", "2000"),
        ("variable_dtmf_digits", "4111111111111111"),
        ("Unique-ID", "6f1c2a9e-0000-4000-8000-000000000001"),
    ]);
    assert_no_canary(&lines, &["4111111111111111"]).unwrap();
    assert_eq!(esl["DTMF-Digit"], "[redacted]");
    assert_eq!(esl["DTMF-Duration"], "[redacted]");
    assert_eq!(esl["variable_dtmf_digits"], "[redacted]");
    assert_eq!(esl["Event-Name"], "DTMF");
    let lines =
        capture_logs(|| tracing::info!(dtmf_digit = "5", digit = "7", num_digits = 4, "gather"));
    let v = parse(&lines);
    assert_eq!(
        (
            v[0]["attrs"]["dtmf_digit"].as_str(),
            v[0]["attrs"]["digit"].as_str()
        ),
        (Some("[redacted]"), Some("[redacted]"))
    );
    assert_eq!(v[0]["attrs"]["num_digits"], 4, "a count, exempt");
}

#[test]
fn esl_channel_names_are_masked() {
    let (lines, esl) = esl_line(&[
        (
            "Channel-Name",
            "sofia/internal/+447700900123@carrier.example",
        ),
        (
            "Caller-Channel-Name",
            "sofia/internal/+447700900123@carrier.example",
        ),
    ]);
    assert_no_canary(&lines, &CANARIES).unwrap();
    assert_eq!(
        (
            esl["Channel-Name"].as_str(),
            esl["Caller-Channel-Name"].as_str()
        ),
        (Some("***123"), Some("***123"))
    );
}

#[test]
fn esl_caller_names_are_masked() {
    let (lines, esl) = esl_line(&[
        ("Caller-Username", "447700900123"),
        ("Caller-Caller-ID-Name", "+447700900123"),
        ("variable_sip_from_display", "Ann +447700900123"),
    ]);
    assert_no_canary(&lines, &CANARIES).unwrap();
    for k in [
        "Caller-Username",
        "Caller-Caller-ID-Name",
        "variable_sip_from_display",
    ] {
        assert_eq!(esl[k], "***123", "{k}");
    }
}

#[test]
fn esl_redirect_and_dialed_numbers_are_masked() {
    let (lines, esl) = esl_line(&[
        ("Caller-RDNIS", "+447700900123"),
        ("variable_dialed_extension", "447700900123"),
    ]);
    assert_no_canary(&lines, &CANARIES).unwrap();
    assert_eq!(
        (
            esl["Caller-RDNIS"].as_str(),
            esl["variable_dialed_extension"].as_str()
        ),
        (Some("***123"), Some("***123"))
    );
}

#[test]
fn esl_dial_strings_are_masked() {
    let (lines, esl) = esl_line(&[
        (
            "variable_originate_string",
            "{origination_uuid=x}sofia/gateway/carrier/+447700900123",
        ),
        ("dial_string", "sofia/gateway/carrier/+447700900123"),
    ]);
    assert_no_canary(&lines, &CANARIES).unwrap();
    assert_eq!(
        (
            esl["variable_originate_string"].as_str(),
            esl["dial_string"].as_str()
        ),
        (Some("***123"), Some("***123"))
    );
}

#[test]
fn esl_urls_and_call_ids_are_masked() {
    let (lines, esl) = esl_line(&[
        (
            "variable_sip_destination_url",
            "sip:+447700900123@10.0.0.5:5080",
        ),
        ("variable_sip_call_id", "+447700900123-4a5b@carrier.example"),
    ]);
    assert_no_canary(&lines, &CANARIES).unwrap();
    assert!(
        esl["variable_sip_destination_url"]
            .as_str()
            .unwrap()
            .starts_with("***")
    );
    assert!(
        esl["variable_sip_call_id"]
            .as_str()
            .unwrap()
            .starts_with("***")
    );
}

/// H1: camelCase names (a Java-style JSON body, a gRPC field logged as is) get the same policy
/// as their snake_case forms, as fields and inside a structured value.
#[test]
fn camel_case_names_are_masked() {
    let body = serde_json::json!({
        "callerNumber": "+447700900123",
        "sipCallId": "a84b4c76e66710@+447700900123",
        "DTMFDigit": "4111111111111111",
        "dtmfDigit": "4111111111111111",
        "tenantId": "acme",
    });
    let lines = capture_logs(|| {
        tracing::info!(
            callerNumber = "+447700900123",
            dtmfDigit = "4111111111111111",
            "fields"
        );
        tracing::info!(body = %Masked(&body), "a camelCase body");
    });
    assert_no_canary(&lines, &CANARIES).unwrap();
    assert_no_canary(&lines, &["4111111111111111"]).unwrap();
    let v = parse(&lines);
    assert_eq!(v[0]["attrs"]["caller_number"], "***123");
    assert_eq!(v[0]["attrs"]["dtmf_digit"], "[redacted]");
    let b: Value = serde_json::from_str(v[1]["attrs"]["body"].as_str().unwrap()).unwrap();
    assert_eq!(b["callerNumber"], "***123");
    assert_eq!(b["sipCallId"], "***123");
    assert_eq!(b["DTMFDigit"], "[redacted]");
    assert_eq!(b["dtmfDigit"], "[redacted]");
    assert_eq!(b["tenantId"], "acme", "plain");
}
