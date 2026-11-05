//! The cross-language masking cases (H1): `parity-contracts/masking/cases.v1.json`, which
//! parity-telephony-controller's Java masking must pass too, so the two can't drift. Every case
//! in the file is asserted; a kind this test doesn't know fails it rather than being skipped.
//! The file is read from `$PARITY_CONTRACTS_DIR` (set by `scripts/ci.sh`), else from the
//! sibling `parity-contracts` directory.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use ie_obs::Masked;
use ie_obs::pii::{FieldClass, classify, mask_phone, normalize};
use serde_json::Value;
use std::path::PathBuf;

fn cases() -> (PathBuf, Value) {
    let dir = std::env::var_os("PARITY_CONTRACTS_DIR").map_or_else(
        || PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../parity-contracts"),
        PathBuf::from,
    );
    let path = dir.join("masking/cases.v1.json");
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("the shared masking cases at {}: {e}", path.display()));
    (
        path,
        serde_json::from_str(&text).expect("cases.v1.json is JSON"),
    )
}

fn str_of(v: &Value) -> &str {
    v.as_str().expect("a string")
}

#[test]
fn ie_obs_passes_every_shared_masking_case() {
    let (path, doc) = cases();
    let doc = doc.as_object().expect("an object");
    assert_eq!(
        doc.get("schema_version").and_then(Value::as_u64),
        Some(1),
        "{}",
        path.display()
    );
    let mut failures = Vec::new();
    let mut checked = 0;
    for (kind, body) in doc {
        match kind.as_str() {
            "schema_version" | "description" => {}
            "normalize" | "mask_phone" => {
                for case in body.as_array().expect("a list") {
                    let (input, want) = (str_of(&case["in"]), str_of(&case["out"]));
                    let got = if kind == "normalize" {
                        normalize(input)
                    } else {
                        mask_phone(input)
                    };
                    checked += 1;
                    if got != want {
                        failures.push(format!("{kind}({input:?}) = {got:?}, want {want:?}"));
                    }
                }
            }
            "classify" => {
                for (class, names) in body.as_object().expect("classes") {
                    let want = match class.as_str() {
                        "phone" => FieldClass::Phone,
                        "secret" => FieldClass::Secret,
                        "plain" => FieldClass::Plain,
                        other => panic!("unknown class {other:?} in {}", path.display()),
                    };
                    for name in names.as_array().expect("names") {
                        let got = classify(str_of(name));
                        checked += 1;
                        if got != want {
                            failures.push(format!("classify({name}) = {got:?}, want {want:?}"));
                        }
                    }
                }
            }
            "mask_structured" => {
                // A value logged under a plain name, through Masked (the production path).
                for case in body.as_array().expect("a list") {
                    let got: Value =
                        serde_json::from_str(&Masked(&case["in"]).to_string()).unwrap();
                    checked += 1;
                    if got != case["out"] {
                        failures.push(format!(
                            "mask_structured({}) = {got}, want {}",
                            case["in"], case["out"]
                        ));
                    }
                }
            }
            other => panic!(
                "unknown case kind {other:?} in {}: teach this test about it",
                path.display()
            ),
        }
    }
    assert!(checked > 0, "no cases in {}", path.display());
    assert!(
        failures.is_empty(),
        "{} of {checked} shared cases failed:\n{}",
        failures.len(),
        failures.join("\n")
    );
    println!("{checked} shared masking cases passed ({})", path.display());
}
