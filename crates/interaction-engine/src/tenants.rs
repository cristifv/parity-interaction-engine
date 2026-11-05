//! The number map (step 4): parity-store's tenant maps file (parity-contracts
//! `schemas/tenants/tenant-maps.v1.json`, 0.4.0), read at start. A dialled number's tenant comes
//! from here, never from a SIP header (PLAN "Tenancy"). The rules IE relies on are checked again:
//! the structure (unknown fields refused), the tenant id format, E.164, unique tenants and
//! numbers, no numbers on a parent; a test file (a `test` section) only with `allow_test_data`.

use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::path::Path;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MapsFile {
    schema_version: u32,
    #[serde(default)]
    #[allow(dead_code)]
    description: Option<String>,
    tenants: Vec<Tenant>,
    #[serde(default)]
    test: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Tenant {
    id: String,
    #[allow(dead_code)]
    name: String,
    #[serde(default)]
    #[allow(dead_code)]
    tier: Option<String>,
    #[serde(default)]
    parent: Option<String>,
    #[serde(default)]
    placement: Option<Placement>,
    #[serde(default)]
    numbers: Vec<String>,
    #[serde(default)]
    #[allow(dead_code)]
    sip_domains: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(dead_code)]
struct Placement {
    home_zone: String,
    #[serde(default)]
    failover_zone: Option<String>,
    zone: String,
    cell: String,
    deployment_unit: String,
    #[serde(default)]
    media_track: Option<String>,
}

/// A number's tenant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NumberEntry {
    pub tenant_id: String,
    /// The tenant's deployment unit (bin); checked against the replica's from step 15.
    pub deployment_unit: String,
}

/// E.164 number → its tenant.
#[derive(Debug, Clone, Default)]
pub struct NumberMap {
    by_number: HashMap<String, NumberEntry>,
    test_data: bool,
}

/// Why the maps file is refused (the rule's code, as in CONVENTIONS.md "Tenant maps").
#[derive(Debug, thiserror::Error)]
pub enum MapsError {
    #[error("reading the tenant maps file: {0}")]
    Read(String),
    #[error("tenant maps file: {code} at {at}")]
    Rule { code: &'static str, at: String },
}

fn rule<T>(code: &'static str, at: impl Into<String>) -> Result<T, MapsError> {
    Err(MapsError::Rule {
        code,
        at: at.into(),
    })
}

fn is_tenant_id(s: &str) -> bool {
    let b = s.as_bytes();
    (1..=63).contains(&b.len())
        && b[0].is_ascii_alphanumeric()
        && b[b.len() - 1].is_ascii_alphanumeric()
        && b.iter()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == b'-')
}

/// A dialled number as the map keys it: E.164 with its `+` (TC may report it without).
pub fn normalise(number: &str) -> String {
    if number.starts_with('+') {
        number.to_owned()
    } else {
        format!("+{number}")
    }
}

impl NumberMap {
    /// Reads and checks the file.
    pub fn load(path: &Path, allow_test_data: bool) -> Result<Self, MapsError> {
        let text = std::fs::read_to_string(path).map_err(|e| MapsError::Read(e.to_string()))?;
        Self::parse(&text, allow_test_data)
    }

    /// Checks a document.
    pub fn parse(text: &str, allow_test_data: bool) -> Result<Self, MapsError> {
        let f: MapsFile = serde_json::from_str(text).map_err(|e| MapsError::Rule {
            code: "schema",
            // The position only: serde's message can quote a value (a number is personal data).
            at: format!("line {} column {}", e.line(), e.column()),
        })?;
        if f.schema_version != 1 {
            return rule("schema", "/schema_version");
        }
        if f.test.is_some() && !allow_test_data {
            return rule("test_data_refused", "/test");
        }
        let parents: HashSet<&str> = f
            .tenants
            .iter()
            .filter_map(|t| t.parent.as_deref())
            .collect();
        let mut ids = HashSet::new();
        let mut by_number = HashMap::new();
        for (i, t) in f.tenants.iter().enumerate() {
            if !is_tenant_id(&t.id) {
                return rule("schema", format!("/tenants/{i}/id"));
            }
            if !ids.insert(t.id.as_str()) {
                return rule("duplicate_tenant", format!("/tenants/{i}/id"));
            }
            if parents.contains(t.id.as_str()) && !t.numbers.is_empty() {
                return rule("parent_takes_traffic", format!("/tenants/{i}"));
            }
            for (j, n) in t.numbers.iter().enumerate() {
                if !ie_core::flow::is_e164(n) {
                    return rule("schema", format!("/tenants/{i}/numbers/{j}"));
                }
                let Some(p) = &t.placement else {
                    return rule("missing_placement", format!("/tenants/{i}"));
                };
                let entry = NumberEntry {
                    tenant_id: t.id.clone(),
                    deployment_unit: p.deployment_unit.clone(),
                };
                if by_number.insert(n.clone(), entry).is_some() {
                    return rule("duplicate_number", format!("/tenants/{i}/numbers/{j}"));
                }
            }
        }
        Ok(Self {
            by_number,
            test_data: f.test.is_some(),
        })
    }

    /// The tenant of a dialled number.
    pub fn get(&self, number: &str) -> Option<&NumberEntry> {
        self.by_number.get(&normalise(number))
    }

    /// The tenant id of a number (for the flow file's check).
    pub fn tenant_of(&self, number: &str) -> Option<String> {
        self.get(number).map(|e| e.tenant_id.clone())
    }

    /// Whether the file was test data.
    pub fn is_test_data(&self) -> bool {
        self.test_data
    }

    pub fn len(&self) -> usize {
        self.by_number.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_number.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shared() -> Option<String> {
        let dir = std::env::var("PARITY_CONTRACTS_DIR").unwrap_or_else(|_| {
            concat!(env!("CARGO_MANIFEST_DIR"), "/../../../parity-contracts").into()
        });
        std::fs::read_to_string(format!("{dir}/tenants/test-tenants.v1.json")).ok()
    }

    #[test]
    fn the_shared_test_tenants_load_only_as_test_data() {
        let text = shared().expect("parity-contracts' tenants/test-tenants.v1.json (0.4.0)");
        assert!(matches!(
            NumberMap::parse(&text, false),
            Err(MapsError::Rule {
                code: "test_data_refused",
                ..
            })
        ));
        let m = NumberMap::parse(&text, true).unwrap();
        assert!(m.is_test_data());
        assert_eq!(m.tenant_of("+15550100100").as_deref(), Some("acme"));
        assert_eq!(m.tenant_of("15550101100").as_deref(), Some("globex"));
        assert_eq!(
            m.get("+15550100100").map(|e| e.deployment_unit.as_str()),
            Some("bin-a")
        );
        assert_eq!(m.tenant_of("+15550109999"), None);
    }

    #[test]
    fn a_number_on_two_tenants_or_an_unknown_field_refuses_the_file() {
        let text = shared().unwrap();
        let dup = text.replacen("\"+15550101100\"", "\"+15550100100\"", 1);
        assert!(matches!(
            NumberMap::parse(&dup, true),
            Err(MapsError::Rule {
                code: "duplicate_number",
                ..
            })
        ));
        let extra = text.replacen("\"tier\": \"small\"", "\"tier\": \"small\", \"x\": 1", 1);
        assert!(matches!(
            NumberMap::parse(&extra, true),
            Err(MapsError::Rule { code: "schema", .. })
        ));
    }
}
