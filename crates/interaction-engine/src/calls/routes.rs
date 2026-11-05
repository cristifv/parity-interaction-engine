//! A dialled number's route: its tenant from the number map (parity-store's tenant maps file)
//! and its flow from the flow file, joined and checked at start: a number may only be bound to a
//! flow of its own tenant.

use crate::config::Config;
use crate::tenants::{NumberMap, normalise};
use ie_core::Route;
use ie_core::flow::FlowFile;
use std::collections::HashMap;
use std::sync::Arc;

/// Every bound number's route.
#[derive(Debug, Clone, Default)]
pub struct Routes {
    by_number: HashMap<String, Route>,
    map: NumberMap,
}

impl Routes {
    /// Loads both files named in the configuration.
    pub fn load(cfg: &Config) -> Result<Self, String> {
        let t = cfg.tenants.as_ref().ok_or("no [tenants]")?;
        let f = cfg.flows.as_ref().ok_or("no [flows]")?;
        let allow = t.allow_test_data
            && cfg
                .telephony
                .as_ref()
                .is_some_and(|tel| tel.profile != ie_telephony_client::Profile::Production);
        let map = NumberMap::load(&t.maps_file, allow).map_err(|e| e.to_string())?;
        let text = std::fs::read_to_string(&f.file).map_err(|e| format!("flows.file: {e}"))?;
        let routing = cfg.routing.as_ref().is_some_and(|r| r.enabled);
        let recording = cfg
            .recording
            .as_ref()
            .is_some_and(|r| r.mode == crate::config::RecordingMode::NullSink);
        Ok(Self::build(map, &text)?
            .with_routing(routing)
            .with_recording(recording))
    }

    /// `[recording] mode = "null_sink"`: a granting consent starts recordings (step 6).
    pub fn with_recording(mut self, on: bool) -> Self {
        for r in self.by_number.values_mut() {
            r.recording = on;
        }
        self
    }

    /// `[routing] enabled`: a `dial` to a queue enqueues (step 5a); off, it is skipped.
    pub fn with_routing(mut self, on: bool) -> Self {
        for r in self.by_number.values_mut() {
            r.routing = on;
        }
        self
    }

    /// Joins a number map with a flow file's text.
    pub fn build(map: NumberMap, flow_text: &str) -> Result<Self, String> {
        let file = FlowFile::load(flow_text, |n| map.tenant_of(n)).map_err(|e| e.to_string())?;
        let flows: HashMap<String, Arc<ie_core::Flow>> = file
            .flows
            .iter()
            .map(|(k, v)| (k.clone(), Arc::new(v.clone())))
            .collect();
        let mut by_number = HashMap::new();
        for (number, b) in &file.numbers {
            let (Some(flow), Some(tenant)) = (flows.get(&b.flow), map.tenant_of(number)) else {
                return Err("a bound number lost its flow or tenant".into());
            };
            by_number.insert(
                normalise(number),
                Route {
                    consent: file.consent.get(&tenant).cloned().unwrap_or_default(),
                    tenant_id: tenant,
                    flow_name: b.flow.clone(),
                    flow: Arc::clone(flow),
                    routing: false,
                    recording: false,
                },
            );
        }
        Ok(Self { by_number, map })
    }

    /// The route of a dialled number: None if the number isn't bound to a flow.
    pub fn get(&self, number: &str) -> Option<&Route> {
        self.by_number.get(&normalise(number))
    }

    /// The number map's tenant of a number (bound to a flow or not).
    pub fn tenant_of(&self, number: &str) -> Option<String> {
        self.map.tenant_of(number)
    }

    pub fn len(&self) -> usize {
        self.by_number.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_number.is_empty()
    }

    pub fn test_data(&self) -> bool {
        self.map.is_test_data()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn contracts() -> String {
        std::env::var("PARITY_CONTRACTS_DIR").unwrap_or_else(|_| {
            concat!(env!("CARGO_MANIFEST_DIR"), "/../../../parity-contracts").into()
        })
    }

    fn map() -> NumberMap {
        let text = std::fs::read_to_string(format!("{}/tenants/test-tenants.v1.json", contracts()))
            .expect("parity-contracts 0.4.0's shared test tenants");
        NumberMap::parse(&text, true).unwrap()
    }

    fn demo() -> String {
        std::fs::read_to_string(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../demo/flows.json"
        ))
        .unwrap()
    }

    #[test]
    fn the_demo_flows_bind_acme_numbers_with_acme_s_consent() {
        let r = Routes::build(map(), &demo()).unwrap();
        let main = r.get("+15550100100").unwrap();
        assert_eq!(
            (main.tenant_id.as_str(), main.flow_name.as_str()),
            ("acme", "acme-main")
        );
        assert_eq!(main.consent.mode, ie_core::ConsentMode::AnnounceOptOut);
        assert!(
            r.get("15550100200").is_some(),
            "TC may report the number without its +"
        );
        // globex's number is in the map but bound to no flow: refused (404) at arrival.
        assert!(r.get("+15550101100").is_none());
        assert_eq!(r.tenant_of("+15550101100").as_deref(), Some("globex"));
    }

    #[test]
    fn an_acme_flow_bound_to_a_globex_number_refuses_the_file() {
        let cross = demo().replacen("\"+15550100200\"", "\"+15550101100\"", 1);
        let e = Routes::build(map(), &cross).unwrap_err();
        assert!(e.contains("globex"), "{e}");
    }
}
