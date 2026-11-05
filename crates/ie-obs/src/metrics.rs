//! Metrics (ARCHITECTURE.md §7.5; ADR 0001 §7.3): the OpenTelemetry SDK exporting into a
//! Prometheus registry, which also carries the process collector. The five common labels
//! (`service`, `zone`, `instance`, `version`, `bin`) are the registry's constant labels, so
//! every series has them.

use crate::logging::Identity;
use opentelemetry::metrics::{Meter, MeterProvider as _};
use opentelemetry_sdk::metrics::SdkMeterProvider;
use prometheus::{Encoder, Registry, TextEncoder};
use std::collections::HashMap;

/// The content type of `/metrics`.
pub const CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

/// The service's meter and the registry `/metrics` renders.
pub struct Metrics {
    registry: Registry,
    provider: SdkMeterProvider,
    meter: Meter,
}

impl Metrics {
    /// Builds the registry with the common labels, the process collector, the OpenTelemetry
    /// exporter, and `interaction_engine_build_info`.
    pub fn new(identity: &Identity) -> anyhow::Result<Self> {
        let labels: HashMap<String, String> = [
            ("service", &identity.service),
            ("zone", &identity.zone),
            ("instance", &identity.instance),
            ("version", &identity.version),
            ("bin", &identity.bin),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_owned(), v.clone()))
        .collect();
        let registry = Registry::new_custom(None, Some(labels))?;
        registry.register(Box::new(
            prometheus::process_collector::ProcessCollector::for_self(),
        ))?;
        let exporter = opentelemetry_prometheus::exporter()
            .with_registry(registry.clone())
            .without_target_info()
            .scope_info_enabled(false)
            .build()?;
        let provider = SdkMeterProvider::builder().with_reader(exporter).build();
        let meter = provider.meter("parity-interaction-engine");
        meter
            .u64_observable_gauge("interaction_engine_build_info")
            .with_description("Always 1; the version is in the common labels")
            .with_callback(|o| o.observe(1, &[]))
            .build();
        Ok(Self {
            registry,
            provider,
            meter,
        })
    }

    /// The meter every component creates its instruments from.
    pub fn meter(&self) -> &Meter {
        &self.meter
    }

    /// The Prometheus text exposition of everything registered.
    pub fn render(&self) -> anyhow::Result<String> {
        let mut out = Vec::new();
        TextEncoder::new().encode(&self.registry.gather(), &mut out)?;
        Ok(String::from_utf8(out)?)
    }

    /// Flushes and stops the meter provider (at exit).
    pub fn shutdown(&self) {
        let _ = self.provider.shutdown();
    }
}
