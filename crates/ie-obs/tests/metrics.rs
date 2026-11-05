//! S0-AC4: the metrics registry: names, the five common labels, the process collector.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use ie_obs::Metrics;
use ie_obs::testing::identity;

#[test]
fn every_series_has_the_common_labels_and_the_prefix() {
    let metrics = Metrics::new(&identity()).unwrap();
    let counter = metrics
        .meter()
        .u64_counter("interaction_engine_example_events")
        .build();
    counter.add(2, &[]);
    let text = metrics.render().unwrap();
    assert!(text.contains("interaction_engine_build_info{"), "{text}");
    assert!(
        text.contains("interaction_engine_example_events_total{"),
        "{text}"
    );
    assert!(text.contains("process_resident_memory_bytes"), "{text}");
    for line in text
        .lines()
        .filter(|l| !l.starts_with('#') && !l.is_empty())
    {
        let name = line.split(['{', ' ']).next().unwrap();
        assert!(
            name.starts_with("interaction_engine_") || name.starts_with("process_"),
            "{line}"
        );
        for label in [
            "service=\"parity-interaction-engine\"",
            "zone=\"local\"",
            "instance=\"test-1\"",
            "version=\"0.0.0-test\"",
            "bin=\"bin-test\"",
        ] {
            assert!(line.contains(label), "{label} missing: {line}");
        }
        assert!(!line.contains("tenant"), "{line}");
    }
}
