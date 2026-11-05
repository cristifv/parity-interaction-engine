//! The configuration file (ADR 0001 §5): TOML, unknown keys refused, validated at start.

use ie_telephony_client::{ClientConfig, NodeConfig, Profile, Timing};
use serde::Deserialize;
use std::net::SocketAddr;
use std::path::Path;

/// Why the configuration can't be used. Startup exits 2 on any of these.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("reading {path}: {source}")]
    Read {
        path: String,
        source: std::io::Error,
    },
    #[error("parsing {path}: {source}")]
    Parse {
        path: String,
        source: toml::de::Error,
    },
    #[error("{0}")]
    Invalid(String),
}

/// The whole file.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub service: ServiceConfig,
    #[serde(default)]
    pub management: ManagementConfig,
    #[serde(default)]
    pub lifecycle: LifecycleConfig,
    #[serde(default)]
    pub logging: LoggingConfig,
    /// The media nodes' TCs (step 3). Absent: no call path (the step-0 process only).
    #[serde(default)]
    pub telephony: Option<TelephonyConfig>,
    /// etcd, for the STEP-3 MINIMAL owner claim. Required with `telephony`.
    #[serde(default)]
    pub etcd: Option<EtcdConfig>,
    /// The number map: parity-store's tenant maps file (step 4; step 10 reads it from parity-store).
    #[serde(default)]
    pub tenants: Option<TenantsConfig>,
    /// The static flow file: flows, the numbers bound to them, consent (step 4).
    #[serde(default)]
    pub flows: Option<FlowsConfig>,
    /// The CDR database (step 4).
    #[serde(default)]
    pub cdr: Option<CdrConfig>,
    /// routing-engine (step 5a). Absent or off: a `dial` to a queue is skipped and counted.
    #[serde(default)]
    pub routing: Option<RoutingConfig>,
    /// Recordings (step 6). Absent: off.
    #[serde(default)]
    pub recording: Option<RecordingConfig>,
    /// The agents' legs (step 6): their DTMF action codes.
    #[serde(default)]
    pub agents: AgentsConfig,
}

/// Recordings. Until step 9's keys (R9) no audio may be written (MS-012): `null_sink` starts
/// recordings on TC's null sink (state and events only, nothing written; TC's local profiles),
/// with a throwaway data key and kek id `local-null-sink`. Refused in production.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordingConfig {
    #[serde(default)]
    pub mode: RecordingMode,
}

/// See [`RecordingConfig`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecordingMode {
    /// A granting consent is counted; nothing is started.
    #[default]
    Off,
    /// StartRecording on TC's null sink (local profiles).
    NullSink,
}

/// An agent leg's DTMF action codes (P5; tenant configuration in step 10, these are the platform
/// defaults: `*1` pause, `*2` resume, `*3` mute toggle). The codes are digits: never logged.
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentsConfig {
    #[serde(default = "default_dtmf_actions")]
    pub dtmf_actions: Vec<DtmfBinding>,
    /// Between a code's digits (TC's default 2000).
    #[serde(default = "default_inter_digit_ms")]
    pub dtmf_inter_digit_ms: u32,
}

impl Default for AgentsConfig {
    fn default() -> Self {
        Self {
            dtmf_actions: default_dtmf_actions(),
            dtmf_inter_digit_ms: default_inter_digit_ms(),
        }
    }
}

impl std::fmt::Debug for AgentsConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The codes are digits (PCI rule: never printed).
        f.debug_struct("AgentsConfig")
            .field("dtmf_actions", &self.dtmf_actions.len())
            .field("dtmf_inter_digit_ms", &self.dtmf_inter_digit_ms)
            .finish()
    }
}

/// One action code.
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DtmfBinding {
    /// `^\*[0-9#A-D]{1,7}$`.
    pub sequence: String,
    pub action: AgentDtmfAction,
}

/// What an agent's code may do (P5's agent role).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentDtmfAction {
    RecordingPause,
    RecordingResume,
    RecordingToggle,
    Mute,
    Unmute,
    MuteToggle,
}

fn default_dtmf_actions() -> Vec<DtmfBinding> {
    [
        ("*1", AgentDtmfAction::RecordingPause),
        ("*2", AgentDtmfAction::RecordingResume),
        ("*3", AgentDtmfAction::MuteToggle),
    ]
    .into_iter()
    .map(|(s, a)| DtmfBinding {
        sequence: s.to_owned(),
        action: a,
    })
    .collect()
}

fn default_inter_digit_ms() -> u32 {
    2000
}

impl AgentsConfig {
    fn validate(&self) -> Result<(), String> {
        if self.dtmf_actions.len() > 8 || !(500..=10_000).contains(&self.dtmf_inter_digit_ms) {
            return Err("agents: at most 8 dtmf_actions; dtmf_inter_digit_ms 500-10000".into());
        }
        let ok = |s: &str| {
            (2..=8).contains(&s.len())
                && s.starts_with('*')
                && s[1..]
                    .bytes()
                    .all(|b| b.is_ascii_digit() || matches!(b, b'#' | b'A'..=b'D'))
        };
        for (i, a) in self.dtmf_actions.iter().enumerate() {
            if !ok(&a.sequence) {
                return Err(format!(
                    "agents.dtmf_actions[{i}]: a sequence is * then 1-7 of 0-9 # A-D"
                ));
            }
            for (j, b) in self.dtmf_actions.iter().enumerate() {
                if i != j && b.sequence.starts_with(&a.sequence) {
                    return Err(format!(
                        "agents.dtmf_actions[{i}] is a prefix of [{j}] (or equal)"
                    ));
                }
            }
        }
        Ok(())
    }
}

/// routing-engine's client (step 5a). **Off by default, and refused in production** until a
/// routing engine exists (parity-cc-routing, step 5b) and IE places the agent's leg (step 6):
/// until then the agent's leg is a stub.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoutingConfig {
    #[serde(default)]
    pub enabled: bool,
    /// `http://host:port` of the bin's routing engine.
    #[serde(default)]
    pub endpoint: String,
    /// IE-059: every instance of the bin's routing engine (its leader and standbys), when it
    /// runs HA: requests and the stream follow the leader (`parity-leader`). Joined with
    /// `endpoint`; one of the two is required.
    #[serde(default)]
    pub endpoints: Vec<String>,
    /// Per try (CONVENTIONS: 300 ms).
    #[serde(default = "default_routing_rpc_ms")]
    pub rpc_timeout_ms: u64,
    /// How long a queued caller's `Enqueue` is retried while RE is unreachable (60 s).
    #[serde(default = "default_enqueue_budget_s")]
    pub enqueue_budget_s: u64,
}

fn default_routing_rpc_ms() -> u64 {
    300
}

fn default_enqueue_budget_s() -> u64 {
    60
}

impl RoutingConfig {
    /// The client's configuration for this replica.
    pub fn client_config(&self, replica_id: &str, bin: &str) -> ie_routing_client::Config {
        let mut all: Vec<String> = Vec::new();
        for e in std::iter::once(&self.endpoint).chain(&self.endpoints) {
            if !e.is_empty() && !all.contains(e) {
                all.push(e.clone());
            }
        }
        let mut c = ie_routing_client::Config::with_endpoints(&all, replica_id, bin);
        c.rpc_timeout = std::time::Duration::from_millis(self.rpc_timeout_ms);
        c.enqueue_budget = std::time::Duration::from_secs(self.enqueue_budget_s);
        c
    }
}

/// The tenant maps file (parity-contracts `schemas/tenants/tenant-maps.v1.json`).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TenantsConfig {
    pub maps_file: std::path::PathBuf,
    /// Accept a file with a `test` section (the shared test tenants); never in production.
    #[serde(default)]
    pub allow_test_data: bool,
}

/// The flow file.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FlowsConfig {
    pub file: std::path::PathBuf,
}

/// Postgres for the CDR.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CdrConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub dbname: String,
    /// A file holding the password (mode 600); never in this file.
    pub password_file: std::path::PathBuf,
    /// Apply `migrations/0001_cdr.sql` at start (dev, demo and test profiles only).
    #[serde(default)]
    pub migrate: bool,
    /// The writer's queue, in records.
    #[serde(default = "default_cdr_queue")]
    pub queue: usize,
}

fn default_cdr_queue() -> usize {
    10_000
}

/// The telephony client's settings; the replica and the bin come from `[service]`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TelephonyConfig {
    /// `dev`, `demo` or `test` until step 8 (the ENDED records aren't durable before it).
    pub profile: Profile,
    /// The media nodes (static in v1).
    pub nodes: Vec<NodeConfig>,
    /// Sends `x-parity-test-peer` until TC has mTLS (TC step 8). Never in production.
    #[serde(default)]
    pub test_peer_header: bool,
    /// TC's ADR timings.
    #[serde(default)]
    pub timing: Timing,
}

impl TelephonyConfig {
    /// The client's configuration for this replica.
    pub fn client_config(&self, replica_id: &str, bin: &str) -> ClientConfig {
        ClientConfig {
            replica_id: replica_id.to_owned(),
            bin: bin.to_owned(),
            profile: self.profile,
            nodes: self.nodes.clone(),
            test_peer_header: self.test_peer_header,
            timing: self.timing.clone(),
        }
    }
}

/// etcd (STEP-3 MINIMAL ownership).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EtcdConfig {
    /// `http://host:port` of etcd's client port.
    pub endpoint: String,
    /// The replica's lease; its keys go this long after the process dies.
    #[serde(default = "default_lease_ttl_s")]
    pub lease_ttl_s: i64,
    /// Each request's timeout.
    #[serde(default = "default_etcd_timeout_ms")]
    pub timeout_ms: u64,
}

fn default_lease_ttl_s() -> i64 {
    10
}

fn default_etcd_timeout_ms() -> u64 {
    1_000
}

/// Who this replica is.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceConfig {
    /// `^[a-z0-9-]{1,32}$`; `local` for development and tests.
    pub zone: String,
    /// `^[a-z0-9-]{1,32}$`
    pub bin: String,
    /// Defaults to `$HOSTNAME`.
    #[serde(default)]
    pub replica_id: Option<String>,
}

/// The management port: `/livez`, `/readyz`, `/metrics`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagementConfig {
    pub listen: SocketAddr,
}

impl Default for ManagementConfig {
    fn default() -> Self {
        Self {
            listen: SocketAddr::from(([127, 0, 0, 1], 9600)),
        }
    }
}

/// ARCHITECTURE.md §7.3's timings.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LifecycleConfig {
    /// Step 2 of the shutdown contract; at most 10 000.
    pub deregistration_delay_ms: u64,
    /// From the signal to the end of step 3; at most 50 000 (the grace is 60 s).
    pub drain_timeout_ms: u64,
}

impl Default for LifecycleConfig {
    fn default() -> Self {
        Self {
            deregistration_delay_ms: 5_000,
            drain_timeout_ms: 45_000,
        }
    }
}

/// Logging.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LoggingConfig {
    /// `EnvFilter` directives; `IE_LOG` overrides.
    pub filter: String,
    /// The non-blocking writer's queue, in lines.
    pub buffer_lines: usize,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            filter: "info".into(),
            buffer_lines: 65_536,
        }
    }
}

fn is_label(s: &str) -> bool {
    (1..=32).contains(&s.len())
        && s.bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
}

fn is_replica_id(s: &str) -> bool {
    let b = s.as_bytes();
    (1..=64).contains(&b.len())
        && b[0].is_ascii_alphanumeric()
        && b.iter()
            .all(|c| c.is_ascii_alphanumeric() || b"._-".contains(c))
}

impl Config {
    /// Reads, parses and validates `path`.
    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        let shown = path.display().to_string();
        let text = std::fs::read_to_string(path).map_err(|source| ConfigError::Read {
            path: shown.clone(),
            source,
        })?;
        Self::parse(&text, &shown)
    }

    /// Parses and validates a document.
    pub fn parse(text: &str, shown: &str) -> Result<Self, ConfigError> {
        let mut cfg: Config = toml::from_str(text).map_err(|source| ConfigError::Parse {
            path: shown.to_owned(),
            source,
        })?;
        if cfg.service.replica_id.is_none() {
            cfg.service.replica_id = std::env::var("HOSTNAME").ok().filter(|h| !h.is_empty());
        }
        cfg.validate()?;
        Ok(cfg)
    }

    fn validate(&self) -> Result<(), ConfigError> {
        let bad = |m: String| Err(ConfigError::Invalid(m));
        if !is_label(&self.service.zone) {
            return bad(format!(
                "service.zone {:?} must match ^[a-z0-9-]{{1,32}}$",
                self.service.zone
            ));
        }
        if !is_label(&self.service.bin) {
            return bad(format!(
                "service.bin {:?} must match ^[a-z0-9-]{{1,32}}$",
                self.service.bin
            ));
        }
        match &self.service.replica_id {
            None => return bad("service.replica_id is unset and $HOSTNAME is empty".into()),
            Some(r) if !is_replica_id(r) => {
                return bad(format!(
                    "service.replica_id {r:?} must match ^[A-Za-z0-9][A-Za-z0-9._-]{{0,63}}$"
                ));
            }
            Some(_) => {}
        }
        let l = &self.lifecycle;
        if l.deregistration_delay_ms > 10_000 {
            return bad("lifecycle.deregistration_delay_ms must be at most 10000".into());
        }
        if l.drain_timeout_ms > 50_000 {
            return bad(
                "lifecycle.drain_timeout_ms must be at most 50000 (the grace period is 60 s)"
                    .into(),
            );
        }
        if l.drain_timeout_ms <= l.deregistration_delay_ms {
            return bad(
                "lifecycle.drain_timeout_ms must be longer than deregistration_delay_ms".into(),
            );
        }
        if !(1..=1_000_000).contains(&self.logging.buffer_lines) {
            return bad("logging.buffer_lines must be between 1 and 1000000".into());
        }
        self.validate_calls()
    }

    fn validate_calls(&self) -> Result<(), ConfigError> {
        let bad = |m: String| Err(ConfigError::Invalid(m));
        let Some(tel) = &self.telephony else {
            if self.etcd.is_some()
                || self.tenants.is_some()
                || self.flows.is_some()
                || self.cdr.is_some()
                || self.routing.is_some()
            {
                return bad("etcd, tenants, flows, cdr and routing need [telephony]".into());
            }
            return Ok(());
        };
        if tel.profile == Profile::Production {
            return bad(
                "telephony.profile production is refused until step 8 (ENDED records aren't durable)"
                    .into(),
            );
        }
        if let Err(e) = tel
            .client_config(self.replica_id(), &self.service.bin)
            .validate()
        {
            return bad(e.to_string());
        }
        let Some(etcd) = &self.etcd else {
            return bad("[telephony] needs [etcd] (the owner claim TC verifies)".into());
        };
        if !etcd.endpoint.starts_with("http://") && !etcd.endpoint.starts_with("https://") {
            return bad("etcd.endpoint must be an http:// or https:// URI".into());
        }
        if !(5..=60).contains(&etcd.lease_ttl_s) || !(50..=10_000).contains(&etcd.timeout_ms) {
            return bad("etcd.lease_ttl_s must be 5-60 and etcd.timeout_ms 50-10000".into());
        }
        if self.tenants.is_none() || self.flows.is_none() || self.cdr.is_none() {
            return bad("[telephony] needs [tenants], [flows] and [cdr]".into());
        }
        if let Some(r) = self.routing.as_ref().filter(|r| r.enabled) {
            // Phase 1's routing engine is local-only (parity-cc-routing refuses other envs).
            if tel.profile == Profile::Production {
                return bad(
                    "routing.enabled is refused in production (routing-engine phase 1)".into(),
                );
            }
            if let Err(e) = r
                .client_config(self.replica_id(), &self.service.bin)
                .validate()
            {
                return bad(e.to_string());
            }
        }
        if self
            .recording
            .as_ref()
            .is_some_and(|r| r.mode == RecordingMode::NullSink)
            && tel.profile == Profile::Production
        {
            return bad("recording.mode null_sink is for the dev, demo and test profiles".into());
        }
        if let Err(e) = self.agents.validate() {
            return bad(e);
        }
        if let Some(c) = &self.cdr
            && (c.host.is_empty()
                || c.user.is_empty()
                || c.dbname.is_empty()
                || c.port == 0
                || !(1..=1_000_000).contains(&c.queue))
        {
            return bad("cdr: host, port, user and dbname are required; queue 1-1000000".into());
        }
        Ok(())
    }

    /// The replica id (validated to be set).
    pub fn replica_id(&self) -> &str {
        self.service.replica_id.as_deref().unwrap_or("unknown")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const OK: &str = r#"
[service]
zone = "local"
bin = "bin-a"
replica_id = "ie-1"
"#;

    #[test]
    fn defaults_fill_the_rest() {
        let c = Config::parse(OK, "t").unwrap();
        assert_eq!(
            c.management.listen,
            SocketAddr::from(([127, 0, 0, 1], 9600))
        );
        assert_eq!(c.lifecycle.drain_timeout_ms, 45_000);
        assert_eq!(c.logging.filter, "info");
    }

    const CALLS: &str = r#"
[telephony]
profile = "demo"
test_peer_header = true
[[telephony.nodes]]
id = "media-1"
grpc = "http://127.0.0.1:9700"

[etcd]
endpoint = "http://127.0.0.1:2379"

[tenants]
maps_file = "/x/test-tenants.v1.json"
allow_test_data = true

[flows]
file = "/x/flows.json"

[cdr]
host = "127.0.0.1"
port = 5432
user = "parity_ie"
dbname = "parity_interaction_engine"
password_file = "/x/pg-password"
migrate = true
"#;

    #[test]
    fn the_call_path_parses_and_checks_its_rules() {
        let c = Config::parse(&format!("{OK}{CALLS}"), "t").unwrap();
        assert_eq!(c.etcd.as_ref().map(|e| e.lease_ttl_s), Some(10));
        assert_eq!(c.cdr.as_ref().map(|d| d.queue), Some(10_000));
        for (from, to) in [
            (r#"profile = "demo""#, r#"profile = "production""#),
            ("[etcd]\nendpoint = \"http://127.0.0.1:2379\"\n", ""),
            ("[flows]\nfile = \"/x/flows.json\"\n", ""),
            (r#"port = 5432"#, r#"port = 0"#),
        ] {
            let text = format!("{OK}{}", CALLS.replace(from, to));
            assert!(
                matches!(Config::parse(&text, "t"), Err(ConfigError::Invalid(_))),
                "{to:?}"
            );
        }
        let no_telephony = format!("{OK}[flows]\nfile = \"/x\"\n");
        assert!(Config::parse(&no_telephony, "t").is_err());
    }

    #[test]
    fn routing_is_off_by_default_and_checked_when_on() {
        let c = Config::parse(&format!("{OK}{CALLS}"), "t").unwrap();
        assert!(c.routing.is_none());
        let on = "[routing]\nenabled = true\nendpoint = \"http://127.0.0.1:9800\"\n";
        let c = Config::parse(&format!("{OK}{CALLS}{on}"), "t").unwrap();
        let r = c.routing.unwrap();
        assert_eq!(
            (r.enabled, r.rpc_timeout_ms, r.enqueue_budget_s),
            (true, 300, 60)
        );
        for bad in [
            "[routing]\nenabled = true\n",
            "[routing]\nenabled = true\nendpoint = \"https://re:9800\"\n",
            "[routing]\nenabled = true\nendpoint = \"http://re:9800\"\nrpc_timeout_ms = 0\n",
        ] {
            assert!(
                matches!(
                    Config::parse(&format!("{OK}{CALLS}{bad}"), "t"),
                    Err(ConfigError::Invalid(_))
                ),
                "{bad:?}"
            );
        }
        // Off, nothing is checked; without [telephony], refused.
        assert!(Config::parse(&format!("{OK}{CALLS}[routing]\nenabled = false\n"), "t").is_ok());
        assert!(Config::parse(&format!("{OK}{on}"), "t").is_err());
    }

    #[test]
    fn recording_is_off_by_default_and_agents_get_the_platform_codes() {
        let c = Config::parse(&format!("{OK}{CALLS}"), "t").unwrap();
        assert!(c.recording.is_none());
        assert_eq!(c.agents.dtmf_actions.len(), 3);
        assert_eq!(c.agents.dtmf_inter_digit_ms, 2000);
        // The codes are digits: never in a Debug print of the configuration.
        assert!(!format!("{c:?}").contains("*1"));
        let c = Config::parse(
            &format!("{OK}{CALLS}[recording]\nmode = \"null_sink\"\n"),
            "t",
        )
        .unwrap();
        assert_eq!(c.recording.unwrap().mode, RecordingMode::NullSink);
        for bad in [
            "[recording]\nmode = \"clear\"\n",
            "[agents]\ndtmf_actions = [{sequence = \"1\", action = \"mute\"}]\n",
            "[agents]\ndtmf_actions = [{sequence = \"*1\", action = \"mute\"}, {sequence = \"*12\", action = \"unmute\"}]\n",
            "[agents]\ndtmf_actions = [{sequence = \"*1\", action = \"barge\"}]\n",
            "[agents]\ndtmf_inter_digit_ms = 0\n",
        ] {
            assert!(
                Config::parse(&format!("{OK}{CALLS}{bad}"), "t").is_err(),
                "{bad:?}"
            );
        }
    }

    #[test]
    fn unknown_keys_and_bad_values_are_refused() {
        assert!(matches!(
            Config::parse(&format!("{OK}typo = 1\n"), "t"),
            Err(ConfigError::Parse { .. })
        ));
        let bad_zone = OK.replace("local", "Local");
        assert!(matches!(
            Config::parse(&bad_zone, "t"),
            Err(ConfigError::Invalid(_))
        ));
        let long_drain =
            format!("{OK}[lifecycle]\nderegistration_delay_ms = 5000\ndrain_timeout_ms = 55000\n");
        assert!(matches!(
            Config::parse(&long_drain, "t"),
            Err(ConfigError::Invalid(_))
        ));
    }
}
