//! `parity-interaction-engine [--config <file>] [--check-config]`
//!
//! Exit codes: 0 drained; 1 the drain ran out of time, or a fatal error; 2 a configuration or
//! startup error; 128 + n a second signal n during the drain.

use clap::Parser;
use ie_obs::{Identity, LogConfig, Metrics};
use interaction_engine::config::Config;
use interaction_engine::files::{DEFAULT_DIR, SERVICE, StateFiles};
use interaction_engine::lifecycle::{self, Outcome, Signals};
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

#[derive(Parser)]
#[command(name = "parity-interaction-engine", version)]
struct Cli {
    /// The configuration file.
    #[arg(
        long,
        env = "IE_CONFIG",
        default_value = "/etc/parity-interaction-engine/config.toml"
    )]
    config: PathBuf,
    /// Load and validate the configuration, print the result, and exit.
    #[arg(long)]
    check_config: bool,
}

/// One JSON line on stderr, for errors before logging exists.
#[allow(clippy::print_stderr)]
fn startup_error(message: &str, error: &str) -> ExitCode {
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    let line = serde_json::json!({
        "ts_ms": ts, "level": "ERROR", "target": "parity_interaction_engine",
        "message": message, "error": error, "service": SERVICE,
    });
    eprintln!("{line}");
    ExitCode::from(2)
}

/// Loads and validates the configuration; `IE_STATE_DIR` is for zone `local` only (F13).
fn load_config(cli: &Cli, state_override: bool) -> Result<Config, ExitCode> {
    let cfg = Config::load(&cli.config)
        .map_err(|e| startup_error("invalid configuration", &e.to_string()))?;
    if state_override && cfg.service.zone != "local" {
        return Err(startup_error(
            "IE_STATE_DIR is for zone local only",
            &format!(
                "IE_STATE_DIR is set and service.zone is {:?}",
                cfg.service.zone
            ),
        ));
    }
    Ok(cfg)
}

fn main() -> ExitCode {
    // The command line first (F-B): `--help`, `--version` and `--check-config` are run inside
    // live containers, so they must never touch the serving process's healthy file.
    let cli = match Cli::try_parse() {
        Ok(c) => c,
        Err(e) if !e.use_stderr() => {
            let _ = e.print();
            return ExitCode::SUCCESS;
        }
        Err(e) => return startup_error("bad arguments", &e.to_string()),
    };
    let state_override = std::env::var_os("IE_STATE_DIR").map(PathBuf::from);
    if cli.check_config {
        return match load_config(&cli, state_override.is_some()) {
            Ok(_) => {
                #[allow(clippy::print_stdout)]
                {
                    println!(
                        "{}",
                        serde_json::json!({"config": "ok", "path": cli.config.display().to_string()})
                    );
                }
                ExitCode::SUCCESS
            }
            Err(code) => code,
        };
    }
    // Serving: §7.3's stale files are removed before the configuration is parsed, so even an
    // invalid configuration can't leave one behind.
    for dir in std::iter::once(PathBuf::from(DEFAULT_DIR)).chain(state_override.clone()) {
        if let Err(e) = StateFiles::new(&dir).remove_stale() {
            return startup_error(
                "can't remove the stale healthy and drained files",
                &format!("{}: {e}", dir.display()),
            );
        }
    }
    let cfg = match load_config(&cli, state_override.is_some()) {
        Ok(c) => c,
        Err(code) => return code,
    };
    let files = StateFiles::new(state_override.unwrap_or_else(|| PathBuf::from(DEFAULT_DIR)));
    let identity = Identity {
        service: SERVICE.into(),
        version: env!("CARGO_PKG_VERSION").into(),
        zone: cfg.service.zone.clone(),
        bin: cfg.service.bin.clone(),
        instance: cfg.replica_id().to_owned(),
    };
    let guard = match ie_obs::logging::init(&LogConfig {
        identity: identity.clone(),
        filter: cfg.logging.filter.clone(),
        buffer_lines: cfg.logging.buffer_lines,
    }) {
        Ok(g) => g,
        Err(e) => return startup_error("can't start logging", &format!("{e:#}")),
    };
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(r) => r,
        Err(e) => return startup_error("can't start the runtime", &e.to_string()),
    };
    let outcome = runtime.block_on(async {
        // Signals first, so a signal during startup takes the drain path.
        let signals = match Signals::install() {
            Ok(s) => s,
            Err(e) => return Outcome::Startup(format!("can't install signal handlers: {e}")),
        };
        let metrics = match Metrics::new(&identity) {
            Ok(m) => Arc::new(m),
            Err(e) => return Outcome::Startup(format!("can't start metrics: {e:#}")),
        };
        let dropped = guard.dropped_counter();
        metrics
            .meter()
            .u64_observable_counter("interaction_engine_log_lines_dropped")
            .with_description("Log lines dropped because the non-blocking writer's queue was full")
            .with_callback(move |o| o.observe(dropped.dropped_lines() as u64, &[]))
            .build();
        // log-record.v1's `log.dropped`: at most every 10 s while records are being dropped.
        let reporter = guard.dropped_counter();
        tokio::spawn(async move {
            let mut seen = 0usize;
            let mut tick = tokio::time::interval(Duration::from_secs(10));
            loop {
                tick.tick().await;
                let now = reporter.dropped_lines();
                if now > seen {
                    tracing::warn!(
                        event = "log.dropped",
                        dropped = now - seen,
                        "log records were dropped: the log queue was full"
                    );
                    seen = now;
                }
            }
        });
        tracing::info!(event = "service.starting",  listen = %cfg.management.listen, "starting");
        let outcome = lifecycle::run(&cfg, files, metrics.clone(), signals).await;
        metrics.shutdown();
        outcome
    });
    let code = outcome.exit_code();
    match &outcome {
        Outcome::Drained => tracing::info!(event = "service.stopped", exit_code = code, "exiting"),
        other => {
            tracing::error!(event = "service.stopped",  exit_code = code, outcome = ?other, "exiting")
        }
    }
    runtime.shutdown_timeout(Duration::from_secs(1));
    drop(guard);
    ExitCode::from(code)
}
