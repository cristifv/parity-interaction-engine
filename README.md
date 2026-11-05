# parity-interaction-engine

The brain of each interaction on the contact-center platform (`ARCHITECTURE.md` §3.5): it
runs every call's IVR, queue experience, offers to agents, transfers, conferences,
voicemail and callbacks.
- It controls freeswitch over ESL with epoch-fenced commands.
- It drives parity-ms, which carries all the media.
- It asks routing-engine (parity-poc) for agents.
- It publishes `interaction.events`.
- It survives its own crashes through etcd ownership, `ie.calls` checkpoints and adoption.

CPaaS calls from parity-voice-api run on the same runtime. Rust (tokio); zone, one copy per
deployment bin.

**Status (2026-10-25): step 0 built** (contracts and skeleton). No call logic yet: the binary
starts, reports readiness, drains, logs JSON with masking and serves metrics. See
[`PROGRESS.md`](PROGRESS.md).

- [`PLAN.md`](PLAN.md): the implementation plan (assessment, design, R1–R15, steps 0–18,
  questions). All 18 proposals stand as answers (the user, 2026-10-24).
- [`docs/adr/0001-contracts-and-skeleton.md`](docs/adr/0001-contracts-and-skeleton.md): step
  0's design, revision 2, approved.
- The contracts are in `../parity-contracts/` (`routing.proto`, the event and command
  schemas, `CONVENTIONS.md`, fixtures).

## Running

```
cargo build --locked
IE_CONFIG=config/parity-interaction-engine.example.toml target/debug/parity-interaction-engine
parity-interaction-engine --check-config --config <file>   # validate and exit
```

| | |
|---|---|
| Configuration | One TOML file (`--config` or `IE_CONFIG`); unknown keys refused. Example: `config/parity-interaction-engine.example.toml` |
| Health files | `/tmp/parity-interaction-engine.healthy` once every readiness check passed, removed only when draining starts; `/tmp/parity-interaction-engine.drained` after a successful drain. Both removed at start. `IE_STATE_DIR` moves them, for zone `local` only |
| Management port | 9600: `/livez`, `/readyz` (503 with the failing checks), `/metrics` (Prometheus) |
| Shutdown | `SIGTERM` or `SIGINT`: healthy file removed, a deregistration delay (5 s), drain (45 s budget), drained file, exit |
| Exit codes | 0 drained; 1 the drain ran out of time or a fatal error; 2 a configuration or startup error; 128 + n a second signal n during the drain |
| Logs | JSON lines on stdout. Phone numbers keep only their last 3 digits; digits, SDP and secrets are `[redacted]`. `IE_LOG` overrides the filter |

**Code rules:** log messages are constant text; data goes in fields; phone numbers travel as
`ie_obs::Phone`; structured values are logged through `ie_obs::Masked`. Every crate that logs
runs its paths through `ie_obs::testing` with the canary.

## Checking

```
scripts/tools.sh   # once: cargo-deny 0.20.2, the digest-pinned buf image
scripts/ci.sh      # fmt, clippy -D warnings, tests, cargo-deny, then parity-contracts' CI
```

## Layout

```
crates/ie-obs/              logging, masking, metrics (the shared crate of §5.2)
crates/interaction-engine/  the binary: config, readiness gate, management port, lifecycle
config/                     the example configuration
scripts/                    ci.sh, tools.sh
docs/adr/                   design notes
```
