#!/usr/bin/env bash
# parity-interaction-engine local CI (ADR 0001 §8.1): fmt, clippy -D warnings, tests,
# cargo-deny, then parity-contracts' CI (buf format, lint and breaking in the hardened
# container; contract-check; the JSON Schema break gate), then the log check: the log-record.v1
# records the tests captured (target/test-logs/*.ndjson) against the schema, IE's catalogue
# (logging/events.parity-interaction-engine.v1.json) and the masking rules. Stops at the first
# failure.
#   CI_OFFLINE=1  cargo --offline, cargo-deny without fetching its advisory database
#   PARITY_CONTRACTS_DIR  where parity-contracts is (default: ../parity-contracts); the tests
#                         read its shared masking cases (masking/cases.v1.json) from there too
set -euo pipefail
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
CONTRACTS=${PARITY_CONTRACTS_DIR:-$ROOT/../parity-contracts}
export PARITY_CONTRACTS_DIR=$CONTRACTS
# ADR 0003 R1-10: the contract release IE is built against. Moving it is a deliberate IE change.
EXPECTED_CONTRACTS_VERSION=0.17.1
step() { printf '== %s\n' "$*"; }
die() { printf 'ci: %s\n' "$*" >&2; exit 3; }
cargo_flags=(--locked)
deny_flags=()
if [[ "${CI_OFFLINE:-0}" == 1 ]]; then cargo_flags+=(--offline); deny_flags+=(--disable-fetch); fi
# shellcheck source=/dev/null
source "$CONTRACTS/scripts/tools.env"

step "tools"
[[ "$(cat "$CONTRACTS/VERSION")" == "$EXPECTED_CONTRACTS_VERSION" ]] \
  || die "parity-contracts is $(cat "$CONTRACTS/VERSION"), IE expects $EXPECTED_CONTRACTS_VERSION (scripts/ci.sh)"
# ADR 0003 §5: the telephony scripts run against fake-telephony-controller, TC's jar. Never skipped.
if [[ -z "${PARITY_TC_JAR:-}" ]]; then
  PARITY_TC_JAR=$(ls "$ROOT"/../parity-telephony-controller/target/parity-telephony-controller-*.jar 2>/dev/null | grep -v -- '-plain' | head -1 || true)
fi
[[ -n "$PARITY_TC_JAR" && -f "$PARITY_TC_JAR" ]] \
  || die "fake-telephony-controller's jar is missing: build it (parity-telephony-controller: mvn -q -DskipTests package) or set PARITY_TC_JAR"
export PARITY_TC_JAR
command -v java >/dev/null || die "java (21) is missing: the telephony scripts need it"
command -v cargo >/dev/null || die "cargo is missing"
command -v docker >/dev/null || die "docker is missing"
deny_version=$(cargo deny --version 2>/dev/null || true)
[[ "$deny_version" == "cargo-deny $CARGO_DENY_VERSION" ]] \
  || die "need cargo-deny $CARGO_DENY_VERSION, found '${deny_version:-none}': run scripts/tools.sh"
echo "$(cd "$ROOT" && rustc --version); $deny_version"

cd "$ROOT"
step "fmt"
cargo fmt --all -- --check
step "clippy"
cargo clippy "${cargo_flags[@]}" --workspace --all-targets -- -D warnings
step "test"
# H1: ie-obs must pass every cross-language masking case (Rust and Java can't drift).
[[ -f "$CONTRACTS/masking/cases.v1.json" ]] || die "missing $CONTRACTS/masking/cases.v1.json"
# The tests write what IE logs (the production formatter) here; checked in the "logs" step.
export IE_TEST_LOG_DIR="$ROOT/target/test-logs"
mkdir -p "$IE_TEST_LOG_DIR"
find "$IE_TEST_LOG_DIR" -maxdepth 1 -name '*.ndjson' -type f -delete
cargo test "${cargo_flags[@]}" --workspace
step "deny"
cargo deny --locked check --hide-inclusion-graph "${deny_flags[@]}" advisories bans licenses sources
step "contracts"
# G3: IE's CI never passes a breaking-change bypass on, whatever the caller's environment.
env -u CI_ALLOW_BREAK -u PARITY_RELEASE_TOKEN "$CONTRACTS/scripts/ci.sh"
step "logs (log-record.v1)"
# contract-check as the contracts CI just built it.
CC="$CONTRACTS/rust/target/debug/contract-check"
[[ -x "$CC" ]] || die "contract-check is missing at $CC"
mapfile -t captured < <(find "$IE_TEST_LOG_DIR" -maxdepth 1 -name '*.ndjson' -type f -size +0 | sort)
(( ${#captured[@]} >= 3 )) || die "the tests captured only ${#captured[@]} log files in $IE_TEST_LOG_DIR"
log_args=()
for f in "${captured[@]}"; do log_args+=(--logs "$f"); done
"$CC" --root "$CONTRACTS" --catalogue "$ROOT/logging/events.parity-interaction-engine.v1.json" \
  "${log_args[@]}" --skip-non-json
step "parity-interaction-engine ok"
