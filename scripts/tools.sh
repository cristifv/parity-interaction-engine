#!/usr/bin/env bash
# Installs what parity-interaction-engine's CI needs (ADR 0001 §8.1): cargo-deny 0.20.2 into
# ~/.cargo/bin, the toolchain of rust-toolchain.toml, and parity-contracts' tools (the
# digest-pinned buf image). Never uses sudo, never touches another repository.
set -euo pipefail
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
CONTRACTS=${PARITY_CONTRACTS_DIR:-$ROOT/../parity-contracts}
(cd "$ROOT" && rustup show active-toolchain >/dev/null)
"$CONTRACTS/scripts/tools.sh"
