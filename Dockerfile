# parity-interaction-engine's image (parity-deploy PLAN.md §2.1, P1a).
# The build context is the WORKSPACE ROOT (parity-proto is a path dependency on
# ../parity-contracts until there is git); Dockerfile.dockerignore next to this file limits it:
#   docker build -f parity-interaction-engine/Dockerfile -t localhost/parity-interaction-engine:dev .
# Bases pinned by digest; each was at least 14 days old when pinned (2026-10-27).
FROM docker.io/library/rust:1.95.0-slim-trixie@sha256:e14e87345b4d5964ddcc3491d27ee046a0f23820f340c3c1e24da6880141f7c0 AS build
WORKDIR /src
COPY parity-contracts/rust ./parity-contracts/rust
COPY parity-contracts/proto ./parity-contracts/proto
COPY parity-interaction-engine/Cargo.toml parity-interaction-engine/Cargo.lock parity-interaction-engine/clippy.toml ./parity-interaction-engine/
COPY parity-interaction-engine/crates ./parity-interaction-engine/crates
# cdr.rs embeds migrations/0001_cdr.sql (include_str!, PL-050).
COPY parity-interaction-engine/migrations ./parity-interaction-engine/migrations
WORKDIR /src/parity-interaction-engine
RUN cargo build --release --locked -p interaction-engine

FROM docker.io/library/fedora@sha256:a651ddf48ea28a06ed4e1e6519f51c9f47e7a5a138722ade87369b8fbb7e5b42
COPY --from=build /src/parity-interaction-engine/target/release/parity-interaction-engine /usr/local/bin/parity-interaction-engine
# Non-root; read-only root filesystem at run time with /tmp as a tmpfs (health and drained files).
USER 10001:10001
# 9600: /livez /readyz /metrics. gRPC InteractionControl: 9801 (proposed, parity-deploy ports.yml).
ENTRYPOINT ["/usr/local/bin/parity-interaction-engine"]
