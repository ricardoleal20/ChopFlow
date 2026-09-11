#syntax=docker/dockerfile:1
#
# ChopFlow — multi-stage build.
#
# The broker embeds the dashboard UI from the committed `broker/ui/dist/`
# bundle (see `rust-embed`), so the release image needs no Node toolchain:
# a pure Rust build produces a broker that serves gRPC, the HTTP/JSON API and
# the dashboard from a single binary.

ARG RUST_VERSION=1.97

# --- Build stage -----------------------------------------------------------
FROM rust:${RUST_VERSION}-slim AS builder

# Protobuf compiler is required by tonic-build at compile time.
RUN apt-get update \
    && apt-get install -y --no-install-recommends protobuf-compiler pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Pre-create the workspace member directories so we can copy manifests first
# and cache dependencies across source-only changes.
COPY Cargo.toml Cargo.lock ./
COPY core/Cargo.toml        core/Cargo.toml
COPY proto/Cargo.toml       proto/Cargo.toml
COPY proto/build.rs         proto/build.rs
COPY proto/proto            proto/proto
COPY broker/Cargo.toml      broker/Cargo.toml
COPY worker/Cargo.toml      worker/Cargo.toml
COPY cli/Cargo.toml         cli/Cargo.toml
COPY demos/Cargo.toml       demos/Cargo.toml
COPY broker/ui/dist         broker/ui/dist

# Create stub libs so `cargo build --release` of the workspace doesn't fail
# fetching member sources that don't exist yet during the dependency pre-build.
RUN mkdir -p core/src proto/src broker/src worker/src cli/src demos/src demos/src/bin \
    && echo "pub fn lib() {}" > core/src/lib.rs \
    && echo "" > proto/src/lib.rs \
    && echo "fn main() {}" > broker/src/main.rs \
    && echo "fn main() {}" > worker/src/main.rs \
    && echo "fn main() {}" > cli/src/main.rs \
    && echo "fn main() {}" > demos/src/main.rs \
    && echo "fn main() {}" > demos/src/bin/seed.rs \
    && cargo build --release -p chopflow_broker -p chopflow_worker -p chopflow_cli || true

# Now copy the real sources and build the release binaries.
COPY core     core
COPY proto    proto
COPY broker   broker
COPY worker   worker
COPY cli      cli
COPY demos    demos

RUN touch core/src/lib.rs proto/src/lib.rs broker/src/main.rs worker/src/main.rs \
        cli/src/main.rs demos/src/main.rs demos/src/bin/seed.rs \
    && cargo build --release -p chopflow_broker -p chopflow_worker -p chopflow_cli

# --- Runtime stage ---------------------------------------------------------
FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /build/target/release/chopflow_broker  /usr/local/bin/chopflow_broker
COPY --from=builder /build/target/release/chopflow_worker  /usr/local/bin/chopflow_worker
COPY --from=builder /build/target/release/chopflow_cli     /usr/local/bin/chopflow_cli

# gRPC and HTTP/dashboard ports.
EXPOSE 8000 8080

# Default to the broker. Override the entrypoint to run a worker instead:
#   docker run --rm chopflow chopflow_worker start --broker http://broker:8000
ENTRYPOINT ["chopflow_broker"]
CMD ["start", "--host", "0.0.0.0", "--port", "8000", "--http-port", "8080", "--storage", "sqlite", "--db-path", "/data/chopflow.db"]
