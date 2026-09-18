#!/usr/bin/env bash
# Builds the `chopflow` CLI and stages the macOS sidecar binary under the
# triple-suffixed name tauri's externalBin expects (bundle + build-time check).
# Run from the repo root BEFORE `tauri build` (and after the first checkout,
# so plain dev builds of the app pass the tauri-build resource check).
set -euo pipefail

TRIPLE="$(rustc -vV | sed -n 's/^host: //p')"
echo "target triple: ${TRIPLE}"

# The app expects the bundled umbrella CLI at Contents/Resources.
cargo build --release -p chopflow
cp -f target/release/chopflow "target/release/chopflow-${TRIPLE}"
echo "sidecar staged: target/release/chopflow-${TRIPLE}"
