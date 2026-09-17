#!/usr/bin/env bash
#
# setup-hooks.sh — point git at the version-controlled .githooks/ directory.
#
# Run once after cloning:
#
#   bash scripts/setup-hooks.sh
#
# This sets `core.hooksPath = .githooks` for this repo only, so the pre-push
# CI mirror is active. No global git config is touched. Re-run is harmless.
#
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

HOOKS_DIR="$REPO_ROOT/.githooks"

if [[ ! -d "$HOOKS_DIR" ]]; then
  echo "error: $HOOKS_DIR not found" >&2
  exit 1
fi

# Make every hook executable (git refuses to run non-executable hooks).
chmod +x "$HOOKS_DIR"/* 2>/dev/null || true
chmod +x "$REPO_ROOT/scripts/ci-local.sh"

git config core.hooksPath ".githooks"

echo "Git hooks configured: core.hooksPath = .githooks"
echo "  pre-push  → runs scripts/ci-local.sh (CI mirror)"
echo ""
echo "Bypass once with: SKIP_CI_HOOK=1 git push"
