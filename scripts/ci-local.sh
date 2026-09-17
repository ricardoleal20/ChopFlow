#!/usr/bin/env bash
#
# ci-local.sh — run the local mirror of .github/workflows/ci.yml.
#
# Every check GitHub Actions runs (minus the non-blocking tarpaulin coverage
# job) is run here in the same order, with the same flags. Use it directly to
# preview CI before pushing, or let the pre-push hook (.githooks/pre-push)
# invoke it for you.
#
#   bash scripts/ci-local.sh            # run every step
#   bash scripts/ci-local.sh --fast     # only the fast checks (fmt · lint · format)
#
# Design choices:
#   * Stop on the first failing step — same signal CI gives, faster feedback.
#   * A step whose toolchain is missing is SKIPPED with a warning, never a
#     failure. The hook's job is to catch regressions in the stacks you have
#     locally; CI is still the source of truth for anything skipped here.
#   * The tarpaulin coverage job is intentionally omitted (it is
#     `continue-on-error` in CI, so it never blocks a PR).
#
set -uo pipefail

# Resolve the repo root regardless of where the script is invoked from.
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT" || exit 1

FAST_ONLY=0
for arg in "$@"; do
  case "$arg" in
    --fast) FAST_ONLY=1 ;;
    -h|--help)
      sed -n '2,16p' "$0"; exit 0 ;;
    *) echo "unknown arg: $arg" >&2; exit 2 ;;
  esac
done

# Pretty printing.
if [[ -t 1 ]]; then
  BOLD=$'\033[1m'; DIM=$'\033[2m'; GREEN=$'\033[32m'; YELLOW=$'\033[33m'; RED=$'\033[31m'; RESET=$'\033[0m'
else
  BOLD=''; DIM=''; GREEN=''; YELLOW=''; RED=''; RESET=''
fi

STEP_NUM=0

# run_step <label> <check-command...>
# Prints a header, runs the command, and stops the script on the first real
# failure. A missing tool (the command string exits 127) is a SKIP, not a
# failure — the real CI is still the source of truth for skipped stacks.
run_step() {
  local label="$1"; shift
  STEP_NUM=$((STEP_NUM + 1))
  printf '\n%s[%02d]%s %s\n' "$BOLD" "$STEP_NUM" "$RESET" "$label"

  "$@"
  local rc=$?
  if [[ $rc -eq 127 ]]; then
    printf '  %sSKIP%s — required tool not on PATH\n' "$YELLOW" "$RESET"
    return 0
  elif [[ $rc -ne 0 ]]; then
    printf '  %sFAIL%s (exit %d)\n' "$RED" "$RESET" "$rc"
    printf '\n%sCI-local checks FAILED.%s Fix the step above, or bypass once with: SKIP_CI_HOOK=1 git push\n' "$RED" "$RESET"
    exit 1
  fi
  printf '  %sok%s\n' "$GREEN" "$RESET"
  return 0
}

# Each step runs in a child `bash -c`, which cannot see this shell's
# functions — so every command string self-guards with a FUNCTIONAL version
# check (`<tool> <verflag>`) rather than `command -v`. A version check actually
# exercises the runtime, so a tool that is on PATH but broken (e.g. `mvn` with
# no JDK) is treated as SKIP, not a false FAIL. run_step treats exit 127 as a
# SKIP (missing or broken toolchain), any other non-zero as a real FAIL.

# ---------------------------------------------------------------------------
# Rust job: fmt · clippy · test  (mirrors ci.yml "rust")
# ---------------------------------------------------------------------------
run_step "Rust · cargo fmt --check" \
  bash -c 'cargo --version >/dev/null 2>&1 || exit 127; cargo fmt --all --check'

run_step "Rust · cargo clippy (-D warnings)" \
  bash -c 'cargo --version >/dev/null 2>&1 || exit 127; cargo clippy --workspace --all-targets -- -D warnings'

if [[ $FAST_ONLY -eq 0 ]]; then
  run_step "Rust · cargo test --workspace" \
    bash -c 'cargo --version >/dev/null 2>&1 || exit 127; cargo test --workspace'
fi

# ---------------------------------------------------------------------------
# Frontend job: lint · format:check · build  (mirrors ci.yml "frontend")
# ---------------------------------------------------------------------------
run_step "Frontend · eslint" \
  bash -c 'pnpm --version >/dev/null 2>&1 || exit 127; cd broker/ui && pnpm lint'

run_step "Frontend · prettier --check" \
  bash -c 'pnpm --version >/dev/null 2>&1 || exit 127; cd broker/ui && pnpm format:check'

if [[ $FAST_ONLY -eq 0 ]]; then
  run_step "Frontend · pnpm build" \
    bash -c 'pnpm --version >/dev/null 2>&1 || exit 127; cd broker/ui && pnpm build'
fi

# ---------------------------------------------------------------------------
# Python job: ruff check · ruff format --check · pytest  (mirrors ci.yml "python")
# ---------------------------------------------------------------------------
run_step "Python · ruff check" \
  bash -c 'ruff --version >/dev/null 2>&1 || exit 127; cd clients/python && ruff check .'

run_step "Python · ruff format --check" \
  bash -c 'ruff --version >/dev/null 2>&1 || exit 127; cd clients/python && ruff format --check .'

if [[ $FAST_ONLY -eq 0 ]]; then
  run_step "Python · pytest --cov" \
    bash -c 'pytest --version >/dev/null 2>&1 || exit 127; cd clients/python && pytest --cov=chopflow --cov-report=term-missing'
fi

# ---------------------------------------------------------------------------
# Java job: mvn verify  (mirrors ci.yml "java")
# `mvn -v` requires a JDK, so this guard skips when Java is absent locally
# even if the `mvn` launcher is on PATH — CI still gates Java in that case.
# ---------------------------------------------------------------------------
if [[ $FAST_ONLY -eq 0 ]]; then
  run_step "Java · mvn verify" \
    bash -c 'mvn -v >/dev/null 2>&1 || exit 127; cd clients/java && mvn -B -ntp verify'
fi

# ---------------------------------------------------------------------------
# Verdict
# ---------------------------------------------------------------------------
echo
printf '%sAll CI-local checks passed.%s\n' "$GREEN" "$RESET"
exit 0
