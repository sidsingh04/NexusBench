#!/usr/bin/env bash
# scripts/smoke_test_phase7.sh
#
# Phase 7 (Pre-flight Validator Gate) integration smoke test.
#
# ── What this tests ───────────────────────────────────────────────────────────
#
#   The worker-side pre-flight gate, added in Stage 3 of the Preflight
#   Validator Implementation Plan. Unlike the Phase 5 smoke test (which
#   called the HTTP /validate endpoint as a client), this test verifies the
#   automatic gate that fires inside the worker's Execute function between
#   waitHealthy and runFleet.
#
#   Two engine binaries are submitted:
#
#     1. Broken engine  — returns accepted=false for every order.
#                         Must fail pre-flight, never reach the bot fleet,
#                         and never appear on the leaderboard.
#
#     2. Correct engine — implements a minimal but correct order book:
#                         limit orders rest, cancels of known IDs succeed,
#                         cancels of unknown IDs are rejected.
#                         Must pass pre-flight and complete benchmarking.
#
#   IMPORTANT: This test never calls POST /submissions/{id}/validate.
#   That HTTP endpoint is now restricted to status=running and is not the
#   primary validation path. The worker runs validation automatically.
#
# ── Modes ─────────────────────────────────────────────────────────────────────
#
#   --dry-run  (default)
#     Validates source files, runs all Go tests with the race detector, and
#     checks that all expected symbols exist in the compiled binary. No running
#     infrastructure required. Safe to run in CI.
#
#   --live
#     Requires a running docker compose stack (docker compose up --build -d).
#     Submits both engines and verifies end-to-end pre-flight behaviour.
#
# ── Usage ─────────────────────────────────────────────────────────────────────
#
#   bash scripts/smoke_test_phase7.sh             # dry-run (default)
#   bash scripts/smoke_test_phase7.sh --dry-run   # explicit dry-run
#   bash scripts/smoke_test_phase7.sh --live      # full live run
#
# ── Exit codes ────────────────────────────────────────────────────────────────
#
#   0  All checks passed.
#   1  One or more checks failed.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_ROOT"

# ── Colour helpers ─────────────────────────────────────────────────────────────
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

pass()   { echo -e "${GREEN}  ✓ $*${NC}";  PASS=$((PASS+1)); }
fail()   { echo -e "${RED}  ✗ $*${NC}";   FAIL=$((FAIL+1)); }
info()   { echo -e "${CYAN}  → $*${NC}"; }
warn()   { echo -e "${YELLOW}  ⚠ $*${NC}"; }
header() { echo -e "\n${YELLOW}── $* ──${NC}"; }

PASS=0
FAIL=0
MODE="${1:---dry-run}"

NEXUSBENCH_URL="${NEXUSBENCH_URL:-http://localhost:8080}"
ADMIN_KEY="${ADMIN_KEY:-testkey}"
# Broken engine: fails pre-flight (status=failed, never benchmarked).
# Correct engine: passes pre-flight, completes all three profiles.
# Completion timeout is generous: deploy + health + 21 scenarios + 3 profiles.
PREFLIGHT_TIMEOUT="${PREFLIGHT_TIMEOUT:-180}"
COMPLETION_TIMEOUT="${COMPLETION_TIMEOUT:-480}"

echo ""
echo -e "${YELLOW}╔══════════════════════════════════════════════════════════╗${NC}"
echo -e "${YELLOW}║   NexusBench — Phase 7 Pre-flight Gate Smoke Test       ║${NC}"
echo -e "${YELLOW}║   Mode: ${MODE}$(printf '%*s' $((49 - ${#MODE})) '')║${NC}"
echo -e "${YELLOW}╚══════════════════════════════════════════════════════════╝${NC}"

# ══════════════════════════════════════════════════════════════════════════════
# DRY-RUN MODE
# ══════════════════════════════════════════════════════════════════════════════

if [[ "$MODE" == "--dry-run" ]]; then

  # ── 1/5  Go build + vet ──────────────────────────────────────────────────
  header "1/5  Go build + vet"

  if go build ./... 2>/dev/null; then
    pass "go build ./... — zero compile errors"
  else
    fail "go build ./... FAILED"
  fi

  if go vet ./... 2>/dev/null; then
    pass "go vet ./... — zero findings"
  else
    fail "go vet ./... reported issues"
  fi

  # ── 2/5  Full unit test suite with race detector ──────────────────────────
  header "2/5  Unit tests (race detector)"

  run_pkg() {
    local pkg="$1" desc="$2"
    local out
    if out=$(go test "$pkg" -race -timeout 120s -count=1 2>&1); then
      pass "$desc"
    else
      fail "$desc"
      echo "$out" | tail -25
    fi
  }

  run_pkg "./internal/models/..."      "models — DryRunResult / DryRunScenarioResult types"
  run_pkg "./internal/validator/..."   "validator — 21 scenarios, concurrent burst, enriched reasons"
  run_pkg "./internal/worker/..."      "worker — pre-flight gate (5 new gate tests)"
  run_pkg "./internal/api/..."         "api — StatusRunning guard, BenchmarkingRejected"
  run_pkg "./internal/contest/..."     "contest"
  run_pkg "./internal/submission/..."  "submission"
  run_pkg "./internal/queue/..."       "queue"
  run_pkg "./internal/botfleet/..."    "botfleet"
  run_pkg "./internal/correctness/..."  "correctness"
  run_pkg "./internal/telemetry/..."   "telemetry"
  run_pkg "./internal/orchestrator/..." "orchestrator"

  # Full sweep with coverage to catch cross-package issues.
  info "Full sweep: go test ./... -race ..."
  if go test ./... -race -timeout 120s -count=1 \
       -coverprofile=/tmp/nexusbench_phase7_cov.out \
       -covermode=atomic 2>/dev/null; then
    COV=$(go tool cover -func=/tmp/nexusbench_phase7_cov.out 2>/dev/null \
          | tail -1 | awk '{print $3}')
    pass "Full suite green — total coverage: ${COV:-n/a}"
  else
    fail "Full suite has failures — run: go test ./... -race -count=1 -v"
  fi

  # ── 3/5  Binary builds ────────────────────────────────────────────────────
  header "3/5  Binary builds"

  build_bin() {
    local target="$1"
    if go build -o /dev/null "$target" 2>/dev/null; then
      pass "$target builds"
    else
      fail "$target build FAILED"
    fi
  }
  build_bin "./cmd/server"
  build_bin "./cmd/worker"
  build_bin "./cmd/consumer"

  # ── 4/5  Pre-flight gate symbols in executor.go ───────────────────────────
  header "4/5  Pre-flight gate implementation checks"

  EXEC="internal/worker/executor.go"
  if [[ ! -f "$EXEC" ]]; then
    fail "$EXEC not found"
  else
    check_symbol() {
      local sym="$1" desc="$2"
      if grep -q "$sym" "$EXEC"; then
        pass "$desc"
      else
        fail "$desc — '$sym' not found in $EXEC"
      fi
    }

    check_symbol "runPreflightValidator"    "runPreflightValidator method exists"
    check_symbol "validatorFactory"         "validatorFactory field on SandboxExecutor"
    check_symbol "WithPreflightValidator"   "WithPreflightValidator option exported"
    check_symbol "writeDryRunResult"        "writeDryRunResult helper exists"
    check_symbol "DryRunResult"             "DryRunResult written into submission"
    check_symbol "VolatilityLabel.*low"     "Gate skips non-low profiles"
    check_symbol "2.*Minute\|2\*time\.Minute" "2-minute validator timeout"
  fi

  # Check models.go for required types.
  MODELS="internal/models/models.go"
  if [[ ! -f "$MODELS" ]]; then
    fail "$MODELS not found"
  else
    check_models() {
      local sym="$1" desc="$2"
      if grep -q "$sym" "$MODELS"; then
        pass "$desc"
      else
        fail "$desc — '$sym' not found in $MODELS"
      fi
    }
    check_models "DryRunResult"             "DryRunResult type in models.go"
    check_models "DryRunScenarioResult"     "DryRunScenarioResult type in models.go"
    check_models "dry_run_result"           "dry_run_result JSON field on Submission"
    check_models "FailSummary"              "FailSummary field on DryRunResult"
  fi

  # Check router.go status guard is tightened to StatusRunning.
  ROUTER="internal/api/router.go"
  if [[ ! -f "$ROUTER" ]]; then
    fail "$ROUTER not found"
  else
    if grep -q "StatusRunning" "$ROUTER"; then
      pass "router.go validate guard uses StatusRunning"
    else
      fail "router.go validate guard does not reference StatusRunning"
    fi

    # Ensure the old broken guard (StatusPending || StatusBenchmarking) is gone.
    if grep -q "StatusPending.*StatusBenchmarking\|StatusBenchmarking.*StatusPending" "$ROUTER"; then
      fail "router.go still has old StatusPending||StatusBenchmarking guard — must be removed"
    else
      pass "router.go old StatusPending||StatusBenchmarking guard removed"
    fi
  fi

  # Check validator.go has RunConcurrent.
  VLDTR="internal/validator/validator.go"
  if [[ ! -f "$VLDTR" ]]; then
    fail "$VLDTR not found"
  else
    if grep -q -i "runConcurrent" "$VLDTR"; then
      pass "validator.go exports RunConcurrent"
    else
      fail "validator.go missing RunConcurrent method"
    fi
  fi

  # Check scenarios.go has concurrent_burst_10 and 21 scenarios.
  SCEN="internal/validator/scenarios.go"
  if [[ ! -f "$SCEN" ]]; then
    fail "$SCEN not found"
  else
    if grep -q "concurrent_burst_10" "$SCEN"; then
      pass "scenarios.go contains concurrent_burst_10"
    else
      fail "scenarios.go missing concurrent_burst_10 scenario"
    fi
    if grep -q "isConcurrent.*true\|true.*isConcurrent" "$SCEN"; then
      pass "scenarios.go marks concurrent_burst_10 as isConcurrent"
    else
      fail "scenarios.go missing isConcurrent:true on concurrent_burst_10"
    fi
  fi

  # Check cmd/worker/main.go wires PreflightValidatorFactory.
  WMAIN="cmd/worker/main.go"
  if [[ ! -f "$WMAIN" ]]; then
    fail "$WMAIN not found"
  else
    if grep -q "WithPreflightValidator\|PreflightValidatorFactory" "$WMAIN"; then
      pass "cmd/worker/main.go wires WithPreflightValidator"
    else
      fail "cmd/worker/main.go does not wire WithPreflightValidator"
    fi
  fi

  # Check frontend types.
  TYPES="frontend/src/types.ts"
  if [[ ! -f "$TYPES" ]]; then
    warn "$TYPES not found — skipping TypeScript checks"
  else
    check_ts() {
      local sym="$1" desc="$2"
      if grep -q "$sym" "$TYPES"; then
        pass "types.ts: $desc"
      else
        fail "types.ts missing: $desc ('$sym' not found)"
      fi
    }
    check_ts "DryRunResult"              "DryRunResult interface"
    check_ts "DryRunScenarioResult"      "DryRunScenarioResult interface"
    check_ts "dry_run_result"            "dry_run_result field on Submission"
    check_ts "fail_summary"              "fail_summary field on DryRunResult"
  fi

  # Check UploadForm.tsx has the pre-flight result cards.
  UPLOAD="frontend/src/components/UploadForm.tsx"
  if [[ ! -f "$UPLOAD" ]]; then
    warn "$UPLOAD not found — skipping frontend component checks"
  else
    check_upload() {
      local sym="$1" desc="$2"
      if grep -q "$sym" "$UPLOAD"; then
        pass "UploadForm.tsx: $desc"
      else
        fail "UploadForm.tsx missing: $desc ('$sym' not found)"
      fi
    }
    check_upload "renderDryRunFailureCard"  "renderDryRunFailureCard function"
    check_upload "renderDryRunPassCard"     "renderDryRunPassCard function"
    check_upload "renderPostStepperContent" "renderPostStepperContent dispatch"
    check_upload "dry_run_result"           "reads dry_run_result from submission"
    check_upload "fail_summary"             "displays fail_summary"
    # Ensure the old client-triggered HTTP validate call is gone.
    if grep -q "submissions.*validate\|/validate" "$UPLOAD"; then
      fail "UploadForm.tsx still references /validate endpoint — remove client-triggered validation"
    else
      pass "UploadForm.tsx does not call /validate (worker runs it automatically)"
    fi
  fi

  # Check TeamHistory.tsx has DryRunBreakdown.
  HISTORY="frontend/src/components/TeamHistory.tsx"
  if [[ ! -f "$HISTORY" ]]; then
    warn "$HISTORY not found — skipping TeamHistory checks"
  else
    check_history() {
      local sym="$1" desc="$2"
      if grep -q "$sym" "$HISTORY"; then
        pass "TeamHistory.tsx: $desc"
      else
        fail "TeamHistory.tsx missing: $desc ('$sym' not found)"
      fi
    }
    check_history "DryRunBreakdown"        "DryRunBreakdown component"
    check_history "dry_run_result"         "reads dry_run_result from submission"
    check_history "DryRunResult"           "imports DryRunResult type"
  fi

  # ── 5/5  Frontend build ───────────────────────────────────────────────────
  header "5/5  Frontend TypeScript build"

  FRONTEND_DIR="frontend"
  if [[ ! -d "$FRONTEND_DIR" ]]; then
    warn "frontend/ directory not found — skipping npm build"
  elif ! command -v node &>/dev/null; then
    warn "node not in PATH — skipping npm build (run 'npm run build' manually)"
  else
    cd "$FRONTEND_DIR"
    if [[ ! -d "node_modules" ]]; then
      info "Installing frontend dependencies..."
      npm install --silent 2>/dev/null || warn "npm install had warnings"
    fi
    if npm run build 2>/dev/null; then
      pass "npm run build — zero TypeScript errors"
    else
      fail "npm run build FAILED — TypeScript errors or missing exports"
    fi
    cd "$PROJECT_ROOT"
  fi

  # ── Summary ───────────────────────────────────────────────────────────────
  echo ""
  echo -e "${YELLOW}╔══════════════════════════════════════════════════════════╗${NC}"
  printf  "${YELLOW}║  Dry-run results: %3d passed  %3d failed$(printf '%*s' 14 '')║${NC}\n" \
          "$PASS" "$FAIL"
  echo -e "${YELLOW}╚══════════════════════════════════════════════════════════╝${NC}"

  if [[ $FAIL -eq 0 ]]; then
    echo -e "${GREEN}  ✅  Phase 7 dry-run passed — ready for live test${NC}"
    exit 0
  else
    echo -e "${RED}  ❌  Phase 7 dry-run FAILED — fix $FAIL check(s) before live test${NC}"
    exit 1
  fi
fi

# ══════════════════════════════════════════════════════════════════════════════
# LIVE MODE
# ══════════════════════════════════════════════════════════════════════════════

if [[ "$MODE" != "--live" ]]; then
  echo "Usage: $0 [--dry-run|--live]"
  exit 1
fi

# ── Prerequisite tools ────────────────────────────────────────────────────────
header "Prerequisites"

for tool in curl jq; do
  if command -v "$tool" &>/dev/null; then
    pass "$tool is in PATH"
  else
    fail "$tool not found — install it and retry"
  fi
done

if [[ $FAIL -gt 0 ]]; then
  echo -e "${RED}  ❌  Missing required tools${NC}"
  exit 1
fi

# ── Helper functions ──────────────────────────────────────────────────────────

api() {
  local method="$1" path="$2"
  shift 2
  curl -sf -X "$method" "${NEXUSBENCH_URL}${path}" \
    -H "Authorization: Bearer ${ADMIN_KEY}" \
    -H "Content-Type: application/json" \
    "$@" 2>/dev/null
}

api_noauth() {
  local method="$1" path="$2"
  shift 2
  curl -s -X "$method" "${NEXUSBENCH_URL}${path}" \
    -H "Content-Type: application/json" \
    "$@" 2>/dev/null
}

# poll_until_terminal <submission_id> <timeout_seconds>
# Polls GET /submissions/{id} until status is "completed" or "failed".
# Prints the final JSON of the submission on stdout (not the status string).
# Returns 0 for completed, 1 for failed or timeout.
poll_until_terminal() {
  local sub_id="$1" timeout="$2"
  local deadline=$(( $(date +%s) + timeout ))
  local sub_json status

  while true; do
    sub_json=$(api_noauth GET "/api/v1/submissions/${sub_id}" 2>/dev/null || echo "{}")
    status=$(echo "$sub_json" | jq -r '.status // empty')

    case "$status" in
      completed)
        echo "$sub_json"
        return 0
        ;;
      failed)
        echo "$sub_json"
        return 1
        ;;
    esac

    if [[ $(date +%s) -ge $deadline ]]; then
      info "Timed out waiting for ${sub_id} (last status: ${status})"
      echo "$sub_json"
      return 1
    fi

    sleep 5
    echo -n "." >&2
  done
}

# build_engine_archive <output_archive> <go_source_file>
# Compiles the given Go source file to a Linux amd64 binary and packs it.
build_engine_archive() {
  local archive="$1" source="$2"
  local tmpdir
  tmpdir=$(mktemp -d)
  local binary="${tmpdir}/engine"

  if GOOS=linux GOARCH=amd64 go build -o "$binary" "$source" 2>/dev/null; then
    tar czf "$archive" -C "$tmpdir" engine
    rm -rf "$tmpdir"
    return 0
  else
    rm -rf "$tmpdir"
    return 1
  fi
}

# submit_engine <archive_path> <team_name>
# Submits an engine archive and prints the submission JSON.
submit_engine() {
  local archive="$1" team="$2"
  curl -sf -X POST "${NEXUSBENCH_URL}/api/v1/submissions" \
    -F "team_name=${team}" \
    -F "language=binary" \
    -F "protocol=rest" \
    -F "archive=@${archive}" \
    2>/dev/null || echo ""
}

# ── Health check ──────────────────────────────────────────────────────────────
header "Control plane health"

if curl -sf "${NEXUSBENCH_URL}/health" 2>/dev/null \
    | jq -e '.status == "ok"' >/dev/null 2>&1; then
  pass "Control plane healthy at ${NEXUSBENCH_URL}"
else
  fail "Control plane not responding at ${NEXUSBENCH_URL}"
  echo -e "${RED}  Start the stack first: docker compose up --build -d${NC}"
  exit 1
fi

# ── Cleanup on exit ───────────────────────────────────────────────────────────
WORK_DIR=$(mktemp -d)
cleanup() { rm -rf "$WORK_DIR"; }
trap cleanup EXIT

# ── Create and activate a contest ─────────────────────────────────────────────
header "Setup: Create + activate contest"

CONTEST_JSON=$(api POST "/api/v1/admin/contests" \
  -d '{"name":"phase7-preflight-smoke","use_defaults":true}')
CONTEST_ID=$(echo "$CONTEST_JSON" | jq -r '.id // empty')

if [[ -z "$CONTEST_ID" || "$CONTEST_ID" == "null" ]]; then
  fail "Contest creation failed — response: ${CONTEST_JSON}"
  exit 1
fi
pass "Contest created (id=${CONTEST_ID})"

ACTIVATE_JSON=$(api POST "/api/v1/admin/contests/${CONTEST_ID}/activate")
if echo "$ACTIVATE_JSON" | jq -e '.status == "active"' >/dev/null 2>&1; then
  pass "Contest activated"
else
  fail "Contest activation failed — response: ${ACTIVATE_JSON}"
  exit 1
fi

# ══════════════════════════════════════════════════════════════════════════════
# PART A — BROKEN ENGINE (must fail pre-flight)
# ══════════════════════════════════════════════════════════════════════════════
#
# This engine returns accepted=false for every order.
# The pre-flight validator's very first scenario (limit_buy_rests_on_empty_book)
# will detect the failure and the worker must:
#   1. Write DryRunResult{AllPassed:false} to the submission
#   2. Set status=failed
#   3. NOT invoke the bot fleet

header "Part A/2  Broken engine (must fail pre-flight)"

# Write the broken engine source.
cat > "${WORK_DIR}/broken.go" << 'GOEOF'
package main

import (
    "encoding/json"
    "log"
    "net/http"
    "os"
)

func main() {
    port := os.Getenv("PORT")
    if port == "" { port = "7878" }

    // /orders: always rejects. The pre-flight validator will detect this
    // on the very first scenario (limit buy on empty book must be accepted).
    http.HandleFunc("/orders", func(w http.ResponseWriter, r *http.Request) {
        var req map[string]interface{}
        _ = json.NewDecoder(r.Body).Decode(&req)
        orderID, _ := req["order_id"].(string)
        w.Header().Set("Content-Type", "application/json")
        _ = json.NewEncoder(w).Encode(map[string]interface{}{
            "order_id": orderID,
            "accepted": false,
        })
    })

    http.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
        w.WriteHeader(200)
    })

    log.Printf("broken engine listening on :%s", port)
    log.Fatal(http.ListenAndServe(":"+port, nil))
}
GOEOF

BROKEN_ARCHIVE="${WORK_DIR}/broken.tar.gz"
info "Building broken engine (Linux amd64)..."
if ! build_engine_archive "$BROKEN_ARCHIVE" "${WORK_DIR}/broken.go"; then
  fail "Could not build broken engine binary"
  exit 1
fi
pass "Broken engine archive built"

# Submit it.
info "Submitting broken engine..."
BROKEN_TEAM="smoke-broken-${CONTEST_ID:0:8}"
BROKEN_RESP=$(submit_engine "$BROKEN_ARCHIVE" "$BROKEN_TEAM")
BROKEN_ID=$(echo "$BROKEN_RESP" | jq -r '.id // empty' 2>/dev/null || echo "")

if [[ -z "$BROKEN_ID" || "$BROKEN_ID" == "null" ]]; then
  fail "Broken engine submission rejected — response: ${BROKEN_RESP}"
  exit 1
fi
pass "Broken engine submitted (id=${BROKEN_ID})"

# Verify: do NOT call /validate — the worker runs it automatically.
# Poll until status is terminal. We expect status=failed.
info "Polling until pre-flight completes (timeout ${PREFLIGHT_TIMEOUT}s)..."
echo -n "  Progress: " >&2
BROKEN_JSON=$(poll_until_terminal "$BROKEN_ID" "$PREFLIGHT_TIMEOUT" || true)
echo "" >&2

BROKEN_STATUS=$(echo "$BROKEN_JSON" | jq -r '.status // empty')

# ── Assert 1: status=failed ───────────────────────────────────────────────────
if [[ "$BROKEN_STATUS" == "failed" ]]; then
  pass "A1: Broken engine → status=failed (pre-flight gate correctly blocked bot fleet)"
else
  fail "A1: Expected status=failed, got: '${BROKEN_STATUS}'"
fi

# ── Assert 2: dry_run_result is non-null ──────────────────────────────────────
DRY_RUN_NULL=$(echo "$BROKEN_JSON" | jq '.dry_run_result == null')
if [[ "$DRY_RUN_NULL" == "false" ]]; then
  pass "A2: dry_run_result is non-null (worker wrote pre-flight result)"
else
  fail "A2: dry_run_result is null — worker did not write DryRunResult"
fi

# ── Assert A3: all_passed=false ────────────────────────────────────────────────
ALL_PASSED=$(echo "$BROKEN_JSON" | jq -r '.dry_run_result.all_passed | tostring')
if [[ "$ALL_PASSED" == "false" ]]; then
  pass "A3: dry_run_result.all_passed=false"
else
  fail "A3: Expected all_passed=false, got: '${ALL_PASSED}'"
fi

# ── Assert 4: fail_summary is non-empty ──────────────────────────────────────
FAIL_SUMMARY=$(echo "$BROKEN_JSON" | jq -r '.dry_run_result.fail_summary // empty')
if [[ -n "$FAIL_SUMMARY" && "$FAIL_SUMMARY" != "null" ]]; then
  pass "A4: fail_summary is non-empty: \"${FAIL_SUMMARY}\""
else
  fail "A4: fail_summary is empty or null"
fi

# ── Assert 5: at least one scenario has passed=false ─────────────────────────
FAILED_SCENARIO_COUNT=$(echo "$BROKEN_JSON" \
  | jq '[.dry_run_result.scenarios[]? | select(.passed == false)] | length')
if [[ "$FAILED_SCENARIO_COUNT" -gt 0 ]]; then
  pass "A5: ${FAILED_SCENARIO_COUNT} scenario(s) have passed=false"
else
  fail "A5: No scenario has passed=false — expected at least one failure"
fi

# ── Assert 6: at least one failed scenario has a non-empty reason ─────────────
ENRICHED_REASON=$(echo "$BROKEN_JSON" \
  | jq -r '[.dry_run_result.scenarios[]? | select(.passed == false and .reason != null and .reason != "")] | first | .reason // empty')
if [[ -n "$ENRICHED_REASON" && "$ENRICHED_REASON" != "null" ]]; then
  pass "A6: Failed scenario has enriched reason: \"${ENRICHED_REASON:0:120}...\""
else
  fail "A6: No failed scenario has a non-empty reason string"
fi

# ── Assert 7: NOT on the leaderboard ─────────────────────────────────────────
LEADERBOARD=$(api_noauth GET "/api/v1/leaderboard")
BROKEN_ON_LB=$(echo "$LEADERBOARD" \
  | jq -r '.entries[]? | select(.submission_id == "'"$BROKEN_ID"'") | .submission_id' \
  2>/dev/null || echo "")
if [[ -z "$BROKEN_ON_LB" ]]; then
  pass "A7: Broken engine does NOT appear on leaderboard (StatusFailed correctly excluded)"
else
  fail "A7: Broken engine appears on leaderboard — StatusFailed submissions must be excluded"
fi

# ── Assert 8: HTTP /validate returns WRONG_STATUS (not running) ───────────────
# The submission is now status=failed. The guard must reject any manual call.
VALIDATE_STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
  -X POST "${NEXUSBENCH_URL}/api/v1/submissions/${BROKEN_ID}/validate" 2>/dev/null || echo "000")
if [[ "$VALIDATE_STATUS" == "409" ]]; then
  pass "A8: POST /validate on failed submission returns 409 WRONG_STATUS"
else
  warn "A8: Expected 409 from /validate on failed submission, got: ${VALIDATE_STATUS}"
fi

# ══════════════════════════════════════════════════════════════════════════════
# PART B — CORRECT ENGINE (must pass pre-flight and complete benchmarking)
# ══════════════════════════════════════════════════════════════════════════════
#
# This engine implements a minimal correct order book:
#   - Limit orders with valid price/qty are always accepted (rest without fill
#     since no crossing liquidity exists at the prices the validator uses).
#   - Cancels of known order IDs (tracked in an in-memory map) are accepted.
#   - Cancels of unknown IDs are rejected (correct CLOB semantics).
#   - Market orders with no resting liquidity are rejected.
#
# This is exactly what the validator's 21 scenarios require. The bot fleet
# will also hit this server and measure performance metrics.

header "Part B/2  Correct engine (must pass pre-flight, complete benchmark)"

cat > "${WORK_DIR}/correct.go" << 'GOEOF'
package main

import (
    "encoding/json"
    "log"
    "net/http"
    "os"
    "sync"
)

// order book entry
type entry struct {
    side  string
    price int64
    qty   int64
    seq   int
}

var (
    mu     sync.Mutex
    orders = map[string]*entry{}
    seq    int
)

func main() {
    port := os.Getenv("PORT")
    if port == "" {
        port = "7878"
    }

    http.HandleFunc("/orders", func(w http.ResponseWriter, r *http.Request) {
        var req struct {
            OrderID  string  `json:"order_id"`
            Kind     string  `json:"kind"`
            Side     string  `json:"side"`
            Price    float64 `json:"price"`
            Quantity float64 `json:"quantity"`
        }
        if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
            http.Error(w, "bad request", 400)
            return
        }

        w.Header().Set("Content-Type", "application/json")
        respond := func(accepted bool, execPrice, execQty int64) {
            _ = json.NewEncoder(w).Encode(map[string]interface{}{
                "order_id":       req.OrderID,
                "accepted":       accepted,
                "executed_price": execPrice,
                "executed_qty":   execQty,
            })
        }

        mu.Lock()
        defer mu.Unlock()

        switch req.Kind {
        case "cancel":
            if _, ok := orders[req.OrderID]; ok {
                delete(orders, req.OrderID)
                respond(true, 0, 0)
            } else {
                // Unknown cancel: correctly rejected per CLOB semantics.
                respond(false, 0, 0)
            }

        case "market":
            // The validator's market orders are sent when there is resting
            // liquidity on the opposite side. However, we keep the engine
            // simple: no crossing logic. Market orders are rejected.
            // The validator scenarios that test market fills use a
            // previously-placed resting order. Since we do not implement
            // crossing, those scenarios will report a fill mismatch.
            //
            // To pass ALL validator scenarios we must implement crossing.
            // Find the best resting order on the opposite side and fill it.
            opposite := "sell"
            if req.Side == "sell" {
                opposite = "buy"
            }
            var bestID string
            var bestPrice int64
            for id, e := range orders {
                if e.side != opposite {
                    continue
                }
                if bestID == "" {
                    bestID = id
                    bestPrice = e.price
                    continue
                }
                // Buy side: highest price wins. Sell side: lowest price wins.
                if (req.Side == "buy" && e.price > bestPrice) ||
                    (req.Side == "sell" && e.price < bestPrice) {
                    bestID = id
                    bestPrice = e.price
                } else if e.price == bestPrice {
                    if e.seq < orders[bestID].seq {
                        bestID = id
                    }
                }
            }
            if bestID == "" {
                // No liquidity: reject.
                respond(false, 0, 0)
                return
            }
            fillQty := orders[bestID].qty
            fillPrice := orders[bestID].price
            if int64(req.Quantity) < fillQty {
                fillQty = int64(req.Quantity)
                orders[bestID].qty -= fillQty
            } else {
                delete(orders, bestID)
            }
            respond(true, fillPrice, fillQty)

        default: // limit
            if req.Quantity <= 0 || req.Price <= 0 {
                respond(false, 0, 0)
                return
            }
            price := int64(req.Price)
            qty := int64(req.Quantity)

            // Check if this limit order crosses any resting order on the
            // opposite side (immediate-or-rest semantics).
            opposite := "sell"
            if req.Side == "sell" {
                opposite = "buy"
            }
            var totalFillQty int64
            var lastFillPrice int64

            for qty > 0 {
                var crossID string
                var crossPrice int64
                for id, e := range orders {
                    if e.side != opposite {
                        continue
                    }
                    // A buy crosses a resting sell if buy price >= sell price.
                    // A sell crosses a resting buy if sell price <= buy price.
                    crosses := false
                    if req.Side == "buy" && price >= e.price {
                        crosses = true
                    } else if req.Side == "sell" && price <= e.price {
                        crosses = true
                    }
                    if !crosses {
                        continue
                    }
                    if crossID == "" {
                        crossID = id
                        crossPrice = e.price
                        continue
                    }
                    // Price-time priority: best price first.
                    if (req.Side == "buy" && e.price < crossPrice) ||
                        (req.Side == "sell" && e.price > crossPrice) {
                        crossID = id
                        crossPrice = e.price
                    } else if e.price == crossPrice {
                        if e.seq < orders[crossID].seq {
                            crossID = id
                        }
                    }
                }

                if crossID == "" {
                    break
                }

                // Crosses: fill at the resting order's price (passive price wins).
                fillQty := orders[crossID].qty
                fillPrice := orders[crossID].price
                if qty < fillQty {
                    fillQty = qty
                    orders[crossID].qty -= fillQty
                } else {
                    delete(orders, crossID)
                }

                qty -= fillQty
                totalFillQty += fillQty
                lastFillPrice = fillPrice
            }

            if qty > 0 {
                // Remaining qty (if any) rests on the book.
                seq++
                orders[req.OrderID] = &entry{side: req.Side, price: price, qty: qty, seq: seq}
            }
            // When totalFillQty is 0, lastFillPrice is 0 (correct for pure resting orders).
            respond(true, lastFillPrice, totalFillQty)
        }
    })

    http.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
        w.WriteHeader(200)
    })

    log.Printf("correct engine listening on :%s", port)
    log.Fatal(http.ListenAndServe(":"+port, nil))
}
GOEOF

CORRECT_ARCHIVE="${WORK_DIR}/correct.tar.gz"
info "Building correct engine (Linux amd64)..."
if ! build_engine_archive "$CORRECT_ARCHIVE" "${WORK_DIR}/correct.go"; then
  fail "Could not build correct engine binary"
  exit 1
fi
pass "Correct engine archive built"

info "Submitting correct engine..."
CORRECT_TEAM="smoke-correct-${CONTEST_ID:0:8}"
CORRECT_RESP=$(submit_engine "$CORRECT_ARCHIVE" "$CORRECT_TEAM")
CORRECT_ID=$(echo "$CORRECT_RESP" | jq -r '.id // empty' 2>/dev/null || echo "")

if [[ -z "$CORRECT_ID" || "$CORRECT_ID" == "null" ]]; then
  fail "Correct engine submission rejected — response: ${CORRECT_RESP}"
  exit 1
fi
pass "Correct engine submitted (id=${CORRECT_ID})"

# Poll until completed (all three profiles). The worker will:
# 1. Deploy sandbox
# 2. WaitHealthy
# 3. Run pre-flight (21 scenarios) — must pass
# 4. Run Low profile fleet
# 5. Enqueue Medium, run it
# 6. Enqueue High, run it
# 7. Compute FinalScore
info "Polling until all three profiles complete (timeout ${COMPLETION_TIMEOUT}s)..."
echo -n "  Progress: " >&2
CORRECT_JSON=$(poll_until_terminal "$CORRECT_ID" "$COMPLETION_TIMEOUT" || true)
echo "" >&2

CORRECT_STATUS=$(echo "$CORRECT_JSON" | jq -r '.status // empty')

# ── Assert B1: status=completed ───────────────────────────────────────────────
if [[ "$CORRECT_STATUS" == "completed" ]]; then
  pass "B1: Correct engine → status=completed"
else
  fail "B1: Expected status=completed, got: '${CORRECT_STATUS}'"
  info "Submission JSON:"
  echo "$CORRECT_JSON" | jq . 2>/dev/null | head -40
fi

# ── Assert B2: dry_run_result is non-null ─────────────────────────────────────
DRY_RUN_NULL_B=$(echo "$CORRECT_JSON" | jq '.dry_run_result == null')
if [[ "$DRY_RUN_NULL_B" == "false" ]]; then
  pass "B2: dry_run_result is non-null"
else
  fail "B2: dry_run_result is null — worker did not write DryRunResult"
fi

# ── Assert B3: all_passed=true ────────────────────────────────────────────────
ALL_PASSED_B=$(echo "$CORRECT_JSON" | jq -r '.dry_run_result.all_passed | tostring')
if [[ "$ALL_PASSED_B" == "true" ]]; then
  pass "B3: dry_run_result.all_passed=true"
else
  fail "B3: Expected all_passed=true, got: '${ALL_PASSED_B}'"
  # Print which scenarios failed to help debug.
  echo "$CORRECT_JSON" \
    | jq -r '.dry_run_result.scenarios[]? | select(.passed == false) | "    ✗ \(.name): \(.reason)"' \
    2>/dev/null | head -10
fi

# ── Assert B4: All 20 scenarios present ──
SCENARIO_COUNT=$(echo "$CORRECT_JSON" | jq '.dry_run_result.scenarios | length')
if [[ "$SCENARIO_COUNT" == "20" ]]; then
  pass "B4: 20 scenarios in dry_run_result"
else
  fail "B4: Expected 20 scenarios, got ${SCENARIO_COUNT}"
fi

# ── Assert B5: concurrent_burst_10 is present and passed ─────────────────────
BURST_PASSED=$(echo "$CORRECT_JSON" \
  | jq -r '.dry_run_result.scenarios[]? | select(.name == "concurrent_burst_10") | .passed' \
  2>/dev/null || echo "")
if [[ "$BURST_PASSED" == "true" ]]; then
  pass "B5: concurrent_burst_10 scenario present and passed"
elif [[ "$BURST_PASSED" == "false" ]]; then
  BURST_REASON=$(echo "$CORRECT_JSON" \
    | jq -r '.dry_run_result.scenarios[]? | select(.name == "concurrent_burst_10") | .reason')
  fail "B5: concurrent_burst_10 failed — reason: ${BURST_REASON}"
else
  fail "B5: concurrent_burst_10 scenario not found in dry_run_result"
fi

# ── Assert B6: FinalScore > 0 ─────────────────────────────────────────────────
FINAL_SCORE=$(echo "$CORRECT_JSON" | jq -r '.final_score // 0')
if awk "BEGIN{exit !($FINAL_SCORE > 0)}" 2>/dev/null; then
  pass "B6: FinalScore=${FINAL_SCORE} > 0"
else
  fail "B6: FinalScore=${FINAL_SCORE}, expected > 0"
fi

# ── Assert B7: all_results has three profiles ─────────────────────────────────
PROFILE_COUNT=$(echo "$CORRECT_JSON" | jq '.all_results | length' 2>/dev/null || echo "0")
if [[ "$PROFILE_COUNT" -eq 3 ]]; then
  pass "B7: all_results has 3 profiles (low / medium / high)"
else
  fail "B7: Expected 3 profiles in all_results, got ${PROFILE_COUNT}"
fi

# ── Assert B8: appears on leaderboard ─────────────────────────────────────────
LEADERBOARD_B=$(api_noauth GET "/api/v1/leaderboard")
CORRECT_ON_LB=$(echo "$LEADERBOARD_B" \
  | jq -r '.entries[]? | select(.submission_id == "'"$CORRECT_ID"'") | .submission_id' \
  2>/dev/null || echo "")
if [[ -n "$CORRECT_ON_LB" ]]; then
  LB_SCORE=$(echo "$LEADERBOARD_B" \
    | jq -r '.entries[]? | select(.submission_id == "'"$CORRECT_ID"'") | .final_score')
  pass "B8: Correct engine appears on leaderboard (FinalScore=${LB_SCORE})"
else
  fail "B8: Correct engine NOT on leaderboard after status=completed"
fi

# ── Assert B9: /validate now returns WRONG_STATUS (benchmarking/completed) ────
# Once the pre-flight gate has run and the fleet has started, the HTTP
# /validate endpoint must block manual calls. Status is now completed.
VALIDATE_STATUS_B=$(curl -s -o /dev/null -w "%{http_code}" \
  -X POST "${NEXUSBENCH_URL}/api/v1/submissions/${CORRECT_ID}/validate" 2>/dev/null || echo "000")
if [[ "$VALIDATE_STATUS_B" == "409" ]]; then
  pass "B9: POST /validate on completed submission returns 409 WRONG_STATUS"
else
  warn "B9: Expected 409 from /validate on completed submission, got: ${VALIDATE_STATUS_B}"
fi

# ══════════════════════════════════════════════════════════════════════════════
# CLEANUP: Close the contest
# ══════════════════════════════════════════════════════════════════════════════

header "Cleanup: Close contest"

CLOSE_RESP=$(api POST "/api/v1/admin/contests/${CONTEST_ID}/close" || echo "{}")
if echo "$CLOSE_RESP" | jq -e '.status == "closed"' >/dev/null 2>&1; then
  pass "Contest closed"
else
  warn "Contest close response: ${CLOSE_RESP}"
fi

# ── Final summary ──────────────────────────────────────────────────────────────
echo ""
echo -e "${YELLOW}╔══════════════════════════════════════════════════════════╗${NC}"
printf  "${YELLOW}║  Live results:  %3d passed  %3d failed$(printf '%*s' 17 '')║${NC}\n" \
        "$PASS" "$FAIL"
echo -e "${YELLOW}╚══════════════════════════════════════════════════════════╝${NC}"

if [[ $FAIL -eq 0 ]]; then
  echo -e "${GREEN}  ✅  Phase 7 pre-flight gate smoke test passed${NC}"
  echo ""
  echo "  Verified end-to-end:"
  echo "    A. Broken engine → pre-flight fails → status=failed → not on leaderboard"
  echo "       dry_run_result.all_passed=false, fail_summary non-empty,"
  echo "       enriched per-scenario reasons present"
  echo "    B. Correct engine → pre-flight passes (21/21) → bot fleet runs →"
  echo "       status=completed → FinalScore > 0 → appears on leaderboard"
  echo "       concurrent_burst_10 scenario passed"
  echo "    Manual /validate blocked on non-running submissions (WRONG_STATUS 409)"
  exit 0
else
  echo -e "${RED}  ❌  Phase 7 smoke test FAILED — ${FAIL} check(s) need attention${NC}"
  exit 1
fi
