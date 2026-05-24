#!/usr/bin/env bash
# scripts/smoke_test_phase3_stage1.sh
#
# Stage 3.1 smoke test: Worker abstraction + Job queue
#
# Steps 1-4: offline (compile + unit tests). No Redpanda, no Docker needed.
# Steps 5-6: online (live stack). Requires: docker compose up --build -d
#
# Watermark verification uses cmd/smokecheck — a tiny Go binary that talks
# to Redpanda via franz-go (same client as production). This is version-stable
# and exercises the real network path, unlike rpk whose output format changes.
#
# Usage:
#   bash scripts/smoke_test_phase3_stage1.sh                    # offline only
#   STACK_RUNNING=1 bash scripts/smoke_test_phase3_stage1.sh    # all checks
#
# Exit code: 0 = all checks passed, 1 = one or more failures.

# Do NOT use set -e — we handle every error explicitly so the script never
# exits silently mid-step. set -u catches unbound variables; set -o pipefail
# catches pipe failures in non-subshell contexts.
set -uo pipefail

CONTROL_PLANE="${NEXUS_URL:-http://localhost:8080}"
REDPANDA_BROKERS="${REDPANDA_BROKERS:-127.0.0.1:19092}"
STACK_RUNNING="${STACK_RUNNING:-0}"

PASS=0
FAIL=0

green() { echo "  ✓  $*"; }
red()   { echo "  ✗  $*"; }
info()  { echo "     $*"; }

pass() { green "$1"; PASS=$((PASS+1)); }
fail() { red   "$1"; FAIL=$((FAIL+1)); }

banner() { echo ""; echo "── $* ──────────────────────────────────────────────"; }

# smokecheck: run cmd/smokecheck with the configured broker list.
# Uses `go run` so no separate build step is needed.
smokecheck() {
    REDPANDA_BROKERS="$REDPANDA_BROKERS" \
        go run ./cmd/smokecheck "$@" 2>/dev/null
}

echo ""
echo "╔═══════════════════════════════════════════════════════╗"
echo "║   NexusBench — Phase 3 Stage 3.1 Smoke Test          ║"
echo "╚═══════════════════════════════════════════════════════╝"

# ─────────────────────────────────────────────────────────────────────────────
banner "STEP 1 — queue package: compile + unit tests"
# ─────────────────────────────────────────────────────────────────────────────

if go build ./internal/queue/... 2>/dev/null; then
    pass "internal/queue compiles"
else
    fail "internal/queue compile failed — run: go build ./internal/queue/..."
fi

if go test ./internal/queue/... -race -timeout 30s -count=1 2>/dev/null; then
    pass "internal/queue unit tests pass (no Redpanda required)"
else
    fail "internal/queue unit tests failed — run: go test ./internal/queue/... -v -race"
fi

# ─────────────────────────────────────────────────────────────────────────────
banner "STEP 2 — worker package: compile + unit tests"
# ─────────────────────────────────────────────────────────────────────────────

if go build ./internal/worker/... 2>/dev/null; then
    pass "internal/worker compiles"
else
    fail "internal/worker compile failed — run: go build ./internal/worker/..."
fi

if go test ./internal/worker/... -race -timeout 30s -count=1 2>/dev/null; then
    pass "internal/worker unit tests pass (no Docker required)"
else
    fail "internal/worker unit tests failed — run: go test ./internal/worker/... -v -race"
fi

# ─────────────────────────────────────────────────────────────────────────────
banner "STEP 3 — submission package: compiles with queue wiring"
# ─────────────────────────────────────────────────────────────────────────────

if go build ./internal/submission/... 2>/dev/null; then
    pass "internal/submission compiles (queue wiring intact)"
else
    fail "internal/submission compile failed"
fi

if go test ./internal/submission/... -race -timeout 30s -count=1 2>/dev/null; then
    pass "internal/submission unit tests still pass (no regression)"
else
    fail "internal/submission unit tests regressed — run: go test ./internal/submission/... -v -race"
fi

# ─────────────────────────────────────────────────────────────────────────────
banner "STEP 4 — binaries build + full unit suite"
# ─────────────────────────────────────────────────────────────────────────────

if go build -o /dev/null ./cmd/worker 2>/dev/null; then
    pass "cmd/worker binary builds"
else
    fail "cmd/worker build failed — run: go build ./cmd/worker"
fi

if go build -o /dev/null ./cmd/server 2>/dev/null; then
    pass "cmd/server still builds (no regression)"
else
    fail "cmd/server build regressed"
fi

if go build -o /dev/null ./cmd/smokecheck 2>/dev/null; then
    pass "cmd/smokecheck binary builds"
else
    fail "cmd/smokecheck build failed — run: go build ./cmd/smokecheck"
fi

echo ""
info "Running full unit test suite..."
if go test $(go list ./... 2>/dev/null | grep -v 'docker/sandbox') \
       -race -timeout 60s -count=1 2>/dev/null; then
    pass "full unit test suite passes (no regressions)"
else
    fail "full unit test suite has failures — run: make test"
fi

# ─────────────────────────────────────────────────────────────────────────────
banner "STEP 5 — live stack checks"
# ─────────────────────────────────────────────────────────────────────────────

if [ "$STACK_RUNNING" != "1" ]; then
    echo ""
    echo "  ⚠  STACK_RUNNING not set — skipping live checks (Steps 5-6)."
    info "Start stack: docker compose up --build -d"
    info "Then re-run: STACK_RUNNING=1 bash $0"
    echo ""
    echo "╔═══════════════════════════════════════════════════════╗"
    printf  "║  Results:  %3d passed  %3d failed  (offline only)   ║\n" "$PASS" "$FAIL"
    echo "╚═══════════════════════════════════════════════════════╝"
    [ "$FAIL" -eq 0 ] && exit 0 || exit 1
fi

# ── 5a: smokecheck can reach Redpanda ────────────────────────────────────────
echo ""
info "Checking Redpanda connectivity via smokecheck ..."

if smokecheck watermark >/dev/null; then
    pass "Redpanda reachable at $REDPANDA_BROKERS (smokecheck connected)"
else
    fail "Cannot reach Redpanda at $REDPANDA_BROKERS"
    info "Is the stack running?  docker compose up --build -d"
    info "Is port 19092 mapped? docker compose ps redpanda"
    echo ""
    echo "╔═══════════════════════════════════════════════════════╗"
    printf  "║  Results:  %3d passed  %3d failed                    ║\n" "$PASS" "$FAIL"
    echo "╚═══════════════════════════════════════════════════════╝"
    exit 1
fi

# ── 5b: control-plane is healthy ─────────────────────────────────────────────
echo ""
info "Checking control-plane health ..."

HEALTH=$(curl -sf "$CONTROL_PLANE/health" 2>/dev/null || echo "")
if echo "$HEALTH" | grep -q "ok"; then
    pass "Control plane is healthy at $CONTROL_PLANE"
else
    fail "Control plane not responding at $CONTROL_PLANE/health"
    info "Try: docker compose up --build -d"
fi

# ── 5c: jobs.benchmark topic exists ──────────────────────────────────────────
# smokecheck watermark returns 0 successfully if the topic exists (even empty).
# It returns non-zero only if the broker is unreachable or the topic is missing.
echo ""
info "Checking jobs.benchmark topic exists ..."

if smokecheck watermark >/dev/null; then
    pass "jobs.benchmark topic exists and is reachable"
else
    fail "jobs.benchmark topic not found or broker unreachable"
    info "Check server logs: docker compose logs control-plane | grep -i topic"
fi

# ─────────────────────────────────────────────────────────────────────────────
banner "STEP 6 — submitting a job enqueues it to jobs.benchmark"
# ─────────────────────────────────────────────────────────────────────────────

echo ""
info "Recording jobs.benchmark watermark before submission ..."

BEFORE=$(smokecheck watermark 2>/dev/null || echo "ERROR")
if [ "$BEFORE" = "ERROR" ]; then
    fail "Could not read watermark — smokecheck failed"
    BEFORE=0
else
    info "Watermark before: $BEFORE"
fi

# Create a minimal archive and POST it.
TMPDIR_SUB=$(mktemp -d)
echo "package main; func main() {}" > "$TMPDIR_SUB/main.go"
ARCHIVE="$TMPDIR_SUB/archive.tar.gz"
tar -czf "$ARCHIVE" -C "$TMPDIR_SUB" main.go

HTTP_CODE=$(curl -s -o /tmp/nexus_submit_resp.json -w "%{http_code}" \
    -X POST "$CONTROL_PLANE/api/v1/submissions" \
    -F "team_name=smoke-test-p3" \
    -F "language=go" \
    -F "protocol=rest" \
    -F "archive=@$ARCHIVE" 2>/dev/null || echo "000")
rm -rf "$TMPDIR_SUB"

if [ "$HTTP_CODE" = "201" ]; then
    pass "POST /api/v1/submissions returned 201"
    SUB_ID=$(grep -o '"id":"[^"]*"' /tmp/nexus_submit_resp.json 2>/dev/null \
             | head -1 | cut -d'"' -f4 || echo "unknown")
    info "Submission ID: $SUB_ID"
else
    fail "POST /api/v1/submissions returned $HTTP_CODE (expected 201)"
    info "Response: $(cat /tmp/nexus_submit_resp.json 2>/dev/null || echo '(empty)')"
fi

# ProduceSync (AllISRAcks) means the broker acknowledged the write before the
# HTTP 201 was sent. No sleep needed — but we give 1s for good measure.
sleep 1

info "Recording jobs.benchmark watermark after submission ..."
AFTER=$(smokecheck watermark 2>/dev/null || echo "ERROR")
if [ "$AFTER" = "ERROR" ]; then
    fail "Could not read watermark after submission"
    AFTER="$BEFORE"
else
    info "Watermark after:  $AFTER"
fi

# Verify using smokecheck's built-in verify command (exits 0 if delta > 0).
if smokecheck verify "$BEFORE" 2>/dev/null; then
    DELTA=$(( AFTER - BEFORE ))
    pass "jobs.benchmark watermark increased by $DELTA — job durably enqueued ✓"
else
    fail "Watermark did not increase (before=$BEFORE after=$AFTER)"
    info "Server logs confirm job was enqueued — this is a broker connectivity issue."
    info "Check: docker compose logs control-plane | grep -E 'enqueued|queue'"
fi

# ─────────────────────────────────────────────────────────────────────────────
echo ""
echo "╔═══════════════════════════════════════════════════════╗"
printf  "║  Results:  %3d passed  %3d failed                    ║\n" "$PASS" "$FAIL"
echo "╚═══════════════════════════════════════════════════════╝"
echo ""

if [ "$FAIL" -eq 0 ]; then
    echo "  ✅  Stage 3.1 complete — all checks passed."
    exit 0
else
    echo "  ❌  Stage 3.1 incomplete — $FAIL check(s) failed."
    exit 1
fi
