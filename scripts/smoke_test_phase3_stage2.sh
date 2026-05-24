#!/usr/bin/env bash
# scripts/smoke_test_phase3_stage2.sh
#
# Stage 3.2 smoke test: Orchestrator + Worker Heartbeat
#
# Steps 1-3: offline (compile + unit tests). No infrastructure required.
# Steps 4-6: online (live stack). Requires: docker compose up --build -d
#
# What this validates:
#   - orchestrator package compiles and all registry unit tests pass
#   - worker/heartbeat.go compiles (heartbeater is wired into cmd/worker)
#   - cmd/server and cmd/worker build with all new wiring
#   - /internal/workers/register endpoint is live (routes mounted)
#   - Worker auto-registers with the orchestrator on startup
#   - /internal/workers shows the worker as alive (heartbeat working)
#   - /internal/workers/stats shows correct fleet counts
#
# Usage:
#   bash scripts/smoke_test_phase3_stage2.sh                    # offline only
#   STACK_RUNNING=1 bash scripts/smoke_test_phase3_stage2.sh    # all checks

set -uo pipefail

CONTROL_PLANE="${NEXUS_URL:-http://localhost:8080}"
STACK_RUNNING="${STACK_RUNNING:-0}"

PASS=0
FAIL=0

green() { echo "  ✓  $*"; }
red()   { echo "  ✗  $*"; }
info()  { echo "     $*"; }
pass()  { green "$1"; PASS=$((PASS+1)); }
fail()  { red   "$1"; FAIL=$((FAIL+1)); }
banner(){ echo ""; echo "── $* ──────────────────────────────────────────────"; }

echo ""
echo "╔════════════════════════════════════════════════════════╗"
echo "║   NexusBench — Phase 3 Stage 3.2 Smoke Test           ║"
echo "╚════════════════════════════════════════════════════════╝"

# ─────────────────────────────────────────────────────────────────────────────
banner "STEP 1 — orchestrator package: compile + unit tests"
# ─────────────────────────────────────────────────────────────────────────────

if go build ./internal/orchestrator/... 2>/dev/null; then
    pass "internal/orchestrator compiles"
else
    fail "internal/orchestrator compile failed — run: go build ./internal/orchestrator/..."
fi

if go test ./internal/orchestrator/... -race -timeout 30s -count=1 2>/dev/null; then
    pass "internal/orchestrator unit tests pass (10 tests, no HTTP/Docker required)"
else
    fail "internal/orchestrator unit tests failed — run: go test ./internal/orchestrator/... -v -race"
fi

# ─────────────────────────────────────────────────────────────────────────────
banner "STEP 2 — worker package: heartbeat + executor callbacks compile"
# ─────────────────────────────────────────────────────────────────────────────

if go build ./internal/worker/... 2>/dev/null; then
    pass "internal/worker compiles (heartbeat.go + executor callbacks)"
else
    fail "internal/worker compile failed — run: go build ./internal/worker/..."
fi

if go test ./internal/worker/... -race -timeout 30s -count=1 2>/dev/null; then
    pass "internal/worker unit tests pass"
else
    fail "internal/worker unit tests failed — run: go test ./internal/worker/... -v -race"
fi

# ─────────────────────────────────────────────────────────────────────────────
banner "STEP 3 — all binaries build + full unit suite"
# ─────────────────────────────────────────────────────────────────────────────

if go build -o /dev/null ./cmd/server 2>/dev/null; then
    pass "cmd/server builds (orchestrator wired into NewRouter)"
else
    fail "cmd/server build failed — run: go build ./cmd/server"
fi

if go build -o /dev/null ./cmd/worker 2>/dev/null; then
    pass "cmd/worker builds (heartbeater + status callbacks wired)"
else
    fail "cmd/worker build failed — run: go build ./cmd/worker"
fi

if go build -o /dev/null ./cmd/smokecheck 2>/dev/null; then
    pass "cmd/smokecheck builds"
else
    fail "cmd/smokecheck build failed"
fi

echo ""
info "Running full unit test suite..."
if go test $(go list ./... 2>/dev/null | grep -v 'docker/sandbox') \
       -race -timeout 60s -count=1 2>/dev/null; then
    pass "full unit test suite passes — no regressions"
else
    fail "full unit test suite has failures — run: make test"
fi

# ─────────────────────────────────────────────────────────────────────────────
banner "STEP 4 — live stack: orchestrator routes are reachable"
# ─────────────────────────────────────────────────────────────────────────────

if [ "$STACK_RUNNING" != "1" ]; then
    echo ""
    echo "  ⚠  STACK_RUNNING not set — skipping live checks (Steps 4-6)."
    info "Start stack: docker compose up --build -d"
    info "Then re-run: STACK_RUNNING=1 bash $0"
    echo ""
    echo "╔════════════════════════════════════════════════════════╗"
    printf  "║  Results:  %3d passed  %3d failed  (offline only)    ║\n" "$PASS" "$FAIL"
    echo "╚════════════════════════════════════════════════════════╝"
    [ "$FAIL" -eq 0 ] && exit 0 || exit 1
fi

# Control plane health
echo ""
info "Checking control plane health ..."
HEALTH=$(curl -sf "$CONTROL_PLANE/health" 2>/dev/null || echo "")
if echo "$HEALTH" | grep -q "ok"; then
    pass "Control plane healthy at $CONTROL_PLANE"
else
    fail "Control plane not responding — run: docker compose up --build -d"
    echo ""
    echo "╔════════════════════════════════════════════════════════╗"
    printf  "║  Results:  %3d passed  %3d failed                     ║\n" "$PASS" "$FAIL"
    echo "╚════════════════════════════════════════════════════════╝"
    exit 1
fi

# Check orchestrator routes are mounted (DISTRIBUTED_MODE=true)
echo ""
info "Checking /internal/workers route is mounted ..."
WORKERS_RESP=$(curl -s -o /dev/null -w "%{http_code}" \
    "$CONTROL_PLANE/internal/workers" 2>/dev/null || echo "000")

if [ "$WORKERS_RESP" = "200" ]; then
    pass "GET /internal/workers returned 200 — orchestrator routes are mounted"
else
    fail "GET /internal/workers returned $WORKERS_RESP (expected 200)"
    info "Check DISTRIBUTED_MODE=true in docker-compose.yml"
    info "Check server logs: docker compose logs control-plane | grep orchestrator"
fi

# ─────────────────────────────────────────────────────────────────────────────
banner "STEP 5 — worker self-registers via heartbeater"
# ─────────────────────────────────────────────────────────────────────────────

echo ""
info "Waiting up to 20s for worker to register and send first heartbeat ..."

REGISTERED=0
for i in $(seq 1 20); do
    WORKERS_JSON=$(curl -sf "$CONTROL_PLANE/internal/workers" 2>/dev/null || echo "{}")
    COUNT=$(echo "$WORKERS_JSON" | grep -o '"count":[0-9]*' | grep -o '[0-9]*' || echo "0")
    if [ "${COUNT:-0}" -gt 0 ] 2>/dev/null; then
        REGISTERED=1
        break
    fi
    sleep 1
done

if [ "$REGISTERED" = "1" ]; then
    pass "Worker registered with orchestrator (count=$COUNT)"
    info "Worker list: $(curl -sf "$CONTROL_PLANE/internal/workers" 2>/dev/null || echo '(fetch failed)')"
else
    fail "No workers registered after 20s"
    info "Check worker logs: docker compose logs worker | grep -E 'register|heartbeat'"
    info "Check ORCHESTRATOR_URL=http://control-plane:8080 in docker-compose.yml"
fi

# ─────────────────────────────────────────────────────────────────────────────
banner "STEP 6 — heartbeat keeps worker alive + stats correct"
# ─────────────────────────────────────────────────────────────────────────────

echo ""
info "Waiting 10s (2 heartbeat intervals) then checking worker is still alive ..."
sleep 10

WORKERS_JSON=$(curl -sf "$CONTROL_PLANE/internal/workers" 2>/dev/null || echo "{}")
DEAD_COUNT=$(echo "$WORKERS_JSON" | grep -o '"status":"dead"' | wc -l | tr -d ' ')

if [ "${DEAD_COUNT:-0}" -eq 0 ] 2>/dev/null; then
    pass "No dead workers after 10s — heartbeat is keeping workers alive"
else
    fail "$DEAD_COUNT worker(s) are dead — heartbeat may have stopped"
    info "Check worker logs: docker compose logs worker | grep heartbeat"
fi

# Check /internal/workers/stats
echo ""
info "Checking /internal/workers/stats ..."
STATS=$(curl -sf "$CONTROL_PLANE/internal/workers/stats" 2>/dev/null || echo "{}")
TOTAL=$(echo "$STATS" | grep -o '"total":[0-9]*' | grep -o '[0-9]*' || echo "0")

if [ "${TOTAL:-0}" -gt 0 ] 2>/dev/null; then
    pass "GET /internal/workers/stats shows total=$TOTAL"
    info "Stats: $STATS"
else
    fail "GET /internal/workers/stats shows total=0"
fi

# ─────────────────────────────────────────────────────────────────────────────
echo ""
echo "╔════════════════════════════════════════════════════════╗"
printf  "║  Results:  %3d passed  %3d failed                     ║\n" "$PASS" "$FAIL"
echo "╚════════════════════════════════════════════════════════╝"
echo ""

if [ "$FAIL" -eq 0 ]; then
    echo "  ✅  Stage 3.2 complete — orchestrator + heartbeat working."
    exit 0
else
    echo "  ❌  Stage 3.2 incomplete — $FAIL check(s) failed."
    exit 1
fi
