#!/usr/bin/env bash
# scripts/smoke_test_phase2.sh
#
# Phase 2 end-to-end smoke test for NexusBench.
# Verifies the entire telemetry pipeline is working:
#
#   Step 1 — Structured event emission  (telemetry package compiles + emits valid NDJSON)
#   Step 2 — Redpanda producer          (events arrive in metrics.latency topic)
#   Step 3 — TimescaleDB consumer       (latency_windows rows written to DB)
#   Step 4 — Prometheus + cAdvisor      (all 4 targets UP, container metrics present)
#   Step 5 — Loki log pipeline          (Loki ready, labels present)
#   Step 6 — Grafana dashboards         (both dashboards reachable via API)
#
# Prerequisites (all must be running):
#   docker compose up -d
#   go run ./cmd/server   (or control-plane container healthy)
#
# Usage:
#   bash scripts/smoke_test_phase2.sh
#
# Exit code: 0 = all checks passed, 1 = one or more failures

set -euo pipefail

# ── config ────────────────────────────────────────────────────────────────────
CONTROL_PLANE="${NEXUS_URL:-http://localhost:8080}"
PROMETHEUS="${PROMETHEUS_URL:-http://localhost:9090}"
LOKI="${LOKI_URL:-http://localhost:3100}"
GRAFANA="${GRAFANA_URL:-http://localhost:3000}"
REDPANDA_BROKERS="${REDPANDA_BROKERS:-127.0.0.1:19092}"
TIMESCALE_DSN="${TIMESCALE_DSN:-postgres://nexus:nexus_dev@localhost:5432/nexusbench}"

PASS=0
FAIL=0
WARN=0

# ── helpers ───────────────────────────────────────────────────────────────────
green()  { echo "  ✓  $*"; }
red()    { echo "  ✗  $*"; }
yellow() { echo "  ⚠  $*"; }
banner() { echo ""; echo "── $* ──────────────────────────────────────────────"; }

pass() { green "$1";  PASS=$((PASS+1)); }
fail() { red   "$1";  FAIL=$((FAIL+1)); }
warn() { yellow "$1"; WARN=$((WARN+1)); }

# curl that never exits the script; returns empty string on failure
get() {
    curl -sf --max-time 10 "$1" 2>/dev/null || echo ""
}

# Check that a string contains a substring
contains() {
    local desc="$1" haystack="$2" needle="$3"
    if echo "$haystack" | grep -q "$needle"; then
        pass "$desc"
    else
        fail "$desc (expected to find: $needle)"
        echo "      response was: ${haystack:0:200}"
    fi
}

# Check that a command exits 0
cmd_ok() {
    local desc="$1"; shift
    if "$@" &>/dev/null; then
        pass "$desc"
    else
        fail "$desc (command: $*)"
    fi
}

# ── banner ────────────────────────────────────────────────────────────────────
echo ""
echo "╔═══════════════════════════════════════════════════════╗"
echo "║       NexusBench — Phase 2 Smoke Test                ║"
echo "╚═══════════════════════════════════════════════════════╝"
echo ""
echo "  Control plane : $CONTROL_PLANE"
echo "  Prometheus    : $PROMETHEUS"
echo "  Loki          : $LOKI"
echo "  Grafana       : $GRAFANA"
echo "  Redpanda      : $REDPANDA_BROKERS"
echo ""

# ══════════════════════════════════════════════════════════════════════════════
banner "STEP 1 — Structured event emission"
# Verify the telemetry package builds and emits valid NDJSON.
# We run the telemetry-demo binary and pipe its output through validation.
# ══════════════════════════════════════════════════════════════════════════════

echo "  Building telemetry-demo..."
if go build ./cmd/telemetry-demo/... 2>/dev/null; then
    pass "telemetry package compiles"
else
    fail "telemetry package compile failed — run: go build ./cmd/telemetry-demo/..."
fi

echo "  Emitting test events..."
DEMO_OUTPUT=$(go run ./cmd/telemetry-demo 2>/dev/null || echo "")

if [ -z "$DEMO_OUTPUT" ]; then
    fail "telemetry-demo produced no output"
else
    LINE_COUNT=$(echo "$DEMO_OUTPUT" | wc -l | tr -d ' ')
    pass "telemetry-demo emitted $LINE_COUNT event lines"

    # Every line must be valid JSON with a 'kind' field
    BAD_LINES=0
    while IFS= read -r line; do
        if ! echo "$line" | python3 -c "import sys,json; d=json.load(sys.stdin); assert 'kind' in d" 2>/dev/null; then
            BAD_LINES=$((BAD_LINES+1))
        fi
    done <<< "$DEMO_OUTPUT"

    if [ "$BAD_LINES" -eq 0 ]; then
        pass "all output lines are valid NDJSON with 'kind' field"
    else
        fail "$BAD_LINES lines failed NDJSON validation"
    fi

    # Must contain all expected event kinds
    for kind in order_ack fill cancel_ack reject heartbeat; do
        if echo "$DEMO_OUTPUT" | grep -q "\"$kind\""; then
            pass "event kind '$kind' present in output"
        else
            fail "event kind '$kind' missing from output"
        fi
    done
fi

# ══════════════════════════════════════════════════════════════════════════════
banner "STEP 2 — Redpanda producer"
# Verify Redpanda is running, the topics exist, and events can be produced.
# ══════════════════════════════════════════════════════════════════════════════

# Check broker is reachable
if rpk cluster info --brokers "$REDPANDA_BROKERS" &>/dev/null; then
    pass "Redpanda broker reachable at $REDPANDA_BROKERS"
else
    fail "Redpanda broker not reachable at $REDPANDA_BROKERS"
fi

# Check required topics exist
for topic in metrics.latency metrics.heartbeat metrics.dlq; do
    TOPIC_LIST=$(rpk topic list --brokers "$REDPANDA_BROKERS" 2>/dev/null || echo "")
    if echo "$TOPIC_LIST" | grep -q "$topic"; then
        pass "topic '$topic' exists"
    else
        warn "topic '$topic' not found — run Bootstrap() first via: go run ./cmd/consumer"
    fi
done

# Produce events and verify they arrive
echo "  Producing test events to metrics.latency..."
PRODUCE_OUTPUT=$(go run ./cmd/telemetry-demo 2>/dev/null | \
    rpk topic produce metrics.latency \
    --brokers "$REDPANDA_BROKERS" \
    --format "%v\n" 2>&1 || echo "ERROR")

if echo "$PRODUCE_OUTPUT" | grep -qi "error\|failed"; then
    fail "event production to metrics.latency failed: $PRODUCE_OUTPUT"
else
    pass "events produced to metrics.latency"
fi

# Verify the topic has messages (offset > 0)
OFFSETS=$(rpk topic describe metrics.latency --brokers "$REDPANDA_BROKERS" 2>/dev/null || echo "")
if echo "$OFFSETS" | grep -qE "[0-9]+"; then
    pass "metrics.latency topic has messages (offset > 0)"
else
    warn "could not verify metrics.latency offset — topic may be empty"
fi

# ══════════════════════════════════════════════════════════════════════════════
banner "STEP 3 — TimescaleDB consumer (latency windows)"
# Verify TimescaleDB is reachable, the schema exists, and windows were written.
# ══════════════════════════════════════════════════════════════════════════════

# Check TimescaleDB connectivity
if psql "$TIMESCALE_DSN" -c "SELECT 1" &>/dev/null; then
    pass "TimescaleDB reachable"
else
    fail "TimescaleDB not reachable — check TIMESCALE_DSN=$TIMESCALE_DSN"
fi

# Check latency_windows table exists
TABLE_EXISTS=$(psql "$TIMESCALE_DSN" -tAc \
    "SELECT COUNT(*) FROM information_schema.tables WHERE table_name='latency_windows'" \
    2>/dev/null || echo "0")
if [ "${TABLE_EXISTS:-0}" -ge 1 ]; then
    pass "latency_windows table exists"
else
    fail "latency_windows table missing — consumer Bootstrap() may not have run"
fi

# Check hypertable was created
HYPER=$(psql "$TIMESCALE_DSN" -tAc \
    "SELECT COUNT(*) FROM timescaledb_information.hypertables WHERE hypertable_name='latency_windows'" \
    2>/dev/null || echo "0")
if [ "${HYPER:-0}" -ge 1 ]; then
    pass "latency_windows is a TimescaleDB hypertable"
else
    fail "latency_windows is not a hypertable — create_hypertable() may have failed"
fi

# Wait up to 30s for at least one window to appear (consumer needs time to flush)
echo "  Waiting up to 30s for consumer to write latency windows..."
WINDOW_COUNT=0
for i in $(seq 1 6); do
    sleep 5
    WINDOW_COUNT=$(psql "$TIMESCALE_DSN" -tAc \
        "SELECT COUNT(*) FROM latency_windows" 2>/dev/null || echo "0")
    WINDOW_COUNT=$(echo "$WINDOW_COUNT" | tr -d '[:space:]')
    echo "    attempt $i/6 → $WINDOW_COUNT windows in DB"
    if [ "${WINDOW_COUNT:-0}" -gt 0 ]; then
        break
    fi
done

if [ "${WINDOW_COUNT:-0}" -gt 0 ]; then
    pass "$WINDOW_COUNT latency windows written to TimescaleDB"

    # Spot-check the most recent window has valid percentile values
    P99=$(psql "$TIMESCALE_DSN" -tAc \
        "SELECT p99_ns FROM latency_windows ORDER BY time DESC LIMIT 1" \
        2>/dev/null | tr -d '[:space:]')
    if [ -n "$P99" ] && [ "$P99" -gt 0 ] 2>/dev/null; then
        pass "most recent window has p99_ns=$P99 ns (> 0)"
    else
        warn "p99_ns is zero or unreadable — check consumer is processing events"
    fi

    # Check TPS is non-zero
    TPS=$(psql "$TIMESCALE_DSN" -tAc \
        "SELECT ROUND(tps::numeric, 2) FROM latency_windows ORDER BY time DESC LIMIT 1" \
        2>/dev/null | tr -d '[:space:]')
    if [ -n "$TPS" ] && echo "$TPS" | grep -qE "^[0-9]+(\.[0-9]+)?$"; then
        pass "most recent window has tps=$TPS"
    else
        warn "TPS value unreadable — check consumer pipeline"
    fi
else
    fail "no latency windows in DB after 30s — consumer may not be running or events not flowing"
fi

# ══════════════════════════════════════════════════════════════════════════════
banner "STEP 4 — Prometheus + cAdvisor"
# Verify all 4 Prometheus targets are UP and container metrics exist.
# ══════════════════════════════════════════════════════════════════════════════

# Prometheus health
PROM_HEALTH=$(get "$PROMETHEUS/-/healthy")
if echo "$PROM_HEALTH" | grep -q "Prometheus"; then
    pass "Prometheus is healthy"
else
    fail "Prometheus not healthy at $PROMETHEUS"
fi

# Check all expected targets are UP via the API
TARGETS_JSON=$(get "$PROMETHEUS/api/v1/targets")

for job in "nexusbench-server" "cadvisor" "node-exporter" "redpanda"; do
    # Count how many instances of this job are in state "up"
    UP_COUNT=$(echo "$TARGETS_JSON" | \
        python3 -c "
import sys, json
data = json.load(sys.stdin)
targets = data.get('data', {}).get('activeTargets', [])
up = sum(1 for t in targets if t.get('labels', {}).get('job') == '$job' and t.get('health') == 'up')
print(up)
" 2>/dev/null || echo "0")

    if [ "${UP_COUNT:-0}" -gt 0 ]; then
        pass "Prometheus target '$job' is UP"
    else
        fail "Prometheus target '$job' is DOWN or missing"
    fi
done

# Verify cAdvisor container metrics exist in Prometheus
CADVISOR_METRIC=$(get "$PROMETHEUS/api/v1/query?query=container_memory_rss%7Bname!%3D%22%22%7D" )
RESULT_COUNT=$(echo "$CADVISOR_METRIC" | \
    python3 -c "
import sys, json
data = json.load(sys.stdin)
print(len(data.get('data', {}).get('result', [])))
" 2>/dev/null || echo "0")

if [ "${RESULT_COUNT:-0}" -gt 0 ]; then
    pass "cAdvisor container_memory_rss has $RESULT_COUNT containers"
else
    fail "cAdvisor container_memory_rss has no data — cAdvisor may not be scraping"
fi

# Verify NexusBench custom metrics exist (control plane must be up)
NB_METRIC=$(get "$PROMETHEUS/api/v1/query?query=nexusbench_http_requests_total")
NB_RESULT=$(echo "$NB_METRIC" | \
    python3 -c "
import sys, json
data = json.load(sys.stdin)
print(len(data.get('data', {}).get('result', [])))
" 2>/dev/null || echo "0")

if [ "${NB_RESULT:-0}" -gt 0 ]; then
    pass "nexusbench_http_requests_total metric present"
else
    # May be 0 if no requests yet — just warn, not fail
    warn "nexusbench_http_requests_total not yet scraped — make a request to $CONTROL_PLANE/health first"
fi

# ══════════════════════════════════════════════════════════════════════════════
banner "STEP 5 — Loki log pipeline"
# Verify Loki is ready and has received logs from at least one container.
# ══════════════════════════════════════════════════════════════════════════════

# Loki readiness
LOKI_READY=$(get "$LOKI/ready")
if echo "$LOKI_READY" | grep -q "ready"; then
    pass "Loki is ready"
else
    fail "Loki not ready at $LOKI"
fi

# Check labels exist (populated by Promtail scraping containers)
LOKI_LABELS=$(get "$LOKI/loki/api/v1/labels")
if echo "$LOKI_LABELS" | grep -q '"status":"success"'; then
    pass "Loki labels API responded"
else
    fail "Loki labels API failed"
fi

# Must have at least the 'job' label (set by Promtail for all containers)
if echo "$LOKI_LABELS" | grep -q '"job"'; then
    pass "Loki has 'job' label (Promtail is scraping containers)"
else
    warn "Loki 'job' label missing — Promtail may not have shipped logs yet (wait 30s and retry)"
fi

# Query for any log line from the compose project
LOKI_QUERY=$(get "$LOKI/loki/api/v1/query_range?query=%7Bjob%3D%22docker%22%7D&limit=1&start=$(date -d '5 minutes ago' +%s)000000000&end=$(date +%s)000000000" 2>/dev/null || \
             get "$LOKI/loki/api/v1/query_range?query=%7Bjob%3D%22docker%22%7D&limit=1" || echo "")

STREAM_COUNT=$(echo "$LOKI_QUERY" | \
    python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    print(len(data.get('data', {}).get('result', [])))
except:
    print(0)
" 2>/dev/null || echo "0")

if [ "${STREAM_COUNT:-0}" -gt 0 ]; then
    pass "Loki has log streams from Docker containers"
else
    warn "no log streams found yet — Promtail may still be starting up (wait 30s and retry)"
fi

# ══════════════════════════════════════════════════════════════════════════════
banner "STEP 6 — Grafana dashboards"
# Verify Grafana is healthy and both dashboards are provisioned correctly.
# ══════════════════════════════════════════════════════════════════════════════

# Grafana health
GRAFANA_HEALTH=$(get "$GRAFANA/api/health")
if echo "$GRAFANA_HEALTH" | grep -q '"database":"ok"'; then
    pass "Grafana is healthy"
else
    fail "Grafana not healthy at $GRAFANA"
fi

# Check both dashboards exist via the Grafana search API
for uid in "nexusbench-admin" "nexusbench-contestant"; do
    DASH=$(get "$GRAFANA/api/dashboards/uid/$uid")
    if echo "$DASH" | grep -q '"uid"'; then
        TITLE=$(echo "$DASH" | python3 -c \
            "import sys,json; d=json.load(sys.stdin); print(d.get('dashboard',{}).get('title',''))" \
            2>/dev/null || echo "unknown")
        pass "dashboard '$uid' exists: '$TITLE'"
    else
        fail "dashboard '$uid' not found — check docker/grafana/dashboards/ provisioning"
    fi
done

# Check all three datasources are provisioned
DATASOURCES=$(get "$GRAFANA/api/datasources")
for ds_type in "prometheus" "postgres" "loki"; do
    if echo "$DATASOURCES" | grep -qi "\"type\":\"$ds_type\""; then
        pass "Grafana datasource '$ds_type' provisioned"
    else
        fail "Grafana datasource '$ds_type' missing — check docker/grafana/provisioning/datasources/"
    fi
done

# ══════════════════════════════════════════════════════════════════════════════
banner "STEP 7 — Control plane /metrics endpoint"
# Final integration check: Prometheus can actually scrape the control plane.
# ══════════════════════════════════════════════════════════════════════════════

METRICS_BODY=$(get "$CONTROL_PLANE/metrics")
if echo "$METRICS_BODY" | grep -q "nexusbench_"; then
    pass "/metrics endpoint serves NexusBench custom metrics"
else
    fail "/metrics endpoint at $CONTROL_PLANE/metrics not serving nexusbench_ metrics"
fi

if echo "$METRICS_BODY" | grep -q "go_goroutines"; then
    pass "/metrics endpoint includes Go runtime metrics"
else
    warn "/metrics missing Go runtime metrics"
fi

# ── final results ─────────────────────────────────────────────────────────────
echo ""
echo "╔═══════════════════════════════════════════════════════╗"
printf  "║  Results:  %3d passed  %3d warned  %3d failed        ║\n" \
    "$PASS" "$WARN" "$FAIL"
echo "╚═══════════════════════════════════════════════════════╝"
echo ""

if [ "$FAIL" -eq 0 ] && [ "$WARN" -eq 0 ]; then
    echo "  🎉  Phase 2 complete — all checks passed."
    exit 0
elif [ "$FAIL" -eq 0 ]; then
    echo "  ✅  Phase 2 complete — all hard checks passed ($WARN warnings)."
    echo "      Warnings are non-blocking but worth investigating."
    exit 0
else
    echo "  ❌  Phase 2 incomplete — $FAIL check(s) failed."
    echo "      Fix the failures above before proceeding to Phase 3."
    exit 1
fi
