#!/usr/bin/env bash
# scripts/smoke_test.sh
# End-to-end smoke test against a running NexusBench control plane.
#
# Usage:
#   bash scripts/smoke_test.sh
#   NEXUS_URL=http://localhost:8080 bash scripts/smoke_test.sh

BASE="${NEXUS_URL:-http://localhost:8080}"
PASS=0
FAIL=0

# ── helpers ───────────────────────────────────────────────────────────────────

green()  { echo "  ✓ $*"; }
red()    { echo "  ✗ $*"; }

check() {
    local desc="$1"
    local expected="$2"
    local actual="$3"
    if echo "$actual" | grep -q "$expected"; then
        green "$desc"
        PASS=$((PASS + 1))
    else
        red "$desc"
        echo "    expected to contain: $expected"
        echo "    actual response:     $actual"
        FAIL=$((FAIL + 1))
    fi
}

# Safe curl — never exits the script on failure
get() {
    curl -s --max-time 10 "$BASE$1"
}

post_form() {
    curl -s --max-time 30 -X POST "$BASE$1" "${@:2}"
}

# ── banner ────────────────────────────────────────────────────────────────────

echo ""
echo "═══════════════════════════════════════"
echo " NexusBench Smoke Test → $BASE"
echo "═══════════════════════════════════════"

# ── 1. Health ─────────────────────────────────────────────────────────────────
echo ""
echo "1. Health endpoint"
RESP=$(get /health)
check "status ok"      '"status":"ok"'  "$RESP"
check "service field"  '"service"'      "$RESP"

# ── 2. Images endpoint ────────────────────────────────────────────────────────
echo ""
echo "2. Images endpoint"
RESP=$(get /api/v1/images)
check "has images field" '"images"' "$RESP"
check "go image present" '"go"'     "$RESP"

# ── 3. List submissions ───────────────────────────────────────────────────────
echo ""
echo "3. List submissions"
RESP=$(get /api/v1/submissions)
check "has count field"       '"count"'       "$RESP"
check "has submissions field" '"submissions"' "$RESP"

# ── 4. Validation — missing archive ──────────────────────────────────────────
echo ""
echo "4. Validation — missing archive"
RESP=$(post_form /api/v1/submissions \
    -F "team_name=test" \
    -F "language=go" \
    -F "protocol=rest")
check "returns error code" '"code"' "$RESP"

# ── 5. Validation — bad language ──────────────────────────────────────────────
echo ""
echo "5. Validation — unsupported language"
RESP=$(post_form /api/v1/submissions \
    -F "team_name=test" \
    -F "language=cobol" \
    -F "protocol=rest")
check "returns error for bad language" '"code"' "$RESP"

# ── 6. Real submission ────────────────────────────────────────────────────────
echo ""
echo "6. Submit a real Go archive"

# Build archive in a temp dir that works on Windows (Git Bash) and Linux
TMPDIR_SMOKE="${TEMP:-/tmp}/nexusbench-smoke-$$"
mkdir -p "$TMPDIR_SMOKE"

cat > "$TMPDIR_SMOKE/main.go" << 'GOEOF'
package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
)

func main() {
	port := os.Getenv("NEXUS_LISTEN_PORT")
	if port == "" {
		port = "7878"
	}
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})
	http.HandleFunc("/order", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "ack", "order_id": "smoke-001"})
	})
	fmt.Printf("smoke engine listening on :%s\n", port)
	http.ListenAndServe(":"+port, nil)
}
GOEOF

cat > "$TMPDIR_SMOKE/go.mod" << 'MODEOF'
module smoke-engine
go 1.22
MODEOF

ARCHIVE_PATH="$TMPDIR_SMOKE/smoke.tar.gz"
tar --no-same-owner -czf "$ARCHIVE_PATH" -C "$TMPDIR_SMOKE" main.go go.mod

RESP=$(post_form /api/v1/submissions \
    -F "team_name=smoke-team" \
    -F "language=go" \
    -F "protocol=rest" \
    -F "archive=@$ARCHIVE_PATH")

check "returns id field"     '"id"'       "$RESP"
check "correct team name"    "smoke-team" "$RESP"
check "status is pending"    '"status"'   "$RESP"

# Extract submission ID portably (python3 or grep fallback)
SUB_ID=""
if command -v python3 &>/dev/null; then
    SUB_ID=$(echo "$RESP" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('id',''))" 2>/dev/null || true)
fi
if [ -z "$SUB_ID" ]; then
    # grep fallback — works without python3
    SUB_ID=$(echo "$RESP" | grep -o '"id":"[^"]*"' | head -1 | cut -d'"' -f4)
fi

# ── 7. Get submission by ID ───────────────────────────────────────────────────
echo ""
echo "7. Get submission by ID"
if [ -n "$SUB_ID" ]; then
    RESP=$(get "/api/v1/submissions/$SUB_ID")
    check "id matches in response" "$SUB_ID" "$RESP"
    check "has status field"       '"status"'  "$RESP"
    check "has language field"     '"language"' "$RESP"
else
    red "could not extract submission ID — skipping"
    FAIL=$((FAIL + 1))
fi

# ── 8. Poll for running status (wait up to 30s) ───────────────────────────────
echo ""
echo "8. Wait for container to reach 'running' status"
FINAL_STATUS="unknown"
if [ -n "$SUB_ID" ]; then
    for i in $(seq 1 10); do
        sleep 3
        POLL=$(get "/api/v1/submissions/$SUB_ID")
        FINAL_STATUS=$(echo "$POLL" | grep -o '"status":"[^"]*"' | head -1 | cut -d'"' -f4)
        echo "    poll $i/10 → status: $FINAL_STATUS"
        if [ "$FINAL_STATUS" = "running" ] || [ "$FINAL_STATUS" = "failed" ]; then
            break
        fi
    done
    check "status is running" '"running"' "\"$FINAL_STATUS\""
fi

# ── 9. Leaderboard ────────────────────────────────────────────────────────────
echo ""
echo "9. Leaderboard endpoint"
RESP=$(get /api/v1/leaderboard)
check "has entries field" '"entries"' "$RESP"
check "has count field"   '"count"'   "$RESP"

# ── 10. Stop the test container ───────────────────────────────────────────────
echo ""
echo "10. Stop test container"
if [ -n "$SUB_ID" ] && [ "$FINAL_STATUS" = "running" ]; then
    RESP=$(post_form "/api/v1/submissions/$SUB_ID/stop")
    check "stop returns stopped status" '"stopped"' "$RESP"
else
    echo "    (skipped — container not in running state)"
fi

# ── cleanup ───────────────────────────────────────────────────────────────────
rm -rf "$TMPDIR_SMOKE"

# ── results ───────────────────────────────────────────────────────────────────
echo ""
echo "═══════════════════════════════════════"
echo " Results: ${PASS} passed, ${FAIL} failed"
echo "═══════════════════════════════════════"
echo ""

if [ "$FAIL" -eq 0 ]; then
    echo " Phase 1 complete — all checks passed."
    exit 0
else
    echo " Some checks failed — see details above."
    exit 1
fi
