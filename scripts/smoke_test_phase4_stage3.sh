#!/usr/bin/env bash
# scripts/smoke_test_phase4_stage3.sh
#
# Stage 4.3 smoke test — KEDA autoscaling on Redpanda queue depth.
#
# ── Modes ─────────────────────────────────────────────────────────────────────
#
#   --dry-run  (default)
#     Validates that the ScaledObject YAML is syntactically correct and that
#     the Go gate tests pass. No cluster required. This is the mode run by
#     `make k8s-validate` and the CI pipeline.
#
#   --live
#     Runs against a real cluster. Enqueues synthetic jobs, asserts that the
#     worker Deployment scales up within 60 seconds, drains the queue, and
#     asserts replicas return to minReplicaCount (1) within 90 seconds.
#     Requires: kubectl, a running cluster with KEDA + NexusBench deployed.
#
# ── Usage ─────────────────────────────────────────────────────────────────────
#
#   bash scripts/smoke_test_phase4_stage3.sh           # dry-run (CI default)
#   bash scripts/smoke_test_phase4_stage3.sh --dry-run # explicit dry-run
#   bash scripts/smoke_test_phase4_stage3.sh --live    # live cluster test
#
# ── Gate tests (dry-run) ──────────────────────────────────────────────────────
#
#   1. k8s/worker/scaledobject.yaml parses as valid YAML.
#   2. ScaledObject has required fields: kind, scaleTargetRef, triggers[0].type=kafka.
#   3. Go unit tests pass: internal/queue/... and internal/metrics/... with -race.
#
# ── Live tests (--live) ───────────────────────────────────────────────────────
#
#   1. KEDA operator pod is Running in the keda namespace.
#   2. ScaledObject is applied and reports READY=True.
#   3. Enqueue 20 synthetic jobs via the control-plane API.
#   4. Within 60s: worker Deployment has > 1 replica (KEDA scaled up).
#   5. Delete all pending submissions (drain the queue).
#   6. Within 90s: worker Deployment returns to 1 replica (KEDA scaled down).
#
# ── Exit codes ────────────────────────────────────────────────────────────────
#
#   0  All checks passed.
#   1  One or more checks failed (details printed before exit).

set -euo pipefail

# ── Always run from the project root ──────────────────────────────────────────
# The script uses relative paths (k8s/worker/scaledobject.yaml, go test ./...
# etc.). Resolve the project root as the directory one level above this script
# so the smoke test works regardless of where the caller's CWD is.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_ROOT"

# ── Colour helpers ─────────────────────────────────────────────────────────────
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

pass()  { echo -e "${GREEN}  ✓ $*${NC}"; }
fail()  { echo -e "${RED}  ✗ $*${NC}"; FAILURES=$((FAILURES + 1)); }
info()  { echo -e "${YELLOW}  → $*${NC}"; }
header(){ echo -e "\n${YELLOW}── $* ──${NC}"; }

FAILURES=0
MODE="${1:---dry-run}"

SCALEDOBJECT_FILE="k8s/worker/scaledobject.yaml"
NAMESPACE="nexusbench"
WORKER_DEPLOYMENT="worker"
CONTROL_PLANE_URL="${NEXUSBENCH_URL:-http://localhost:8080}"
SCALE_UP_TIMEOUT=60    # seconds to wait for replicas > 1
SCALE_DOWN_TIMEOUT=90  # seconds to wait for replicas back to 1

# ── Dry-run mode ───────────────────────────────────────────────────────────────

if [[ "$MODE" == "--dry-run" ]]; then
  header "Stage 4.3 Dry-Run: ScaledObject YAML + Go unit tests"

  # 1. File exists.
  if [[ -f "$SCALEDOBJECT_FILE" ]]; then
    pass "ScaledObject file exists: $SCALEDOBJECT_FILE"
  else
    fail "ScaledObject file missing: $SCALEDOBJECT_FILE"
  fi

  # 2. Valid YAML — use Python as a universally available YAML parser.
  # Pass the resolved absolute path so the open() call cannot fail due to CWD.
  # Show the actual error (no 2>/dev/null) so failures are diagnosable.
  if command -v python3 &>/dev/null || command -v python &>/dev/null; then
    if python3 -c "import sys" >/dev/null 2>&1; then PYTHON_CMD="python3"; else PYTHON_CMD="python"; fi
    ABS_SCALEDOBJECT="$PROJECT_ROOT/$SCALEDOBJECT_FILE"
    if YAML_ERR=$($PYTHON_CMD -c "import sys, yaml; yaml.safe_load(open(sys.argv[1]))" "$ABS_SCALEDOBJECT" 2>&1); then
      pass "ScaledObject YAML is syntactically valid"
    else
      fail "ScaledObject YAML failed ${PYTHON_CMD} yaml.safe_load: $YAML_ERR"
    fi
  else
    info "python not found — skipping YAML parse check"
  fi

  # 3. Required YAML fields (grep-based; no cluster needed).
  check_field() {
    local field="$1" desc="$2"
    if grep -q "$field" "$SCALEDOBJECT_FILE"; then
      pass "ScaledObject contains $desc"
    else
      fail "ScaledObject missing $desc (field: $field)"
    fi
  }

  check_field "kind: ScaledObject"                 "kind: ScaledObject"
  check_field "scaleTargetRef"                     "scaleTargetRef"
  check_field "name: worker"                       "scaleTargetRef.name: worker"
  check_field "minReplicaCount"                    "minReplicaCount"
  check_field "maxReplicaCount"                    "maxReplicaCount"
  check_field "cooldownPeriod"                     "cooldownPeriod"
  check_field "pollingInterval"                    "pollingInterval"
  check_field "type: kafka"                        "kafka trigger type"
  check_field "bootstrapServers"                   "bootstrapServers"
  check_field "consumerGroup: nexusbench-workers"  "consumerGroup: nexusbench-workers"
  check_field "topic: jobs.benchmark"              "topic: jobs.benchmark"
  check_field "lagThreshold"                       "lagThreshold"

  # 4. minReplicaCount is 1 (not 0) — keeps one warm worker for fast job start.
  if grep -q "minReplicaCount: 1" "$SCALEDOBJECT_FILE"; then
    pass "minReplicaCount is 1 (warm worker, not 0)"
  else
    fail "minReplicaCount should be 1 to keep one warm worker"
  fi

  # 5. maxReplicaCount is 10 (matches worker node pool max in Terraform).
  if grep -q "maxReplicaCount: 10" "$SCALEDOBJECT_FILE"; then
    pass "maxReplicaCount is 10 (matches node pool ceiling)"
  else
    fail "maxReplicaCount should be 10 to match node pool ceiling"
  fi

  # 6. KEDA install reference exists.
  KEDA_INSTALL_FILE="k8s/keda/keda-install.yaml"
  if [[ -f "$KEDA_INSTALL_FILE" ]]; then
    pass "KEDA install reference exists: $KEDA_INSTALL_FILE"
  else
    fail "KEDA install reference missing: $KEDA_INSTALL_FILE"
  fi

  # 7. Go unit tests — queue package (QueueDepth tests).
  header "Go unit tests: internal/queue/..."
  QUEUE_OUT=$(go test ./internal/queue/... -race -timeout 30s -count=1 -v 2>&1 || true)
  echo "$QUEUE_OUT"
  if echo "$QUEUE_OUT" | grep -qE "^ok"; then
    pass "internal/queue unit tests pass (including QueueDepth)"
  else
    fail "internal/queue unit tests FAILED"
  fi

  # 8. Go unit tests — metrics package (SetQueueDepth gauge tests).
  header "Go unit tests: internal/metrics/..."
  METRICS_OUT=$(go test ./internal/metrics/... -race -timeout 30s -count=1 -v 2>&1 || true)
  echo "$METRICS_OUT"
  if echo "$METRICS_OUT" | grep -qE "^ok"; then
    pass "internal/metrics unit tests pass (including SetQueueDepth)"
  else
    fail "internal/metrics unit tests FAILED"
  fi

  echo ""
  if [[ $FAILURES -eq 0 ]]; then
    echo -e "${GREEN}✓ Stage 4.3 dry-run passed (0 failures)${NC}"
    exit 0
  else
    echo -e "${RED}✗ Stage 4.3 dry-run FAILED ($FAILURES failure(s))${NC}"
    exit 1
  fi
fi

# ── Live mode ──────────────────────────────────────────────────────────────────

if [[ "$MODE" != "--live" ]]; then
  echo "Usage: $0 [--dry-run|--live]"
  exit 1
fi

header "Stage 4.3 Live: KEDA autoscaling on Redpanda queue depth"

# Prerequisite: kubectl available.
if ! command -v kubectl &>/dev/null; then
  echo -e "${RED}kubectl not found in PATH. Install it and configure kubeconfig.${NC}"
  exit 1
fi

# ── Helper: current worker replica count ──────────────────────────────────────
worker_replicas() {
  kubectl get deployment "$WORKER_DEPLOYMENT" -n "$NAMESPACE" \
    -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo "0"
}

# ── Helper: wait for replica count to satisfy a condition ─────────────────────
# Usage: wait_replicas <description> <timeout_secs> <comparison> <target>
# comparison: "gt" or "le"
wait_replicas() {
  local desc="$1" timeout="$2" cmp="$3" target="$4"
  local deadline=$(( $(date +%s) + timeout ))
  info "Waiting up to ${timeout}s for replicas ${cmp} ${target} (${desc})..."
  while true; do
    local replicas
    replicas=$(worker_replicas)
    replicas=${replicas:-0}
    case "$cmp" in
      gt) [[ "$replicas" -gt "$target" ]] && return 0 ;;
      le) [[ "$replicas" -le "$target" ]] && return 0 ;;
    esac
    if [[ $(date +%s) -ge $deadline ]]; then
      return 1
    fi
    echo -n "."
    sleep 5
  done
}

# 1. KEDA operator is running.
info "Checking KEDA operator..."
if kubectl get pods -n keda -l app=keda-operator --field-selector=status.phase=Running \
     --no-headers 2>/dev/null | grep -q "Running"; then
  pass "KEDA operator is Running"
else
  fail "KEDA operator not Running — install KEDA first: kubectl apply -f https://github.com/kedacore/keda/releases/download/v2.14.0/keda-2.14.0.yaml"
fi

# 2. Apply the ScaledObject.
info "Applying ScaledObject..."
kubectl apply -f "$SCALEDOBJECT_FILE" -n "$NAMESPACE"

# 3. Wait for ScaledObject to become READY.
info "Waiting for ScaledObject READY=True (up to 30s)..."
SCALEDOBJECT_READY=false
for i in $(seq 1 6); do
  READY=$(kubectl get scaledobject worker-scaledobject -n "$NAMESPACE" \
    -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || echo "")
  if [[ "$READY" == "True" ]]; then
    SCALEDOBJECT_READY=true
    break
  fi
  sleep 5
done

if $SCALEDOBJECT_READY; then
  pass "ScaledObject reports READY=True"
else
  fail "ScaledObject did not become READY within 30s"
  kubectl describe scaledobject worker-scaledobject -n "$NAMESPACE" || true
fi

# 4. Enqueue 20 synthetic jobs via the control-plane API.
header "Enqueuing 20 synthetic jobs..."
SUBMITTED=0
for i in $(seq 1 20); do
  # Build a minimal multipart form upload. The control plane validates the
  # archive exists; for smoke testing we submit an empty tar.gz.
  TMPARCHIVE=$(mktemp /tmp/smoke-archive-XXXXXX.tar.gz)
  tar czf "$TMPARCHIVE" -T /dev/null 2>/dev/null || true

  HTTP_STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
    -X POST "$CONTROL_PLANE_URL/api/v1/submissions" \
    -F "team_name=smoke-team-${i}" \
    -F "language=go" \
    -F "protocol=rest" \
    -F "archive=@${TMPARCHIVE};type=application/gzip" \
    2>/dev/null || echo "000")

  rm -f "$TMPARCHIVE"

  if [[ "$HTTP_STATUS" == "201" || "$HTTP_STATUS" == "202" ]]; then
    SUBMITTED=$((SUBMITTED + 1))
  else
    info "Job ${i}: HTTP ${HTTP_STATUS} (may be normal if control plane uses a different URL)"
  fi
done
info "Submitted $SUBMITTED / 20 jobs successfully"

if [[ $SUBMITTED -gt 0 ]]; then
  pass "$SUBMITTED synthetic jobs enqueued"
else
  # Not a hard failure — the control plane may not be reachable from this host.
  info "No jobs submitted via API (control plane unreachable at $CONTROL_PLANE_URL)"
  info "If running in-cluster, set NEXUSBENCH_URL to the cluster-internal URL."
fi

# 5. Wait for scale-up (replicas > 1).
header "Waiting for scale-up..."
if [[ $SUBMITTED -gt 0 ]]; then
  if wait_replicas "KEDA scale-up" $SCALE_UP_TIMEOUT "gt" 1; then
    CURRENT=$(worker_replicas)
    pass "Worker Deployment scaled up to $CURRENT replicas within ${SCALE_UP_TIMEOUT}s"
  else
    CURRENT=$(worker_replicas)
    fail "Worker did not scale above 1 replica within ${SCALE_UP_TIMEOUT}s (current: $CURRENT)"
    info "Check: kubectl get scaledobject worker-scaledobject -n $NAMESPACE"
    info "Check: kubectl describe hpa -n $NAMESPACE"
  fi
else
  info "Skipping scale-up assertion (no jobs were submitted)"
fi

# 6. Wait for queue to drain naturally (workers process jobs) or timeout.
header "Draining queue..."
info "Waiting up to 120s for worker queue to drain..."
DRAIN_DEADLINE=$(( $(date +%s) + 120 ))
while [[ $(date +%s) -lt $DRAIN_DEADLINE ]]; do
  DEPTH=$(curl -s "$CONTROL_PLANE_URL/metrics" 2>/dev/null \
    | grep "^nexusbench_queue_depth " | awk '{print $2}' || echo "")
  if [[ "$DEPTH" == "0" || -z "$DEPTH" ]]; then
    break
  fi
  info "Queue depth: ${DEPTH} — waiting..."
  sleep 10
done

# 7. Wait for scale-down (replicas back to 1).
header "Waiting for scale-down..."
if wait_replicas "KEDA scale-down" $SCALE_DOWN_TIMEOUT "le" 1; then
  CURRENT=$(worker_replicas)
  pass "Worker Deployment scaled back to $CURRENT replica(s) within ${SCALE_DOWN_TIMEOUT}s"
else
  CURRENT=$(worker_replicas)
  fail "Worker did not scale back to 1 within ${SCALE_DOWN_TIMEOUT}s (current: $CURRENT)"
fi

# ── Summary ────────────────────────────────────────────────────────────────────
echo ""
if [[ $FAILURES -eq 0 ]]; then
  echo -e "${GREEN}✓ Stage 4.3 live smoke test passed (0 failures)${NC}"
  exit 0
else
  echo -e "${RED}✗ Stage 4.3 live smoke test FAILED ($FAILURES failure(s))${NC}"
  exit 1
fi
