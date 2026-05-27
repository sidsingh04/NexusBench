#!/usr/bin/env bash
# scripts/smoke_test_phase4_stage2.sh
#
# Stage 4.2 smoke test — validates K8s manifests and core connectivity.
#
# MODES
# ─────
#   --dry-run (default, no cluster needed)
#     Validates every manifest's YAML structure and K8s API schema offline
#     using kubeconform (https://github.com/yannh/kubeconform). kubeconform
#     embeds the official K8s JSON schemas locally — it never contacts a cluster.
#
#     If kubeconform is not installed, the script falls back to a YAML syntax
#     check via `python3 -c 'import yaml; yaml.safe_load_all(...)'` and prints
#     clear instructions for installing kubeconform.
#
#     NOTE: `kubectl apply --dry-run=client` is NOT used in this mode.
#     Despite the name "client", kubectl still contacts the cluster's API
#     discovery endpoint to resolve apiVersion/kind → REST resource. Without
#     a reachable cluster it always returns:
#       "the server could not find the requested resource"
#     kubeconform is the correct tool for offline schema validation.
#
#   --live (requires a running cluster with kubectl configured)
#     Applies all manifests, waits for rollouts, then runs HTTP and
#     NetworkPolicy connectivity checks.
#
#   --reset (requires a running cluster with kubectl configured)
#     Deletes the entire nexusbench namespace and all its PVCs, giving a
#     clean slate before a fresh --live run. Use this when PVC specs have
#     changed between runs (PVC spec is immutable after creation).
#
# USAGE
# ─────
#   bash scripts/smoke_test_phase4_stage2.sh               # offline dry-run
#   bash scripts/smoke_test_phase4_stage2.sh --live        # live cluster
#   bash scripts/smoke_test_phase4_stage2.sh --reset       # wipe and start fresh
#   bash scripts/smoke_test_phase4_stage2.sh --reset --live # reset then apply

set -euo pipefail

# Parse all arguments — support --reset combined with --live.
MODE="--dry-run"
DO_RESET=false
for arg in "$@"; do
  case "$arg" in
    --dry-run) MODE="--dry-run" ;;
    --live)    MODE="--live" ;;
    --reset)   DO_RESET=true ;;
    *) echo "Unknown argument: $arg"; exit 1 ;;
  esac
done

K8S_DIR="k8s"
NAMESPACE="nexusbench"
# 240s gives Redpanda adequate time on Docker Desktop (slow storage I/O).
# On GKE with pd-balanced storage, Redpanda typically becomes ready in 20-40s.
TIMEOUT="240s"

RED='\033[0;31m'
GRN='\033[0;32m'
YLW='\033[0;33m'
RST='\033[0m'

pass() { echo -e "${GRN}✓ $*${RST}"; }
fail() { echo -e "${RED}✗ $*${RST}"; exit 1; }
info() { echo -e "${YLW}▶ $*${RST}"; }
warn() { echo -e "${YLW}⚠ $*${RST}"; }

# ── Collect all manifest files (excluding non-YAML files like .gitkeep) ────────
collect_manifests() {
  find "${K8S_DIR}" -type f \( -name "*.yaml" -o -name "*.yml" \) \
    | grep -v '/secrets/' \
    | sort
}

# ── Mode: dry-run (offline schema validation) ──────────────────────────────────

if [[ "$MODE" == "--dry-run" ]]; then

  MANIFESTS=()
  while IFS= read -r f; do
    MANIFESTS+=("$f")
  done < <(collect_manifests)

  if [[ ${#MANIFESTS[@]} -eq 0 ]]; then
    fail "No manifest files found under ${K8S_DIR}/"
  fi

  info "Found ${#MANIFESTS[@]} manifest file(s) to validate"

  if command -v kubeconform >/dev/null 2>&1; then
    info "Using kubeconform for offline schema validation..."
    kubeconform \
      -strict \
      -ignore-missing-schemas \
      -summary \
      -output pretty \
      "${MANIFESTS[@]}" \
      && pass "All manifests passed kubeconform schema validation" \
      || fail "kubeconform found schema errors — fix them before applying to a cluster"

  elif python3 -c "import sys" >/dev/null 2>&1 || python -c "import sys" >/dev/null 2>&1; then
    if python3 -c "import sys" >/dev/null 2>&1; then PYTHON_CMD="python3"; else PYTHON_CMD="python"; fi
    warn "kubeconform not found — falling back to YAML syntax check only."
    warn "Install kubeconform for full K8s schema validation:"
    warn "  macOS:  brew install kubeconform"
    warn "  Linux:  go install github.com/yannh/kubeconform/cmd/kubeconform@latest"
    echo ""
    ERRORS=0
    for f in "${MANIFESTS[@]}"; do
      if $PYTHON_CMD -c "
import sys, yaml
try:
    list(yaml.safe_load_all(open('${f}')))
except yaml.YAMLError as e:
    print(f'YAML error: {e}', file=sys.stderr)
    sys.exit(1)
" 2>&1; then
        pass "  $f (YAML syntax OK)"
      else
        echo -e "${RED}✗   $f${RST}"
        ERRORS=$((ERRORS + 1))
      fi
    done
    [[ $ERRORS -gt 0 ]] && fail "${ERRORS} file(s) have YAML syntax errors"
    pass "All manifests passed YAML syntax check"
    warn "Install kubeconform to also validate K8s API schema"
  else
    fail "Neither kubeconform nor python found — install one to validate manifests"
  fi

  echo ""
  info "NetworkPolicy audit (run --live against a cluster to verify enforcement):"
  echo "  Workers should NOT reach timescaledb:5432"
  echo "  Workers SHOULD reach redpanda-client:9092"
  echo "  Control plane SHOULD reach redpanda-client:9092"
  exit 0
fi

# ── Shared prereq check for --reset and --live ─────────────────────────────────

command -v kubectl >/dev/null 2>&1 || fail "kubectl not found"
kubectl cluster-info >/dev/null 2>&1 \
  || fail "Cannot reach cluster — check your kubeconfig / VPN"

# ── Mode: --reset ──────────────────────────────────────────────────────────────
#
# PVC spec (storageClassName, accessModes, volumeMode) is immutable after
# creation. When these fields change between runs, `kubectl apply` is rejected
# by the API server with "spec is immutable". The only recovery is to delete
# the PVC and recreate it.
#
# Safe deletion sequence:
#   1. Scale all StatefulSets to 0 — releases the PVC mount.
#   2. Delete all PVCs in the namespace.
#   3. Delete the namespace itself (catches any remaining resources).
#
# Data loss warning: this destroys all Redpanda topic data and all TimescaleDB
# rows. Acceptable for a local dev smoke test; never run against production.

do_reset() {
  info "--reset: tearing down namespace '${NAMESPACE}' for a clean slate..."
  warn "This will DELETE all data in the namespace. Press Ctrl-C within 5s to abort."
  sleep 5

  # Scale StatefulSets to 0 first so pods release their PVC mounts.
  # kubectl scale is idempotent — safe if the StatefulSet doesn't exist yet.
  for ss in redpanda timescaledb; do
    if kubectl get statefulset "${ss}" -n "${NAMESPACE}" >/dev/null 2>&1; then
      info "  Scaling statefulset/${ss} to 0..."
      kubectl scale statefulset "${ss}" -n "${NAMESPACE}" --replicas=0
      # Wait for pods to terminate so the PVC is unmounted before deletion.
      kubectl wait --for=delete pod \
        -l "app.kubernetes.io/name=${ss}" \
        -n "${NAMESPACE}" \
        --timeout=60s 2>/dev/null || true
    fi
  done

  # Delete all PVCs in the namespace explicitly.
  # kubectl delete namespace would also delete them, but doing it explicitly
  # ensures PVCs are gone before the namespace finalizer runs.
  info "  Deleting all PVCs in namespace ${NAMESPACE}..."
  kubectl delete pvc --all -n "${NAMESPACE}" --ignore-not-found=true

  # Delete the namespace. This removes every other resource (Deployments,
  # Services, NetworkPolicies, ConfigMaps, RBAC, etc.) in one operation.
  info "  Deleting namespace ${NAMESPACE}..."
  kubectl delete namespace "${NAMESPACE}" --ignore-not-found=true

  # Wait for the namespace to be fully gone before proceeding.
  # The namespace enters Terminating state while finalizers run; we must
  # wait for it to disappear entirely before re-creating resources in it.
  info "  Waiting for namespace deletion to complete..."
  local waited=0
  while kubectl get namespace "${NAMESPACE}" >/dev/null 2>&1; do
    sleep 3
    waited=$((waited + 3))
    if [[ $waited -ge 60 ]]; then
      warn "Namespace is taking longer than 60s to terminate."
      warn "Check for stuck finalizers: kubectl get namespace ${NAMESPACE} -o yaml"
      break
    fi
  done

  pass "--reset complete — cluster is clean"
}

if [[ "$DO_RESET" == "true" ]]; then
  do_reset
  # If only --reset was requested (no --live), exit here.
  [[ "$MODE" != "--live" ]] && exit 0
fi

# ── Mode: --live ───────────────────────────────────────────────────────────────

if [[ "$MODE" != "--live" ]]; then
  fail "Unknown mode. Use --dry-run (default), --live, --reset, or --reset --live"
fi

info "Live mode: applying manifests to cluster (namespace=${NAMESPACE})"

# ── apply_pvc <file> ──────────────────────────────────────────────────────────
# PVC spec is immutable after creation. If a PVC already exists with a different
# storageClassName, kubectl apply fails with "spec is immutable".
#
# This function handles the three possible states:
#   1. PVC does not exist → apply normally (kubectl apply).
#   2. PVC exists with identical spec → apply is a no-op (kubectl apply).
#   3. PVC exists with different storageClassName → cannot patch; abort with
#      a clear message instructing the user to run --reset.
#
# We deliberately do NOT auto-delete PVCs here. Auto-deletion in the hot path
# destroys data silently. The user must opt-in to data destruction via --reset.
apply_pvc() {
  local file="$1"
  local pvc_name
  pvc_name=$(grep '^  name:' "${file}" | head -1 | awk '{print $2}')

  if ! kubectl get pvc "${pvc_name}" -n "${NAMESPACE}" >/dev/null 2>&1; then
    # PVC does not exist — create it.
    kubectl apply -f "${file}"
    pass "  PVC ${pvc_name} created"
    return 0
  fi

  # PVC exists. Try applying. If it fails due to immutability, recreate it safely.
  local apply_output
  if apply_output=$(kubectl apply -f "${file}" 2>&1); then
    pass "  PVC ${pvc_name} applied successfully"
    return 0
  fi

  if echo "${apply_output}" | grep -q "is immutable"; then
    warn "  PVC spec conflict for ${pvc_name} (spec is immutable)."
    warn "  Recreating PVC ${pvc_name}..."

    local target_workload=""
    if [[ "${pvc_name}" == "timescaledb-data" ]]; then
      target_workload="statefulset/timescaledb"
    elif [[ "${pvc_name}" == "submissions-data" ]]; then
      target_workload="deployment/control-plane"
    fi

    if [[ -n "${target_workload}" ]]; then
      if kubectl get "${target_workload}" -n "${NAMESPACE}" >/dev/null 2>&1; then
        info "    Scaling ${target_workload} to 0 to release PVC mount..."
        kubectl scale "${target_workload}" -n "${NAMESPACE}" --replicas=0
        local app_name="${target_workload#*/}"
        kubectl wait --for=delete pod -l "app.kubernetes.io/name=${app_name}" -n "${NAMESPACE}" --timeout=60s 2>/dev/null || true
      fi
    fi

    info "    Deleting PVC ${pvc_name}..."
    kubectl delete pvc "${pvc_name}" -n "${NAMESPACE}" --ignore-not-found=true
    
    info "    Creating PVC ${pvc_name} from manifest..."
    kubectl apply -f "${file}"
    pass "  PVC ${pvc_name} recreated"
  else
    echo -e "${RED}✗ Failed to apply PVC ${pvc_name}:${RST}"
    echo "${apply_output}"
    fail "kubectl apply failed on ${pvc_name}"
  fi
}

# ── Apply in dependency order ──────────────────────────────────────────────────

info "Applying namespace..."
kubectl apply -f "${K8S_DIR}/namespace.yaml"

info "Applying ConfigMaps..."
kubectl apply -f "${K8S_DIR}/configmaps/"

info "Applying RBAC..."
kubectl apply -f "${K8S_DIR}/rbac/"

info "Applying PVCs (immutability-safe)..."
apply_pvc "${K8S_DIR}/timescaledb/pvc.yaml"
apply_pvc "${K8S_DIR}/control-plane/pvc.yaml"

info "Applying Redpanda (service + StatefulSet)..."
if ! apply_output=$(kubectl apply -f "${K8S_DIR}/redpanda/" 2>&1); then
  retry_needed=false

  if echo "${apply_output}" | grep -q "Forbidden: updates to statefulset spec"; then
    warn "StatefulSet update forbidden (likely volumeClaimTemplates conflict)."
    warn "Recreating Redpanda StatefulSet and PVC..."
    if kubectl get statefulset redpanda -n "${NAMESPACE}" >/dev/null 2>&1; then
      info "  Deleting StatefulSet redpanda..."
      kubectl delete statefulset redpanda -n "${NAMESPACE}" --ignore-not-found=true
      kubectl wait --for=delete pod -l "app.kubernetes.io/name=redpanda" -n "${NAMESPACE}" --timeout=60s 2>/dev/null || true
    fi
    info "  Deleting PVC redpanda-data-redpanda-0..."
    kubectl delete pvc redpanda-data-redpanda-0 -n "${NAMESPACE}" --ignore-not-found=true
    retry_needed=true
  fi

  if echo "${apply_output}" | grep -q "may not change once set\|is invalid: spec.clusterIPs"; then
    warn "Service spec conflict (likely headless/ClusterIP change)."
    info "  Deleting Redpanda services..."
    kubectl delete service redpanda redpanda-client -n "${NAMESPACE}" --ignore-not-found=true
    retry_needed=true
  fi

  if [[ "${retry_needed}" == "true" ]]; then
    info "  Re-applying Redpanda manifests..."
    if ! reapply_output=$(kubectl apply -f "${K8S_DIR}/redpanda/" 2>&1); then
      echo -e "${RED}✗ Failed to re-apply Redpanda manifests:${RST}"
      echo "${reapply_output}"
      fail "kubectl apply failed on redpanda after resolving conflicts"
    else
      echo "${reapply_output}"
    fi
  else
    echo -e "${RED}✗ Failed to apply Redpanda manifests:${RST}"
    echo "${apply_output}"
    fail "kubectl apply failed on redpanda"
  fi
else
  # Print the successful output
  echo "${apply_output}"
fi

info "Applying TimescaleDB (service + StatefulSet)..."
# Apply service and StatefulSet but skip the PVC file (already handled above).
kubectl apply -f "${K8S_DIR}/timescaledb/statefulset.yaml"
kubectl apply -f "${K8S_DIR}/timescaledb/service.yaml"

info "Applying Deployments (control-plane, consumer, worker)..."
# Apply service, ingress, deployment — skip the PVC file (already handled above).
kubectl apply -f "${K8S_DIR}/control-plane/deployment.yaml"
kubectl apply -f "${K8S_DIR}/control-plane/service.yaml"
kubectl apply -f "${K8S_DIR}/control-plane/ingress.yaml"
kubectl apply -f "${K8S_DIR}/consumer/"
kubectl apply -f "${K8S_DIR}/worker/"

info "Applying NetworkPolicies..."
kubectl apply -f "${K8S_DIR}/network-policies/"

# ── rollout_wait <kind> <name> ────────────────────────────────────────────────
# Waits for a rollout to complete. On timeout, dumps pod status, describe, and
# container logs so the failure can be diagnosed without manual kubectl access.
rollout_wait() {
  local kind="$1"
  local name="$2"

  info "Waiting for ${kind}/${name} (timeout=${TIMEOUT})..."

  if kubectl rollout status "${kind}/${name}" \
      -n "${NAMESPACE}" --timeout="${TIMEOUT}"; then
    pass "${name} is Ready"
    return 0
  fi

  echo ""
  echo -e "${RED}✗ ${name} rollout timed out — diagnostic dump:${RST}"

  echo ""
  echo "=== pod list ==="
  kubectl get pods -n "${NAMESPACE}" \
    -l "app.kubernetes.io/name=${name}" -o wide 2>/dev/null || true

  local pod
  pod=$(kubectl get pods -n "${NAMESPACE}" \
    -l "app.kubernetes.io/name=${name}" \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")

  if [[ -n "${pod}" ]]; then
    echo ""
    echo "=== kubectl describe pod/${pod} (last 50 lines) ==="
    kubectl describe pod "${pod}" -n "${NAMESPACE}" 2>/dev/null | tail -50 || true

    echo ""
    echo "=== container log: ${pod} (last 40 lines) ==="
    kubectl logs "${pod}" -n "${NAMESPACE}" --tail=40 2>/dev/null || \
      echo "(no logs available — container may not have started)"

    local restarts
    restarts=$(kubectl get pod "${pod}" -n "${NAMESPACE}" \
      -o jsonpath='{.status.containerStatuses[0].restartCount}' 2>/dev/null || echo "0")
    if [[ "${restarts}" -gt 0 ]]; then
      echo ""
      echo "=== previous container log (pod was restarted ${restarts} time(s)) ==="
      kubectl logs "${pod}" -n "${NAMESPACE}" --previous --tail=40 2>/dev/null || true
    fi
  fi

  echo ""
  fail "${name} rollout timed out (see diagnostic dump above)"
}

# ── Wait for all workloads ─────────────────────────────────────────────────────

rollout_wait statefulset redpanda
rollout_wait statefulset timescaledb
rollout_wait deployment  control-plane
rollout_wait deployment  consumer

# ── HTTP health check ──────────────────────────────────────────────────────────

info "Checking control-plane /health via port-forward..."
kubectl port-forward -n "${NAMESPACE}" svc/control-plane 18080:8080 &
PF_PID=$!
sleep 3

HTTP_STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
  http://localhost:18080/health 2>/dev/null || echo "000")
kill "${PF_PID}" 2>/dev/null || true
wait "${PF_PID}" 2>/dev/null || true

[[ "${HTTP_STATUS}" == "200" ]] \
  && pass "control-plane /health returned HTTP 200" \
  || fail "control-plane /health returned HTTP ${HTTP_STATUS} (expected 200)"

# ── NetworkPolicy audit ────────────────────────────────────────────────────────
# Service names:
#   redpanda-client:9092 — ClusterIP service for Kafka clients
#   timescaledb:5432     — ClusterIP service for PostgreSQL clients
# Workers should reach redpanda-client but NOT timescaledb.

WORKER_POD=$(kubectl get pods -n "${NAMESPACE}" \
  -l app.kubernetes.io/name=worker \
  -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")

if [[ -z "${WORKER_POD}" ]]; then
  warn "No worker pod found — skipping NetworkPolicy connectivity checks"
  warn "(Worker may be at replicas=0; KEDA manages scale in Stage 4.3)"
else
  info "Waiting for worker pod ${WORKER_POD} to be Ready..."
  kubectl wait --for=condition=ready "pod/${WORKER_POD}" \
    -n "${NAMESPACE}" --timeout="${TIMEOUT}" \
    || fail "Worker pod did not become Ready within ${TIMEOUT}"

  info "NetworkPolicy audit: worker → timescaledb:5432 (must be BLOCKED)..."
  if kubectl exec -n "${NAMESPACE}" "${WORKER_POD}" -- \
      sh -c 'nc -z -w 3 timescaledb 5432 2>/dev/null'; then
    fail "SECURITY VIOLATION: worker can reach timescaledb:5432 — NetworkPolicy not enforced"
  else
    pass "Worker cannot reach timescaledb:5432 (NetworkPolicy enforced)"
  fi

  info "NetworkPolicy audit: worker → redpanda-client:9092 (must be ALLOWED)..."
  if kubectl exec -n "${NAMESPACE}" "${WORKER_POD}" -- \
      sh -c 'nc -z -w 3 redpanda-client 9092 2>/dev/null'; then
    pass "Worker can reach redpanda-client:9092"
  else
    fail "Worker cannot reach redpanda-client:9092 — check allow-worker-egress-redpanda.yaml"
  fi
fi

echo ""
pass "Stage 4.2 live smoke test complete"
