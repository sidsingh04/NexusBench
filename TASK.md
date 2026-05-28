# TASK.md — Phase 4: Terraform & Infrastructure Automation

> **Status: 🔄 In Progress**
> Phase 3 is ✅ complete. Phase 4 begins here.

---

## Goal

Provision all NexusBench infrastructure with Terraform, deploy every service to
Kubernetes, autoscale the worker fleet based on Redpanda queue depth, and
establish a repeatable CI/CD pipeline — all while enforcing the security model
mandated by PROGRESS.md (disposable worker nodes, strict NetworkPolicies,
read-only code mounts).

---

## Architectural Constraints (non-negotiable)

These come directly from the PROGRESS.md architectural decision record:

1. **No gVisor** — capability-dropping Docker is the isolation mechanism.
2. **Disposable Workers** — worker pods are ephemeral; recycled after every job.
3. **NetworkPolicies** — workers have no outbound internet; isolated from the
   control plane's internal DB/Redpanda APIs.
4. **Read-Only code volumes** — contestant archives are mounted `readOnly: true`.
5. **Spot/Preemptible nodes** — worker node pool uses spot pricing to keep costs
   low and reinforce the ephemeral contract.

---

## Incremental Stage Plan

Phase 4 is split into four stages. Each stage has a clear gate: all gate tests
must pass before moving to the next stage.

```
Stage 4.1  Terraform Cloud Provisioning    (infrastructure as code, no K8s yet)
Stage 4.2  Kubernetes Manifests            (deploy all services, NetworkPolicies)
Stage 4.3  Autoscaling                     (HPA on Redpanda queue depth)
Stage 4.4  CI/CD Pipeline                  (GitHub Actions end-to-end)
```

---

## Stage 4.1 — Terraform Cloud Provisioning

> **Status: ✅ Complete**

### Goal

Write Terraform that provisions a production-grade cloud environment from
scratch: VPC, managed Kubernetes cluster, two node pools (control-plane +
disposable workers), container registry, and a shared persistent-volume claim
for submission data.

All infrastructure is parameterised; no hard-coded cloud credentials, regions,
or resource sizes appear in the code.

### Directory layout

```
terraform/
├── main.tf              # provider + terraform block
├── variables.tf         # all input variables with descriptions + defaults
├── outputs.tf           # kubeconfig, registry URL, cluster endpoint
├── versions.tf          # required_providers with exact version pins
├── modules/
│   ├── cluster/         # managed K8s cluster (GKE or EKS)
│   │   ├── main.tf
│   │   ├── variables.tf
│   │   └── outputs.tf
│   ├── node-pools/      # control-plane pool + disposable worker pool
│   │   ├── main.tf
│   │   ├── variables.tf
│   │   └── outputs.tf
│   └── registry/        # container registry (GCR / ECR)
│       ├── main.tf
│       ├── variables.tf
│       └── outputs.tf
└── envs/
    ├── dev.tfvars        # small sizes, 1 worker node, spot OK
    └── prod.tfvars       # production sizes, min 2 worker nodes, spot OK
```

### Tasks

#### 4.1.1 — Terraform scaffolding

- [x] Create `terraform/versions.tf` — pin `hashicorp/google` (or `hashicorp/aws`) and `kubernetes` providers to exact versions
- [x] Create `terraform/variables.tf` — variables: `project_id`, `region`, `cluster_name`, `worker_node_machine_type`, `worker_node_min`, `worker_node_max`, `control_plane_machine_type`, `registry_location`
- [x] Create `terraform/outputs.tf` — export `cluster_endpoint`, `kubeconfig_raw`, `registry_url`
- [x] Create `terraform/main.tf` — wire modules together; no resource blocks at root level (keep root thin)

#### 4.1.2 — Cluster module

- [x] Create `terraform/modules/cluster/main.tf`
  - Managed K8s cluster (GKE `google_container_cluster` or EKS `aws_eks_cluster`)
  - VPC-native networking enabled
  - Workload Identity / IRSA enabled (so pods can authenticate to cloud APIs without static keys)
  - Private cluster endpoint (no public API server)
- [x] Create `terraform/modules/cluster/variables.tf` + `outputs.tf`

#### 4.1.3 — Node pool module (two pools)

- [x] **Control-plane pool** — standard on-demand nodes, min=1 max=3, taints none
- [x] **Worker pool** — spot/preemptible nodes, min=0 max=10, taint `role=benchmark-worker:NoSchedule`
  - Taint enforces that only benchmark worker pods land on spot nodes
  - Auto-repair and auto-upgrade enabled
  - Surge upgrade strategy (one node upgraded at a time)
- [x] Expose `worker_pool_id` in outputs (referenced by HPA in Stage 4.3)

#### 4.1.4 — Registry module

- [x] Create `terraform/modules/registry/main.tf` — private container registry
- [x] Grant the K8s node service account pull access (Workload Identity binding)

#### 4.1.5 — Environment var files

- [x] Create `terraform/envs/dev.tfvars` — `e2-standard-2` nodes, worker min=0 max=3
- [x] Create `terraform/envs/prod.tfvars` — `c2-standard-8` nodes, worker min=1 max=10

#### 4.1.6 — Validation

- [x] `terraform fmt -recursive` — zero diff
- [x] `terraform validate` — no errors
- [x] `terraform plan -var-file=envs/dev.tfvars` — plan output reviewed, no unexpected destroys
- [x] Add `make tf-validate` target to Makefile

### Gate — Stage 4.1

```bash
make tf-validate   # terraform fmt + validate pass
# Reviewer manually checks: plan shows cluster + 2 node pools + registry
# No credentials or project IDs are hard-coded anywhere
```

---

## Stage 4.2 — Kubernetes Manifests

> **Status: ✅ Complete**

### Goal

Deploy all seven NexusBench services to Kubernetes using raw manifests (no
Helm yet). Every security control from the architectural constraints is
enforced at the manifest level: NetworkPolicies, read-only mounts, pod
disruption budgets, resource requests/limits.

### Directory layout

```
k8s/
├── namespace.yaml
├── configmaps/
│   └── nexusbench-config.yaml
├── secrets/
│   └── .gitkeep              # secrets are managed by Terraform / Sealed Secrets
├── control-plane/
│   ├── deployment.yaml
│   ├── service.yaml
│   └── ingress.yaml
├── worker/
│   ├── deployment.yaml       # disposable pod spec — key security surface
│   └── pdb.yaml              # PodDisruptionBudget: maxUnavailable=1
├── consumer/
│   └── deployment.yaml
├── redpanda/
│   ├── statefulset.yaml
│   └── service.yaml
├── timescaledb/
│   ├── statefulset.yaml
│   ├── service.yaml
│   └── pvc.yaml
├── network-policies/
│   ├── default-deny-all.yaml
│   ├── allow-control-plane.yaml         # control-plane ingress + egress
│   ├── allow-worker-egress-redpanda.yaml # workers → Redpanda + control-plane only
│   ├── allow-consumer-egress.yaml        # consumer → Redpanda + TimescaleDB only
│   ├── allow-ingress-external.yaml       # NGINX → control-plane
│   ├── allow-redpanda-ingress.yaml       # added during live debugging (issue 8)
│   └── allow-timescaledb-ingress.yaml    # added during live debugging (issue 8)
└── rbac/
    ├── worker-serviceaccount.yaml
    └── worker-role.yaml                    # minimal: list pods only
```

### Tasks

#### 4.2.1 — Namespace + base config

- [x] Create `k8s/namespace.yaml` — namespace `nexusbench`, labels for NetworkPolicy selector
- [x] Create `k8s/configmaps/nexusbench-config.yaml` — all non-secret env vars (broker address, image names, bot fleet defaults)
- [x] Add `.gitkeep` to `k8s/secrets/` with a comment explaining secrets are injected by CI/CD

#### 4.2.2 — Control-plane Deployment

- [x] Create `k8s/control-plane/deployment.yaml`
  - `replicas: 1`, image from registry
  - `resources.requests`: cpu=250m memory=256Mi; `limits`: cpu=500m memory=512Mi
  - `livenessProbe` + `readinessProbe` on `/health`
  - `securityContext`: `readOnlyRootFilesystem: true`, `allowPrivilegeEscalation: false`, `runAsNonRoot: true`
  - Mount `submissions-pvc` at `/data/submissions`
- [x] Create `k8s/control-plane/service.yaml` — ClusterIP on port 8080
- [x] Create `k8s/control-plane/ingress.yaml` — NGINX ingress, TLS termination
- [x] Create `k8s/control-plane/pvc.yaml` — 50Gi ReadWriteOnce PVC for contestant archives

#### 4.2.3 — Worker Deployment (security-critical)

- [x] Create `k8s/worker/deployment.yaml`
  - Node selector + toleration for `role=benchmark-worker` spot pool
  - `securityContext`: `readOnlyRootFilesystem: true`, `allowPrivilegeEscalation: false`, capabilities `drop: [ALL]`
  - `terminationGracePeriodSeconds: 60` — allow in-flight job to finish
  - `resources.limits`: cpu=2000m memory=1Gi (hard ceiling per worker)
  - Env vars from ConfigMap; NODE_IP + SANDBOX_HOST injected via Downward API
- [x] Create `k8s/worker/pdb.yaml` — `maxUnavailable: 1` so upgrade drains don't evict all workers at once

#### 4.2.4 — Consumer + StatefulSets

- [x] Create `k8s/consumer/deployment.yaml` — single replica, same security context pattern
- [x] Create `k8s/redpanda/statefulset.yaml` + `service.yaml` — 1 replica (dev), 3 replicas (prod); PVC for data
- [x] Create `k8s/timescaledb/statefulset.yaml` + `service.yaml` + `pvc.yaml` — 1 replica; `storageClassName: standard`

#### 4.2.5 — NetworkPolicies (zero-trust)

- [x] `default-deny-all.yaml` — deny all ingress and egress for every pod in namespace by default
- [x] `allow-control-plane.yaml` — control-plane ingress from NGINX + workers + Prometheus; egress to Redpanda + TimescaleDB + DNS
- [x] `allow-worker-egress-redpanda.yaml` — workers can reach **only** Redpanda (port 9092) and the control-plane heartbeat endpoint; no other egress
- [x] `allow-consumer-egress.yaml` — consumer can reach Redpanda + TimescaleDB only
- [x] `allow-ingress-external.yaml` — NGINX ingress pod can forward to control-plane

#### 4.2.6 — RBAC

- [x] `worker-serviceaccount.yaml` — dedicated ServiceAccount for worker pods; `automountServiceAccountToken: false`
- [x] `worker-role.yaml` — namespace-scoped Role: `get`, `list` on `pods` only; bound to worker ServiceAccount

#### 4.2.7 — Validation

- [x] `make k8s-validate` — offline validation passes with zero errors
- [x] Add `make k8s-validate` target to Makefile (done in Stage 4.1)
- [x] Add `scripts/smoke_test_phase4_stage2.sh` — dry-run and live modes

### Gate — Stage 4.2

```bash
make k8s-validate
# make k8s-validate passes for all manifests
# NetworkPolicy audit: worker pods have no path to TimescaleDB or internet
```

---

## Stage 4.3 — Autoscaling (HPA on Queue Depth)

> **Status: ✅ Complete**

### Goal

Scale the worker `Deployment` automatically based on the number of pending jobs
in the Redpanda `jobs.benchmark` topic. The HPA uses a custom external metric
(consumer lag) exposed via KEDA (Kubernetes Event-Driven Autoscaling), which
natively understands Kafka/Redpanda consumer group lag.

### Design

```
Redpanda jobs.benchmark topic
        │  consumer-group lag
        ▼
   KEDA ScaledObject
        │  external metric: jobs_pending
        ▼
   HPA (managed by KEDA)
        │  scale worker Deployment
        ▼
   worker Deployment   min=1  max=10
```

Target: 1 worker replica per 5 pending jobs, with a 30-second stabilisation
window on scale-down (so a brief burst doesn't cause flapping).

### Tasks

#### 4.3.1 — KEDA installation

- [x] Add KEDA to Terraform: `helm_release` resource for `kedacore/keda` chart, pinned version
- [x] Alternatively, add `k8s/keda/` manifest install as a pre-step in CI

#### 4.3.2 — KEDA ScaledObject

- [x] Create `k8s/worker/scaledobject.yaml`
  - `scaleTargetRef`: worker Deployment
  - `minReplicaCount: 1`, `maxReplicaCount: 10`
  - `cooldownPeriod: 30` (seconds before scale-down after queue drains)
  - Trigger type: `kafka`
    - `bootstrapServers`: Redpanda ClusterIP service
    - `consumerGroup`: `nexusbench-workers`
    - `topic`: `jobs.benchmark`
    - `lagThreshold: "5"` (1 extra worker per 5 unprocessed messages)
  - `advanced.restoreToOriginalReplicaCount: false` (don't snap to original)
  - `advanced.horizontalPodAutoscalerConfig.behavior` — scale-up immediately (5 pods/15s), scale-down slowly (1 pod/30s)
  - `pollingInterval: 15` — matches Prometheus scrape interval

#### 4.3.3 — Internal metric for `internal/queue`

- [x] Add `QueueDepth(ctx) (int64, error)` method to the `Queue` interface in `internal/queue`
- [x] Implement on `RedpandaQueue` using `kadm.Client.ListEndOffsets` + `kadm.Client.FetchOffsets` — compute lag as sum(end - committed) across all partitions
- [x] Implement on `MemoryQueue` (returns `int64(len(ch))`, non-blocking)
- [x] Expose queue depth on a new Prometheus gauge `nexusbench_queue_depth` in `internal/metrics` with `SetQueueDepth(int64)` helper (negative values clamped to 0)
- [x] Wire into control plane: `runQueueDepthScraper` goroutine fires immediately on startup then every 15s; uses a 5s per-poll context timeout; distributed mode only

#### 4.3.4 — Tests

- [x] `TestMemoryQueue_QueueDepth_Empty` — depth is 0 on empty queue
- [x] `TestMemoryQueue_QueueDepth_AfterEnqueue` — depth tracks enqueued count step-by-step
- [x] `TestMemoryQueue_QueueDepth_AfterDequeue` — depth decrements correctly after each Dequeue
- [x] `TestMemoryQueue_QueueDepth_Unbuffered` — always 0 for cap=0 queue
- [x] `TestMemoryQueue_QueueDepth_CancelledCtx` — non-blocking even with cancelled context
- [x] `TestMemoryQueue_QueueDepth_SatisfiesInterface` — compile-time check *MemoryQueue implements Queue
- [x] `TestSetQueueDepth_InitiallyZero` — gauge reads 0 before any call
- [x] `TestSetQueueDepth_UpdatesGauge` — overwrites (not accumulates) on each call
- [x] `TestSetQueueDepth_Zero` — resets gauge to 0
- [x] `TestSetQueueDepth_NegativeClamped` — negative values clamped to 0
- [x] `TestSetQueueDepth_MetricNamePresent` — descriptor appears in /metrics output
- [x] `TestSetQueueDepth_IsolatedFromOtherRegistries` — two registries are independent

#### 4.3.6 — Post-smoke-test fixes (applied during live run)

- [x] `scripts/smoke_test_phase4_stage3.sh` — python stub detection (Windows Store alias guard), pass file path via `sys.argv[1]` so Git Bash path translation works, `--server-side` in KEDA failure hint, removed `tee /dev/stderr` pipeline hang
- [x] `k8s/keda/keda-install.yaml` — both install commands updated to `kubectl apply --server-side` to bypass 256KB CRD annotation limit
- [x] `k8s/network-policies/allow-redpanda-ingress.yaml` — added `namespaceSelector: kubernetes.io/metadata.name: keda` ingress rule on port 9092 so KEDA operator can read consumer-group lag from Redpanda across namespaces

**Known operational gotchas documented (no code change needed):**
- Docker Desktop restart wipes node labels — re-apply with `kubectl label node docker-desktop role=control-plane --overwrite`
- Wait for pod `Ready` before starting `kubectl port-forward` to avoid race condition on port 8080
- KEDA uses exponential backoff on broker failures — allow 2-3 minutes for recovery after Redpanda restarts

- [x] Script `scripts/smoke_test_phase4_stage3.sh`
  - `--dry-run` mode (default/CI): validates ScaledObject YAML fields, KEDA install reference, runs Go unit tests
  - `--live` mode: checks KEDA operator running, applies ScaledObject, enqueues 20 jobs, asserts scale-up within 60s, drains queue, asserts scale-down within 90s

### Gate — Stage 4.3

```bash
go test ./internal/queue/... -race -v       # QueueDepth unit tests pass
go test ./internal/metrics/... -race -v     # gauge tests pass
# KEDA ScaledObject dry-run: make k8s-validate passes
```

---

## Stage 4.4 — CI/CD Pipeline

> **Status: ✅ Complete**

### Goal

A single GitHub Actions workflow that — on every push to `main` and on every
pull request — runs all tests, builds and pushes Docker images, validates
Terraform and Kubernetes manifests, and (on `main` only) deploys to the dev
cluster.

No secrets are hard-coded in workflow files. All cloud credentials flow through
GitHub Environments with Workload Identity Federation (no long-lived keys).

### Directory layout

```
.github/
├── workflows/
│   ├── ci.yml           # PR gate: test + lint + validate
│   └── deploy.yml       # main branch: build + push + deploy
└── actions/
    └── setup-go/        # composite action: Go toolchain + module cache
```

### Tasks

#### 4.4.1 — CI workflow (`ci.yml`)

Triggers: `pull_request` targeting `main`, `push` to `main`.

Jobs (run in parallel where possible):

- [x] **lint** — `golangci-lint run ./...` with config file `.golangci.yml`
- [x] **unit-tests** — `go test $(GO_PKGS) -race -timeout 60s -coverprofile=coverage.out`; upload coverage artifact
- [x] **tf-validate** — `terraform fmt -check -recursive && terraform validate`
- [x] **k8s-validate** — `make k8s-validate`

Create `.golangci.yml`:
- [x] Enable: `errcheck`, `govet`, `staticcheck`, `gosimple`, `ineffassign`, `unused`
- [x] Also enable: `gosec`, `bodyclose`, `nilerr`, `gofmt`, `misspell`
- [x] Disable: `gochecknoglobals` (Prometheus vars are package-level by convention)
- [x] `timeout: 5m`

#### 4.4.2 — Build + push job (deploy.yml, `main` only)

- [x] Authenticate to registry via Workload Identity Federation (no static JSON key)
- [x] Build `Dockerfile.server` → push `control-plane:$SHA`, `control-plane:latest`
- [x] Build each `docker/sandbox/Dockerfile.*` → push `sandbox-{lang}:$SHA`
- [x] Matrix strategy over languages to parallelise sandbox image builds
- [x] BuildKit inline cache: `cache-from/cache-to: type=inline` for fast incremental builds

#### 4.4.3 — Deploy job (deploy.yml, `main` only, after build passes)

- [x] Authenticate to cluster via Workload Identity
- [x] `kubectl set image deployment/control-plane control-plane=$REGISTRY/control-plane:$SHA`
- [x] `kubectl set image deployment/worker worker=$REGISTRY/server:$SHA`
- [x] `kubectl rollout status deployment/control-plane --timeout=120s`
- [x] `kubectl rollout status deployment/worker --timeout=120s`
- [x] Run smoke test against the dev cluster endpoint
- [x] Print deployment summary to GitHub Step Summary (pod list + image tags)

#### 4.4.4 — Composite action (`setup-go`)

- [x] Create `.github/actions/setup-go/action.yml`
  - Steps: `actions/setup-go@v5` (version from input), module cache via `cache: true` built into setup-go
  - `go-version` input defaults to `"file"` (reads from `go.mod` automatically)
  - Used by all jobs that need Go — avoids repetition

#### 4.4.5 — Makefile additions

- [x] `make lint` — runs `golangci-lint`
- [x] `make ci` — runs `make lint test tf-validate k8s-validate` (mirrors CI locally)
- [x] `make build-push` — builds + tags all images (local use with `REGISTRY` override)
- [x] `make test` updated to emit `-coverprofile=coverage.out -covermode=atomic` and print summary line
- [x] `coverage.out` added to `.gitignore`

#### 4.4.6 — Validation (offline)

- [x] All workflow YAML files are syntactically valid (no cloud credentials required to verify)
- [x] All new Makefile targets documented in the Makefile header comment block
- [x] `.golangci.yml` present at repo root
- [x] `make ci` target chains lint + test + tf-validate + k8s-validate

### Gate — Stage 4.4

```bash
make ci              # lint + test + tf-validate + k8s-validate all pass locally
# Open a PR → CI workflow triggers → all jobs green
# Merge to main → deploy workflow triggers → dev cluster updated
```

---

## Full Phase 4 Gate Checklist

Before PROGRESS.md is updated to mark Phase 4 complete, every item below must be checked:

| Check | Command / Evidence |
|---|---|
| All unit tests pass with race detector | `make test` |
| golangci-lint clean | `make lint` |
| Terraform fmt + validate | `make tf-validate` |
| K8s manifests dry-run | `make k8s-validate` |
| No hard-coded secrets anywhere | `git grep -E "(password|secret|key)\s*=" terraform/ k8s/ .github/` returns nothing sensitive |
| Worker pods tolerate only spot node pool | manifest review |
| NetworkPolicy blocks worker → TimescaleDB | manifest review |
| KEDA ScaledObject parseable | `make k8s-validate` |
| CI workflow green on PR | GitHub Actions UI |
| Deploy workflow green on merge to main | GitHub Actions UI |

---

## Key Design Decisions

- **Thin Terraform root** — all resource blocks live in modules; `main.tf` only wires modules together. This keeps the root plan readable and modules reusable across envs.
- **KEDA over custom HPA adapter** — KEDA has first-class Kafka/Redpanda support via consumer group lag; building a custom metrics adapter would duplicate that for no benefit.
- **Workload Identity, no static keys** — long-lived cloud credentials in CI are a supply-chain risk. WIF lets GitHub's OIDC token authenticate to GCP/AWS without any secret stored in GitHub.
- **PodDisruptionBudget on workers** — prevents cluster upgrades from simultaneously evicting all workers during an active benchmark run.
- **`QueueDepth` on the Queue interface** — keeps the metric computation inside the `queue` package (deep module principle); callers never need to know Kafka offset math.
- **Matrix sandbox image builds** — parallelises five independent Docker builds; total CI time drops from ~15 min sequential to ~4 min.
