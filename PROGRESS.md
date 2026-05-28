# NexusBench — Development Progress

> **Hackathon:** IICPC Summer Hackathon 2026 (May 9 – June 10)
> **Platform:** Distributed Benchmarking and Hosting Platform for trading algorithms
> **Module:** `github.com/nexusbench/nexusbench`

---

## What NexusBench Does

Contestants upload a trading engine (matching engine / orderbook) written in C++, Rust, Go, or Python. NexusBench:

1. Sandboxes and deploys the engine in an isolated container with strict CPU and memory limits
2. Bombards it with a distributed fleet of trading bots simulating extreme market conditions
3. Captures p50/p90/p99 latency, max TPS, and correctness (price-time priority) in real time
4. Streams results to a live leaderboard ranked by composite score

---

## Overall Roadmap

| Phase | Name | Status |
|-------|------|--------|
| Phase 1 | Core MVP | ✅ Complete |
| Phase 2 | Telemetry | ✅ Complete |
| Phase 3 | Distributed Workers | ✅ Complete |
| Phase 4 | Terraform & Infra Automation | 🔄 In Progress |
| Phase 5 | Advanced Benchmarking | ⏳ Pending |

---

## Phases Completed

### Phase 1 — Core MVP ✅

**Goal:** upload algo → run in container → replay data → show metrics

**What was built:**

- `internal/models` — core domain types: `Submission`, `BenchmarkResults`, `LeaderboardEntry`, all lifecycle statuses (`pending` → `deploying` → `running` → `benchmarking` → `completed` / `failed`)
- `internal/config` — single `Config` struct loaded from environment variables; `ImageForLanguage`, `AllImages` helpers
- `internal/sandbox` — `DockerManager`: deploys contestant code into isolated containers with cgroup CPU pinning, memory limits, capability dropping, `CopyToContainer` archive injection, and port allocation from a configurable pool
- `internal/submission` — `Service` + `DiskStore`: validates uploads, stores archives on disk, orchestrates container lifecycle; `Store` interface for testability
- `internal/api` — HTTP router (gorilla/mux): `POST /api/v1/submissions`, `GET /api/v1/submissions/{id}`, `GET /api/v1/leaderboard`, `GET /health`, `GET /metrics`
- `cmd/server` — control plane binary
- `docker/sandbox/` — five Dockerfile variants: `go`, `rust`, `cpp`, `python`, `binary`; each extracts the archive and runs the engine on port 7878

### Phase 2 — Telemetry ✅

**Goal:** live metrics → dashboard → logs

**What was built:**

- `internal/telemetry` — `Event` type, `Emitter` interface with `Emit` + `BatchEmit`, `StdoutEmitter`, `RedpandaEmitter` (franz-go, AllISRAcks), `RecordingEmitter` (tests), `NoopEmitter`; topic layout: `metrics.latency`, `metrics.heartbeat`, `metrics.dlq`
- `internal/consumer` — `Consumer` polls `metrics.latency` from Redpanda, writes rows to TimescaleDB via `pgxpool`; `PercentileStore` computes p50/p90/p99 from the time-series table
- `internal/metrics` — Prometheus `Registry`: HTTP request counter + duration histogram
- `docker-compose.yml` — full observability stack: Redpanda + Console, TimescaleDB, Prometheus, Grafana, Loki, Promtail, cAdvisor, Node Exporter
- Grafana dashboards provisioned automatically on startup

### Phase 3 — Distributed Workers ✅

**Goal:** multiple benchmark nodes + scheduler

**Stages completed:**

| Stage | Description | Status |
|-------|-------------|--------|
| 3.1 | Worker abstraction + Redpanda job queue | ✅ |
| 3.2 | Orchestrator + worker heartbeat registry | ✅ |
| 3.3 | Real distributed bot fleet + correctness engine | ✅ |

**Key packages added in Phase 3:**

| Package | Purpose |
|---|---|
| `internal/queue` | `Job` type, `Queue` interface, `MemoryQueue` (tests), `RedpandaQueue` (production) |
| `internal/worker` | `Worker` poll loop, `SandboxExecutor`, `Heartbeater` |
| `internal/orchestrator` | `WorkerRegistry`, HTTP handler for fleet visibility routes |
| `internal/botfleet` | `Fleet`, `Bot`, `OrderGenerator`, `RESTTransport`, `ComputeStats` |
| `internal/correctness` | `GoldenOrderbook`, `Checker`, deterministic price-time priority matching |

**Critical bug fixed in Stage 3.3:** Docker-in-Docker networking — workers running inside containers were connecting to `localhost:{sandboxPort}` but sandbox ports are published on the host machine's network interface, not inside the worker container's namespace. Fixed via `SANDBOX_HOST=host.docker.internal` (Docker Desktop) with `WithSandboxHost` executor option.

---

## Phase 4 — Terraform & Infra Automation 🔄

**Goal:** Provision cloud infrastructure with Terraform, deploy all services to Kubernetes, implement autoscaling on worker fleet based on queue depth, and establish a CI/CD pipeline.

> **⚠️ ARCHITECTURAL DECISION (Security vs. Stability):** 
> We are deliberately **skipping gVisor** (which was mentioned in `docker.go` comments) for this hackathon. GVisor introduces syscall overhead that skews p99 latency and TPS metrics. Since correctness and stability matter most, we will stick to native Docker capability-dropping. 
> To mitigate the container-escape risk without a "screening service", Phase 4 MUST implement **Disposable Workers**: 
> 1. Worker pods/nodes must be highly ephemeral (e.g., rapid recycling or using Spot instances).
> 2. Implement strict Kubernetes NetworkPolicies (no outbound internet access for workers, isolated from the control plane).
> 3. Mount contestant code as Read-Only volumes.

> **See TASK.md for the full incremental execution plan with per-stage gates.**

### Stage breakdown

| Stage | Description | Status |
|-------|-------------|--------|
| 4.1 | Terraform Cloud Provisioning — VPC, managed K8s cluster, two node pools (on-demand control-plane + spot worker), container registry | ✅ Complete |
| 4.2 | Kubernetes Manifests — all seven services deployed, zero-trust NetworkPolicies, RBAC, PodDisruptionBudgets, read-only worker mounts | ✅ Complete |
| 4.3 | Autoscaling — KEDA ScaledObject on Redpanda consumer-group lag; `QueueDepth` method on `Queue` interface; Prometheus gauge; dry-run + live smoke tests passing | ✅ Complete |
| 4.4 | CI/CD Pipeline — GitHub Actions: lint + test + tf-validate + k8s-validate on PRs; build + push + rolling deploy on `main` | ⏳ Pending |

### New files planned

```
terraform/
├── main.tf / variables.tf / outputs.tf / versions.tf
└── modules/
    ├── cluster/      # managed K8s cluster
    ├── node-pools/   # control-plane (on-demand) + worker (spot, tainted)
    └── registry/     # container registry + Workload Identity binding
k8s/
├── namespace.yaml
├── configmaps / secrets/
├── control-plane / worker / consumer
├── redpanda / timescaledb StatefulSets
├── network-policies/   # default-deny-all + allow-* per service
└── rbac/               # minimal worker ServiceAccount + Role
.github/
├── workflows/ci.yml      # PR gate
└── workflows/deploy.yml  # main → build + push + rolling deploy
```

### Code changes planned

| Package | Change |
|---|---|
| `internal/queue` | Add `QueueDepth(ctx) (int64, error)` to `Queue` interface; implement on `RedpandaQueue` and `MemoryQueue` |
| `internal/metrics` | Add `nexusbench_queue_depth` Prometheus gauge; wired via 15s background scrape in control plane |
| `Makefile` | Add `lint`, `ci`, `tf-validate`, `k8s-validate`, `build-push` targets |

### Stage 4.2 — What was built

**17 files created across `k8s/`.**

| File | Key decisions |
|---|---|
| `k8s/namespace.yaml` | `nexusbench.io/network-policy: enabled` label used as namespaceSelector anchor in all NetworkPolicy objects |
| `k8s/configmaps/nexusbench-config.yaml` | Single ConfigMap for all non-secret env vars across all services; `REGISTRY` placeholder replaced by CI `sed` before apply |
| `k8s/secrets/.gitkeep` | Documents three secret management strategies: `kubectl create secret` (dev), GitHub Actions Environments (CI), Sealed Secrets / External Secrets Operator (prod) |
| `k8s/rbac/worker-serviceaccount.yaml` | `automountServiceAccountToken: false` — eliminates the default token mount that contestant code could exfiltrate |
| `k8s/rbac/worker-role.yaml` | Namespace-scoped Role (not ClusterRole); only `get`+`list` on `pods`; RoleBinding wires it to the ServiceAccount |
| `k8s/redpanda/statefulset.yaml` | Stable pod DNS via headless service; uid 101 (redpanda image user); capabilities DROP ALL; per-pod PVC via `volumeClaimTemplates` |
| `k8s/redpanda/service.yaml` | Two services: headless (stable per-pod DNS for broker-to-broker RPC) + ClusterIP (client bootstrap address `redpanda:9092`) |
| `k8s/timescaledb/statefulset.yaml` | uid 1000; pg-run emptyDir (tmpfs) for socket files; PGDATA in PVC subdirectory to avoid `lost+found` incompatibility |
| `k8s/timescaledb/service.yaml` | ClusterIP only — no ingress, no NodePort; NetworkPolicies block workers from reaching it |
| `k8s/timescaledb/pvc.yaml` | 20Gi ReadWriteOnce; capacity math documented (200M rows at ~100B/row) |
| `k8s/control-plane/deployment.yaml` | No Docker socket; `readOnlyRootFilesystem`; `/tmp` emptyDir for multipart form buffering; NODE_IP Downward API injection |
| `k8s/control-plane/service.yaml` | ClusterIP:8080 — used by workers for heartbeats and by NGINX for forwarding |
| `k8s/control-plane/ingress.yaml` | Rate limiting (1 RPS / 10 connections per IP); 256 MiB proxy-body-size; 300s proxy timeout; TLS termination |
| `k8s/control-plane/pvc.yaml` | 50Gi ReadWriteOnce; comment documents why RWO is sufficient (workers fetch archives over HTTP, not via volume mount) |
| `k8s/worker/deployment.yaml` | Both `nodeSelector` AND `toleration` required — nodeSelector targets the label, toleration permits scheduling on the tainted node; SANDBOX_HOST set from NODE_IP Downward API so workers reach Docker-published ports on the host |
| `k8s/worker/pdb.yaml` | `maxUnavailable: 1` — serialises GKE upgrade drains across the worker pool; does not protect against spot evictions (by design) |
| `k8s/consumer/deployment.yaml` | TIMESCALE_DSN from Secret; REDPANDA_BROKERS + CONSUMER_GROUP_ID from ConfigMap individual keys (not envFrom, to avoid pulling in unneeded vars) |
| `k8s/network-policies/default-deny-all.yaml` | Empty podSelector selects all pods; empty ingress/egress lists + explicit policyTypes = total deny |
| `k8s/network-policies/allow-control-plane.yaml` | Split into two NetworkPolicy objects (ingress + egress) for clarity; DNS egress to kube-system/kube-dns included |
| `k8s/network-policies/allow-worker-egress-redpanda.yaml` | Explicit `ingress: []` makes intent clear; Docker socket is a Unix socket, not pod-network traffic — not governed by NetworkPolicy |
| `k8s/network-policies/allow-consumer-egress.yaml` | Minimal: Redpanda read + TimescaleDB write + DNS only |
| `k8s/network-policies/allow-ingress-external.yaml` | Scoped to `ingress-nginx` namespace + pod label — only the actual NGINX controller pod can forward traffic |
| `k8s/network-policies/allow-redpanda-ingress.yaml` | Added after live-cluster testing revealed the missing ingress side. Two separate rules: port 9092 from worker/consumer/control-plane; port 33145 (broker-to-broker RPC) from redpanda pods only — split to prevent clients reaching the internal RPC port |
| `k8s/network-policies/allow-timescaledb-ingress.yaml` | Added after live-cluster testing. Permits port 5432 from consumer + control-plane only; workers explicitly excluded |
| `scripts/smoke_test_phase4_stage2.sh` | Two modes: `--dry-run` (kubeconform offline schema validation, python3 YAML syntax fallback) and `--live` (apply + rollout wait + HTTP health check + NetworkPolicy connectivity audit with `kubectl wait` before `nc -z` to eliminate race condition) |

### Stage 4.3 — What was built

**7 files created/updated across `k8s/`, `internal/`, `cmd/`, and `scripts/`.**

| File | Key decisions |
|---|---|
| `k8s/keda/keda-install.yaml` | Reference/comment manifest documenting KEDA v2.14.0 install procedure via `kubectl apply` or Helm; kept as a doc artifact rather than embedding the 2000-line upstream manifest to avoid stale copy-paste |
| `k8s/worker/scaledobject.yaml` | KEDA `ScaledObject` targeting the worker `Deployment`; kafka trigger on `jobs.benchmark` topic, `consumerGroup: nexusbench-workers`, `lagThreshold: "5"`; `minReplicaCount: 1` (one warm worker always up), `maxReplicaCount: 10`; `cooldownPeriod: 30s`, `pollingInterval: 15s`; HPA behavior: scale-up by 5 pods/15s immediately, scale-down by 1 pod/30s; `restoreToOriginalReplicaCount: false` |
| `internal/queue/job.go` | Added `QueueDepth(ctx context.Context) (int64, error)` to the `Queue` interface with full godoc explaining the contract for both implementations |
| `internal/queue/memory.go` | Implements `QueueDepth` as `int64(len(m.ch))` — zero-allocation, non-blocking, safe to call with a cancelled ctx |
| `internal/queue/redpanda.go` | Implements `QueueDepth` via two admin RPCs: `adminCl.ListEndOffsets` (partition heads) + `adminCl.FetchOffsets` (committed offsets); lag = sum(end - committed) across all partitions; missing committed offset treated as full-partition lag (worst-case, correct for cold-start) |
| `internal/metrics/metrics.go` | Added `QueueDepth prometheus.Gauge` field; registered as `nexusbench_queue_depth`; `SetQueueDepth(int64)` helper clamps negative values to 0 |
| `cmd/server/main.go` | `runQueueDepthScraper(ctx, queue, metrics)` goroutine: fires immediately on startup (gauge populated before first Prometheus scrape), then every 15s via `time.NewTicker`; each poll uses a 5s deadline context so a slow broker never blocks across ticks; logs `slog.Warn` on error and skips gauge update; started only in distributed mode; cancelled via `defer scraperCancel()` on shutdown |
| `scripts/smoke_test_phase4_stage3.sh` | Two modes: `--dry-run` (YAML field checks + Go unit tests, no cluster) and `--live` (KEDA operator check, ScaledObject apply, 20-job enqueue, 60s scale-up wait, queue drain, 90s scale-down wait) |

**12 new unit tests across `internal/queue/` and `internal/metrics/`.**

| Test | Package | What it verifies |
|---|---|---|
| `TestMemoryQueue_QueueDepth_Empty` | queue | Returns 0 before any enqueue |
| `TestMemoryQueue_QueueDepth_AfterEnqueue` | queue | Depth increments by 1 per enqueue, checked at each step |
| `TestMemoryQueue_QueueDepth_AfterDequeue` | queue | Depth decrements by 1 per dequeue, checked at each step |
| `TestMemoryQueue_QueueDepth_Unbuffered` | queue | Always 0 for cap=0 (no buffer to measure) |
| `TestMemoryQueue_QueueDepth_CancelledCtx` | queue | Non-blocking even with already-cancelled context |
| `TestMemoryQueue_QueueDepth_SatisfiesInterface` | queue | Compile-time: `*MemoryQueue` satisfies `Queue` interface |
| `TestSetQueueDepth_InitiallyZero` | metrics | Gauge reads 0 before `SetQueueDepth` is called |
| `TestSetQueueDepth_UpdatesGauge` | metrics | Latest call overwrites; does not accumulate |
| `TestSetQueueDepth_Zero` | metrics | `SetQueueDepth(0)` resets gauge to 0 |
| `TestSetQueueDepth_NegativeClamped` | metrics | Negative depth clamped to 0 |
| `TestSetQueueDepth_MetricNamePresent` | metrics | Descriptor appears in `/metrics` output |
| `TestSetQueueDepth_IsolatedFromOtherRegistries` | metrics | Two `New()` registries are independent |


The following issues were found during live-cluster smoke testing and corrected:

| # | Issue | Fix applied | Production note |
|---|---|---|---|
| 1 | `REGISTRY` placeholder is uppercase — K8s rejects image names with uppercase chars | Changed to `nexusbench/control-plane:latest` for local dev | CI pipeline must substitute the real Artifact Registry URL via `envsubst` before `kubectl apply` |
| 2 | All five `nodeSelector` blocks were commented out during local testing | Restored as live YAML in all five manifests; local-dev override documented in `worker/deployment.yaml` header | **Never** comment out nodeSelector in production — use the `kubectl patch` procedure instead |
| 3 | Redpanda crashed: `command:` bypasses the `rpk` entrypoint wrapper | Changed `command:` to `args:` so flags flow through the wrapper correctly | Keep permanently |
| 4 | Redpanda AIO limit too low on Docker Desktop VM | Raised via `wsl -d docker-desktop sysctl fs.aio-max-nr=1048576` | GKE node pools have sufficient AIO limits by default; add privileged DaemonSet only if needed |
| 5 | `nexusbench-secrets` Secret did not exist | Created manually for local dev via `kubectl create secret generic` | CI must inject secrets before apply — see `k8s/secrets/.gitkeep` for three management strategies |
| 6 | `imagePullPolicy: Always` caused ImagePullBackOff for locally built images | Changed to `imagePullPolicy: IfNotPresent` for local dev | Use `Always` with a real remote registry and SHA-pinned image tags in production |
| 7 | Control plane crashed with "docker daemon unreachable" in distributed mode | Wrapped `NewDockerManager` in `if !cfg.DistributedMode` in `cmd/server/main.go` | Keep permanently — decouples the control plane from container lifecycle in K8s |
| 8 | NetworkPolicies only opened egress from clients; Redpanda + TimescaleDB had no matching ingress rules | Added `allow-redpanda-ingress.yaml` and `allow-timescaledb-ingress.yaml` | Keep permanently; both sides of every TCP connection require matching policy rules |
| 9 | Smoke test ran `kubectl exec` before worker container was ready — misleading false-pass | Added `kubectl wait --for=condition=ready pod` before the `nc -z` NetworkPolicy audit | Keep permanently |
| 10 | Worker crashed: `mkdir /data/submissions` failed on read-only root FS | Added `SUBMISSION_DIR=/tmp/submissions` env override in `worker/deployment.yaml` | Keep permanently |
| 11 | Worker crashed: Docker socket permission denied when `runAsUser: 1001` set | Removed `runAsNonRoot`, `runAsUser`, `runAsGroup` from worker securityContext; worker runs as root (per Dockerfile) | Keep permanently; remaining hardening: capabilities drop ALL + readOnlyRootFilesystem + NetworkPolicy |
| 12 | Redpanda rollout timed out (120s): root cause was three compounding bugs | (a) `serviceName: redpanda` referenced the old headless Service named `redpanda-headless` — mismatch means pod DNS unresolvable, broker fails cluster-membership check; (b) readiness probe used `rpk cluster info` which requires the advertised Kafka address to resolve — circular dependency with (a); (c) `storageClassName: standard` does not exist on Docker Desktop — PVC stays Pending, pod never starts; (d) 120s timeout too short for Docker Desktop storage I/O | Keep all four fixes permanently — see `k8s/redpanda/` |
| 13 | Headless Service renamed, ClusterIP service added as `redpanda-client` | `serviceName` in StatefulSet now matches headless Service `redpanda`; Kafka clients bootstrap via `redpanda-client:9092`; ConfigMap `REDPANDA_BROKERS` updated accordingly | Keep permanently |
| 14 | Redpanda readiness probe replaced | `rpk cluster info` exec replaced with `httpGet /v1/status/ready` on admin port 9644; `initialDelaySeconds` raised to 30, `failureThreshold` to 12 (120s window after delay) | Keep permanently |
| 15 | All three PVCs had `storageClassName: standard` | Removed `storageClassName` from `redpanda` volumeClaimTemplate, `timescaledb/pvc.yaml`, and `control-plane/pvc.yaml` — cluster uses its default StorageClass | Keep permanently |
| 16 | Smoke test timeout too short and gave no diagnostic output on failure | Raised `TIMEOUT` to 240s; `rollout_wait` helper now dumps pod describe + container log + previous-container log on failure | Keep permanently |

### Stage 4.1 — What was built

**16 files created across `terraform/` and `Makefile`.**

| File | Purpose |
|---|---|
| `terraform/versions.tf` | Pins `hashicorp/google ~> 5.30`, `google-beta ~> 5.30`, `kubernetes ~> 2.30`; GCS remote backend block |
| `terraform/variables.tf` | All 15 input variables with descriptions, type constraints, and validation rules; no defaults for sensitive vars |
| `terraform/outputs.tf` | 6 outputs: `cluster_name`, `cluster_endpoint` (sensitive), `cluster_ca_certificate` (sensitive), `kubeconfig_command`, `registry_url`, `worker_pool_id`, `workload_identity_pool` |
| `terraform/main.tf` | Thin root: 2 data sources + 3 module calls, zero resource blocks |
| `terraform/modules/cluster/main.tf` | VPC + subnet (with alias IP ranges for pods/services), Cloud Router + NAT, GKE node service account + 3 IAM role bindings, Workload Identity Pool + OIDC provider, private VPC-native GKE cluster with Calico NetworkPolicy, STABLE release channel, weekly maintenance window |
| `terraform/modules/cluster/variables.tf` | 10 variables scoped to cluster concerns |
| `terraform/modules/cluster/outputs.tf` | 7 outputs consumed by node-pools and registry modules |
| `terraform/modules/node-pools/main.tf` | Control-plane pool (on-demand, fixed count, surge upgrades) + worker pool (spot, cluster autoscaler, `role=benchmark-worker:NoSchedule` taint, 100 GB SSD) |
| `terraform/modules/node-pools/variables.tf` | 8 variables |
| `terraform/modules/node-pools/outputs.tf` | 6 outputs including taint key/value for manifest cross-reference |
| `terraform/modules/registry/main.tf` | Artifact Registry Docker repo with 10-tag keep + 7-day untagged cleanup policies; node SA reader binding; CI push SA + WIF binding scoped to `github_repository` |
| `terraform/modules/registry/variables.tf` | 8 variables |
| `terraform/modules/registry/outputs.tf` | `repository_url`, `repository_id`, `ci_push_service_account_email` |
| `terraform/envs/dev.tfvars` | `e2-standard-2`, worker min=0 max=3, single zone, open master CIDR |
| `terraform/envs/prod.tfvars` | `c2-standard-8`, worker min=1 max=10, 3 zones, IAP + CI CIDR |
| `Makefile` (updated) | `tf-validate` (fmt-check + init -backend=false + validate), `k8s-validate` (kubectl dry-run, graceful skip if kubectl absent), `lint`, `ci`, `build-push` targets; all documented in header |

*(Stages updated as work proceeds — see TASK.md for current status)*

---

## Architecture Diagram

```
  Contestant Browser / curl
         │
         ▼
  ┌─────────────────────────────┐
  │  Control Plane (:8080)      │
  │  cmd/server                 │
  │  ├─ POST /api/v1/submissions│──► Enqueue ──► jobs.benchmark (Redpanda)
  │  ├─ GET  /api/v1/leaderboard│◄── store reads
  │  └─ GET  /internal/workers  │◄── WorkerRegistry (in-memory)
  └─────────────────────────────┘
         ▲ heartbeat (5s)
         │ register
  ┌──────┴──────────────────────┐
  │  Worker (cmd/worker)        │
  │  ├─ Heartbeater goroutine   │
  │  └─ Worker poll loop        │◄── Dequeue ◄── jobs.benchmark
  │       └─ SandboxExecutor    │
  │            ├─ Deploy        │──► Docker sandbox container
  │            ├─ WaitHealthy   │
  │            ├─ [StatusBenchmarking set]
  │            ├─ Bot Fleet     │──► N goroutine bots ──► sandbox /orders
  │            │    └─ FleetResult{Stats, Results}
  │            ├─ GoldenOrderbook → CorrectnessResult
  │            ├─ BuildResults  │──► CompositeScore (p99+TPS+correctness)
  │            └─ BatchEmit     │──► metrics.latency (Redpanda)
  └─────────────────────────────┘
         │ writes results
         ▼
  ┌─────────────────────────────┐         ┌───────────────────────┐
  │  DiskStore (shared volume)  │         │  Consumer             │
  │  /data/submissions/{id}/    │         │  metrics.latency      │
  │  meta.json                  │         │  → TimescaleDB        │
  └─────────────────────────────┘         │  → Grafana dashboard  │
                                          └───────────────────────┘

  Phase 4 target — Kubernetes:
  ┌──────────────────────────────────────────────────────────┐
  │  GKE / EKS Cluster                                       │
  │  ├─ Deployment: control-plane  (1 replica)               │
  │  ├─ Deployment: metrics-consumer (1 replica)             │
  │  ├─ Deployment: worker  ──► HPA (scale on queue depth)   │
  │  │       min=1  max=10  target=5 pending jobs/replica     │
  │  ├─ StatefulSet: Redpanda (3 replicas, PVC storage)      │
  │  ├─ StatefulSet: TimescaleDB (1 replica, PVC storage)    │
  │  ├─ PersistentVolumeClaim: submissions-data (RWX)        │
  │  └─ Ingress: NGINX → control-plane :8080                 │
  └──────────────────────────────────────────────────────────┘
```

---

## Running the Stack

```bash
# Build sandbox images (one-time)
make images

# Start full stack with distributed mode
docker compose up --build -d

# Run smoke tests
STACK_RUNNING=1 bash scripts/smoke_test_phase3_stage3.sh

# Run all unit tests (offline, no infrastructure)
make test

# Scale to 3 workers
docker compose up --scale worker=3 -d
```

---

## Test Coverage

| Package | Tests | Infrastructure required |
|---|---|---|
| `internal/queue` | 9 | None |
| `internal/worker` | 13 (8 worker + 5 executor) | None |
| `internal/orchestrator` | 10 | None |
| `internal/botfleet` | 12 | None (httptest.Server) |
| `internal/correctness` | 13 | None |
| `internal/submission` | existing | None |
| `internal/telemetry` | existing + 4 BatchEmit | None (unit) / Redpanda (integration) |

All unit tests run in < 5 seconds total. The race detector is enabled on every `make test` run.

---

## What's Next — Phase 5

### Advanced Benchmarking

- Stress benchmark: volatile-only market data replay via Redpanda historical feed
- Latency injection: artificial jitter between bot orders to model real network conditions
- Chaos engineering: random container kills, network partition simulation
- Pause / Resume / Kill controls on live benchmark runs via the API
- FIX protocol transport for `BotTransport` interface
- WebSocket transport for streaming order feeds

---

## Running the Stack (updated for Phase 4)

```bash
# Local dev (unchanged from Phase 3)
make images && docker compose up --build -d

# Validate Terraform (no cloud credentials needed)
make tf-validate

# Validate K8s manifests (no cluster needed)
make k8s-validate

# Full local CI gate (mirrors GitHub Actions)
make ci

# Deploy to dev cluster (requires kubeconfig + registry access)
make build-push deploy
```
