# NexusBench

**Distributed Benchmarking and Hosting Platform** — IICPC Summer Hackathon 2026

NexusBench stress-tests contestant trading engines (matching engines / orderbooks) by:
1. Securely containerising and deploying submitted code
2. Bombarding it with a distributed fleet of trading bots
3. Capturing p50/p90/p99 latency, TPS, and correctness metrics
4. Streaming results to a live leaderboard

---

## Repository Structure

```
NexusBench/
├── cmd/
│   └── server/          ← Control plane entrypoint (go run ./cmd/server)
├── internal/
│   ├── api/             ← HTTP router + handlers
│   ├── config/          ← Environment-based configuration
│   ├── models/          ← Shared domain types
│   ├── sandbox/         ← Docker container lifecycle manager
│   └── submission/      ← Upload, validate, persist, deploy logic
├── docker/
│   └── sandbox/         ← Dockerfile + entrypoint for contestant runtime
├── scripts/
│   ├── dev.sh           ← One-command local dev bootstrap
│   └── smoke_test.sh    ← End-to-end smoke tests
├── docker-compose.yml   ← Local dev stack
├── Dockerfile.server    ← Control plane container image
└── go.mod
```

---

## Development Phases

| Phase | Description | Status |
|-------|-------------|--------|
| **1 — Core MVP** | Submission engine, sandboxing, REST API | ✅ In progress |
| **2 — Telemetry** | Live metrics, ClickHouse, Grafana dashboard | ⬜ Planned |
| **3 — Bot Fleet** | Distributed load generator, Redpanda integration | ⬜ Planned |
| **4 — IaC** | Terraform, K8s manifests, autoscaling | ⬜ Planned |
| **5 — Advanced** | Chaos engineering, latency injection, correctness engine | ⬜ Planned |

---

## Quick Start

### Prerequisites
- Go 1.22+
- Docker + Docker Compose

### 1. Build the sandbox image

```bash
docker build -t nexusbench-sandbox:latest ./docker/sandbox
```

### 2. Start the control plane

```bash
docker compose up --build control-plane
# or for local hot-reload:
go run ./cmd/server
```

The API is now live at `http://localhost:8080`.

### 3. Submit a trading engine

```bash
# Package your code
tar -czf my-engine.tar.gz -C /path/to/your/engine .

# Submit
curl -X POST http://localhost:8080/api/v1/submissions \
  -F "team_name=my-team" \
  -F "language=go" \
  -F "protocol=rest" \
  -F "archive=@my-engine.tar.gz"
```

### 4. Check status

```bash
curl http://localhost:8080/api/v1/submissions/{id}
```

---

## API Reference

### `GET /health`
Returns service health.

### `POST /api/v1/submissions`
Upload a submission. Multipart form fields:

| Field | Type | Values |
|-------|------|--------|
| `team_name` | string | Any non-empty string |
| `language` | string | `go`, `rust`, `cpp`, `python`, `binary` |
| `protocol` | string | `rest`, `websocket`, `fix` |
| `archive` | file | `.tar.gz` or `.zip` of source code or binary |

### `GET /api/v1/submissions`
List all submissions (newest first).

### `GET /api/v1/submissions/{id}`
Get a single submission with full status and results.

### `POST /api/v1/submissions/{id}/stop`
Tear down the sandbox container.

### `GET /api/v1/leaderboard`
Completed submissions ranked by composite score.

---

## Sandbox Contract

Your trading engine **must**:
- Listen on the port in `$NEXUS_LISTEN_PORT` (default: `7878`)
- Implement a `/health` endpoint returning `{"status":"ok"}`
- Expose an `/order` endpoint (for REST protocol) accepting order payloads

The platform will inject these environment variables:

| Variable | Description |
|----------|-------------|
| `NEXUS_SUBMISSION_ID` | UUID of your submission |
| `NEXUS_TEAM` | Your team name |
| `NEXUS_LANGUAGE` | Language declared on upload |
| `NEXUS_PROTOCOL` | Protocol declared on upload |
| `NEXUS_LISTEN_PORT` | Port to listen on (always `7878`) |

---

## Scoring Formula

```
CompositeScore = 0.4 × (1 / p99_latency_ms)
              + 0.4 × normalized_TPS
              + 0.2 × correctness_rate
```

Where `correctness_rate` is the fraction of orders correctly matched with price-time priority maintained.

---

## Configuration (Environment Variables)

| Variable | Default | Description |
|----------|---------|-------------|
| `LISTEN_ADDR` | `:8080` | Control plane bind address |
| `SUBMISSION_DIR` | `/tmp/nexusbench/submissions` | Archive storage path |
| `SANDBOX_IMAGE` | `nexusbench-sandbox:latest` | Docker image for sandboxes |
| `SANDBOX_CPU_QUOTA` | `100000` | CPU quota (µs/100ms, 100000 = 1 core) |
| `SANDBOX_MEMORY_BYTES` | `536870912` | Memory limit per container (512 MiB) |
| `SANDBOX_TIMEOUT` | `30m` | Max sandbox lifetime |
| `SANDBOX_PORT_MIN` | `20000` | Start of host port allocation range |
| `SANDBOX_PORT_MAX` | `21000` | End of host port allocation range |
| `MAX_UPLOAD_BYTES` | `268435456` | Max upload size (256 MiB) |
