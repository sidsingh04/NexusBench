# Graph Report - NexusBench  (2026-05-16)

## Corpus Check
- 14 files · ~6,053 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 132 nodes · 183 edges · 14 communities
- Extraction: 92% EXTRACTED · 8% INFERRED · 0% AMBIGUOUS · INFERRED: 15 edges (avg confidence: 0.8)
- Token cost: 0 input · 0 output

## Community Hubs (Navigation)
- [[_COMMUNITY_Community 0|Community 0]]
- [[_COMMUNITY_Community 1|Community 1]]
- [[_COMMUNITY_Community 2|Community 2]]
- [[_COMMUNITY_Community 3|Community 3]]
- [[_COMMUNITY_Community 4|Community 4]]
- [[_COMMUNITY_Community 5|Community 5]]
- [[_COMMUNITY_Community 6|Community 6]]
- [[_COMMUNITY_Community 7|Community 7]]
- [[_COMMUNITY_Community 8|Community 8]]

## God Nodes (most connected - your core abstractions)
1. `DockerManager` - 12 edges
2. `writeJSON()` - 10 edges
3. `handler` - 8 edges
4. `Load()` - 8 edges
5. `Service` - 8 edges
6. `Claude Engineering Guidelines` - 8 edges
7. `NexusBench` - 8 edges
8. `writeError()` - 7 edges
9. `DiskStore` - 7 edges
10. `API Reference` - 7 edges

## Surprising Connections (you probably didn't know these)
- `main()` --calls--> `Load()`  [INFERRED]
  cmd/server/main.go → internal/config/config.go
- `main()` --calls--> `NewDockerManager()`  [INFERRED]
  cmd/server/main.go → internal/sandbox/docker.go
- `main()` --calls--> `NewRouter()`  [INFERRED]
  cmd/server/main.go → internal/api/router.go
- `main()` --calls--> `NewDiskStore()`  [INFERRED]
  cmd/server/main.go → internal/submission/service.go
- `main()` --calls--> `NewService()`  [INFERRED]
  cmd/server/main.go → internal/submission/service.go

## Communities (14 total, 0 thin omitted)

### Community 0 - "Community 0"
Cohesion: 0.17
Nodes (8): DiskStore, Service, archiveExt(), readJSON(), validateLanguage(), validateProtocol(), writeJSON(), Store

### Community 1 - "Community 1"
Cohesion: 0.2
Nodes (9): handler, responseWriter, classifyError(), corsMiddleware(), writeError(), writeJSON(), SubmissionService, Language (+1 more)

### Community 2 - "Community 2"
Cohesion: 0.18
Nodes (4): buildEnv(), int64ptr(), toDockerPath(), DockerManager

### Community 3 - "Community 3"
Cohesion: 0.12
Nodes (15): API Reference, code:block1 (NexusBench/), code:block6 (CompositeScore = 0.4 × (1 / p99_latency_ms)), Configuration (Environment Variables), Development Phases, `GET /api/v1/leaderboard`, `GET /api/v1/submissions`, `GET /api/v1/submissions/{id}` (+7 more)

### Community 4 - "Community 4"
Cohesion: 0.2
Nodes (11): NewRouter(), NewDockerManager(), main(), mockDocker, NewDiskStore(), NewService(), makeFakeFileHeader(), TestDiskStore_List() (+3 more)

### Community 5 - "Community 5"
Cohesion: 0.2
Nodes (9): Benchmarking Rules, Claude Engineering Guidelines, Code Modification Rules, Communication Rules, Distributed Systems Rules, graphify, Reliability Rules, Simplicity Rules (+1 more)

### Community 6 - "Community 6"
Cohesion: 0.2
Nodes (10): 1. Build the sandbox image, 2. Start the control plane, 3. Submit a trading engine, 4. Check status, code:bash (docker build -t nexusbench-sandbox:latest ./docker/sandbox), code:bash (docker compose up --build control-plane), code:bash (# Package your code), code:bash (curl http://localhost:8080/api/v1/submissions/{id}) (+2 more)

### Community 7 - "Community 7"
Cohesion: 0.42
Nodes (6): Config, getEnv(), getEnvDuration(), getEnvInt(), getEnvInt64(), Load()

### Community 8 - "Community 8"
Cohesion: 0.29
Nodes (6): APIError, BenchmarkResults, LeaderboardEntry, Submission, SubmissionStatus, SubmitRequest

## Knowledge Gaps
- **32 isolated node(s):** `SubmissionStatus`, `Submission`, `BenchmarkResults`, `LeaderboardEntry`, `APIError` (+27 more)
  These have ≤1 connection - possible missing edges or undocumented components.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `main()` connect `Community 4` to `Community 7`?**
  _High betweenness centrality (0.290) - this node is a cross-community bridge._
- **Why does `NewRouter()` connect `Community 4` to `Community 1`?**
  _High betweenness centrality (0.186) - this node is a cross-community bridge._
- **Why does `NewDockerManager()` connect `Community 4` to `Community 2`?**
  _High betweenness centrality (0.133) - this node is a cross-community bridge._
- **Are the 3 inferred relationships involving `Load()` (e.g. with `main()` and `TestValidation_BadLanguage()`) actually correct?**
  _`Load()` has 3 INFERRED edges - model-reasoned connections that need verification._
- **What connects `SubmissionStatus`, `Submission`, `BenchmarkResults` to the rest of the system?**
  _32 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `Community 3` be split into smaller, more focused modules?**
  _Cohesion score 0.12 - nodes in this community are weakly interconnected._