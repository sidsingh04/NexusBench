#!/usr/bin/env bash
# scripts/dev.sh — one-command local development bootstrap
#
# Usage:
#   ./scripts/dev.sh           # start the full stack
#   ./scripts/dev.sh build     # build the sandbox image only
#   ./scripts/dev.sh test      # run unit tests
#   ./scripts/dev.sh submit    # submit a test archive (requires curl)

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

CMD="${1:-up}"

case "$CMD" in
up)
    echo "▶ Building sandbox image..."
    docker build -t nexusbench-sandbox:latest ./docker/sandbox

    echo "▶ Starting control plane..."
    docker compose up --build control-plane
    ;;

build)
    docker build -t nexusbench-sandbox:latest ./docker/sandbox
    echo "✓ nexusbench-sandbox:latest built"
    ;;

test)
    echo "▶ Running unit tests..."
    go test ./... -v -race -timeout 60s
    ;;

submit)
    TEAM="${TEAM:-test-team}"
    LANG="${LANG:-binary}"
    PROTO="${PROTO:-rest}"
    ARCHIVE="${ARCHIVE:-}"

    if [ -z "$ARCHIVE" ]; then
        # Create a tiny dummy binary for smoke testing
        echo "▶ Creating dummy submission archive..."
        TMP=$(mktemp -d)
        cat > "$TMP/main.go" << 'EOF'
package main

import (
    "fmt"
    "net/http"
    "os"
)

func main() {
    port := os.Getenv("NEXUS_LISTEN_PORT")
    if port == "" { port = "7878" }
    http.HandleFunc("/order", func(w http.ResponseWriter, r *http.Request) {
        fmt.Fprintf(w, `{"status":"ack","order_id":"test-123"}`)
    })
    http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
        fmt.Fprintf(w, `{"status":"ok"}`)
    })
    fmt.Printf("dummy engine listening on :%s\n", port)
    http.ListenAndServe(":"+port, nil)
}
EOF
        cp go.mod "$TMP/" 2>/dev/null || true
        ARCHIVE=$(mktemp /tmp/nexusbench-test-XXXXXX.tar.gz)
        tar -czf "$ARCHIVE" -C "$TMP" .
        echo "✓ Created test archive: $ARCHIVE"
    fi

    echo "▶ Submitting to http://localhost:8080..."
    curl -s -X POST http://localhost:8080/api/v1/submissions \
        -F "team_name=${TEAM}" \
        -F "language=${LANG}" \
        -F "protocol=${PROTO}" \
        -F "archive=@${ARCHIVE}" \
        | python3 -m json.tool
    ;;

*)
    echo "Unknown command: $CMD"
    echo "Usage: $0 [up|build|test|submit]"
    exit 1
    ;;
esac
