// NexusBench — Distributed Benchmarking and Hosting Platform
// This file is intentionally minimal. The real entrypoint is cmd/server/main.go.
// Run: go run ./cmd/server
package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Fprintln(os.Stderr, "Run `go run ./cmd/server` to start the control plane.")
	fmt.Fprintln(os.Stderr, "Or: docker compose up --build")
	os.Exit(1)
}
