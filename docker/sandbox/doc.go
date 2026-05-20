//go:build ignore

// This file intentionally uses the "ignore" build tag to prevent the Go
// toolchain from treating docker/sandbox/ as a Go package.
//
// The files Dockerfile.golang, Dockerfile.rust, etc. in this directory use the
// .go extension purely for editor syntax highlighting. They are Docker build
// files, not Go source. Without this sentinel, `go build ./...` fails with
// "expected 'package', found FROM".
//
// To build the sandbox images run:
//
//	make images        (all languages)
//	make image-go      (Go only)
//	make image-rust    (Rust only)
package sandbox
