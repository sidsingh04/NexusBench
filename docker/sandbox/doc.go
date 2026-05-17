// Package sandbox contains Dockerfiles for NexusBench sandbox images.
// These are NOT Go source files — this doc.go exists only to give the
// directory a valid package declaration so `go list ./...` does not error
// on Dockerfile.go (which has a .go extension for editor syntax highlighting
// but is a Docker build file, not Go source).
//
// To build the images, run:
//
//	make images          # all languages
//	make image-go        # single language
package sandbox
