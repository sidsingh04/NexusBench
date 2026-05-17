package telemetry

import "fmt"

// errorf is a package-private helper so that event.go and emitter.go can
// return formatted errors without a top-level fmt import cluttering the
// public API surface.
func errorf(format string, args ...any) error {
	return fmt.Errorf("telemetry: "+format, args...)
}
