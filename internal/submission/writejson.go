package submission

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// writeJSON atomically writes v as indented JSON to path.
//
// Strategy: write to a sibling temp file in the same directory, sync to disk,
// then rename over the target. os.Rename is atomic on POSIX filesystems and
// on Windows within the same volume. This guarantees that concurrent readers
// (the leaderboard watcher running in a separate process in distributed mode)
// never observe a 0-byte or partially-written file — they see either the old
// complete file or the new complete file, never a truncated intermediate.
//
// The temp file is placed in the same directory as path so the rename is
// always within the same filesystem, avoiding cross-device link errors that
// would occur if os.TempDir() resolves to a different mount.
func writeJSON(path string, v any) error {
	dir := filepath.Dir(path)

	tmp, err := os.CreateTemp(dir, ".meta-*.json.tmp")
	if err != nil {
		return fmt.Errorf("writeJSON: create temp: %w", err)
	}
	tmpPath := tmp.Name()

	success := false
	defer func() {
		if !success {
			_ = os.Remove(tmpPath)
		}
	}()

	enc := json.NewEncoder(tmp)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("writeJSON: encode: %w", err)
	}

	// Sync to disk before rename so data is durable even on crash.
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("writeJSON: sync: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("writeJSON: close: %w", err)
	}

	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("writeJSON: rename: %w", err)
	}

	success = true
	return nil
}

func readJSON(path string, v any) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	return json.NewDecoder(f).Decode(v)
}
