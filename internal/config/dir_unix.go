//go:build !windows

package config

// defaultSubmissionDir returns the default submission storage path on
// Linux and macOS. /tmp is accessible to Docker on both platforms.
func defaultSubmissionDir() string {
	return "/tmp/nexusbench/submissions"
}
