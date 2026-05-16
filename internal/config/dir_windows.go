//go:build windows

package config

// defaultSubmissionDir returns a Windows path that Docker Desktop can bind-mount.
// Docker Desktop shares C:\ by default (Settings → Resources → File Sharing).
// /tmp on Windows is not a real path and cannot be bind-mounted.
func defaultSubmissionDir() string {
	return `C:\nexusbench\submissions`
}
