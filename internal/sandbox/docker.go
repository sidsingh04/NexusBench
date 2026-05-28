// Package sandbox manages Docker container lifecycle for contestant submissions.
package sandbox

import (
	"archive/tar"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
	"github.com/nexusbench/nexusbench/internal/config"
	"github.com/nexusbench/nexusbench/internal/models"
)

const (
	containerPort     = "7878/tcp"
	labelSubmissionID = "nexusbench.submission_id"
)

// DockerManager wraps the Docker SDK client and owns the port pool
// and image registry for all active sandboxes.
type DockerManager struct {
	cli       *client.Client
	cfg       *config.Config
	portMu    sync.Mutex
	usedPorts map[int]string // host port → submission ID
}

func NewDockerManager(cfg *config.Config) (*DockerManager, error) {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, fmt.Errorf("connect to docker daemon: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := cli.Ping(ctx); err != nil {
		return nil, fmt.Errorf("docker daemon unreachable: %w", err)
	}
	return &DockerManager{
		cli:       cli,
		cfg:       cfg,
		usedPorts: make(map[int]string),
	}, nil
}

func (m *DockerManager) VerifyImages(ctx context.Context) error {
	var missing []string
	for lang, image := range m.cfg.AllImages() {
		if _, _, err := m.cli.ImageInspectWithRaw(ctx, image); err != nil {
			missing = append(missing, fmt.Sprintf("%s → %s", lang, image))
			slog.Warn("sandbox image not found locally", "language", lang, "image", image)
		} else {
			slog.Info("sandbox image verified", "language", lang, "image", image)
		}
	}
	if len(missing) > 0 {
		slog.Warn("some sandbox images are missing — those languages will fail at deploy time", "missing", missing)
		if len(missing) == len(m.cfg.AllImages()) {
			return fmt.Errorf("no sandbox images found; run `make images` to build them")
		}
	}
	return nil
}

// Deploy starts a sandbox container for the given submission.
//
// Security model (Phase 1 — local dev):
//
//	The entrypoint runs as root briefly to:
//	  1. Extract the archive (needs write access to /app)
//	  2. chown /app to runner  (needs CAP_CHOWN)
//	  3. su to runner          (needs CAP_SETUID + CAP_SETGID)
//	After exec, the contestant binary runs as uid=1000 with no capabilities.
//
//	Capabilities kept:  CHOWN, SETUID, SETGID (dropped after exec via su)
//	Capabilities dropped: everything else (NET_RAW, SYS_ADMIN, etc.)
//	no-new-privileges: intentionally OFF — su requires it to be off
//
//	Phase 4 will replace this with gVisor (runsc) for true kernel isolation.
func (m *DockerManager) Deploy(ctx context.Context, sub *models.Submission) (containerID string, hostPort int, err error) {
	// ── 1. Resolve image ──────────────────────────────────────────────────────
	image, ok := m.cfg.ImageForLanguage(string(sub.Language))
	if !ok {
		return "", 0, fmt.Errorf("no sandbox image configured for language %q", sub.Language)
	}
	if _, _, errInspect := m.cli.ImageInspectWithRaw(ctx, image); errInspect != nil {
		return "", 0, fmt.Errorf(
			"sandbox image %q not found locally — run `make image-%s` first",
			image, sub.Language,
		)
	}

	slog.Info("deploying sandbox",
		"submission_id", sub.ID,
		"team", sub.TeamName,
		"language", sub.Language,
		"image", image,
	)

	// ── 2. Allocate host port ─────────────────────────────────────────────────
	hostPort, err = m.allocatePort(sub.ID)
	if err != nil {
		return "", 0, err
	}
	defer func() {
		if err != nil {
			m.releasePort(hostPort)
		}
	}()

	// ── 3. (Removed bind mount in favor of CopyToContainer) ──────────────────

	// ── 4. Container config ───────────────────────────────────────────────────
	portBindings := nat.PortMap{
		nat.Port(containerPort): []nat.PortBinding{
			{HostIP: "0.0.0.0", HostPort: fmt.Sprintf("%d", hostPort)},
		},
	}
	exposedPorts := nat.PortSet{nat.Port(containerPort): struct{}{}}

	resources := container.Resources{
		Memory:    m.cfg.SandboxMemoryBytes,
		CPUQuota:  m.cfg.SandboxCPUQuota,
		CPUPeriod: 100_000,
		PidsLimit: int64ptr(512),
	}

	containerName := fmt.Sprintf("nexusbench-%s", sub.ID[:8])

	resp, err := m.cli.ContainerCreate(ctx,
		&container.Config{
			Image:        image,
			ExposedPorts: exposedPorts,
			Env:          buildEnv(sub),
			Labels: map[string]string{
				labelSubmissionID:     sub.ID,
				"nexusbench.team":     sub.TeamName,
				"nexusbench.language": string(sub.Language),
				"nexusbench.image":    image,
			},
		},
		&container.HostConfig{
			PortBindings: portBindings,
			Resources:    resources,
			NetworkMode:  container.NetworkMode(m.cfg.SandboxNetworkMode),

			// ── Capability model ──────────────────────────────────────────────
			// Drop everything, then add back only what the entrypoint needs:
			//   CHOWN   — chown /app to runner after extraction
			//   SETUID  — su runner (switch uid)
			//   SETGID  — su runner (switch gid)
			// Once `su` exec's the contestant binary, Linux drops these
			// automatically because the binary runs as non-root uid=1000.
			//
			// no-new-privileges is intentionally NOT set here —
			// it would block the `su` call inside the entrypoint.
			// Phase 4 will enforce kernel-level isolation via gVisor instead.
			CapDrop: []string{
				"NET_RAW",
				"SYS_ADMIN",
				"SYS_PTRACE",
				"SYS_MODULE",
				"SYS_RAWIO",
				"SYS_PACCT",
				"SYS_NICE",
				"SYS_RESOURCE",
				"SYS_TIME",
				"SYS_TTY_CONFIG",
				"AUDIT_WRITE",
				"AUDIT_CONTROL",
				"MAC_OVERRIDE",
				"MAC_ADMIN",
				"NET_ADMIN",
				"SYSLOG",
				"DAC_READ_SEARCH",
				"LINUX_IMMUTABLE",
				"NET_BROADCAST",
				"IPC_LOCK",
				"IPC_OWNER",
				"SYS_BOOT",
				"LEASE",
				"WAKE_ALARM",
				"BLOCK_SUSPEND",
			},
			// Capabilities intentionally kept (not in CapDrop):
			//   CHOWN, SETUID, SETGID, DAC_OVERRIDE, FOWNER, FSETID

			OomScoreAdj:   500,
			RestartPolicy: container.RestartPolicy{Name: "no"},
		},
		nil, nil,
		containerName,
	)
	if err != nil {
		return "", 0, fmt.Errorf("create container: %w", err)
	}

	// ── 4.5. Inject archive ───────────────────────────────────────────────────
	slog.Info("injecting archive into sandbox", "archive", sub.ArchivePath)
	if err := m.injectArchive(ctx, resp.ID, sub.ArchivePath); err != nil {
		_ = m.cli.ContainerRemove(ctx, resp.ID, container.RemoveOptions{Force: true})
		return "", 0, fmt.Errorf("inject archive: %w", err)
	}

	// ── 5. Start ──────────────────────────────────────────────────────────────
	if err := m.cli.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		_ = m.cli.ContainerRemove(ctx, resp.ID, container.RemoveOptions{Force: true})
		return "", 0, fmt.Errorf("start container: %w", err)
	}

	slog.Info("sandbox container started",
		"container_id", resp.ID[:12],
		"name", containerName,
		"image", image,
		"host_port", hostPort,
		"submission_id", sub.ID,
	)

	go m.tailLogs(context.WithoutCancel(ctx), resp.ID, sub.ID)
	return resp.ID, hostPort, nil
}

func (m *DockerManager) Stop(ctx context.Context, containerID string) error {
	timeout := 10
	if err := m.cli.ContainerStop(ctx, containerID, container.StopOptions{Timeout: &timeout}); err != nil {
		return fmt.Errorf("stop container %s: %w", containerID[:12], err)
	}
	if err := m.cli.ContainerRemove(ctx, containerID, container.RemoveOptions{}); err != nil {
		slog.Warn("failed to remove container after stop", "id", containerID[:12], "err", err)
	}
	m.releasePortByContainerID(containerID)
	return nil
}

func (m *DockerManager) ContainerHealthy(ctx context.Context, containerID string) (bool, error) {
	info, err := m.cli.ContainerInspect(ctx, containerID)
	if err != nil {
		return false, err
	}
	if !info.State.Running {
		return false, nil
	}
	if info.State.Health == nil {
		return true, nil
	}
	return info.State.Health.Status == "healthy", nil
}

func (m *DockerManager) PruneStale(ctx context.Context) error {
	f := filters.NewArgs()
	f.Add("label", labelSubmissionID)
	f.Add("status", "exited")
	list, err := m.cli.ContainerList(ctx, container.ListOptions{All: true, Filters: f})
	if err != nil {
		return err
	}
	for _, c := range list {
		slog.Info("pruning stale container", "id", c.ID[:12], "image", c.Image)
		_ = m.cli.ContainerRemove(ctx, c.ID, container.RemoveOptions{Force: true})
	}
	return nil
}

func (m *DockerManager) Inspect(ctx context.Context, containerID string) (*types.ContainerJSON, error) {
	info, err := m.cli.ContainerInspect(ctx, containerID)
	if err != nil {
		return nil, err
	}
	return &info, nil
}

// ── Port pool ─────────────────────────────────────────────────────────────────

func (m *DockerManager) allocatePort(submissionID string) (int, error) {
	m.portMu.Lock()
	defer m.portMu.Unlock()
	for port := m.cfg.SandboxPortMin; port <= m.cfg.SandboxPortMax; port++ {
		if _, used := m.usedPorts[port]; !used {
			m.usedPorts[port] = submissionID
			return port, nil
		}
	}
	return 0, fmt.Errorf("port pool exhausted (%d–%d)", m.cfg.SandboxPortMin, m.cfg.SandboxPortMax)
}

func (m *DockerManager) releasePort(port int) {
	m.portMu.Lock()
	defer m.portMu.Unlock()
	delete(m.usedPorts, port)
}

func (m *DockerManager) releasePortByContainerID(containerID string) {
	m.portMu.Lock()
	defer m.portMu.Unlock()
	for port, id := range m.usedPorts {
		if id == containerID {
			delete(m.usedPorts, port)
			return
		}
	}
}

// ── Helpers ───────────────────────────────────────────────────────────────────

func buildEnv(sub *models.Submission) []string {
	return []string{
		"NEXUS_SUBMISSION_ID=" + sub.ID,
		"NEXUS_TEAM=" + sub.TeamName,
		"NEXUS_LANGUAGE=" + string(sub.Language),
		"NEXUS_PROTOCOL=" + string(sub.Protocol),
		"NEXUS_LISTEN_PORT=7878",
	}
}

func (m *DockerManager) tailLogs(ctx context.Context, containerID, submissionID string) {
	reader, err := m.cli.ContainerLogs(ctx, containerID, container.LogsOptions{
		ShowStdout: true,
		ShowStderr: true,
		Follow:     true,
	})
	if err != nil {
		slog.Warn("could not attach container log stream", "container", containerID[:12], "err", err)
		return
	}
	defer reader.Close()

	buf := make([]byte, 4096)
	for {
		n, err := reader.Read(buf)
		if n > 8 {
			slog.Info("[sandbox log]",
				"submission_id", submissionID,
				"container", containerID[:12],
				"msg", strings.TrimSpace(string(buf[8:n])),
			)
		}
		if err == io.EOF || err != nil {
			break
		}
	}
}

func int64ptr(i int64) *int64 { return &i }

func (m *DockerManager) EnsureImage(ctx context.Context, image string) error {
	if _, _, err := m.cli.ImageInspectWithRaw(ctx, image); err == nil {
		return nil
	}
	slog.Info("pulling image", "image", image)
	rc, err := m.cli.ImagePull(ctx, image, types.ImagePullOptions{})
	if err != nil {
		return fmt.Errorf("pull %s: %w", image, err)
	}
	defer rc.Close()
	_, _ = io.Copy(os.Stdout, rc)
	return nil
}

func (m *DockerManager) injectArchive(ctx context.Context, containerID string, archivePath string) error {
	fileInfo, err := os.Stat(archivePath)
	if err != nil {
		return err
	}

	pr, pw := io.Pipe()
	go func() {
		defer func() { _ = pw.Close() }()
		tw := tar.NewWriter(pw)
		defer func() { _ = tw.Close() }()

		file, err := os.Open(archivePath)
		if err != nil {
			pw.CloseWithError(err)
			return
		}
		defer func() { _ = file.Close() }()

		hdr := &tar.Header{
			Name: filepath.Base(archivePath),
			Mode: 0644,
			Size: fileInfo.Size(),
		}
		if err := tw.WriteHeader(hdr); err != nil {
			pw.CloseWithError(err)
			return
		}
		if _, err := io.Copy(tw, file); err != nil {
			pw.CloseWithError(err)
			return
		}
	}()

	return m.cli.CopyToContainer(ctx, containerID, "/submission", pr, types.CopyToContainerOptions{})
}
