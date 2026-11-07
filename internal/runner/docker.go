package runner

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	cerrdefs "github.com/containerd/errdefs"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
	"github.com/docker/go-connections/nat"
)

// Docker manages the study's containers.
type Docker struct {
	cli     *client.Client
	network string
}

// NewDocker connects using host, or the environment if host is empty.
func NewDocker(host, network string) (*Docker, error) {
	opts := []client.Opt{client.FromEnv, client.WithAPIVersionNegotiation()}
	if host != "" {
		opts = append(opts, client.WithHost(host))
	}
	cli, err := client.NewClientWithOpts(opts...)
	if err != nil {
		return nil, fmt.Errorf("docker client: %w", err)
	}
	return &Docker{cli: cli, network: network}, nil
}

// Close releases the client.
func (d *Docker) Close() error { return d.cli.Close() }

// Spec describes a Go component's container.
type Spec struct {
	Name, Image string
	Env         map[string]string
	Port        string // published on the same host port; empty for none
	MemoryBytes int64
}

// Recreate replaces the container. Docker fixes a container's environment at
// creation, so a restart could never clear a crash point (SPEC 7).
func (d *Docker) Recreate(ctx context.Context, spec Spec) error {
	if err := d.Remove(ctx, spec.Name); err != nil {
		return err
	}
	env := make([]string, 0, len(spec.Env))
	for key, value := range spec.Env {
		env = append(env, key+"="+value)
	}
	init := true
	config := &container.Config{Image: spec.Image, Env: env}
	host := &container.HostConfig{
		NetworkMode: container.NetworkMode(d.network),
		Init:        &init,
		Resources:   container.Resources{Memory: spec.MemoryBytes},
	}
	if spec.Port != "" {
		port := nat.Port(spec.Port + "/tcp")
		config.ExposedPorts = nat.PortSet{port: {}}
		host.PortBindings = nat.PortMap{port: {{HostIP: "127.0.0.1", HostPort: spec.Port}}}
	}
	created, err := d.cli.ContainerCreate(ctx, config, host, nil, nil, spec.Name)
	if err != nil {
		return fmt.Errorf("create %s: %w", spec.Name, err)
	}
	if err := d.cli.ContainerStart(ctx, created.ID, container.StartOptions{}); err != nil {
		return fmt.Errorf("start %s: %w", spec.Name, err)
	}
	return nil
}

// Remove deletes the container if it exists.
func (d *Docker) Remove(ctx context.Context, name string) error {
	err := d.cli.ContainerRemove(ctx, name, container.RemoveOptions{Force: true})
	if err != nil && !cerrdefs.IsNotFound(err) {
		return fmt.Errorf("remove %s: %w", name, err)
	}
	return nil
}

// Signal sends a signal to the container's init, which forwards it.
func (d *Docker) Signal(ctx context.Context, name, signal string) error {
	if err := d.cli.ContainerKill(ctx, name, signal); err != nil {
		return fmt.Errorf("signal %s: %w", name, err)
	}
	return nil
}

// Stop stops the container gracefully.
func (d *Docker) Stop(ctx context.Context, name string) error {
	if err := d.cli.ContainerStop(ctx, name, container.StopOptions{}); err != nil {
		return fmt.Errorf("stop %s: %w", name, err)
	}
	return nil
}

// Kill sends SIGKILL.
func (d *Docker) Kill(ctx context.Context, name string) error { return d.Signal(ctx, name, "SIGKILL") }

// Start starts a stopped container.
func (d *Docker) Start(ctx context.Context, name string) error {
	if err := d.cli.ContainerStart(ctx, name, container.StartOptions{}); err != nil {
		return fmt.Errorf("start %s: %w", name, err)
	}
	return nil
}

// Restart restarts the container gracefully.
func (d *Docker) Restart(ctx context.Context, name string) error {
	if err := d.cli.ContainerRestart(ctx, name, container.StopOptions{}); err != nil {
		return fmt.Errorf("restart %s: %w", name, err)
	}
	return nil
}

// Pause freezes the container.
func (d *Docker) Pause(ctx context.Context, name string) error {
	if err := d.cli.ContainerPause(ctx, name); err != nil {
		return fmt.Errorf("pause %s: %w", name, err)
	}
	return nil
}

// Unpause resumes the container.
func (d *Docker) Unpause(ctx context.Context, name string) error {
	if err := d.cli.ContainerUnpause(ctx, name); err != nil {
		return fmt.Errorf("unpause %s: %w", name, err)
	}
	return nil
}

// ConnectAlias joins the container to a network under an extra name.
func (d *Docker) ConnectAlias(ctx context.Context, networkName, name, alias string) error {
	settings := &network.EndpointSettings{Aliases: []string{alias}}
	if err := d.cli.NetworkConnect(ctx, networkName, name, settings); err != nil {
		return fmt.Errorf("connect %s to %s: %w", name, networkName, err)
	}
	return nil
}

// EnvValue reads one variable from the container's configuration.
func (d *Docker) EnvValue(ctx context.Context, name, key string) (string, error) {
	info, err := d.cli.ContainerInspect(ctx, name)
	if err != nil {
		return "", fmt.Errorf("inspect %s: %w", name, err)
	}
	for _, pair := range info.Config.Env {
		if value, ok := strings.CutPrefix(pair, key+"="); ok {
			return value, nil
		}
	}
	return "", nil
}

// Logs returns everything the container has written.
func (d *Docker) Logs(ctx context.Context, name string) ([]byte, error) {
	stream, err := d.cli.ContainerLogs(ctx, name, container.LogsOptions{ShowStdout: true, ShowStderr: true})
	if err != nil {
		return nil, fmt.Errorf("logs of %s: %w", name, err)
	}
	defer func() { _ = stream.Close() }()
	var out bytes.Buffer
	if _, err := stdcopy.StdCopy(&out, &out, stream); err != nil {
		return nil, fmt.Errorf("read logs of %s: %w", name, err)
	}
	return out.Bytes(), nil
}

// WaitForLog polls until marker appears in the container's log.
func (d *Docker) WaitForLog(ctx context.Context, name, marker string) error {
	for {
		logs, err := d.Logs(ctx, name)
		if err != nil {
			return err
		}
		if bytes.Contains(logs, []byte(marker)) {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// WaitForExit returns the container's exit code once it has stopped.
func (d *Docker) WaitForExit(ctx context.Context, name string) (int, error) {
	for {
		info, err := d.cli.ContainerInspect(ctx, name)
		if err != nil {
			return 0, fmt.Errorf("inspect %s: %w", name, err)
		}
		if !info.State.Running {
			return info.State.ExitCode, nil
		}
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// cpuSample is a point on a container's CPU counters.
type cpuSample struct {
	container, system uint64
	cpus              uint32
}

// CPU reads a container's CPU counters.
func (d *Docker) CPU(ctx context.Context, name string) (cpuSample, error) {
	stats, err := d.cli.ContainerStatsOneShot(ctx, name)
	if err != nil {
		return cpuSample{}, fmt.Errorf("stats of %s: %w", name, err)
	}
	defer func() { _ = stats.Body.Close() }()
	var body container.StatsResponse
	if err := json.NewDecoder(stats.Body).Decode(&body); err != nil {
		return cpuSample{}, fmt.Errorf("decode stats of %s: %w", name, err)
	}
	return cpuSample{body.CPUStats.CPUUsage.TotalUsage, body.CPUStats.SystemUsage, body.CPUStats.OnlineCPUs}, nil
}

// cpuPercent is CPU use between two samples, where 100 is one full core.
func cpuPercent(before, after cpuSample) float64 {
	system := after.system - before.system
	if system == 0 {
		return 0
	}
	return float64(after.container-before.container) / float64(system) * float64(after.cpus) * 100
}

// Versions lists the image each named container runs, with its digest.
func (d *Docker) Versions(ctx context.Context, containers, images []string) (map[string]string, error) {
	versions := make(map[string]string)
	for _, name := range containers {
		info, err := d.cli.ContainerInspect(ctx, name)
		if err != nil {
			return nil, fmt.Errorf("inspect %s: %w", name, err)
		}
		versions[info.Config.Image] = strings.TrimPrefix(info.Image, "sha256:")[:12]
	}
	for _, image := range images {
		info, err := d.cli.ImageInspect(ctx, image)
		if err != nil {
			return nil, errors.Join(fmt.Errorf("inspect image %s", image), err)
		}
		versions[image] = strings.TrimPrefix(info.ID, "sha256:")[:12]
	}
	return versions, nil
}
