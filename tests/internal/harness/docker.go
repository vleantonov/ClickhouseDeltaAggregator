package harness

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"strings"
	"time"
)

// Docker drives the running compose cluster via the docker CLI. Consumer
// (aggregator) instances are addressed by their compose *service* name; the
// ClickHouse and Keeper nodes have explicit container_name values, so they are
// addressed directly.
type Docker struct {
	cfg Config
}

func NewDocker(cfg Config) *Docker { return &Docker{cfg: cfg} }

func (d *Docker) exec(ctx context.Context, name string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	var out, errBuf bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errBuf
	if err := cmd.Run(); err != nil {
		return out.String(), fmt.Errorf("%s %s: %w: %s", name, strings.Join(args, " "), err, strings.TrimSpace(errBuf.String()))
	}
	return out.String(), nil
}

// run invokes the plain docker CLI (for container/network operations).
func (d *Docker) run(ctx context.Context, args ...string) (string, error) {
	return d.exec(ctx, "docker", args...)
}

// compose invokes the configured compose command (e.g. the standalone
// `docker-compose` binary or the `docker compose` plugin) against this project.
func (d *Docker) compose(ctx context.Context, args ...string) (string, error) {
	parts := strings.Fields(d.cfg.ComposeBin)
	bin, lead := parts[0], parts[1:]
	full := append(lead, "-f", d.cfg.ComposeFile, "-p", d.cfg.ComposeProject)
	full = append(full, args...)
	return d.exec(ctx, bin, full...)
}

// --- consumer (aggregator) instances, by compose service name ---

// StopService gracefully stops a consumer instance (restart policy does not
// resurrect a manually-stopped container).
func (d *Docker) StopService(ctx context.Context, service string) error {
	_, err := d.compose(ctx, "stop", service)
	return err
}

// StartService (re)starts a previously stopped consumer instance.
func (d *Docker) StartService(ctx context.Context, service string) error {
	_, err := d.compose(ctx, "start", service)
	return err
}

// KillService sends SIGKILL to a consumer instance, simulating an abrupt crash.
func (d *Docker) KillService(ctx context.Context, service string) error {
	_, err := d.compose(ctx, "kill", service)
	return err
}

// StopAllAggregators stops every consumer instance. Used at reset time so state
// can be cleared without a live consumer racing us.
func (d *Docker) StopAllAggregators(ctx context.Context) error {
	for _, svc := range d.cfg.AggregatorServices {
		if err := d.StopService(ctx, svc); err != nil {
			return err
		}
	}
	return nil
}

// StartAllAggregators starts every consumer instance.
func (d *Docker) StartAllAggregators(ctx context.Context) error {
	for _, svc := range d.cfg.AggregatorServices {
		if err := d.StartService(ctx, svc); err != nil {
			return err
		}
	}
	return nil
}

// --- ClickHouse / Keeper nodes, by container name ---

// StopContainer stops a named container (e.g. a ClickHouse replica or a Keeper).
func (d *Docker) StopContainer(ctx context.Context, name string) error {
	_, err := d.run(ctx, "stop", name)
	return err
}

// StartContainer starts a named container.
func (d *Docker) StartContainer(ctx context.Context, name string) error {
	_, err := d.run(ctx, "start", name)
	return err
}

// PauseContainer freezes a container's processes (SIGSTOP), so existing/new TCP
// connections hang — modelling an unresponsive node / network black hole without
// tearing the process down.
func (d *Docker) PauseContainer(ctx context.Context, name string) error {
	_, err := d.run(ctx, "pause", name)
	return err
}

// UnpauseContainer resumes a paused container.
func (d *Docker) UnpauseContainer(ctx context.Context, name string) error {
	_, err := d.run(ctx, "unpause", name)
	return err
}

// WaitContainerRunning blocks until a named container reports the "running"
// state, or the timeout elapses.
func (d *Docker) WaitContainerRunning(ctx context.Context, name string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		out, err := d.run(ctx, "inspect", "-f", "{{.State.Running}}", name)
		if err == nil && strings.TrimSpace(out) == "true" {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("container %s not running within %s", name, timeout)
		}
		time.Sleep(500 * time.Millisecond)
	}
}
