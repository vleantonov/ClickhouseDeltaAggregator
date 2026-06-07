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

// StopAllAggregators stops every consumer instance and waits until every
// container is confirmed not running. Used at reset time so state can be
// cleared without a live consumer racing us: docker-compose stop returns as
// soon as the SIGTERM is delivered, but the process (and its ZooKeeper session)
// can remain alive for several seconds afterward. Proceeding to ResetKeeperState
// while a container is still up risks the container re-writing the znodes we
// just deleted, leaving stale COMPLETED state that silently drops messages on
// the next run.
func (d *Docker) StopAllAggregators(ctx context.Context) error {
	for _, svc := range d.cfg.AggregatorServices {
		if err := d.StopService(ctx, svc); err != nil {
			return err
		}
	}
	for _, svc := range d.cfg.AggregatorServices {
		if err := d.WaitServiceStopped(ctx, svc, 2*time.Minute); err != nil {
			return err
		}
	}
	return nil
}

// WaitServiceStopped blocks until every container belonging to a compose service
// reports a non-running state, or the timeout elapses. It inspects the
// container by the service name as docker-compose sees it
// (<project>-<service>-<replica>).
func (d *Docker) WaitServiceStopped(ctx context.Context, service string, timeout time.Duration) error {
	// Ask compose for the container IDs it manages for this service.
	deadline := time.Now().Add(timeout)
	for {
		out, err := d.compose(ctx, "ps", "-q", service)
		if err != nil || strings.TrimSpace(out) == "" {
			// No containers found — service is down.
			return nil
		}
		ids := strings.Fields(strings.TrimSpace(out))
		allStopped := true
		for _, id := range ids {
			running, _ := d.run(ctx, "inspect", "-f", "{{.State.Running}}", id)
			if strings.TrimSpace(running) == "true" {
				allStopped = false
				break
			}
		}
		if allStopped {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("service %s still running after %s", service, timeout)
		}
		time.Sleep(300 * time.Millisecond)
	}
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
