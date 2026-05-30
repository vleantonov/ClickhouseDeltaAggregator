//go:build acceptance

// Package acceptance contains black-box, end-to-end acceptance tests that verify
// the core guarantee of the ClickhouseDeltaAggregator: messages flowing from a
// YDB topic into the distributed ClickHouse cluster are written EXACTLY ONCE —
// no loss and no double counting — even while the system is being disturbed.
//
// Each test produces a known dataset (a fixed number of transactions with a
// known total amount and unique transaction ids), injects a distributed-systems
// fault, then asserts the exactly-once invariant against ClickHouse:
//
//	distinct(transaction_id) == produced count   (else => LOSS)
//	count(rows)              == produced count    (else => DOUBLE-WRITE)
//	sum(amount) raw          == produced sum
//	sum(amount) reports      == produced sum      (else => DOUBLE-COUNT via MV)
//
// These tests drive the live docker-compose cluster and are therefore guarded by
// the `acceptance` build tag and skipped automatically when the infra is not
// reachable. See tests/README.md for how to run them.
package acceptance

import (
	"context"
	"testing"
	"time"

	"acceptance/internal/harness"
)

// testContext returns a context bounded a little beyond the completion timeout so
// a hung scenario fails the test rather than the whole `go test` run.
func testContext(t *testing.T, h *harness.Harness) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), h.Cfg.CompletionTimeout+5*time.Minute)
}

// --- Baseline: no faults ----------------------------------------------------
//
// The happy path. Both consumers run undisturbed; every produced transaction
// must land in ClickHouse exactly once. If this fails, the more aggressive
// fault scenarios are not worth interpreting — fix the baseline first.
func TestExactlyOnce_NoFaults(t *testing.T) {
	h := harness.New(t)
	ctx, cancel := testContext(t, h)
	defer cancel()

	// No fault injection: produce, let both consumers process, assert.
	h.RunScenario(ctx, t, nil)
}

// --- Case 1: adding and removing consumers on the topic ---------------------
//
// Managing the topic's consumer set (a YDB topic can carry several independent
// named consumers) must not disturb the aggregator's own consumer. We add a
// second consumer and drop it again mid-flight, and require the aggregator's
// stream to remain exactly-once.
func TestExactlyOnce_AddRemoveTopicConsumers(t *testing.T) {
	h := harness.New(t)
	ctx, cancel := testContext(t, h)
	defer cancel()

	const extra = "observer"

	h.RunScenario(ctx, t, func(ctx context.Context, t *testing.T) {
		if err := h.YDB.AddConsumer(ctx, extra); err != nil {
			t.Fatalf("add consumer %q: %v", extra, err)
		}
		assertConsumerPresent(ctx, t, h, extra, true)

		time.Sleep(5 * time.Second)

		if err := h.YDB.DropConsumer(ctx, extra); err != nil {
			t.Fatalf("drop consumer %q: %v", extra, err)
		}
		assertConsumerPresent(ctx, t, h, extra, false)

		// The aggregator's own consumer must still be there.
		assertConsumerPresent(ctx, t, h, h.Cfg.Consumer, true)
	})
}

// --- Case 2: disabling one of several consumers -----------------------------
//
// With two consumer instances sharing the topic, stopping one must not stall the
// stream: the survivor takes over the dropped partitions and finishes the work,
// still exactly-once.
func TestExactlyOnce_DisableOneConsumer(t *testing.T) {
	h := harness.New(t)
	ctx, cancel := testContext(t, h)
	defer cancel()

	h.RunScenario(ctx, t, func(ctx context.Context, t *testing.T) {
		// Let both instances pick up partitions, then permanently stop one.
		time.Sleep(10 * time.Second)
		victim := h.Cfg.AggregatorServices[1]
		t.Logf("stopping consumer %q; the survivor must finish the stream", victim)
		if err := h.Docker.StopService(ctx, victim); err != nil {
			t.Fatalf("stop consumer: %v", err)
		}
	})
}

// --- Case 3: disabling and re-enabling one of several consumers -------------
//
// Same as case 2, but the stopped instance is brought back. Processing must
// continue throughout, and the rejoin must not introduce duplicates (the
// returning instance may replay an in-progress range; the dedup token absorbs
// it).
func TestExactlyOnce_DisableAndReenableConsumer(t *testing.T) {
	h := harness.New(t)
	ctx, cancel := testContext(t, h)
	defer cancel()

	h.RunScenario(ctx, t, func(ctx context.Context, t *testing.T) {
		victim := h.Cfg.AggregatorServices[1]

		time.Sleep(10 * time.Second)
		t.Logf("stopping consumer %q", victim)
		if err := h.Docker.StopService(ctx, victim); err != nil {
			t.Fatalf("stop consumer: %v", err)
		}

		time.Sleep(20 * time.Second)
		t.Logf("re-enabling consumer %q", victim)
		if err := h.Docker.StartService(ctx, victim); err != nil {
			t.Fatalf("start consumer: %v", err)
		}
	})
}

// --- Case 4: disabling ClickHouse replicas ----------------------------------
//
// Stopping one replica of a shard (here clickhouse-01-01, as `make drop_replica`
// does) must not break exactly-once: with insert_quorum=auto the surviving
// replicas accept the quorum write. The replica is restarted on cleanup.
func TestExactlyOnce_ClickHouseReplicaDown(t *testing.T) {
	h := harness.New(t)
	ctx, cancel := testContext(t, h)
	defer cancel()

	const replica = "clickhouse-01-01"

	h.RunScenario(ctx, t, func(ctx context.Context, t *testing.T) {
		t.Cleanup(func() {
			_ = h.Docker.StartContainer(context.Background(), replica)
		})
		time.Sleep(5 * time.Second)
		t.Logf("stopping ClickHouse replica %q (shard still has quorum)", replica)
		if err := h.Docker.StopContainer(ctx, replica); err != nil {
			t.Fatalf("stop replica: %v", err)
		}
	})
}

// --- Case 5: breaking the connection between ClickHouse and the consumers ----
//
// A transient network outage to the storage layer: we freeze every ClickHouse
// node (their TCP connections hang), hold the partition for a window, then thaw.
// In-flight inserts fail and are retried/replayed; the result must still be
// exactly-once once connectivity is restored.
func TestExactlyOnce_ClickHouseConnectionBreak(t *testing.T) {
	h := harness.New(t)
	ctx, cancel := testContext(t, h)
	defer cancel()

	nodes := clickHouseNodes()

	h.RunScenario(ctx, t, func(ctx context.Context, t *testing.T) {
		time.Sleep(10 * time.Second)
		t.Log("severing consumer<->ClickHouse connectivity (pausing all CH nodes)")
		pause(ctx, t, h, nodes)
		t.Cleanup(func() { unpause(context.Background(), t, h, nodes) })

		// Hold the outage, then restore and let the consumers recover.
		time.Sleep(30 * time.Second)
		t.Log("restoring ClickHouse connectivity")
		unpause(ctx, t, h, nodes)
	})
}

// --- Extra case 6: ClickHouse-Keeper node outage ----------------------------
//
// The aggregator's locks and offset state live in a 3-node Keeper ensemble.
// Losing one node keeps quorum, so the exactly-once machinery must keep working.
//
// We PAUSE the node rather than stop it: the aggregator's initZKConn panics if a
// configured Keeper hostname fails to resolve, and `docker stop` removes the
// container's DNS record. Pausing keeps the name resolvable while the node is
// unresponsive — the realistic "node down, ensemble keeps quorum" condition.
func TestExactlyOnce_KeeperNodeDown(t *testing.T) {
	h := harness.New(t)
	ctx, cancel := testContext(t, h)
	defer cancel()

	const keeperNode = "clickhouse-keeper-03"

	h.RunScenario(ctx, t, func(ctx context.Context, t *testing.T) {
		t.Cleanup(func() {
			_ = h.Docker.UnpauseContainer(context.Background(), keeperNode)
		})
		time.Sleep(5 * time.Second)
		t.Logf("pausing Keeper node %q (ensemble keeps quorum)", keeperNode)
		if err := h.Docker.PauseContainer(ctx, keeperNode); err != nil {
			t.Fatalf("pause keeper: %v", err)
		}
		// Hold the outage for a while, then bring the node back.
		time.Sleep(45 * time.Second)
		t.Logf("unpausing Keeper node %q", keeperNode)
		if err := h.Docker.UnpauseContainer(ctx, keeperNode); err != nil {
			t.Fatalf("unpause keeper: %v", err)
		}
	})
}

// --- Extra case 7: chaos — repeatedly SIGKILL a consumer --------------------
//
// We abruptly SIGKILL a consumer several times during consumption (Docker's
// restart:always brings it back). A crash between push and commit is the classic
// exactly-once hazard; recovery + dedup must absorb every replay.
func TestExactlyOnce_ConsumerChaosKill(t *testing.T) {
	h := harness.New(t)
	ctx, cancel := testContext(t, h)
	defer cancel()

	h.RunScenario(ctx, t, func(ctx context.Context, t *testing.T) {
		victim := h.Cfg.AggregatorServices[0]
		for i := 0; i < 4; i++ {
			time.Sleep(12 * time.Second)
			t.Logf("chaos: SIGKILL %q (round %d)", victim, i+1)
			if err := h.Docker.KillService(ctx, victim); err != nil {
				t.Logf("kill (ignored): %v", err)
			}
			if err := h.Docker.StartService(ctx, victim); err != nil {
				t.Logf("restart (ignored): %v", err)
			}
		}
	})
}

// --- Extra case 8: rolling ClickHouse restart -------------------------------
//
// Restart the replicas of one shard one at a time, never losing quorum. This
// models a rolling deploy of the storage layer happening while the stream is
// live; exactly-once must survive it.
func TestExactlyOnce_RollingClickHouseRestart(t *testing.T) {
	h := harness.New(t)
	ctx, cancel := testContext(t, h)
	defer cancel()

	shardReplicas := []string{"clickhouse-01-01", "clickhouse-01-02", "clickhouse-01-03"}

	h.RunScenario(ctx, t, func(ctx context.Context, t *testing.T) {
		time.Sleep(10 * time.Second)
		for _, node := range shardReplicas {
			t.Logf("rolling restart: %q", node)
			if err := h.Docker.StopContainer(ctx, node); err != nil {
				t.Fatalf("stop %s: %v", node, err)
			}
			time.Sleep(8 * time.Second)
			if err := h.Docker.StartContainer(ctx, node); err != nil {
				t.Fatalf("start %s: %v", node, err)
			}
			if err := h.Docker.WaitContainerRunning(ctx, node, 60*time.Second); err != nil {
				t.Fatalf("wait %s: %v", node, err)
			}
			// Let the replica rejoin before disturbing the next one.
			time.Sleep(8 * time.Second)
		}
	})
}

// --- helpers ----------------------------------------------------------------

func clickHouseNodes() []string {
	return []string{
		"clickhouse-01-01", "clickhouse-01-02", "clickhouse-01-03",
		"clickhouse-02-01", "clickhouse-02-02", "clickhouse-02-03",
	}
}

func pause(ctx context.Context, t *testing.T, h *harness.Harness, nodes []string) {
	t.Helper()
	for _, n := range nodes {
		if err := h.Docker.PauseContainer(ctx, n); err != nil {
			t.Fatalf("pause %s: %v", n, err)
		}
	}
}

func unpause(ctx context.Context, t *testing.T, h *harness.Harness, nodes []string) {
	t.Helper()
	for _, n := range nodes {
		// Unpausing an already-running container errors harmlessly; ignore it.
		_ = h.Docker.UnpauseContainer(ctx, n)
	}
}

func assertConsumerPresent(ctx context.Context, t *testing.T, h *harness.Harness, name string, want bool) {
	t.Helper()
	consumers, err := h.YDB.ListConsumers(ctx)
	if err != nil {
		t.Fatalf("list consumers: %v", err)
	}
	found := false
	for _, c := range consumers {
		if c == name {
			found = true
			break
		}
	}
	if found != want {
		t.Errorf("consumer %q present=%v, want %v (consumers=%v)", name, found, want, consumers)
	}
}
