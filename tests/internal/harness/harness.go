package harness

import (
	"context"
	"testing"
	"time"
)

// Harness bundles every collaborator a scenario needs and provides the common
// arrange/act/assert steps shared by all acceptance tests.
type Harness struct {
	Cfg    Config
	YDB    *YDB
	CH     *CH
	Docker *Docker
}

// New builds a Harness or skips the test if the live infrastructure is not
// reachable. Acceptance tests require `make infra_with_db` to have been run.
func New(t *testing.T) *Harness {
	t.Helper()
	cfg := DefaultConfig()

	ch, err := ConnectClickHouse(cfg)
	if err != nil {
		t.Skipf("ClickHouse not reachable (%v); run `make infra_with_db` first", err)
	}
	ydb, err := ConnectYDB(cfg)
	if err != nil {
		ch.Close()
		t.Skipf("YDB not reachable (%v); run `make infra_with_db` first", err)
	}

	h := &Harness{Cfg: cfg, YDB: ydb, CH: ch, Docker: NewDocker(cfg)}
	t.Cleanup(func() {
		h.YDB.Close()
		h.CH.Close()
	})
	return h
}

// Reset brings the system to a clean, well-defined starting point:
//  1. stop all consumers so nothing races the reset,
//  2. recreate the ClickHouse schema (empty tables),
//  3. recreate the topic (offsets back to 0),
//  4. clear the aggregator's Keeper offset/lock state.
func (h *Harness) Reset(ctx context.Context, t *testing.T) {
	t.Helper()

	// The built-in load generator writes into the same topic; it must be off for
	// the whole run or its messages contaminate the controlled dataset.
	if err := h.Docker.StopService(ctx, h.Cfg.GeneratorService); err != nil {
		t.Logf("stop generator (ignored, may not exist): %v", err)
	}
	if err := h.Docker.StopAllAggregators(ctx); err != nil {
		t.Fatalf("stop aggregators: %v", err)
	}
	if err := h.CH.ResetSchema(ctx); err != nil {
		t.Fatalf("reset clickhouse schema: %v", err)
	}
	if err := h.YDB.RecreateTopic(ctx); err != nil {
		t.Fatalf("recreate topic: %v", err)
	}
	if err := ResetKeeperState(h.Cfg); err != nil {
		t.Fatalf("reset keeper state: %v", err)
	}
	t.Log("reset complete: empty schema, fresh topic, cleared keeper state")
}

// Produce writes the dataset into the topic.
func (h *Harness) Produce(ctx context.Context, t *testing.T, ds Dataset) {
	t.Helper()
	start := time.Now()
	if err := h.YDB.Produce(ctx, ds); err != nil {
		t.Fatalf("produce dataset: %v", err)
	}
	t.Logf("produced %d transactions (sum=%d) in %s", ds.Count, ds.Sum, time.Since(start).Round(time.Millisecond))
}

// StartAggregators starts every consumer instance and ensures the suite leaves
// them running on exit.
func (h *Harness) StartAggregators(ctx context.Context, t *testing.T) {
	t.Helper()
	if err := h.Docker.StartAllAggregators(ctx); err != nil {
		t.Fatalf("start aggregators: %v", err)
	}
	t.Cleanup(func() {
		// Best effort: leave the cluster healthy for the next test/run.
		_ = h.Docker.StartAllAggregators(context.Background())
	})
}

// AssertExactlyOnce is the heart of the suite. It waits until every produced
// transaction is present in ClickHouse, then verifies that the data is there
// exactly once — no loss and no double counting — across the raw distributed
// table and the aggregated reports.
func (h *Harness) AssertExactlyOnce(ctx context.Context, t *testing.T, ds Dataset) {
	t.Helper()

	want := uint64(ds.Count)
	got, err := h.CH.WaitForDistinct(ctx, want, h.Cfg.CompletionTimeout, h.Cfg.PollInterval)
	if err != nil {
		// Falling short of `want` distinct IDs means messages were LOST.
		t.Fatalf("not all transactions reached ClickHouse (LOSS): %v", err)
	}
	t.Logf("all %d distinct transactions present after consumption", got)

	// Give in-flight replays/merges a brief moment to settle, then snapshot.
	time.Sleep(2 * time.Second)

	rawRows, err := h.CH.RawRowCount(ctx)
	if err != nil {
		t.Fatalf("raw row count: %v", err)
	}
	distinct, err := h.CH.DistinctTransactions(ctx)
	if err != nil {
		t.Fatalf("distinct count: %v", err)
	}
	rawSum, err := h.CH.RawSum(ctx)
	if err != nil {
		t.Fatalf("raw sum: %v", err)
	}
	reportsSum, err := h.CH.ReportsSum(ctx)
	if err != nil {
		t.Fatalf("reports sum: %v", err)
	}

	t.Logf("invariants: rawRows=%d distinct=%d rawSum=%d reportsSum=%d (expected count=%d sum=%d)",
		rawRows, distinct, rawSum, reportsSum, ds.Count, ds.Sum)

	// No loss: every produced transaction id is present.
	if distinct != want {
		t.Errorf("LOSS: distinct transaction ids = %d, want %d", distinct, want)
	}
	// No duplicates in the raw table: physical rows == produced count.
	if rawRows != want {
		t.Errorf("DOUBLE-WRITE: raw rows = %d, want %d (dedup token failed)", rawRows, want)
	}
	// Exact sum in the raw table.
	if rawSum != ds.Sum {
		t.Errorf("raw sum = %d, want %d", rawSum, ds.Sum)
	}
	// No double counting through the materialized view into reports.
	if reportsSum != ds.Sum {
		t.Errorf("DOUBLE-COUNT: reports sum = %d, want %d", reportsSum, ds.Sum)
	}
}

// RunScenario is the standard skeleton: reset, produce, start consumers, run the
// caller's fault-injection during consumption, then assert exactly-once.
//
// inject is called right after the consumers start; it should perform (and, for
// transient faults, undo) the scenario's disturbance. It may block for as long
// as the disturbance lasts. Permanent faults (e.g. a replica left down) need
// only ensure the surviving topology can still make progress.
func (h *Harness) RunScenario(ctx context.Context, t *testing.T, inject func(ctx context.Context, t *testing.T)) {
	t.Helper()
	ds := GenerateDataset(h.Cfg.DatasetSize)

	h.Reset(ctx, t)
	h.Produce(ctx, t, ds)
	h.StartAggregators(ctx, t)

	if inject != nil {
		inject(ctx, t)
	}

	h.AssertExactlyOnce(ctx, t, ds)
}
