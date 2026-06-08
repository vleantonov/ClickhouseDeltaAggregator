//go:build soak

// Soak / chaos test: a long-running session that continuously produces messages
// while periodically injecting EVERY fault the suite knows about, then verifies
// — at the record level, not just by aggregate — that ClickHouse ends up holding
// exactly the data that was produced.
//
// The test keeps an authoritative in-memory record of every transaction it
// successfully writes to the topic (keyed by transaction_id). After the chaos
// window it restores the cluster to health, waits for the backlog to drain, and
// compares that record against every row read back from ClickHouse:
//
//	missing      => message LOSS
//	duplicate    => DOUBLE write
//	unexpected   => data from outside the controlled dataset (contamination)
//	field diff   => corruption
//
// It is guarded by the `soak` build tag (separate from `acceptance`) and skipped
// when the infra is unreachable. Run it explicitly:
//
//	cd tests && go test -tags soak -v -timeout 60m -run TestSoak_ChaosExactlyOnce
package acceptance

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"acceptance/internal/harness"
)

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

// ---- configuration ---------------------------------------------------------

type soakConfig struct {
	duration        time.Duration // total chaos window
	produceInterval time.Duration // gap between production waves
	batchSize       int           // transactions per wave
	faultInterval   time.Duration // gap between fault injections
	drainTimeout    time.Duration // max wait for ClickHouse to catch up afterwards
}

func loadSoakConfig() soakConfig {
	return soakConfig{
		duration:        envDuration("SOAK_DURATION", 20*time.Minute),
		produceInterval: envDuration("SOAK_PRODUCE_INTERVAL", 15*time.Second),
		batchSize:       envInt("SOAK_BATCH_SIZE", 150),
		faultInterval:   envDuration("SOAK_FAULT_INTERVAL", 60*time.Second),
		drainTimeout:    envDuration("SOAK_DRAIN_TIMEOUT", 15*time.Minute),
	}
}

// ---- authoritative record of produced data --------------------------------

type recorder struct {
	mu  sync.Mutex
	m   map[string]harness.Transaction
	sum int64
}

func newRecorder() *recorder { return &recorder{m: make(map[string]harness.Transaction)} }

func (r *recorder) record(ds harness.Dataset) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, tx := range ds.Transactions {
		r.m[tx.TransactionID] = tx
		r.sum += tx.Amount
	}
}

func (r *recorder) snapshot() (map[string]harness.Transaction, int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[string]harness.Transaction, len(r.m))
	for k, v := range r.m {
		out[k] = v
	}
	return out, r.sum
}

// ---- the test --------------------------------------------------------------

func TestSoak_ChaosExactlyOnce(t *testing.T) {
	h := harness.New(t)
	sc := loadSoakConfig()

	// Outer budget: chaos window + drain + slack for fault recovery (node
	// restarts, paused connections, Docker restart:always cycles).
	ctx, cancel := context.WithTimeout(context.Background(), sc.duration+sc.drainTimeout+15*time.Minute)
	defer cancel()

	t.Logf("soak config: duration=%s produceEvery=%s batch=%d faultEvery=%s drain=%s",
		sc.duration, sc.produceInterval, sc.batchSize, sc.faultInterval, sc.drainTimeout)

	h.Reset(ctx, t)
	h.StartAggregators(ctx, t)

	rec := newRecorder()

	// Run the producer and the fault injector concurrently for the chaos window.
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); produceLoop(ctx, t, h, rec, sc) }()
	go func() { defer wg.Done(); faultLoop(ctx, t, h, sc) }()
	wg.Wait()

	// Bring everything back to health so the backlog can drain.
	t.Log("chaos window over; restoring cluster to a healthy state")
	restoreAll(t, h)

	produced, producedSum := rec.snapshot()
	t.Logf("produced %d distinct transactions (sum=%d); waiting for drain", len(produced), producedSum)

	if _, err := h.CH.WaitForDistinct(ctx, uint64(len(produced)), sc.drainTimeout, h.Cfg.PollInterval); err != nil {
		t.Fatalf("ClickHouse did not catch up to the produced set (LOSS): %v", err)
	}
	time.Sleep(3 * time.Second) // let any last replay/merge settle

	// Force every replica to catch up on its replication queue before reading.
	// With insert_quorum=auto a row acked by quorum may not yet be on a replica
	// that was down/paused during the chaos window, and the round-robin verifier
	// could otherwise read a lagging replica and report phantom loss.
	t.Log("syncing all replicas before final content check")
	if err := h.CH.SyncReplicas(ctx); err != nil {
		t.Fatalf("sync replicas before verification: %v", err)
	}

	stored, err := h.CH.FetchAllTransactions(ctx)
	if err != nil {
		t.Fatalf("fetch all transactions: %v", err)
	}

	compareContent(t, produced, producedSum, stored)
}

// ---- production loop -------------------------------------------------------

func produceLoop(ctx context.Context, t *testing.T, h *harness.Harness, rec *recorder, sc soakConfig) {
	stop := time.After(sc.duration)
	ticker := time.NewTicker(sc.produceInterval)
	defer ticker.Stop()

	wave := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-stop:
			t.Logf("producer finished after %d waves", wave)
			return
		case <-ticker.C:
			ds := harness.GenerateDataset(sc.batchSize)
			// A wave gets its own short-lived context so a transient outage of the
			// topic does not abort the whole producer; failed waves are simply not
			// recorded (we only record what the server acked).
			waveCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			err := h.YDB.Produce(waveCtx, ds)
			cancel()
			if err != nil {
				t.Logf("produce wave %d failed (will retry next tick): %v", wave, err)
				continue
			}
			rec.record(ds)
			wave++
		}
	}
}

// ---- fault loop ------------------------------------------------------------

type fault struct {
	name string
	run  func(t *testing.T, h *harness.Harness)
}

func faultLoop(ctx context.Context, t *testing.T, h *harness.Harness, sc soakConfig) {
	faults := faultCatalog()
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	// Start from a random offset and walk the catalog so order varies per run.
	idx := rng.Intn(len(faults))

	stop := time.After(sc.duration)
	ticker := time.NewTicker(sc.faultInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-stop:
			return
		case <-ticker.C:
			f := faults[idx%len(faults)]
			idx++
			t.Logf("injecting fault: %s", f.name)
			f.run(t, h)
		}
	}
}

// faultCatalog enumerates every disturbance. Each fault is self-contained and
// self-healing: it injects the disturbance and restores the affected component
// before returning, so faults compose cleanly across the soak. Docker/YDB
// operations use background-derived contexts so a fault always completes its own
// cleanup even near the end of the chaos window.
func faultCatalog() []fault {
	const (
		replica   = "clickhouse-01-01"
		keeper    = "clickhouse-keeper-03"
		extraCons = "soak-observer"
	)
	shard := []string{"clickhouse-01-01", "clickhouse-01-02", "clickhouse-01-03"}

	op := func() (context.Context, context.CancelFunc) {
		return context.WithTimeout(context.Background(), 3*time.Minute)
	}

	return []fault{
		{"add+remove topic consumer", func(t *testing.T, h *harness.Harness) {
			ctx, cancel := op()
			defer cancel()
			if err := h.YDB.AddConsumer(ctx, extraCons); err != nil {
				t.Logf("  add consumer: %v", err)
				return
			}
			time.Sleep(5 * time.Second)
			if err := h.YDB.DropConsumer(ctx, extraCons); err != nil {
				t.Logf("  drop consumer: %v", err)
			}
		}},

		{"disable+reenable one consumer", func(t *testing.T, h *harness.Harness) {
			ctx, cancel := op()
			defer cancel()
			victim := h.Cfg.AggregatorServices[1]
			_ = h.Docker.StopService(ctx, victim)
			time.Sleep(20 * time.Second)
			_ = h.Docker.StartService(ctx, victim)
		}},

		{"clickhouse replica down", func(t *testing.T, h *harness.Harness) {
			ctx, cancel := op()
			defer cancel()
			_ = h.Docker.StopContainer(ctx, replica)
			time.Sleep(25 * time.Second)
			_ = h.Docker.StartContainer(ctx, replica)
			_ = h.Docker.WaitContainerRunning(ctx, replica, 60*time.Second)
		}},

		{"consumer<->clickhouse connection break", func(t *testing.T, h *harness.Harness) {
			ctx, cancel := op()
			defer cancel()
			nodes := allClickHouseNodes()
			for _, n := range nodes {
				_ = h.Docker.PauseContainer(ctx, n)
			}
			time.Sleep(25 * time.Second)
			for _, n := range nodes {
				_ = h.Docker.UnpauseContainer(ctx, n)
			}
		}},

		{"keeper node unreachable", func(t *testing.T, h *harness.Harness) {
			ctx, cancel := op()
			defer cancel()
			// Pause, not stop: stopping removes the container's DNS record and the
			// aggregator's initZKConn panics on an unresolvable Keeper host. Pausing
			// models "node down, ensemble keeps quorum" with the name still resolvable.
			_ = h.Docker.PauseContainer(ctx, keeper)
			time.Sleep(25 * time.Second)
			_ = h.Docker.UnpauseContainer(ctx, keeper)
		}},

		{"consumer SIGKILL", func(t *testing.T, h *harness.Harness) {
			ctx, cancel := op()
			defer cancel()
			victim := h.Cfg.AggregatorServices[0]
			_ = h.Docker.KillService(ctx, victim)
			time.Sleep(3 * time.Second)
			_ = h.Docker.StartService(ctx, victim)
		}},

		{"rolling clickhouse restart (one shard)", func(t *testing.T, h *harness.Harness) {
			ctx, cancel := op()
			defer cancel()
			for _, node := range shard {
				_ = h.Docker.StopContainer(ctx, node)
				time.Sleep(8 * time.Second)
				_ = h.Docker.StartContainer(ctx, node)
				_ = h.Docker.WaitContainerRunning(ctx, node, 60*time.Second)
				time.Sleep(5 * time.Second)
			}
		}},
	}
}

// ---- restore + comparison --------------------------------------------------

func allClickHouseNodes() []string {
	return []string{
		"clickhouse-01-01", "clickhouse-01-02", "clickhouse-01-03",
		"clickhouse-02-01", "clickhouse-02-02", "clickhouse-02-03",
	}
}

func allKeeperNodes() []string {
	return []string{"clickhouse-keeper-01", "clickhouse-keeper-02", "clickhouse-keeper-03"}
}

// restoreAll returns the cluster to full health: every node up and unpaused,
// both consumers running, and the extra topic consumer removed.
func restoreAll(t *testing.T, h *harness.Harness) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	for _, n := range append(allClickHouseNodes(), allKeeperNodes()...) {
		_ = h.Docker.UnpauseContainer(ctx, n)
		_ = h.Docker.StartContainer(ctx, n)
		_ = h.Docker.WaitContainerRunning(ctx, n, 90*time.Second)
	}
	if err := h.Docker.StartAllAggregators(ctx); err != nil {
		t.Logf("restore aggregators: %v", err)
	}
	_ = h.YDB.DropConsumer(ctx, "soak-observer")
}

func sameDay(a, b time.Time) bool {
	ay, am, ad := a.UTC().Date()
	by, bm, bd := b.UTC().Date()
	return ay == by && am == bm && ad == bd
}

// compareContent is the record-level exactly-once assertion: ClickHouse must hold
// precisely the produced transactions — no missing, no duplicate, no unexpected,
// no corrupted rows — and the total amount must match.
func compareContent(t *testing.T, produced map[string]harness.Transaction, producedSum int64, stored []harness.StoredTransaction) {
	seen := make(map[string]int, len(produced))
	var (
		unexpected    []string
		fieldMismatch []string
		storedSum     int64
	)

	for _, s := range stored {
		seen[s.TransactionID]++
		storedSum += s.Amount

		p, ok := produced[s.TransactionID]
		if !ok {
			if len(unexpected) < 10 {
				unexpected = append(unexpected, s.TransactionID)
			}
			continue
		}
		// Compare fields only on the first occurrence to avoid noisy repeats.
		if seen[s.TransactionID] == 1 {
			if p.UserID != s.UserID || p.ProductID != s.ProductID || p.Amount != s.Amount || !sameDay(p.Date, s.Date) {
				if len(fieldMismatch) < 10 {
					fieldMismatch = append(fieldMismatch, fmt.Sprintf("%s: produced{%s,%s,%d,%s} stored{%s,%s,%d,%s}",
						s.TransactionID, p.UserID, p.ProductID, p.Amount, p.Date.UTC().Format("2006-01-02"),
						s.UserID, s.ProductID, s.Amount, s.Date.UTC().Format("2006-01-02")))
				}
			}
		}
	}

	var missing []string
	var missingMin, missingMax string
	missingTotal := 0
	for id := range produced {
		if seen[id] == 0 {
			missingTotal++
			if len(missing) < 10 {
				missing = append(missing, id)
			}
			if missingMin == "" || id < missingMin {
				missingMin = id
			}
			if id > missingMax {
				missingMax = id
			}
		}
	}
	duplicateTotal := 0
	for _, c := range seen {
		if c > 1 {
			duplicateTotal += c - 1
		}
	}

	t.Logf("comparison: produced=%d storedRows=%d distinctStored=%d producedSum=%d storedSum=%d",
		len(produced), len(stored), len(seen), producedSum, storedSum)

	if missingTotal > 0 {
		t.Logf("missing key range: [%s, %s]", missingMin, missingMax)
		t.Errorf("LOSS: %d produced transactions missing from ClickHouse; key range [%s, %s] (e.g. %v)", missingTotal, missingMin, missingMax, missing)
	}
	if duplicateTotal > 0 {
		t.Errorf("DOUBLE-WRITE: %d duplicate rows in ClickHouse", duplicateTotal)
	}
	if len(unexpected) > 0 {
		t.Errorf("CONTAMINATION: rows in ClickHouse that were never produced (e.g. %v)", unexpected)
	}
	if len(fieldMismatch) > 0 {
		t.Errorf("CORRUPTION: %d rows with mismatched fields:\n  %v", len(fieldMismatch), fieldMismatch)
	}
	if storedSum != producedSum {
		t.Errorf("sum mismatch: stored=%d produced=%d", storedSum, producedSum)
	}

	if !t.Failed() {
		t.Logf("EXACTLY-ONCE HOLDS: ClickHouse content is identical to the produced input")
	}
}
