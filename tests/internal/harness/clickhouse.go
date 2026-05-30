package harness

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// CH is a thin ClickHouse client used both to verify the exactly-once invariant
// (reads against the distributed tables, so it spans all shards/replicas) and to
// reset the schema between scenarios.
type CH struct {
	cfg  Config
	conn driver.Conn
}

func openCH(addrs []string, database string) (driver.Conn, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: addrs,
		Auth: clickhouse.Auth{Database: database, Username: "default"},
		// Spread reads across every reachable node so a single downed replica
		// does not make the verifier itself unavailable.
		ConnOpenStrategy: clickhouse.ConnOpenRoundRobin,
		DialTimeout:      5 * time.Second,
	})
	if err != nil {
		return nil, err
	}
	return conn, nil
}

// ConnectClickHouse dials the cluster and pings to confirm reachability.
func ConnectClickHouse(cfg Config) (*CH, error) {
	conn, err := openCH(cfg.ClickHouseAddrs, cfg.Database)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := conn.Ping(ctx); err != nil {
		return nil, fmt.Errorf("ping clickhouse: %w", err)
	}
	return &CH{cfg: cfg, conn: conn}, nil
}

func (c *CH) Close() { _ = c.conn.Close() }

// scan runs a single-value query, retrying a few times so a transient failure
// against a deliberately-downed node (replica/keeper outage scenarios) — which
// the round-robin pool routes around on reconnect — does not flake the verifier.
func (c *CH) scan(ctx context.Context, query string, dst any) error {
	var lastErr error
	for attempt := 0; attempt < 5; attempt++ {
		if err := c.conn.QueryRow(ctx, query).Scan(dst); err != nil {
			lastErr = err
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Second):
			}
			continue
		}
		return nil
	}
	return lastErr
}

func (c *CH) scanUint(ctx context.Context, query string) (uint64, error) {
	var v uint64
	return v, c.scan(ctx, query, &v)
}

func (c *CH) scanInt(ctx context.Context, query string) (int64, error) {
	var v int64
	return v, c.scan(ctx, query, &v)
}

// RawRowCount is the total number of rows physically in the raw table across all
// shards. If exactly-once held it equals the produced count; if a dedup token
// ever failed it would be larger (double write).
func (c *CH) RawRowCount(ctx context.Context) (uint64, error) {
	return c.scanUint(ctx, "SELECT count() FROM transactions_d")
}

// DistinctTransactions counts unique transaction IDs in the raw table. It equals
// the produced count once everything is consumed; staying below means loss.
func (c *CH) DistinctTransactions(ctx context.Context) (uint64, error) {
	return c.scanUint(ctx, "SELECT uniqExact(transaction_id) FROM transactions_d")
}

// RawSum is the sum of amounts in the raw table.
func (c *CH) RawSum(ctx context.Context) (int64, error) {
	return c.scanInt(ctx, "SELECT sum(amount) FROM transactions_d")
}

// ReportsSum is the sum of amounts as aggregated through the materialized view
// into the ReplicatedAggregatingMergeTree. Summing across unmerged parts is safe
// for SimpleAggregateFunction(sum). A value above the produced sum reveals double
// counting in the dependent MV.
func (c *CH) ReportsSum(ctx context.Context) (int64, error) {
	return c.scanInt(ctx, "SELECT sum(amount) FROM reports_d")
}

// WaitForDistinct polls until uniqExact(transaction_id) reaches want or the
// context/timeout expires, returning the last observed value.
func (c *CH) WaitForDistinct(ctx context.Context, want uint64, timeout, interval time.Duration) (uint64, error) {
	deadline := time.Now().Add(timeout)
	t := time.NewTicker(interval)
	defer t.Stop()

	var last uint64
	for {
		got, err := c.DistinctTransactions(ctx)
		if err == nil {
			last = got
			if got >= want {
				return got, nil
			}
		}
		if time.Now().After(deadline) {
			return last, fmt.Errorf("timed out waiting for %d distinct transactions, last seen %d", want, last)
		}
		select {
		case <-ctx.Done():
			return last, ctx.Err()
		case <-t.C:
		}
	}
}

// StoredTransaction is a row as persisted in ClickHouse, used for full
// content-level comparison against the produced input.
type StoredTransaction struct {
	TransactionID string
	UserID        string
	ProductID     string
	Amount        int64
	Date          time.Time
}

// FetchAllTransactions streams every row of the raw distributed table. Callers
// compare this against the recorded input to assert that ClickHouse holds
// exactly the produced data — same ids, same fields, no extras, no duplicates.
func (c *CH) FetchAllTransactions(ctx context.Context) ([]StoredTransaction, error) {
	rows, err := c.conn.Query(ctx,
		"SELECT transaction_id, user_id, product_id, amount, date FROM transactions_d")
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var out []StoredTransaction
	for rows.Next() {
		var s StoredTransaction
		if err := rows.Scan(&s.TransactionID, &s.UserID, &s.ProductID, &s.Amount, &s.Date); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// ResetSchema drops and recreates the schema so each scenario starts from an
// empty, well-defined database. Because the `accounting` database uses the
// Replicated engine, DDL executed against one node is auto-propagated to every
// replica — so running migrations/clean.sql + init.sql on a single connection is
// sufficient for the whole cluster.
func (c *CH) ResetSchema(ctx context.Context) error {
	// The schema connection targets the accounting DB. We assume the DB itself
	// already exists (created once by `make infra_with_db`); ensure it anyway.
	if err := c.ensureDatabase(ctx); err != nil {
		return err
	}
	for _, name := range []string{"clean.sql", "init.sql"} {
		if err := c.runSQLFile(ctx, filepath.Join(c.cfg.RepoRoot, "migrations", name)); err != nil {
			return fmt.Errorf("run %s: %w", name, err)
		}
	}
	return nil
}

func (c *CH) ensureDatabase(ctx context.Context) error {
	conn, err := openCH(c.cfg.ClickHouseAddrs, "default")
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	stmt := fmt.Sprintf(
		"CREATE DATABASE IF NOT EXISTS %s ON CLUSTER '{cluster}' ENGINE = Replicated('/databases/%s', '{shard}', '{replica}')",
		c.cfg.Database, c.cfg.Database,
	)
	// A pre-existing database may already be Replicated; ignore the benign error.
	if err := conn.Exec(ctx, stmt); err != nil && !strings.Contains(err.Error(), "already exists") {
		return err
	}
	return nil
}

func (c *CH) runSQLFile(ctx context.Context, path string) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	for _, stmt := range strings.Split(string(raw), ";") {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		if err := c.conn.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("exec %q: %w", firstLine(stmt), err)
		}
	}
	return nil
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}
