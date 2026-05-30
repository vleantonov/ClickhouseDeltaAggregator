// Package harness provides the orchestration primitives the acceptance suite
// needs to exercise the aggregator against a live docker-compose cluster:
// producing into the YDB topic, driving fault injection (stopping containers,
// pausing nodes, partitioning the network), resetting state, and asserting the
// exactly-once invariant against ClickHouse.
package harness

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds every tunable knob of the suite. Everything has a sensible
// default matching deployments/docker-compose.yaml + the Makefile, and can be
// overridden through environment variables so the same tests run unchanged on a
// remapped or remote cluster.
type Config struct {
	// YDBEndpoint is the YDB DSN reachable *from the host running the tests*.
	// docker-compose maps ydb's 2135 to localhost:2135. NOTE: YDB discovery may
	// advertise the in-compose hostname "ydb"; if so, add `127.0.0.1 ydb` to your
	// /etc/hosts (see tests/README.md).
	YDBEndpoint string

	// ClickHouseAddrs are the host-mapped native-protocol endpoints of the six
	// ClickHouse nodes (2 shards x 3 replicas).
	ClickHouseAddrs []string

	// KeeperAddrs are the host-mapped ClickHouse-Keeper (ZooKeeper protocol)
	// endpoints used to reset the aggregator's offset/lock state between runs.
	KeeperAddrs []string

	// Database is the ClickHouse database the schema lives in.
	Database string

	// Topic / Consumer mirror the constants hardcoded in both cmd/main.go files.
	Topic               string
	Consumer            string
	MinActivePartitions int64

	// RepoRoot is the repository root (the dir that holds deployments/ and
	// migrations/). The tests run with cwd=tests/, so the default is "..".
	RepoRoot string
	// ComposeFile is the docker-compose file driving the cluster.
	ComposeFile string
	// ComposeBin is the compose command. Some hosts only ship the standalone
	// `docker-compose` binary (no `docker compose` plugin); a value with a space
	// (e.g. "docker compose") is split into command + leading args.
	ComposeBin string
	// ComposeProject is the compose project name; it prefixes network names
	// (e.g. <project>_cluster_2S_1R). Defaults to the compose dir basename.
	ComposeProject string
	// ClusterNetwork is the docker network connecting consumers <-> ClickHouse.
	ClusterNetwork string

	// AggregatorServices are the compose service names of the consumer instances.
	AggregatorServices []string
	// GeneratorService is the compose service of the built-in load generator. It
	// writes into the SAME topic and must be stopped during tests, otherwise its
	// messages contaminate the controlled dataset.
	GeneratorService string

	// CompletionTimeout bounds how long we wait for all produced messages to be
	// durably present in ClickHouse. It stays generous to absorb the recovery time
	// of the injected faults (node restarts, paused connections, Docker
	// restart:always cycles after a reader-error crash).
	CompletionTimeout time.Duration
	// PollInterval is how often the completion check polls ClickHouse.
	PollInterval time.Duration
	// DatasetSize is the number of transactions produced per scenario.
	DatasetSize int
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envDur(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

// DefaultConfig builds a Config from the environment, falling back to the values
// baked into deployments/docker-compose.yaml.
func DefaultConfig() Config {
	clickhouse := []string{
		"127.0.0.1:9011", // clickhouse-01-01
		"127.0.0.1:9012", // clickhouse-01-02
		"127.0.0.1:9013", // clickhouse-01-03
		"127.0.0.1:9021", // clickhouse-02-01
		"127.0.0.1:9022", // clickhouse-02-02
		"127.0.0.1:9023", // clickhouse-02-03
	}
	if v := os.Getenv("CLICKHOUSE_ADDRS"); v != "" {
		clickhouse = strings.Split(v, ",")
	}

	keeper := []string{
		"127.0.0.1:9181",
		"127.0.0.1:9182",
		"127.0.0.1:9183",
	}
	if v := os.Getenv("KEEPER_ADDRS"); v != "" {
		keeper = strings.Split(v, ",")
	}

	project := env("COMPOSE_PROJECT_NAME", "deployments")

	return Config{
		YDBEndpoint:         env("YDB_ENDPOINT", "grpc://localhost:2135/local"),
		ClickHouseAddrs:     clickhouse,
		KeeperAddrs:         keeper,
		Database:            env("CLICKHOUSE_DB", "accounting"),
		Topic:               env("YDB_TOPIC", "purchases_topic"),
		Consumer:            env("YDB_CONSUMER", "aggregator"),
		MinActivePartitions: int64(envInt("YDB_PARTITIONS", 3)),
		RepoRoot:            env("REPO_ROOT", ".."),
		ComposeFile:         env("COMPOSE_FILE", "../deployments/docker-compose.yaml"),
		ComposeBin:          env("COMPOSE_BIN", "docker-compose"),
		ComposeProject:      project,
		ClusterNetwork:      env("CLUSTER_NETWORK", project+"_cluster_2S_1R"),
		AggregatorServices:  []string{"aggregator-1", "aggregator-2"},
		GeneratorService:    env("GENERATOR_SERVICE", "generator"),
		CompletionTimeout:   envDur("COMPLETION_TIMEOUT", 6*time.Minute),
		PollInterval:        envDur("POLL_INTERVAL", 3*time.Second),
		DatasetSize:         envInt("DATASET_SIZE", 1000),
	}
}
