# Acceptance tests — exactly-once YDB → ClickHouse

Black-box, end-to-end tests that verify the project's core guarantee: every
message produced into the YDB topic lands in the distributed ClickHouse cluster
**exactly once** — no loss, no double counting — while the system is being
disturbed by failures.

The tests run on the *host*, drive the live `deployments/docker-compose.yaml`
cluster (produce into YDB, inject faults by stopping/pausing/partitioning
containers), and assert against ClickHouse. They are a separate Go module
(`acceptance`) and are guarded by the `acceptance` build tag, so a plain
`go test ./...` never picks them up.

## The exactly-once invariant

After each scenario the suite produces a known dataset (`N` transactions with
unique `transaction_id`s and a known total `amount`), then waits for consumption
and checks four things against the distributed tables:

| Check | Query | Violation means |
|------|-------|-----------------|
| No loss | `uniqExact(transaction_id) == N` | messages **lost** |
| No duplicate rows | `count() == N` over `transactions_d` | **double write** (dedup token failed) |
| Exact raw sum | `sum(amount) == S` over `transactions_d` | corruption |
| No double count | `sum(amount) == S` over `reports_d` | **double count** through the materialized view |

Reading through the `*_d` Distributed tables means the assertion spans every
shard and replica.

## Scenarios

| Test | Fault injected | Expectation |
|------|----------------|-------------|
| `NoFaults` | none (baseline / happy path) | exactly-once with no disturbance |
| `AddRemoveTopicConsumers` | add then drop an extra topic consumer mid-stream | aggregator's stream unaffected, exactly-once |
| `DisableOneConsumer` | permanently stop one of two consumer instances | survivor finishes the stream |
| `DisableAndReenableConsumer` | stop then restart one consumer instance | rejoin causes no duplicates |
| `ClickHouseReplicaDown` | stop `clickhouse-01-01` (shard keeps quorum) | quorum writes continue |
| `ClickHouseConnectionBreak` | pause all CH nodes (TCP black hole), then resume | retries/replays recover cleanly |
| `KeeperNodeDown` | stop one of three Keeper nodes (quorum kept) | locks/offsets keep working |
| `ConsumerChaosKill` | repeatedly `SIGKILL` + restart a consumer | crash-between-push-and-commit absorbed by recovery + dedup |
| `RollingClickHouseRestart` | restart a shard's replicas one at a time | rolling deploy survives |

## Soak / chaos test (record-level comparison)

`soak_test.go` (build tag `soak`, separate from `acceptance`) is a long-running
test that does **not** just check aggregates: it keeps an authoritative
in-memory record of every transaction it produces and, at the end, compares it
against **every row read back from ClickHouse**.

It runs two loops concurrently for a configurable chaos window:

- a **producer** that emits a wave of transactions on an interval and records
  each one (only after the server acks it), and
- a **fault injector** that walks the full fault catalog above, one disturbance
  at a time, each self-healing.

When the window closes it restores the cluster to health, waits for the backlog
to drain, then reconciles produced vs. stored:

| Symptom in comparison | Meaning |
|---|---|
| produced id missing in ClickHouse | message **LOSS** |
| id present more than once | **DOUBLE write** |
| id in ClickHouse never produced | **contamination** |
| fields differ for an id | **corruption** |
| `sum(stored) != sum(produced)` | aggregate mismatch |

Run it (tune the window with env vars):

```sh
make soak-test
# or, with a custom profile:
cd tests && SOAK_DURATION=30m SOAK_FAULT_INTERVAL=60s SOAK_BATCH_SIZE=200 \
  go test -tags soak -v -timeout 90m -run TestSoak_ChaosExactlyOnce
```

| Env | Default | Meaning |
|-----|---------|---------|
| `SOAK_DURATION` | `20m` | length of the chaos window |
| `SOAK_PRODUCE_INTERVAL` | `15s` | gap between production waves |
| `SOAK_BATCH_SIZE` | `150` | transactions per wave |
| `SOAK_FAULT_INTERVAL` | `60s` | gap between fault injections |
| `SOAK_DRAIN_TIMEOUT` | `15m` | max wait for ClickHouse to catch up afterwards |

## Prerequisites

- Docker + the `docker compose` plugin (the suite shells out to `docker`).
- A Go toolchain.
- The cluster brought up once:

  ```sh
  make infra_with_db
  ```

- **YDB host reachability.** The tests dial YDB at `grpc://localhost:2135/local`.
  YDB discovery may advertise the in-compose hostname `ydb`; if connecting fails
  with an unresolvable `ydb` host, map it on the host:

  ```sh
  echo "127.0.0.1 ydb" | sudo tee -a /etc/hosts
  ```

The suite **stops the `generator` service** at the start of every run: it writes
into the same `purchases_topic` and would otherwise contaminate the controlled
dataset. The topic is dropped (and the drop confirmed) and recreated each run, so
each scenario sees only its own messages.

If the infra is not reachable, every test **skips** (it does not fail).

## Running

From the `tests/` directory:

```sh
# all scenarios — they are slow (see timeouts below), so raise the test timeout
go test -tags acceptance -v -timeout 90m ./...

# a single scenario
go test -tags acceptance -v -timeout 30m -run TestExactlyOnce_DisableOneConsumer
```

> **Timing.** Consumption itself is prompt, but each scenario still includes
> fault-recovery time — node restarts, paused connections draining, and the
> aggregator's `restart: always` cycle after a reader-error crash. The completion
> timeout is therefore kept generous (default 6 min per scenario, overridable via
> `COMPLETION_TIMEOUT`).

Each test resets state first (stops consumers, recreates the schema, recreates
the topic so offsets restart at 0, and clears the aggregator's Keeper
offset/lock znodes), so scenarios are independent and repeatable.

## Configuration

Everything is overridable via environment variables (defaults match
`deployments/docker-compose.yaml` and the `Makefile`):

| Env | Default | Meaning |
|-----|---------|---------|
| `YDB_ENDPOINT` | `grpc://localhost:2135/local` | YDB DSN from the host |
| `CLICKHOUSE_ADDRS` | the six `127.0.0.1:901x/902x` nodes | comma-separated CH native endpoints |
| `KEEPER_ADDRS` | `127.0.0.1:9181,9182,9183` | Keeper (ZK protocol) endpoints |
| `CLICKHOUSE_DB` | `accounting` | database |
| `YDB_TOPIC` / `YDB_CONSUMER` | `purchases_topic` / `aggregator` | must match `cmd/main.go` |
| `YDB_PARTITIONS` | `3` | min active partitions on the topic |
| `DATASET_SIZE` | `1000` | transactions produced per scenario |
| `COMPLETION_TIMEOUT` | `6m` | max wait for full consumption |
| `POLL_INTERVAL` | `3s` | ClickHouse poll cadence |
| `COMPOSE_FILE` | `../deployments/docker-compose.yaml` | compose file |
| `COMPOSE_PROJECT_NAME` | `deployments` | prefixes the docker network name |
| `REPO_ROOT` | `..` | where `migrations/` lives |

## Layout

```
tests/
  acceptance_test.go        # baseline + the 8 fault scenarios (build tag: acceptance)
  soak_test.go              # long-running chaos + record-level comparison (build tag: soak)
  internal/harness/
    config.go               # env-driven configuration
    dataset.go              # deterministic-sum dataset generator
    clickhouse.go           # verifier + schema reset
    ydb.go                  # topic (re)creation, consumer mgmt, producer
    keeper.go               # clears offset/lock znodes between runs
    docker.go               # stop/start/kill/pause/partition containers
    harness.go              # arrange/act/assert orchestration (RunScenario)
```
