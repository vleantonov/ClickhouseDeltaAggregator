# ClickHouse Delta Aggregator

A study/experiment in **deduplicated (exactly-once) writing of messages from a YDB
topic into a distributed ClickHouse cluster**. A continuous stream of purchase
transactions flows through a YDB topic (Kafka-like); an **aggregator** consumes
them and writes into a replicated ClickHouse cluster (2 shards × 3 replicas),
where a `ReplicatedAggregatingMergeTree` + materialized view maintains per-period
sum reports. The goal is **no loss and no double counting** even when services
crash, the network partitions, or quorum writes fail mid-flight.

The dedup/exactly-once design follows ClickHouse's official Kafka connector,
[clickhouse-kafka-connect](https://github.com/ClickHouse/clickhouse-kafka-connect).

## Problem

We have a continuous delta stream and want fast aggregated reports by period
(day, week, month, …) under these requirements:

- **Accuracy** — no loss, no distortion, no double counting.
- **Durability** — once a part of the stream is acknowledged, it is durably stored.
- **Fast period reports** — pre-aggregated, not computed on read.
- **Resilience** — correct on an unreliable network and under component failures.

The chosen domain is a stream of small purchases with day granularity. A stream
record (see [transaction.go](aggregator/internal/domain/transaction.go)):

```json
{
  "transaction_id": "9b8d…",
  "user_id": "user1",
  "product_id": "product1",
  "amount": 100,
  "date": "2025-03-01T00:00:00Z"
}
```

## Architecture

```
 generator ──▶ YDB topic (purchases_topic) ──▶ aggregator ──▶ ClickHouse cluster
                                                  │                (2 shards × 3 replicas)
                                                  ├─ per-partition lock  ┐
                                                  └─ offset-range state  ┴─ ClickHouse-Keeper (3 nodes)
```

- **[generator/](generator/)** — produces synthetic purchase batches into the topic.
- **[aggregator/](aggregator/)** — the consumer/writer service (the main logic).
- **ClickHouse cluster** — 2 shards × 3 replicas; the `accounting` database uses
  the `Replicated` engine, so DDL auto-propagates to every replica.
- **ClickHouse-Keeper** — a 3-node ensemble holding the per-partition locks and
  the per-partition offset-range state.

### ClickHouse schema ([migrations/init.sql](migrations/init.sql))

- `transactions` (`ReplicatedMergeTree`) — raw rows incl. `partition_id`,
  `offset`; the app writes to the `transactions_d` **Distributed** table, sharded
  by `xxHash64(partition_id)`.
- `reports` (`ReplicatedAggregatingMergeTree`, `SimpleAggregateFunction(sum/max,…)`)
  fed by the `reports_mv` materialized view aggregating from `transactions`
  grouped by `(date, user_id, product_id)`; queried via the `reports_d`
  Distributed table.

## Correctness model (exactly-once)

Exactly-once is achieved by combining four mechanisms across the per-partition
loop in [reader.go](aggregator/internal/reader/reader.go):

1. **Per-partition distributed lock** — a Keeper-backed `TTLLock`
   ([locker.go](aggregator/internal/lockers/zookeeper/locker.go)) ensures only one
   aggregator processes a topic partition at a time. Locks live in a TTL cache;
   expiry auto-unlocks, so a stalled/crashed holder eventually releases.

2. **Offset-range state in Keeper** — the `OffsetManager`
   ([keeper.go](aggregator/internal/offset_manager/keeper/keeper.go)) stores, per
   partition, a `{MinOffset, MaxOffset, State}` triple recording exactly which
   offset range was last attempted (`UNKNOWN → IN_PROGRESS → COMPLETED`).

3. **Recovery / replay reconciliation** — when a batch arrives, incoming offsets
   are compared against the stored range. Messages within an already-attempted
   range are validated against it (and skipped if already `COMPLETED`); everything
   else must be a contiguous range continuing from the previous `MaxOffset+1`.

4. **ClickHouse insert deduplication** — `PushTransactions`
   ([repository.go](aggregator/internal/repository/clickhouse/repository.go)) sets
   an `insert_deduplication_token` derived from `partitionID + Min + Max`, plus
   `insert_quorum=auto`, `distributed_foreground_insert=1`, and dedup in dependent
   MVs. Replaying an identical range is a no-op at the storage layer.

Ordering in `pushAndStoreOffset` matters: rows are pushed to ClickHouse **first**,
the Keeper state is set to `COMPLETED` **after**, and only then is the YDB batch
**committed**. A crash between any two steps is recoverable on replay because (3)
reconstructs the range and (4) makes the re-push idempotent.

The service runs the reader once and `panic`s on any fatal error; on a crash the
container's `restart: always` policy brings it back and recovery makes the replay
idempotent. (Network/host faults are injected externally — see Testing.)

## Layout

Three independent Go modules (no root `go.mod` — run `go` from inside each):

- [aggregator/](aggregator/) — module `delta_aggregator`, the consumer/writer.
- [generator/](generator/) — module `generator`, the synthetic load producer.
- [tests/](tests/) — module `acceptance`, the black-box acceptance + soak suites.
- [clickhouse_tester/](clickhouse_tester/) — an **exploratory** prototype (toxiproxy
  fault injection + a quorum-offset-read spike). Not part of the main flow and not
  built by docker-compose; treat as legacy.

Plus [migrations/](migrations/) (ClickHouse schema) and [deployments/](deployments/)
(docker-compose cluster + per-node config).

## Quick start

Everything is orchestrated through the [Makefile](Makefile):

```sh
make infra_with_db    # bring up the full cluster (ydb, 6 clickhouse, 3 keeper, 2 aggregators, generator) + init schema
make aggregator-logs  # follow an aggregator's logs
make connect_db       # clickhouse client into the accounting db (localhost:9011)
make drop_infra       # tear everything down
```

Other useful targets: `make infra`, `make init_db`, `make clean_db`,
`make drop_replica` (simulate a replica outage), `make toxic-connectors`
(create toxiproxy proxies for the ClickHouse/YDB connections).

### Build / test a module

```sh
cd aggregator && go build ./...
cd aggregator && go test ./...          # unit tests live next to the code
```

## Testing (exactly-once verification)

The [tests/](tests/) module drives the live cluster and asserts the exactly-once
guarantee under fault injection. See [tests/README.md](tests/README.md) for full
details. Both suites **skip** automatically if the infra is unreachable.

After consumption, the invariant checked against the distributed tables is:

| Check | Meaning if violated |
|---|---|
| `uniqExact(transaction_id) == produced count` | message **loss** |
| `count() == produced count` (raw table) | **double write** (dedup failed) |
| `sum(amount)` raw `==` produced sum | corruption |
| `sum(amount)` reports `==` produced sum | **double count** via the MV |

- **Acceptance suite** (`-tags acceptance`) — a baseline plus 8 fault scenarios,
  each its own test: adding/removing topic consumers, disabling / disabling +
  re-enabling a consumer, a ClickHouse replica down, a consumer↔ClickHouse
  connection break, a Keeper node unreachable, repeated consumer SIGKILL, and a
  rolling ClickHouse restart.

  ```sh
  make acceptance-test                                  # all scenarios
  make acceptance-test TEST=TestExactlyOnce_NoFaults    # one scenario
  ```

- **Soak / chaos suite** (`-tags soak`) — a long-running session that continuously
  produces while periodically injecting **every** fault, then compares ClickHouse
  content against an authoritative in-memory record **row by row** (missing →
  loss, duplicate → double write, unexpected → contamination, field diff →
  corruption).

  ```sh
  make soak-test     # tune via SOAK_* env vars (see tests/README.md)
  ```

## Conventions

- Logging is `log/slog` to a rotating file (`lumberjack`, `logs/aggregator.log`);
  the `*slog.Logger` is passed down through constructors.
- Connection endpoints (YDB `ydb:2135`, keeper `clickhouse-keeper-0x:9181`, the
  six ClickHouse `clickhouse-0x-0x:9000`) are hardcoded in `cmd/main.go` as
  docker-compose service names — services run inside the compose network.
- The shared topic is `purchases_topic`, consumer `aggregator` (constants
  duplicated in both `cmd/main.go` files — keep them in sync).

## Future work

- Turn the toxiproxy-based fault injection (latency, packet drops, full network
  isolation with recovery) into a first-class provider that can perturb every
  network call — the [clickhouse_tester/](clickhouse_tester/) prototype sketches
  this; the acceptance suite currently partitions via `docker pause` / `network
  disconnect`.
- Wire the captured OS signal (`parCtx`) into the reader loop for graceful
  shutdown on `SIGTERM`.
