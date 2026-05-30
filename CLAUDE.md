# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A study/experiment in **deduplicated (exactly-once) writing of messages from YDB topics into ClickHouse**. The implementation approach is modeled on ClickHouse's official Kafka connector, [clickhouse-kafka-connect](https://github.com/ClickHouse/clickhouse-kafka-connect).

A continuous stream of purchase transactions flows through a **YDB topic** (Kafka-like). The **aggregator** consumes them and writes into a **distributed, replicated ClickHouse cluster** (2 shards × 3 replicas), where a `ReplicatedAggregatingMergeTree` + materialized view maintains per-period sum reports. The core goal is *no loss and no double-counting* even when services crash, the network partitions, or quorum writes fail mid-flight. See [README.md](README.md) for the problem statement (partly in Russian).

The chosen domain metric is the `Transaction` entity ([aggregator/internal/domain/transaction.go](aggregator/internal/domain/transaction.go)) — a single customer's purchase of a product. The three collaborators in the consume loop are the **Reader** (reads the topic), the **TTLLocker** (one Reader per partition at a time), and the **Repository** (persists to ClickHouse); see the correctness model below.

## Layout

Three independent Go modules — there is **no root go.mod**, run `go` commands from inside each:

- [aggregator/](aggregator/) — module `delta_aggregator`. The consumer/writer service. Main logic.
- [generator/](generator/) — module `generator`. Produces synthetic purchase batches into the YDB topic.
- [clickhouse_tester/](clickhouse_tester/) — module `delta_aggregator` (separate experimental harness, imports a `delta_aggregator/entities` package that is not present in this tree; treat as exploratory, not part of the main flow).

`migrations/` holds the ClickHouse schema, `deployments/` the docker-compose cluster + per-node config volumes.

## Commands

All cluster/db orchestration goes through the [Makefile](Makefile):

```sh
make infra            # docker-compose up the full cluster (ydb, 6 clickhouse, 3 keeper, 2 aggregators, generator)
make init_db          # create_db + clean_db + apply migrations/init.sql (run after infra)
make infra_with_db    # infra then init_db — full bring-up
make drop_infra       # docker-compose down
make connect_db       # clickhouse client into the accounting db (port 9011)
make clean_db         # drop all tables (migrations/clean.sql)
make drop_replica     # stop clickhouse-01-01 to simulate a replica outage

make generator-run    # go run generator/main.go  (NOTE: actual entrypoint is generator/cmd/main.go)
make aggregator-run   # go run ./aggregator        (NOTE: actual entrypoint is aggregator/cmd/main.go)
make aggregator-logs  # docker logs -f deployments-aggregator-1
```

ClickHouse client targets `localhost:9011` (a mapped node port), database `accounting`.

### Build / test (per module)

```sh
cd aggregator && go build ./...
cd aggregator && go test ./...                              # all tests
cd aggregator && go test ./internal/reader/                 # one package
cd aggregator && go test -run TestName ./internal/offset_manager/keeper/
```

Tests live next to their code (`*_test.go`) in `reader`, `repository/clickhouse`, `lockers/zookeeper`, `offset_manager/keeper`, and `clients/clickhouse_cluster_pool`.

## Correctness model (the important part)

Exactly-once is achieved by combining four mechanisms across the per-partition processing loop in [aggregator/internal/reader/reader.go](aggregator/internal/reader/reader.go):

1. **Per-partition distributed lock** — `TTLLock` (ZooKeeper/ClickHouse-Keeper `zk.Lock`, [lockers/zookeeper/locker.go](aggregator/internal/lockers/zookeeper/locker.go)) ensures only one aggregator processes a YDB partition at a time. Locks are held in a TTL cache; expiry auto-`Unlock`s, so a stalled/crashed holder eventually releases.

2. **Offset range state in Keeper** — the `OffsetManager` ([offset_manager/keeper/keeper.go](aggregator/internal/offset_manager/keeper/keeper.go)) stores, per partition, a `{MinOffset, MaxOffset, State}` triple (17 bytes, big-endian). `State` is `UNKNOWN → IN_PROGRESS → COMPLETED`. This records exactly which offset range was last attempted.

3. **Recovery / replay reconciliation** — when a batch arrives, the reader compares incoming message offsets against the stored range. Messages `<= MaxOffset` of an `IN_PROGRESS` range are "old insert" (a replay of a previously-attempted-but-uncommitted range) and are validated to *exactly* match the stored range before being re-pushed; everything else is "new insert" and must be a contiguous range monotonically continuing from the previous `MaxOffset+1`. The validators (`validateOldInsertRecords` / `validateNewInsertRecords`) enforce contiguity and count, returning an error (which exits the loop) on any gap or mismatch.

4. **ClickHouse insert deduplication** — `PushTransactions` ([repository/clickhouse/repository.go](aggregator/internal/repository/clickhouse/repository.go)) sets an `insert_deduplication_token` derived from `partitionID + MinOffset + MaxOffset`, plus `insert_quorum=auto`, `distributed_foreground_insert=1`, and dedup in dependent MVs. So replaying the identical range is a no-op at the storage layer.

The ordering within `pushAndStoreOffset` matters: rows are pushed to ClickHouse **first**, the Keeper state is set to `COMPLETED` **after**, and only then is the YDB batch `Commit`ted. A crash between any two steps is recoverable on replay because (3) reconstructs the in-progress range and (4) makes the re-push idempotent.

### Fault injection

`aggregator/cmd/main.go` runs the reader once and `panic`s on any fatal error; resilience is exercised externally rather than by in-process self-sabotage. On a crash the container's `restart: always` policy brings it back, and recovery (offset-range reconciliation + ClickHouse dedup) makes the replay idempotent. Faults are injected from the outside: the `tests/` acceptance + soak suites stop/pause/kill containers and partition the network, and **toxiproxy** (`make toxic-connectors`, and the `clickhouse_tester` harness) injects latency, drops, and partitions on the ClickHouse and YDB connections. (An earlier version self-sabotaged with a random 0–240s sleep and a random `os.Exit`/context-cancel loop; that has been removed.)

## ClickHouse schema ([migrations/init.sql](migrations/init.sql))

- `transactions` (`ReplicatedMergeTree`) — raw rows incl. `partition_id`, `offset`; sharded by `xxHash64(partition_id)` via the `transactions_d` Distributed table (what the app writes to).
- `reports` (`ReplicatedAggregatingMergeTree`, `SimpleAggregateFunction(sum/max,...)`) fed by `reports_mv` materialized view aggregating from `transactions` grouped by `(date, user_id, product_id)`; queried via `reports_d` Distributed table.
- The database itself uses the `Replicated` engine on cluster `{cluster}` (macro `cluster_2S_1R`).

## Conventions

- Logging is `log/slog` to a rotating file (`lumberjack`, `logs/aggregator.log`); pass the `*slog.Logger` down through constructors.
- Connection endpoints (YDB `ydb:2135`, keeper `clickhouse-keeper-0x:9181`, the six ClickHouse `clickhouse-0x-0x:9000`) are **hardcoded** in `cmd/main.go` — they are docker-compose service names, so the services run inside the compose network, not against `localhost`.
- The shared topic name is `purchases_topic` and consumer `aggregator` (constants duplicated in both `cmd/main.go` files — keep them in sync).
