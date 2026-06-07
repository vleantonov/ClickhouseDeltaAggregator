# Fix (07-06-2026): Crash-safe offset advance (soak-test data LOSS)

## Problem

`TestSoak_ChaosExactlyOnce` failed with **genuine data LOSS**: 239 000 transactions were
produced into the YDB topic but ClickHouse ended up with only 226 000 — 13 000 rows
permanently missing. All gaps were in **partition 2**, scattered inside its offset range:

```
partition 2: min_offset=0, max_offset=86999, only 74000 distinct offsets
missing ranges: 6000–13999 (8000), ~41000 (3000), 47000–47999 (1000), 72000–72999 (1000)
```

### Root cause

The Keeper stored a **single `{MinOffset, MaxOffset, State}` triple** per partition,
written with a blind `Set`/`Create` (no semantic ordering guarantee). This triple cannot
represent "everything below offset N is done *except* this hole."

With **2 aggregators** and **3 partitions** under `restart: always`, the following race
caused permanent loss:

1. Aggregator A processes partition 2 and calls `pushAndStoreOffset([13000, 13999])`.
   It writes `IN_PROGRESS` to Keeper, then issues a distributed ClickHouse INSERT.
2. The INSERT returns error 210/198 ("Connection refused") because one replica is
   temporarily down — **even though the rows actually landed on a healthy replica**.
   (`distributed_foreground_insert=1` + `insert_quorum=auto` require *all* replicas to
   confirm; a single down node makes the whole call fail.)
3. After 3 retries the insert call gives up. `cmd/main.go` panics, the container
   restarts.
4. **While A was struggling**, the YDB topic rebalanced partition 2 to aggregator B.
   B successfully processed later ranges (e.g. `[40000, 40999]`) and wrote `COMPLETED`
   for them — **overwriting** A's `IN_PROGRESS` marker for `[13000, 13999]`.
5. A restarts. `GetPartitionStartOffset` reads the Keeper and sees the latest COMPLETED
   record for `[40000, 40999]`, so it calls `StartFrom(40001)`. YDB starts delivering
   from 40001 — **skipping offsets 13000–39999 forever**. Those batches are never
   redelivered, never written. **Permanent loss.**

The single triple structurally *cannot* encode two things at once: "the range `[40000,
40999]` is done" and "the range `[13000, 13999]` was attempted but its outcome is
unknown." Any later write silently erased the evidence of the earlier un-committed range.

### Why the insert trigger alone does not cause loss

A panic mid-range, with no racing second aggregator, is safe: the IN_PROGRESS marker
survives, the start-offset callback re-reads it, and `StartFrom(MinOffset)` re-delivers
the range. The ClickHouse dedup token (`transactions-insert-<partition>-<min>-<max>`)
makes the re-insert a no-op even if the original insert partially landed. The bug only
manifests when a *second writer* clobbers the IN_PROGRESS marker before recovery
completes.

---

## Fix

Replaced the single triple with a **two-field durable record** in the Keeper:

```
CompletedUpTo  uint64        // exclusive watermark: offsets [0, CompletedUpTo) are
                             // durably in ClickHouse, guaranteed, contiguous
InProgress     RangeOffset   // the range currently being attempted
HasInProgress  bool
```

### Invariants

- **`CompletedUpTo` is forward-only.** It advances *only* on a COMPLETED write whose
  `Min == CompletedUpTo` (contiguous extension). It can never decrease.
- **A gap is now representable.** `CompletedUpTo=6000` + `InProgress={13000,13999}`
  unambiguously means: "0–5999 are durable; 13000–13999 is being attempted; 6000–12999
  are still owed."
- **Stale writes are no-ops, not overwrites.** Any COMPLETED or IN_PROGRESS write whose
  `Min < CompletedUpTo` is silently ignored (already covered by the watermark).
- **Writes above the watermark are refused.** A COMPLETED or IN_PROGRESS with
  `Min > CompletedUpTo` implies a gap below it. The keeper returns `errGapBelowWatermark`
  so the caller does not commit and recovery re-fills from the watermark.

### CAS loop

`StoreRangeOffsetState` is now a **read-modify-write loop** on the ZooKeeper node
version (`stat.Version`), retrying on `zk.ErrBadVersion`. This means a stale writer that
paused mid-handoff loses the race, re-reads the current record, and either finds the
transition is already a no-op or re-applies it correctly.

### Start-offset callback

`cmd/main.go`'s `WithReaderGetPartitionStartOffset` now always resumes from
`CompletedUpTo` — for both COMPLETED and IN_PROGRESS states. This has a hard correctness
constraint: YDB rejects a `StartFrom` value *below* the consumer's committed offset
(`read_offset < committed` → stream error). Because `CompletedUpTo` is the exact mirror
of the YDB committed offset (the reader only calls `Commit` after `COMPLETED` is written
to Keeper), it is always `>= committed` and always `>= any un-written offset`.

> **Critical non-obvious constraint:** using `min(CompletedUpTo, InProgress.Min)` in the
> IN_PROGRESS branch was tried during development and caused `trying to commit to
> position that is less than committed` stream errors. A stale IN_PROGRESS marker below
> the watermark (written by a racing aggregator) would have dropped the start offset
> below committed. The watermark alone is the correct resume point.

### Legacy decode

The 17-byte legacy record format is still decoded for backward compatibility:
- `{Min, Max, COMPLETED}` → `CompletedUpTo = Max+1`, no in-progress.
- `{Min, Max, IN_PROGRESS}` → `InProgress = {Min, Max}`, `CompletedUpTo = Min`.

---

## Files changed

| File | Change |
|------|--------|
| `aggregator/internal/offset_manager/offset_manager.go` | Added `CompletedUpTo uint64` to `RangeOffsetState` |
| `aggregator/internal/offset_manager/keeper/keeper.go` | New 25-byte layout; `StoreRangeOffsetState` → CAS loop with forward-only watermark; `GetRangeOffsetState` with legacy decode; `applyTransition` with all six transition cases |
| `aggregator/internal/offset_manager/keeper/keeper_test.go` | Replaced 17-byte layout assertions; added pure-logic tests for encode/decode, legacy migration, `toState`, and all `applyTransition` cases (happy path, stale no-op, gap refusal); updated integration test to use an isolated partition |
| `aggregator/cmd/main.go` | `StartFrom(state.CompletedUpTo)` in both COMPLETED and IN_PROGRESS branches |

---

## Crash-safety argument

| Scenario | Outcome |
|----------|---------|
| Crash between IN_PROGRESS write and ClickHouse INSERT | `CompletedUpTo` unchanged; restart re-delivers from watermark; dedup token makes re-insert a no-op. **No loss.** |
| Crash between INSERT and COMPLETED write | Same as above. **No loss.** |
| Crash between COMPLETED write and YDB `Commit` | Watermark advanced, YDB committed hasn't moved yet. Restart: `StartFrom(CompletedUpTo) >= committed` ✓; re-read range is dedup-collapsed. **No loss, no duplicate.** |
| Rebalance while stale lock held | New owner blocks until ZK session expires. Old writer's delayed write loses CAS version race (`ErrBadVersion`) or is a stale no-op (below watermark). Watermark never moves backward. **No loss.** |
| Two aggregators race on same partition | Both writes go through CAS. The later watermark advance wins; the earlier one's COMPLETED is either contiguous (applied) or stale (no-op). **No loss.** |

Worst-case degradation is a **stall** (non-contiguous range refused, triggers restart from
watermark to re-fill the gap) or a **duplicate** (re-read after crash, collapsed by the
ClickHouse `insert_deduplication_token`). Neither is loss.

---

## Verification

```
# Unit tests (pure logic: encode/decode, CAS state machine, legacy decode)
cd aggregator && go test ./internal/offset_manager/keeper/ ./internal/reader/

# Soak test result
produced=239000 storedRows=239000 distinctStored=239000
producedSum=59930942 storedSum=59930942
EXACTLY-ONCE HOLDS: ClickHouse content is identical to the produced input
--- PASS: TestSoak_ChaosExactlyOnce (245.68s)

# Per-partition contiguity (count == max(offset)+1 == uniqExact(offset) for all partitions)
SELECT partition_id, count(), max(offset)+1, uniqExact(offset),
       count() = max(offset)+1 AND count() = uniqExact(offset) AS contiguous
FROM transactions_d GROUP BY partition_id;
-- Result: contiguous=1 for all three partitions
```

---

## Known residual (out of scope)

- **Noisy `less than committed` panics during rebalance** (~7 per aggregator per soak
  run): the in-line `r.Commit(batch)` can try to commit a batch whose offsets are below
  what YDB already committed (because the partition migrated to another aggregator that
  advanced it). This causes a panic → restart but **not loss** — the data is already
  durable and the watermark is consistent. The test passes. A follow-up fix would catch
  this specific error and return `nil` instead of panicking.
- **Insert quorum / `distributed_foreground_insert`**: the original *trigger* for loss
  was `code: 210/198` from a single down replica making a distributed insert fail even
  though rows landed on a healthy replica. That setting is unchanged; this fix makes
  recovery from that failure safe. A separate fix could relax the quorum requirement so
  the trigger itself doesn't fire.
