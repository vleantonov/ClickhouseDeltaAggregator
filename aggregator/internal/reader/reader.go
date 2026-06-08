package reader

import (
	"context"
	"delta_aggregator/internal/domain"
	"delta_aggregator/internal/lockers"
	offsetmanager "delta_aggregator/internal/offset_manager"
	"delta_aggregator/internal/repository"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicreader"
)

const maxRetries = 3

type Reader struct {
	*topicreader.Reader

	om         offsetmanager.OffsetManager
	l          lockers.TTLLocker
	repository repository.Repository

	logger *slog.Logger

	// pendingOld accumulates replayed records for an IN_PROGRESS range that has not
	// yet been fully redelivered (YDB may split a stored range across several reads).
	// Keyed by partition. The range is re-pushed only once every offset in
	// [MinOffset,MaxOffset] has been collected, so the ClickHouse dedup token (derived
	// from the full range) matches the original attempt — re-pushing a partial
	// sub-range would double-count (had the original insert succeeded) or drop rows
	// (had it not). Accessed only from the single Run goroutine.
	pendingOld map[int64][]domain.Transaction
}

func NewReader(
	ydbReader *topicreader.Reader,
	repository repository.Repository,
	om offsetmanager.OffsetManager,
	l lockers.TTLLocker,
	logger *slog.Logger,
) *Reader {
	return &Reader{
		Reader:     ydbReader,
		repository: repository,
		logger:     logger,

		om: om,
		l:  l,

		pendingOld: make(map[int64][]domain.Transaction),
	}
}

func (r *Reader) Run(parCtx context.Context) error {
	for {
		if parCtx.Err() != nil {
			return parCtx.Err()
		}

		if err := r.processNextBatch(parCtx); err != nil {
			return err
		}
	}
}

// processNextBatch reads and processes a single batch. A returned error is fatal
// for the reader loop; a nil return means "continue with the next batch" (the
// equivalent of the previous loop's `continue`).
func (r *Reader) processNextBatch(parCtx context.Context) error {
	r.logger.Debug("waiting for next batch")
	readStart := time.Now()
	batch, err := r.ReadMessagesBatch(
		parCtx,
		topicreader.WithBatchMaxCount(1000),
	)
	if err != nil {
		return fmt.Errorf("failed to read batch: %w", err)
	}

	if len(batch.Messages) == 0 {
		r.logger.Debug("empty batch received", slog.Duration("read_latency", time.Since(readStart)))
		return nil
	}

	firstOffset := batch.Messages[0].Offset
	lastOffset := batch.Messages[len(batch.Messages)-1].Offset
	partitionID := batch.PartitionID()

	r.logger.Info("batch received",
		slog.Int64("partition_id", partitionID),
		slog.Int("count", len(batch.Messages)),
		slog.Int64("first_offset", firstOffset),
		slog.Int64("last_offset", lastOffset),
		slog.Duration("read_latency", time.Since(readStart)),
	)

	ctx, cancel := context.WithTimeout(parCtx, 1*time.Minute)
	defer cancel()

	lockStart := time.Now()
	r.logger.Info("acquiring partition lock", slog.Int64("partition_id", partitionID))
	if err := r.acquireLockWithRetry(ctx, partitionID); err != nil {
		return fmt.Errorf("failed to lock partition %d: %w", partitionID, err)
	}
	r.logger.Info("partition lock acquired",
		slog.Int64("partition_id", partitionID),
		slog.Duration("lock_latency", time.Since(lockStart)),
	)

	r.logger.Info("fetching stored range state", slog.Int64("partition_id", partitionID))
	getRangeStart := time.Now()
	rangeState, err := r.om.GetRangeOffsetState(partitionID)
	if err != nil {
		return fmt.Errorf("failed to get range state for partition %d: %w", partitionID, err)
	}

	r.logger.Info(
		"stored range state fetched",
		slog.Duration("duration", time.Since(getRangeStart)),
		slog.Int64("partition_id", partitionID),
		slog.String("state", rangeState.State.String()),
		slog.Uint64("min_offset", rangeState.MinOffset),
		slog.Uint64("max_offset", rangeState.MaxOffset),
	)

	// A stored range exists for any state other than UNKNOWN. Messages whose offset
	// falls inside it are a replay of an already-attempted range (either left
	// IN_PROGRESS or fully COMPLETED), everything beyond it is a new insert.
	hasStoredRange := rangeState.State != offsetmanager.UNKNOWN

	oldCap := 0
	if hasStoredRange {
		oldCap = int(rangeState.MaxOffset - rangeState.MinOffset + 1)
	}
	// The redelivered batch may be shorter than the stored range, so clamp to avoid
	// a negative (panicking) capacity.
	if oldCap > len(batch.Messages) {
		oldCap = len(batch.Messages)
	}

	oldInsertRecords := make([]domain.Transaction, 0, oldCap)
	newInsertRecords := make([]domain.Transaction, 0, len(batch.Messages)-oldCap)
	var oldInsertRangeState, newInsertRangeState offsetmanager.RangeOffsetState

	for idx, msg := range batch.Messages {

		if rangeState.State != offsetmanager.UNKNOWN && msg.Offset < int64(rangeState.MinOffset) {
			continue
		}

		var record domain.Transaction
		data, err := io.ReadAll(msg)
		if err != nil {
			return fmt.Errorf("failed to read %d message data: %w", idx, err)
		}

		if err := json.Unmarshal(data, &record); err != nil {
			return fmt.Errorf("failed to unmarshal message data: %w", err)
		}

		record.PartitionID = partitionID
		record.Offset = uint64(msg.Offset)

		if hasStoredRange && msg.Offset <= int64(rangeState.MaxOffset) {
			oldInsertRecords, oldInsertRangeState = r.addRecord(oldInsertRecords, record, oldInsertRangeState)
		} else {
			newInsertRecords, newInsertRangeState = r.addRecord(newInsertRecords, record, newInsertRangeState)
		}
	}

	if batch.Context().Err() != nil {
		r.logger.Error("batch context expired after message parsing", slog.String("error", batch.Context().Err().Error()), slog.Int64("partition_id", partitionID))
		r.logger.Info("releasing partition lock", slog.Int64("partition_id", partitionID))
		unlockStart := time.Now()
		if err := r.l.Unlock(ctx, partitionID); err != nil {
			r.logger.Error("failed to unlock partition", slog.Int64("partition_id", partitionID), slog.String("error", err.Error()))
		} else {
			r.logger.Info("partition lock released", slog.Int64("partition_id", partitionID), slog.Duration("duration", time.Since(unlockStart)))
		}
		return nil
	}

	// Old insert: records replaying a range the offset manager already knows about.
	// These are reconciled FIRST; only once the old range is settled do we touch the
	// new records below.
	if len(oldInsertRecords) > 0 {
		r.logger.Info(
			"validating old insert records (replay)",
			slog.Int64("partition_id", partitionID),
			slog.Int("count", len(oldInsertRecords)),
			slog.Uint64("batch_min_offset", oldInsertRangeState.MinOffset),
			slog.Uint64("batch_max_offset", oldInsertRangeState.MaxOffset),
			slog.String("stored_state", rangeState.State.String()),
			slog.Uint64("stored_min_offset", rangeState.MinOffset),
			slog.Uint64("stored_max_offset", rangeState.MaxOffset),
		)
		if err := r.validateOldInsertRecords(oldInsertRecords, oldInsertRangeState, rangeState); err != nil {
			return fmt.Errorf("failed to validate old insert records: %w", err)
		}

		if rangeState.State == offsetmanager.IN_PROGRESS {
			// The previous attempt persisted IN_PROGRESS but never reached COMPLETED, so
			// the ClickHouse insert may or may not have landed. We re-push the range — the
			// dedup token makes that idempotent — but the token is derived from the full
			// (MinOffset,MaxOffset), so we must re-push the WHOLE range at once. YDB may
			// redeliver it across several reads, so accumulate the replayed records until
			// the full range is present. Until then we deliberately do NOT commit, so an
			// incomplete reconciliation can never be lost.
			pending := mergePendingOld(r.pendingOld[partitionID], oldInsertRecords)
			r.pendingOld[partitionID] = pending

			if !coversFullRange(pending, rangeState.RangeOffset) {
				r.logger.Info(
					"in-progress range partially redelivered; accumulating",
					slog.Int64("partition_id", partitionID),
					slog.Int("have", len(pending)),
					slog.Int("want", int(rangeState.MaxOffset-rangeState.MinOffset+1)),
					slog.Uint64("stored_min_offset", rangeState.MinOffset),
					slog.Uint64("stored_max_offset", rangeState.MaxOffset),
				)
				// A contiguous batch that stops inside the range carries no new-insert
				// records, so there is nothing else to process; wait for the remainder.
				return nil
			}

			r.logger.Info(
				"re-pushing complete in-progress range",
				slog.Int64("partition_id", partitionID),
				slog.Int("count", len(pending)),
				slog.Uint64("min_offset", rangeState.MinOffset),
				slog.Uint64("max_offset", rangeState.MaxOffset),
			)
			if err := r.pushAndStoreOffset(ctx, partitionID, pending, rangeState); err != nil {
				return fmt.Errorf("failed to push and store offset for old insert records: %w", err)
			}
			delete(r.pendingOld, partitionID)
		}
		// A COMPLETED range is already durably written to ClickHouse and recorded in the
		// offset manager — this is a pure replay, so we must NOT re-push it (and must not
		// treat a partial redelivery as a gap). We simply skip it.
		r.logger.Info("old insert records processed",
			slog.Int64("partition_id", partitionID),
			slog.String("stored_state", rangeState.State.String()),
		)
	}

	if batch.Context().Err() != nil {
		r.logger.Error("batch context expired after old-insert processing", slog.String("error", batch.Context().Err().Error()), slog.Int64("partition_id", partitionID))
		r.logger.Info("releasing partition lock", slog.Int64("partition_id", partitionID))
		unlockStart := time.Now()
		if err := r.l.Unlock(ctx, partitionID); err != nil {
			r.logger.Error("failed to unlock partition", slog.Int64("partition_id", partitionID), slog.String("error", err.Error()))
		} else {
			r.logger.Info("partition lock released", slog.Int64("partition_id", partitionID), slog.Duration("duration", time.Since(unlockStart)))
		}
		return nil
	}

	if len(newInsertRecords) > 0 {
		r.logger.Info(
			"validating new insert records",
			slog.Int64("partition_id", partitionID),
			slog.Int("count", len(newInsertRecords)),
			slog.Uint64("min_offset", newInsertRangeState.MinOffset),
			slog.Uint64("max_offset", newInsertRangeState.MaxOffset),
		)
		if err := r.validateNewInsertRecords(newInsertRecords, newInsertRangeState, rangeState); err != nil {
			return fmt.Errorf("failed to validate new insert: %w", err)
		}

		pushStart := time.Now()
		r.logger.Info(
			"pushing new insert records",
			slog.Int64("partition_id", partitionID),
			slog.Int("count", len(newInsertRecords)),
			slog.Uint64("min_offset", newInsertRangeState.MinOffset),
			slog.Uint64("max_offset", newInsertRangeState.MaxOffset),
		)
		if err := r.pushAndStoreOffset(ctx, partitionID, newInsertRecords, newInsertRangeState); err != nil {
			return fmt.Errorf("failed to push and store offset for new insert records: %w", err)
		}
		r.logger.Info("new insert records committed to ClickHouse",
			slog.Int64("partition_id", partitionID),
			slog.Int("count", len(newInsertRecords)),
			slog.Uint64("min_offset", newInsertRangeState.MinOffset),
			slog.Uint64("max_offset", newInsertRangeState.MaxOffset),
			slog.Duration("push_duration", time.Since(pushStart)),
		)
	}

	commitStart := time.Now()
	var commitErr error
	for attempt := 1; attempt <= maxRetries; attempt++ {
		commitErr = r.Commit(ctx, batch)
		if commitErr == nil {
			break
		}
		r.logger.Warn("batch commit failed, will retry",
			slog.Int64("partition_id", partitionID),
			slog.Int("attempt", attempt),
			slog.Int("max_retries", maxRetries),
			slog.String("error", commitErr.Error()),
		)
		if attempt < maxRetries {
			time.Sleep(time.Duration(attempt) * 500 * time.Millisecond)
		}
	}
	if commitErr != nil {
		return fmt.Errorf("commit batch failed after %d attempts: %w", maxRetries, commitErr)
	}
	r.logger.Info("batch offset committed to YDB",
		slog.Int64("partition_id", partitionID),
		slog.Int("count", len(batch.Messages)),
		slog.Int64("first_offset", firstOffset),
		slog.Int64("last_offset", lastOffset),
		slog.Duration("commit_duration", time.Since(commitStart)),
	)
	return nil
}

// acquireLockWithRetry attempts TTLLock repeatedly until the outer ctx is done.
// Each attempt uses a 20-second per-attempt deadline so a Keeper outage shorter
// than that doesn't immediately exhaust the caller's 1-minute budget; we just
// keep retrying until Keeper recovers or the budget runs out.
func (r *Reader) acquireLockWithRetry(ctx context.Context, partitionID int64) error {
	const attemptTimeout = 20 * time.Second
	const backoff = 2 * time.Second
	var attempt int
	for {
		attempt++
		r.logger.Info("TTLLock attempt",
			slog.Int64("partition_id", partitionID),
			slog.Int("attempt", attempt),
		)
		attemptCtx, cancel := context.WithTimeout(ctx, attemptTimeout)
		attemptStart := time.Now()
		err := r.l.TTLLock(attemptCtx, partitionID)
		elapsed := time.Since(attemptStart)
		cancel()
		if err == nil {
			r.logger.Info("TTLLock acquired",
				slog.Int64("partition_id", partitionID),
				slog.Int("attempt", attempt),
				slog.Duration("duration", elapsed),
			)
			return nil
		}
		r.logger.Warn("TTLLock failed, will retry",
			slog.Int64("partition_id", partitionID),
			slog.Int("attempt", attempt),
			slog.Duration("duration", elapsed),
			slog.String("error", err.Error()),
		)
		// If the outer context is already done, propagate that error.
		if ctx.Err() != nil {
			return fmt.Errorf("lock acquire aborted: %w", ctx.Err())
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("lock acquire aborted: %w", ctx.Err())
		case <-time.After(backoff):
		}
	}
}

func (r *Reader) addRecord(
	records []domain.Transaction,
	record domain.Transaction,
	state offsetmanager.RangeOffsetState,
) ([]domain.Transaction, offsetmanager.RangeOffsetState) {
	if len(records) == 0 {
		state = offsetmanager.RangeOffsetState{
			State: offsetmanager.IN_PROGRESS,
			RangeOffset: offsetmanager.RangeOffset{
				MinOffset: uint64(record.Offset),
				MaxOffset: uint64(record.Offset),
			},
		}
	}
	state.MinOffset = min(
		state.MinOffset,
		uint64(record.Offset),
	)
	state.MaxOffset = max(
		state.MaxOffset,
		uint64(record.Offset),
	)

	return append(records, record), state
}

// validateOldInsertRecords checks records that replay an already-attempted range.
//
// YDB does not guarantee that a redelivered batch reproduces the original batch
// boundaries, so the replayed records may be only a *subset* of the stored range
// rather than an exact match. We therefore only require that the records form a
// contiguous run fully contained within the stored range — an exact boundary
// match is NOT required and must not be treated as fatal (doing so previously
// sent the reader into a panic/restart loop that stalled the partition). A
// genuine gap (non-contiguous offsets) or an offset outside the stored range is
// still rejected.
func (r *Reader) validateOldInsertRecords(
	records []domain.Transaction,
	state offsetmanager.RangeOffsetState,
	storedState offsetmanager.RangeOffsetState,
) error {
	if state.MinOffset < storedState.MinOffset || state.MaxOffset > storedState.MaxOffset {
		return fmt.Errorf(
			"old insert range [%d,%d] is not contained in stored range [%d,%d]",
			state.MinOffset, state.MaxOffset,
			storedState.MinOffset, storedState.MaxOffset,
		)
	}
	if len(records) != int(state.MaxOffset-state.MinOffset+1) {
		return fmt.Errorf("old insert records are not contiguous")
	}

	return nil
}

func (r *Reader) validateNewInsertRecords(
	records []domain.Transaction,
	state offsetmanager.RangeOffsetState,
	previousState offsetmanager.RangeOffsetState,
) error {

	if previousState.State != offsetmanager.UNKNOWN && state.MinOffset != previousState.MaxOffset+1 {
		return fmt.Errorf("new insert state doesn't monotonously increase")
	}

	if len(records) != int(state.MaxOffset-state.MinOffset+1) {
		return fmt.Errorf("new insert records count doesn't equal current in progress state")
	}

	return nil
}

// pushAndStoreOffset durably writes a range with a write-ahead marker:
//
//	IN_PROGRESS (offset manager)  →  insert (ClickHouse)  →  COMPLETED (offset manager)
//
// Persisting IN_PROGRESS *before* the ClickHouse insert is what makes recovery
// possible: a crash anywhere between the marker and COMPLETED leaves a durable
// IN_PROGRESS range that the replay path re-pushes idempotently (the ClickHouse
// dedup token, derived from the range, turns the repeat into a no-op).
//
// The whole sequence is retried up to maxRetries times on transient failure.
// Re-marking IN_PROGRESS on a retry is safe because the state machine is
// idempotent for that transition, and the ClickHouse dedup token makes
// re-inserting the same range a no-op.
func (r *Reader) pushAndStoreOffset(
	ctx context.Context,
	partitionID int64,
	records []domain.Transaction,
	state offsetmanager.RangeOffsetState,
) error {
	var lastErr error
	for attempt := 1; attempt <= maxRetries; attempt++ {
		lastErr = r.pushAndStoreOffsetOnce(ctx, partitionID, records, state)
		if lastErr == nil {
			return nil
		}
		r.logger.Warn("pushAndStoreOffset failed, will retry",
			slog.Int("attempt", attempt),
			slog.Int("max_retries", maxRetries),
			slog.Int64("partition_id", partitionID),
			slog.String("error", lastErr.Error()),
		)
		if attempt < maxRetries {
			time.Sleep(time.Duration(attempt) * 500 * time.Millisecond)
		}
	}
	return fmt.Errorf("pushAndStoreOffset failed after %d attempts: %w", maxRetries, lastErr)
}

func (r *Reader) pushAndStoreOffsetOnce(
	ctx context.Context,
	partitionID int64,
	records []domain.Transaction,
	state offsetmanager.RangeOffsetState,
) error {
	r.logger.Info("marking range IN_PROGRESS in keeper",
		slog.Int64("partition_id", partitionID),
		slog.Uint64("min_offset", state.MinOffset),
		slog.Uint64("max_offset", state.MaxOffset),
	)
	state.State = offsetmanager.IN_PROGRESS
	t0 := time.Now()
	if err := r.om.StoreRangeOffsetState(partitionID, state); err != nil {
		return fmt.Errorf("failed to store in-progress range state: %w", err)
	}
	r.logger.Info("keeper IN_PROGRESS write done", slog.Int64("partition_id", partitionID), slog.Duration("duration", time.Since(t0)))

	r.logger.Info("pushing transactions to ClickHouse",
		slog.Int64("partition_id", partitionID),
		slog.Int("count", len(records)),
		slog.Uint64("min_offset", state.MinOffset),
		slog.Uint64("max_offset", state.MaxOffset),
	)
	t1 := time.Now()
	if err := r.repository.PushTransactions(ctx, partitionID, records, state.RangeOffset); err != nil {
		return fmt.Errorf("failed to push records: %w", err)
	}
	r.logger.Info("ClickHouse insert done",
		slog.Int64("partition_id", partitionID),
		slog.Int("count", len(records)),
		slog.Duration("duration", time.Since(t1)),
	)

	r.logger.Info("marking range COMPLETED in keeper",
		slog.Int64("partition_id", partitionID),
		slog.Uint64("min_offset", state.MinOffset),
		slog.Uint64("max_offset", state.MaxOffset),
	)
	state.State = offsetmanager.COMPLETED
	t2 := time.Now()
	if err := r.om.StoreRangeOffsetState(partitionID, state); err != nil {
		return fmt.Errorf("failed to store completed range state: %w", err)
	}
	r.logger.Info("keeper COMPLETED write done", slog.Int64("partition_id", partitionID), slog.Duration("duration", time.Since(t2)))

	return nil
}

// mergePendingOld merges newly redelivered old-insert records into the pending
// reconciliation buffer, de-duplicating by offset and keeping the slice sorted by
// offset (ascending). YDB redelivers in order, but a range can be split across
// reads and the same offset can be redelivered more than once.
func mergePendingOld(pending, incoming []domain.Transaction) []domain.Transaction {
	seen := make(map[uint64]struct{}, len(pending)+len(incoming))
	merged := make([]domain.Transaction, 0, len(pending)+len(incoming))
	for _, group := range [][]domain.Transaction{pending, incoming} {
		for _, rec := range group {
			if _, ok := seen[rec.Offset]; ok {
				continue
			}
			seen[rec.Offset] = struct{}{}
			merged = append(merged, rec)
		}
	}
	sort.Slice(merged, func(i, j int) bool { return merged[i].Offset < merged[j].Offset })
	return merged
}

// coversFullRange reports whether records contain every offset in [Min,Max].
// records must be sorted ascending and free of duplicate offsets (as produced by
// mergePendingOld), so a matching count plus matching endpoints implies the run
// is contiguous and complete.
func coversFullRange(records []domain.Transaction, r offsetmanager.RangeOffset) bool {
	return len(records) == int(r.MaxOffset-r.MinOffset+1) &&
		records[0].Offset == r.MinOffset &&
		records[len(records)-1].Offset == r.MaxOffset
}
