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
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicreader"
)

type Reader struct {
	*topicreader.Reader

	om         offsetmanager.OffsetManager
	l          lockers.TTLLocker
	repository repository.Repository

	logger *slog.Logger
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
	r.logger.Info("read batch")
	batch, err := r.ReadMessagesBatch(
		parCtx,
		topicreader.WithBatchMaxCount(10),
	)
	if err != nil {
		return fmt.Errorf("failed to read batch: %w", err)
	}

	r.logger.Info("batch read", slog.Int("count", len(batch.Messages)))

	if len(batch.Messages) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(parCtx, 1*time.Minute)
	defer cancel()

	r.logger.Info("start batch processing", slog.Int("count", len(batch.Messages)))

	partitionID := batch.PartitionID()
	if err := r.l.TTLLock(ctx, partitionID); err != nil {
		return fmt.Errorf("failed to lock partition %d: %w", partitionID, err)
	}
	rangeState, err := r.om.GetRangeOffsetState(partitionID)
	if err != nil {
		return fmt.Errorf("failed to get range state for partition %d: %w", partitionID, err)
	}

	r.logger.Info(
		"range state",
		slog.Int("state", int(rangeState.State)),
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
		r.logger.Error("batch context error", slog.String("error", batch.Context().Err().Error()))
		if err := r.l.Unlock(ctx, partitionID); err != nil {
			r.logger.Error("failed to unlock range state", slog.String("error", err.Error()))
		}
		return nil
	}

	if len(oldInsertRecords) > 0 {
		r.logger.Info(
			"validate old insert records",
			slog.Int("count", len(oldInsertRecords)),
			slog.Uint64("min_offset", oldInsertRangeState.MinOffset),
			slog.Uint64("max_offset", oldInsertRangeState.MaxOffset),
			slog.Int64("partition_id", partitionID),
		)
		if err := r.validateOldInsertRecords(oldInsertRecords, oldInsertRangeState, rangeState); err != nil {
			return fmt.Errorf("failed to validate old insert records: %w", err)
		}

		// A COMPLETED range is already durably written to ClickHouse and recorded in
		// the offset manager — this is a pure replay, so we must NOT re-push (and must
		// not treat it as a gap). We only re-push when the previous attempt was left
		// IN_PROGRESS; the ClickHouse dedup token makes that push idempotent.
		if rangeState.State == offsetmanager.IN_PROGRESS {
			r.logger.Info(
				"push old insert records",
				slog.Int("count", len(oldInsertRecords)),
				slog.Uint64("min_offset", oldInsertRangeState.MinOffset),
				slog.Uint64("max_offset", oldInsertRangeState.MaxOffset),
				slog.Int64("partition_id", partitionID),
			)
			if err := r.pushAndStoreOffset(ctx, partitionID, oldInsertRecords, oldInsertRangeState); err != nil {
				r.logger.Error("failed to push and store offset for old insert records", slog.String("error", err.Error()))
				return nil
			}
		}
		r.logger.Info("old insert successfully processed")
	}

	if batch.Context().Err() != nil {
		r.logger.Error("batch context error", slog.String("error", batch.Context().Err().Error()))
		if err := r.l.Unlock(ctx, partitionID); err != nil {
			r.logger.Error("failed to unlock range state", slog.String("error", err.Error()))
		}
		return nil
	}

	if len(newInsertRecords) > 0 {
		r.logger.Info(
			"validate new insert records",
			slog.Int("count", len(newInsertRecords)),
			slog.Uint64("min_offset", newInsertRangeState.MinOffset),
			slog.Uint64("max_offset", newInsertRangeState.MaxOffset),
			slog.Int64("partition_id", partitionID),
		)
		if err := r.validateNewInsertRecords(newInsertRecords, newInsertRangeState, rangeState); err != nil {
			return fmt.Errorf("failed to validate new insert: %w", err)
		}

		r.logger.Info(
			"push new insert records",
			slog.Int("count", len(newInsertRecords)),
			slog.Uint64("min_offset", newInsertRangeState.MinOffset),
			slog.Uint64("max_offset", newInsertRangeState.MaxOffset),
			slog.Int64("partition_id", partitionID),
		)
		if err := r.pushAndStoreOffset(ctx, partitionID, newInsertRecords, newInsertRangeState); err != nil {
			r.logger.Error("failed to push and store offset for new insert records", slog.String("error", err.Error()))
			return nil
		}
		r.logger.Info("new insert successfully processed")
	}

	if err := r.Commit(ctx, batch); err != nil {
		r.logger.Error("failed to commit batch", slog.String("error", err.Error()))
	}
	r.logger.Info("batch committed", slog.Int("count", len(batch.Messages)))
	return nil
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

func (r *Reader) validateOldInsertRecords(
	records []domain.Transaction,
	state offsetmanager.RangeOffsetState,
	inProgressState offsetmanager.RangeOffsetState,
) error {
	if state.MinOffset != inProgressState.MinOffset || state.MaxOffset != inProgressState.MaxOffset {
		return fmt.Errorf("old insert state doesn't equal current in progress state")
	}
	if len(records) != int(state.MaxOffset-state.MinOffset+1) {
		return fmt.Errorf("old insert records count doesn't equal current in progress state")
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

func (r *Reader) pushAndStoreOffset(
	ctx context.Context,
	partitionID int64,
	records []domain.Transaction,
	state offsetmanager.RangeOffsetState,
) error {

	state.State = offsetmanager.IN_PROGRESS
	r.logger.Info("push transactions", slog.Int("count", len(records)))
	if err := r.repository.PushTransactions(ctx, partitionID, records, state.RangeOffset); err != nil {
		return fmt.Errorf("failed to push records: %w", err)
	}

	r.logger.Info("store range state",
		slog.Int64("partition_id", partitionID),
		slog.Int("state", int(state.State)),
		slog.Uint64("min_offset", state.MinOffset),
		slog.Uint64("max_offset", state.MaxOffset),
	)
	state.State = offsetmanager.COMPLETED
	if err := r.om.StoreRangeOffsetState(partitionID, state); err != nil {
		return fmt.Errorf("failed to store range state: %w", err)
	}

	return nil
}
