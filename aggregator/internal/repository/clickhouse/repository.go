package clickhouse

import (
	"context"
	"delta_aggregator/internal/domain"
	offsetmanager "delta_aggregator/internal/offset_manager"
	"fmt"
	"log/slog"

	"github.com/ClickHouse/clickhouse-go/v2"
)

const (
	insertTokenTmpl = "transactions-insert-%d-%d-%d"
	insertQuery     = `
	INSERT INTO transactions_d
		(date, transaction_id, 
		 user_id, product_id, 
		 amount, partition_id, offset
		) 
	VALUES (?, ?, ?, ?, ?, ?, ?)
`
)

type Repository struct {
	conn   clickhouse.Conn
	logger *slog.Logger
}

func NewRepository(
	conn clickhouse.Conn,
	logger *slog.Logger,
) *Repository {
	return &Repository{
		conn:   conn,
		logger: logger,
	}
}

func (r *Repository) PushTransactions(ctx context.Context, partitionID int64, transactions []domain.Transaction, offsetRange offsetmanager.RangeOffset) error {
	if len(transactions) == 0 {
		slog.Warn("empty transactions to push")
		return nil
	}

	if err := r.validateRangeOffset(offsetRange); err != nil {
		return fmt.Errorf("invalid range offset: %w", err)
	}

	ctx = clickhouse.Context(ctx, clickhouse.WithSettings(
		clickhouse.Settings{
			"insert_deduplication_token": fmt.Sprintf(insertTokenTmpl, partitionID, offsetRange.MinOffset, offsetRange.MaxOffset),
			"insert_quorum":              "auto",
			"deduplicate_blocks_in_dependent_materialized_views": 1,
			"distributed_foreground_insert":                      1,
		},
	))

	batch, err := r.conn.PrepareBatch(ctx, insertQuery)
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}
	defer func() { _ = batch.Close() }()

	for _, transaction := range transactions {
		if transaction.PartitionID != partitionID {
			return fmt.Errorf("invalid partition id for transaction: %d, expected %d", transaction.PartitionID, partitionID)
		}

		if err := batch.Append(
			transaction.Date,
			transaction.TransactionID,
			transaction.UserID,
			transaction.ProductID,
			transaction.Amount,
			transaction.PartitionID,
			transaction.Offset,
		); err != nil {
			return fmt.Errorf("failed to append transaction: %w", err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("failed to send batch: %w", err)
	}

	return nil
}

func (r *Repository) validateRangeOffset(offsetRange offsetmanager.RangeOffset) error {
	if offsetRange.MinOffset > offsetRange.MaxOffset {
		return fmt.Errorf("invalid range offset: min offset must be less or equal max offset: %v", offsetRange)
	}

	return nil
}
