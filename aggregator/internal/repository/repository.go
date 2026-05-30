package repository

import (
	"context"
	"delta_aggregator/internal/domain"
	offsetmanager "delta_aggregator/internal/offset_manager"
)

type Repository interface {
	PushTransactions(ctx context.Context, partitionID int64, transactions []domain.Transaction, offsetRange offsetmanager.RangeOffset) error
}
