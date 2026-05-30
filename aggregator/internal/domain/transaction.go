package domain

import "time"

type Transaction struct {
	UserID        string    `json:"user_id"`
	TransactionID string    `json:"transaction_id"`
	ProductID     string    `json:"product_id"`
	Amount        int64     `json:"amount"`
	Date          time.Time `json:"date"`

	Offset      uint64
	PartitionID int64
}
