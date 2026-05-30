package harness

import (
	"math/rand"
	"time"

	"github.com/google/uuid"
)

// Transaction is the wire format produced into the YDB topic. It mirrors
// aggregator/internal/domain.Transaction's JSON tags exactly so the aggregator
// can unmarshal it. Amount is an integer so the JSON encodes without a decimal
// point (the aggregator unmarshals amount into an int64).
type Transaction struct {
	UserID        string    `json:"user_id"`
	TransactionID string    `json:"transaction_id"`
	ProductID     string    `json:"product_id"`
	Amount        int64     `json:"amount"`
	Date          time.Time `json:"date"`
}

// Dataset is a generated batch of transactions together with the invariants the
// exactly-once assertion checks against.
type Dataset struct {
	Transactions []Transaction
	// Count is the number of distinct transactions produced (== len).
	Count int
	// Sum is the exact sum of all amounts.
	Sum int64
}

var (
	defaultUsers    = []string{"user1", "user2", "user3", "user4", "user5"}
	defaultProducts = []string{"product1", "product2", "product3"}
)

// GenerateDataset builds n transactions with unique transaction IDs and records
// the exact count and amount sum. The randomness is seeded per call; the
// invariants are computed from the generated data, so determinism across runs is
// not required — only that Count/Sum describe exactly what was produced.
func GenerateDataset(n int) Dataset {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	date := time.Date(2025, 3, 1, 0, 0, 0, 0, time.UTC)

	ds := Dataset{
		Transactions: make([]Transaction, 0, n),
		Count:        n,
	}
	for i := 0; i < n; i++ {
		amount := int64(rng.Intn(500) + 1) // 1..500, never zero
		ds.Sum += amount
		ds.Transactions = append(ds.Transactions, Transaction{
			TransactionID: uuid.NewString(),
			UserID:        defaultUsers[rng.Intn(len(defaultUsers))],
			ProductID:     defaultProducts[rng.Intn(len(defaultProducts))],
			Amount:        amount,
			Date:          date,
		})
	}
	return ds
}
