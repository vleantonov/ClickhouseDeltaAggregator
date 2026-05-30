package internal

import (
	"encoding/json"
	"math/rand"
	"time"

	"github.com/google/uuid"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicreader"
)

type Message struct {
	UserID        string    `json:"user_id"`
	TransactionID string    `json:"transaction_id"`
	ProductID     string    `json:"product_id"`
	Amount        float64   `json:"amount"`
	Date          time.Time `json:"date"`
}

func (m *Message) UnmarshalYDBTopicMessage(data []byte) error {
	return json.Unmarshal(data, m)
}

func GenerateConsumptionDistribution(
	availableUsers []string,
	availableProducts []string,
	availableAmount int64,
	maxTransactionAmount int64,
	date time.Time,
) (result []Message) {

	for availableAmount > 0 {
		if maxTransactionAmount > availableAmount {
			maxTransactionAmount = availableAmount
		}
		trAm := rand.Int63n(maxTransactionAmount + 1)
		userID := rand.Intn(len(availableUsers))
		productID := rand.Intn(len(availableProducts))

		result = append(result, Message{
			TransactionID: uuid.New().String(),
			UserID:        availableUsers[userID],
			ProductID:     availableProducts[productID],
			Amount:        float64(trAm),
			Date:          date,
		})

		availableAmount -= trAm
	}

	return
}

func GetFromYDBBatch(batch *topicreader.Batch) ([]Message, error) {
	if len(batch.Messages) == 0 {
		return nil, nil
	}

	messages := make([]Message, len(batch.Messages))
	for idx, msg := range batch.Messages {
		if err := msg.UnmarshalTo(&messages[idx]); err != nil {
			return nil, err
		}
	}

	return messages, nil
}
