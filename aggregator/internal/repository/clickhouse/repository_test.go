package clickhouse

import (
	"context"
	"delta_aggregator/internal/domain"
	offsetmanager "delta_aggregator/internal/offset_manager"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func connect(hosts []string) (driver.Conn, error) {

	var (
		ctx       = context.Background()
		conn, err = clickhouse.Open(&clickhouse.Options{
			Addr: hosts,
			Auth: clickhouse.Auth{
				Database: "accounting",
				Username: "default",
			},
			ClientInfo: clickhouse.ClientInfo{
				Products: []struct {
					Name    string
					Version string
				}{
					{Name: "test-repository-go-client", Version: "0.1"},
				},
			},
			Debugf: func(format string, v ...interface{}) {
				slog.Default().Debug(fmt.Sprintf(format, v))
			},
		})
	)

	if err != nil {
		return nil, err
	}

	if err := conn.Ping(ctx); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			fmt.Printf("Exception [%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		}
		return nil, err
	}
	return conn, nil
}

var allHosts = []string{
	"127.0.0.1:9011",
	"127.0.0.1:9012",
	"127.0.0.1:9013",
	"127.0.0.1:9021",
	"127.0.0.1:9022",
	"127.0.0.1:9023",
}

func TestClickhouseRepository(t *testing.T) {
	ctx := context.Background()

	conn, err := connect(allHosts[:1])
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	r := NewRepository(conn, slog.Default())

	transactions := []domain.Transaction{
		{UserID: "user_1", TransactionID: "transaction_1", ProductID: "product1", Amount: 20, Date: time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC), Offset: 0, PartitionID: 1},
		{UserID: "user_1", TransactionID: "transaction_2", ProductID: "product2", Amount: 30, Date: time.Date(2020, 1, 2, 0, 0, 0, 0, time.UTC), Offset: 1, PartitionID: 1},
		{UserID: "user_1", TransactionID: "transaction_3", ProductID: "product1", Amount: 40, Date: time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC), Offset: 2, PartitionID: 1},
		{UserID: "user_2", TransactionID: "transaction_4", ProductID: "product1", Amount: 50, Date: time.Date(2020, 1, 2, 0, 0, 0, 0, time.UTC), Offset: 3, PartitionID: 1},
		{UserID: "user_2", TransactionID: "transaction_5", ProductID: "product1", Amount: 60, Date: time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC), Offset: 4, PartitionID: 1},
		{UserID: "user_10", TransactionID: "transaction_6", ProductID: "product1", Amount: 70, Date: time.Date(2020, 1, 2, 0, 0, 0, 0, time.UTC), Offset: 5, PartitionID: 1},
	}
	offsetRange := offsetmanager.RangeOffset{MinOffset: 0, MaxOffset: 5}

	err = r.PushTransactions(ctx, 1, transactions, offsetRange)
	require.NoError(t, err)

	var transactionsAmount int64

	row := conn.QueryRow(ctx, "SELECT SUM(amount) FROM transactions_d")
	require.NoError(t, row.Err())
	err = row.Scan(&transactionsAmount)
	require.NoError(t, err)

	assert.Equal(t, int64(270), transactionsAmount)

	row = conn.QueryRow(ctx, "SELECT SUM(amount) FROM reports_d")
	require.NoError(t, row.Err())
	err = row.Scan(&transactionsAmount)
	require.NoError(t, err)

	assert.Equal(t, int64(270), transactionsAmount)
}
