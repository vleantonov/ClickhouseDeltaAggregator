package main

import (
	"context"
	"delta_aggregator/internal/lockers/zookeeper"
	offsetmanager "delta_aggregator/internal/offset_manager"
	"delta_aggregator/internal/offset_manager/keeper"
	"delta_aggregator/internal/reader"
	clickhouserepo "delta_aggregator/internal/repository/clickhouse"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"gopkg.in/natefinch/lumberjack.v2"

	"github.com/samuel/go-zookeeper/zk"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
)

const (
	topic    = "purchases_topic"
	consumer = "aggregator"
)

func initDB() (*ydb.Driver, error) {
	return ydb.Open(context.Background(), "grpc://ydb:2135/local")
}

func initZKConn() (*zk.Conn, error) {
	conn, _, err := zk.Connect([]string{
		"clickhouse-keeper-01:9181",
		"clickhouse-keeper-02:9181",
		"clickhouse-keeper-03:9181",
	}, time.Minute)
	return conn, err
}

func main() {

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill, syscall.SIGTERM)
	defer stop()

	rotatingFile := &lumberjack.Logger{
		Filename:   "logs/aggregator.log",
		MaxSize:    500, // мегабайт
		MaxBackups: 5,   // сколько старых файлов хранить
		MaxAge:     28,  // дней
	}

	handler := slog.NewTextHandler(rotatingFile, nil)
	logger := slog.New(handler)

	logger.Info("init db")
	db, err := initDB()
	if err != nil {
		panic(err)
	}
	defer func() { _ = db.Close(ctx) }()

	logger.Info("init zk")
	zkConn, err := initZKConn()
	if err != nil {
		panic(err)
	}
	defer zkConn.Close()

	logger.Info("init offset manager")
	om, err := keeper.NewKeeperOffsetManager(zkConn, "/offsets")
	if err != nil {
		panic(err)
	}

	// Locker must be initialised before StartReader so the partition-start-offset
	// callback (below) can acquire the lock before reading state from Keeper.
	logger.Info("init locker")
	l := zookeeper.NewZookeeperTTLLocker(zkConn, "/locks", 2*time.Minute, logger)

	logger.Info("init reader")
	r, err := db.Topic().StartReader(
		consumer,
		topicoptions.ReadTopic(topic),
		topicoptions.WithReaderGetPartitionStartOffset(
			func(cbCtx context.Context, req topicoptions.GetPartitionStartOffsetRequest) (topicoptions.GetPartitionStartOffsetResponse, error) {
				var resp topicoptions.GetPartitionStartOffsetResponse

				// Take the partition lock before reading Keeper state so that two
				// aggregator instances cannot concurrently determine the start offset
				// for the same partition. The lock is released immediately after: the
				// run loop re-acquires it exclusively before processing any batch.
				lockCtx, lockCancel := context.WithTimeout(cbCtx, 30*time.Second)
				defer lockCancel()
				if err := l.TTLLock(lockCtx, req.PartitionID); err != nil {
					return resp, fmt.Errorf("start-offset lock partition %d: %w", req.PartitionID, err)
				}

				// Sync flushes any pending writes from the leader to the follower this
				// connection is currently talking to, so GetRangeOffsetState sees the
				// latest committed state rather than a potentially stale local copy.
				if _, err := zkConn.Sync("/offsets"); err != nil {
					return resp, fmt.Errorf("keeper sync for partition %d: %w", req.PartitionID, err)
				}

				state, err := om.GetRangeOffsetState(req.PartitionID)
				if err != nil {
					return resp, err
				}
				switch state.State {
				case offsetmanager.COMPLETED:
					// Skip the already-written range: start from the first unprocessed offset.
					resp.StartFrom(int64(state.MaxOffset + 1))
					logger.Info("partition start offset from keeper (completed)",
						slog.Int64("partition_id", req.PartitionID),
						slog.Uint64("start_from", state.MaxOffset+1),
					)
				case offsetmanager.IN_PROGRESS:
					// Re-deliver the in-progress range so the recovery path can re-push it.
					resp.StartFrom(int64(state.MinOffset))
					logger.Info("partition start offset from keeper (in-progress)",
						slog.Int64("partition_id", req.PartitionID),
						slog.Uint64("start_from", state.MinOffset),
					)
				default:
					// UNKNOWN: let YDB use its own committed offset (no override).
					logger.Info("partition start offset from YDB (unknown state)",
						slog.Int64("partition_id", req.PartitionID),
					)
				}
				return resp, nil
			},
		),
	)

	if err != nil {
		panic(err)
	}

	logger.Info("init clickhouse connection")
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{
			"clickhouse-01-01:9000",
			"clickhouse-01-02:9000",
			"clickhouse-01-03:9000",
			"clickhouse-02-01:9000",
			"clickhouse-02-02:9000",
			"clickhouse-02-03:9000",
		},
		Auth: clickhouse.Auth{
			Database: "accounting",
			Username: "default",
		},
	})
	if err != nil {
		panic(err)
	}
	defer func() { _ = conn.Close() }()
	repository := clickhouserepo.NewRepository(conn, logger)

	logger.Info("init reader")
	appReader := reader.NewReader(r, repository, om, l, logger)
	defer func() { _ = appReader.Close(ctx) }()

	logger.Info("run reader")
	if err := appReader.Run(ctx); err != nil {
		panic(err)
	}
}
