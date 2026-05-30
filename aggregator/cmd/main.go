package main

import (
	"context"
	"delta_aggregator/internal/lockers/zookeeper"
	"delta_aggregator/internal/offset_manager/keeper"
	"delta_aggregator/internal/reader"
	clickhouserepo "delta_aggregator/internal/repository/clickhouse"
	"log/slog"
	"os"
	"os/signal"
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

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
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

	logger.Info("init reader")
	r, err := db.Topic().StartReader(
		consumer,
		topicoptions.ReadTopic(topic),
	)

	if err != nil {
		panic(err)
	}

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

	logger.Info("init locker")
	l := zookeeper.NewZookeeperTTLLocker(zkConn, "/locks", 2*time.Minute, logger)

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
