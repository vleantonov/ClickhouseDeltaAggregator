package main

import (
	"bytes"
	"context"
	"encoding/json"
	"generator/internal"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicwriter"
	"gopkg.in/natefinch/lumberjack.v2"
)

const (
	topic    = "purchases_topic"
	consumer = "aggregator"
)

func initDB() (*ydb.Driver, error) {
	return ydb.Open(context.Background(), "grpc://ydb:2135/local")
}

var (
	availableUsers = []string{
		"user1",
		"user2",
		"user3",
		"user4",
		"user5",
	}
	availableProducts = []string{
		"product1",
		"product2",
		"product3",
	}
	availableAmount      = 1_000_000
	totalAmount          = availableAmount * 1000
	maxTransactionAmount = 500
	startDate            = time.Date(2025, 3, 1, 0, 0, 0, 0, time.UTC)
	endDate              = time.Date(2025, 3, 2, 0, 0, 0, 0, time.UTC)
)

func writer(ctx context.Context) {

	db, err := initDB()
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = db.Close(ctx) }()

	rotatingFile := &lumberjack.Logger{
		Filename:   "logs/aggregator.log",
		MaxSize:    500, // мегабайт
		MaxBackups: 5,   // сколько старых файлов хранить
		MaxAge:     28,  // дней
	}
	defer func() { _ = rotatingFile.Close() }()
	log.SetOutput(rotatingFile)

	writer, err := db.Topic().StartWriter(
		topic,
		topicoptions.WithWriterWaitServerAck(true),
		topicoptions.WithWriterCodec(topictypes.CodecGzip),
		topicoptions.WithWriterSetAutoSeqNo(false),
	)
	if err != nil {
		log.Println("err start writer: ", err)
		return
	}

	curSum := 0
	var i int64 = 1
	for {
		for date := startDate; date.Before(endDate); date = date.AddDate(0, 0, 1) {
			log.Printf("current sum %d", curSum)
			batch := internal.GenerateConsumptionDistribution(
				availableUsers,
				availableProducts,
				int64(availableAmount),
				int64(maxTransactionAmount),
				date,
			)
			preparedMessages := make([]topicwriter.Message, 0, len(batch))
			for _, m := range batch {
				mb, err := json.Marshal(m)
				if err != nil {
					log.Println("err marshal message: ", err)
					return
				}
				preparedMessages = append(preparedMessages, topicwriter.Message{
					SeqNo: i,
					Data:  bytes.NewReader(mb),
				})
				i++
			}

			err = writer.Write(ctx, preparedMessages...)
			if err != nil {
				log.Println("err write prepared messages: ", err)
				return
			}

			if err := writer.Flush(ctx); err != nil {
				log.Println("err flush writer: ", err)
				return
			}
			log.Printf("successfully write amount %d in date %s", availableAmount, date.String())

			curSum += availableAmount
		}

		if curSum >= totalAmount {
			break
		}
	}

}

func main() {
	db, err := initDB()
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	ctx, c := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer c()

	log.Println("init topic")
	if err := db.Topic().Create(ctx, topic, topicoptions.CreateWithConsumer(topictypes.Consumer{
		Name:      consumer,
		Important: true,
	}), topicoptions.CreateWithMinActivePartitions(3),
		topicoptions.CreateWithPartitionCountLimit(10),
	); err != nil {
		log.Println("create topic:", err)
	}

	writer(ctx)
}
