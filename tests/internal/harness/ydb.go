package harness

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/balancers"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicwriter"
)

// YDB wraps the driver and exposes exactly the topic operations the suite needs:
// (re)creating the topic, managing consumers, and producing the dataset.
type YDB struct {
	cfg    Config
	driver *ydb.Driver
}

// ConnectYDB opens the driver against the host-reachable endpoint. SingleConn
// keeps the driver pinned to the dialed endpoint rather than chasing
// discovery-advertised in-compose hostnames.
func ConnectYDB(cfg Config) (*YDB, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	d, err := ydb.Open(ctx, cfg.YDBEndpoint,
		ydb.WithBalancer(balancers.SingleConn()),
		ydb.WithDialTimeout(5*time.Second),
	)
	if err != nil {
		return nil, err
	}
	return &YDB{cfg: cfg, driver: d}, nil
}

func (y *YDB) Close() { _ = y.driver.Close(context.Background()) }

// RecreateTopic drops and recreates the topic, which resets all partition
// offsets to zero AND purges any previously-produced messages — the precondition
// for a clean, repeatable, uncontaminated scenario. We confirm the drop actually
// took effect before recreating, so leftover data (e.g. from the load generator)
// can never bleed into a run.
func (y *YDB) RecreateTopic(ctx context.Context) error {
	if err := y.dropTopicAndConfirm(ctx); err != nil {
		return err
	}
	return y.driver.Topic().Create(ctx, y.cfg.Topic,
		topicoptions.CreateWithConsumer(topictypes.Consumer{
			Name:      y.cfg.Consumer,
			Important: true,
		}),
		topicoptions.CreateWithMinActivePartitions(y.cfg.MinActivePartitions),
		topicoptions.CreateWithPartitionCountLimit(10),
	)
}

// dropTopicAndConfirm drops the topic and polls until Describe reports it gone,
// so the subsequent Create yields a genuinely empty topic.
func (y *YDB) dropTopicAndConfirm(ctx context.Context) error {
	// The topic may not exist yet (first run); a Drop error there is benign.
	_ = y.driver.Topic().Drop(ctx, y.cfg.Topic)

	deadline := time.Now().Add(20 * time.Second)
	for {
		if _, err := y.driver.Topic().Describe(ctx, y.cfg.Topic); err != nil {
			// Describe failing == topic no longer present: the drop took effect.
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("topic %q still present after drop", y.cfg.Topic)
		}
		// Retry the drop in case the first attempt raced an active reader.
		_ = y.driver.Topic().Drop(ctx, y.cfg.Topic)
		time.Sleep(500 * time.Millisecond)
	}
}

// AddConsumer registers an extra named consumer on the topic.
func (y *YDB) AddConsumer(ctx context.Context, name string) error {
	return y.driver.Topic().Alter(ctx, y.cfg.Topic,
		topicoptions.AlterWithAddConsumers(topictypes.Consumer{Name: name}),
	)
}

// DropConsumer removes a named consumer from the topic.
func (y *YDB) DropConsumer(ctx context.Context, name string) error {
	return y.driver.Topic().Alter(ctx, y.cfg.Topic,
		topicoptions.AlterWithDropConsumers(name),
	)
}

// ListConsumers returns the current consumer names on the topic.
func (y *YDB) ListConsumers(ctx context.Context) ([]string, error) {
	desc, err := y.driver.Topic().Describe(ctx, y.cfg.Topic)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(desc.Consumers))
	for _, c := range desc.Consumers {
		names = append(names, c.Name)
	}
	return names, nil
}

// Produce writes the whole dataset into the topic, letting YDB assign sequence
// numbers and spread messages across partitions. It blocks until every message
// is acknowledged by the server.
func (y *YDB) Produce(ctx context.Context, ds Dataset) error {
	w, err := y.driver.Topic().StartWriter(y.cfg.Topic,
		topicoptions.WithWriterWaitServerAck(true),
	)
	if err != nil {
		return fmt.Errorf("start writer: %w", err)
	}
	defer w.Close(ctx)

	msgs := make([]topicwriter.Message, 0, len(ds.Transactions))
	for i := range ds.Transactions {
		payload, err := json.Marshal(ds.Transactions[i])
		if err != nil {
			return fmt.Errorf("marshal transaction: %w", err)
		}
		msgs = append(msgs, topicwriter.Message{Data: bytes.NewReader(payload)})
	}

	if err := w.Write(ctx, msgs...); err != nil {
		return fmt.Errorf("write messages: %w", err)
	}
	if err := w.Flush(ctx); err != nil {
		return fmt.Errorf("flush writer: %w", err)
	}
	return nil
}
