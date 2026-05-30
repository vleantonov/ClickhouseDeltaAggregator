package clickhouseclusterpool

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

type ClusterConfig struct {
	ConnConfig
	ShardsReplicaHosts [][]string
}

type ConnConfig struct {
	Database string
	Username string
	Password string
	Settings clickhouse.Settings
}

type ClusterPool struct {
	shardsReplicaNames [][]string
	shardsConn         [][]clickhouse.Conn

	rnd *rand.Rand
}

func NewClusterPool(conf ClusterConfig) (*ClusterPool, error) {
	if len(conf.ShardsReplicaHosts) == 0 {
		return nil, fmt.Errorf("no shards")
	}

	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))

	shardsConn := make([][]clickhouse.Conn, len(conf.ShardsReplicaHosts))
	for i, shardReplicaHosts := range conf.ShardsReplicaHosts {
		if len(shardReplicaHosts) == 0 {
			return nil, fmt.Errorf("no replicas in shard %d", i)
		}
		shardsConn[i] = make([]clickhouse.Conn, len(shardReplicaHosts))
		for j, replicaHost := range shardReplicaHosts {
			if replicaHost == "" {
				return nil, fmt.Errorf("empty host in shard %d replica %d", i, j)
			}
			conn, err := newConn(replicaHost, conf.ConnConfig)
			if err != nil {
				return nil, fmt.Errorf("can't create host %s connection: %w", replicaHost, err)
			}
			shardsConn[i][j] = conn
		}

		rnd.Shuffle(len(shardReplicaHosts), func(i, j int) {
			shardReplicaHosts[i], shardReplicaHosts[j] = shardReplicaHosts[j], shardReplicaHosts[i]
		})
	}

	return &ClusterPool{
		shardsReplicaNames: conf.ShardsReplicaHosts,
		shardsConn:         shardsConn,

		rnd: rnd,
	}, nil
}

func newConn(host string, conf ConnConfig) (clickhouse.Conn, error) {
	return clickhouse.Open(&clickhouse.Options{
		Addr:         []string{host},
		DialContext:  nil,
		DialStrategy: nil,
		Auth: clickhouse.Auth{
			Database: conf.Database,
			Username: conf.Username,
			Password: conf.Password,
		},
		Settings: conf.Settings,
	})
}

func (p *ClusterPool) GetShardConn(ctx context.Context, shard int) (clickhouse.Conn, error) {
	if shard < 0 || shard >= len(p.shardsConn) {
		return nil, fmt.Errorf("shard %d not found", shard)
	}

	for {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		if len(p.shardsConn[shard]) == 0 {
			return nil, fmt.Errorf("no alive replicas in shard %d", shard)
		}

		r := p.rnd.Intn(len(p.shardsConn[shard]))
		conn := p.shardsConn[shard][r]

		c, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()

		// Move the ping mechanism to a separate thread in real system
		// so that it doesn't get blocked on each shard replica selection operation
		if err := conn.Ping(c); err != nil {
			p.shardsConn[shard][r], p.shardsConn[shard][len(p.shardsConn[shard])-1] = p.shardsConn[shard][len(p.shardsConn[shard])-1], p.shardsConn[shard][r]
			p.shardsConn[shard] = p.shardsConn[shard][:len(p.shardsConn[shard])-1]

			slog.Default().Error(err.Error())
			continue
		}

		return p.shardsConn[shard][r], nil
	}
}

func (p *ClusterPool) GetMultipleShardConn(ctx context.Context, shard int, k int) ([]clickhouse.Conn, error) {
	if k <= 0 {
		return nil, fmt.Errorf("k %d must be not positive", k)
	}

	if shard < 0 || shard >= len(p.shardsConn) {
		return nil, fmt.Errorf("shard %d not found", shard)
	}

	shardConn := p.shardsConn[shard]
	if k > len(shardConn) {
		return nil, fmt.Errorf("k %d is greater than shard %d replicas %d", k, shard, len(shardConn))
	}

mainLoop:
	for {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		if len(p.shardsConn[shard]) == 0 {
			return nil, fmt.Errorf("no alive replicas in shard %d", shard)
		}

		r := p.rnd.Intn(len(shardConn) - k + 1)
		for i := r; i < r+k; i++ {
			conn := p.shardsConn[shard][i]

			c, cancel := context.WithTimeout(ctx, 2*time.Second)
			defer cancel()

			// Move the ping mechanism to a separate thread in real system
			// so that it doesn't get blocked on each shard replica selection operation
			if err := conn.Ping(c); err != nil {
				p.shardsConn[shard][i], p.shardsConn[shard][len(p.shardsConn[shard])-1] = p.shardsConn[shard][len(p.shardsConn[shard])-1], p.shardsConn[shard][i]
				p.shardsConn[shard] = p.shardsConn[shard][:len(p.shardsConn[shard])-1]
				continue mainLoop
			}
		}

		return p.shardsConn[shard][r : r+k], nil
	}
}

func (p *ClusterPool) GetShardsCount() int {
	return len(p.shardsConn)
}

func (p *ClusterPool) Close() error {
	var errs []error
	for i, shardConn := range p.shardsConn {
		for j, conn := range shardConn {
			err := conn.Close()
			if err != nil {
				errs = append(errs, fmt.Errorf("failed to close connection to shard %d replica %d: %w", i, j, err))
			}
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("failed to close connections: %v", errs)
	}
	return nil
}
