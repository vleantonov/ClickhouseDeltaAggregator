package zookeeper

import (
	"context"
	"delta_aggregator/internal/lockers"
	"fmt"
	"log/slog"
	"path"
	"strconv"
	"time"

	"github.com/jellydator/ttlcache/v3"
	"github.com/samuel/go-zookeeper/zk"
)

var _ lockers.TTLLocker = &ZookeeperTTLLocker{}

type ZookeeperTTLLocker struct {
	zk           *zk.Conn
	baseLockPath string

	locks *ttlcache.Cache[int64, *zk.Lock]

	logger *slog.Logger
}

func NewZookeeperTTLLocker(
	conn *zk.Conn,
	baseLockPath string,
	maxTtl time.Duration,
	logger *slog.Logger,
) *ZookeeperTTLLocker {
	ok, _, err := conn.Exists(baseLockPath)
	if err != nil {
		panic(err)
	}

	if !ok {
		if _, err := conn.Create(baseLockPath, nil, 0, zk.WorldACL(zk.PermAll)); err != nil {
			panic(err)
		}
	}

	if maxTtl == 0 {
		maxTtl = ttlcache.DefaultTTL
	}

	cache := ttlcache.New[int64, *zk.Lock](
		ttlcache.WithTTL[int64, *zk.Lock](maxTtl),
	)

	cache.OnEviction(func(ctx context.Context, er ttlcache.EvictionReason, i *ttlcache.Item[int64, *zk.Lock]) {
		logger.Info("start eviction", slog.Int64("partition", i.Key()), slog.Int("reason", int(er)))
		if er == ttlcache.EvictionReasonExpired || er == ttlcache.EvictionReasonDeleted {
			lock := i.Value()
			if err := lock.Unlock(); err != nil {
				logger.ErrorContext(ctx, fmt.Sprintf("failed to unlock partition %d: %s", i.Key(), err.Error()))
			}
		}
	})

	go cache.Start()

	return &ZookeeperTTLLocker{
		zk:           conn,
		baseLockPath: baseLockPath,
		locks:        cache,
		logger:       logger,
	}
}

func (z *ZookeeperTTLLocker) TTLLock(ctx context.Context, partition int64) error {
	// Already held by this instance: a Get hit refreshes the TTL, keeping the lock
	// alive while we keep processing the partition.
	if item := z.locks.Get(partition); item != nil {
		return nil
	}

	lock := zk.NewLock(z.zk, path.Join(z.baseLockPath, strconv.Itoa(int(partition))), zk.WorldACL(zk.PermAll))

	// zk.Lock.Lock() blocks without honoring a context, so run it off-goroutine and
	// race it against ctx cancellation/timeout. Otherwise a competing holder would
	// hang this reader forever (e.g. on consumer outage or a slow network).
	errCh := make(chan error, 1)
	go func() { errCh <- lock.Lock() }()

	select {
	case <-ctx.Done():
		// The blocking Lock() can't be cancelled. If it still gets acquired later,
		// release it so we don't leak a held lock in ZooKeeper.
		go func() {
			if err := <-errCh; err == nil {
				_ = lock.Unlock()
			}
		}()
		return ctx.Err()
	case err := <-errCh:
		if err != nil {
			return fmt.Errorf("failed to acquire lock for partition %d: %w", partition, err)
		}
	}

	z.locks.Set(partition, lock, ttlcache.DefaultTTL)
	z.logger.Info("create partition lock", slog.Int64("partition", partition))
	return nil
}

func (z *ZookeeperTTLLocker) Unlock(ctx context.Context, partition int64) error {
	z.logger.Info("delete partition lock", slog.Int64("partition", partition))
	z.locks.Delete(partition)
	return nil
}

func (z *ZookeeperTTLLocker) Close(ctx context.Context) error {
	z.logger.Info("close zookeeper locker")
	z.locks.Stop()
	return nil
}
