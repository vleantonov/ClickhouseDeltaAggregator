package zookeeper

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/samuel/go-zookeeper/zk"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestZookeeperLocker_LockUnlock(t *testing.T) {
	conn, _, err := zk.Connect([]string{"127.0.0.1:9182", "127.0.0.1:9181", "127.0.0.1:9183"}, time.Second*10)
	require.NoError(t, err)

	l := NewZookeeperTTLLocker(conn, "/locks", time.Minute, slog.Default())

	err = l.TTLLock(context.Background(), 1)
	require.NoError(t, err)

	ok, _, err := conn.Exists("/locks/1")
	require.NoError(t, err)
	assert.True(t, ok)

	children, _, err := conn.Children("/locks/1")
	require.NoError(t, err)
	assert.NotEmpty(t, children)

	slog.Default().Info("children", slog.Any("children", children))

	err = l.Unlock(context.Background(), 1)
	require.NoError(t, err)

	ok, _, err = conn.Exists("/locks/1")
	require.NoError(t, err)
	assert.True(t, ok)

	children, _, err = conn.Children("/locks/1")
	require.NoError(t, err)
	assert.Empty(t, children)
}

func TestZookeeperLocker_TTLLock(t *testing.T) {
	conn, _, err := zk.Connect([]string{"127.0.0.1:9182", "127.0.0.1:9181", "127.0.0.1:9183"}, time.Second*10)
	require.NoError(t, err)

	l := NewZookeeperTTLLocker(conn, "/locks", time.Second*3, slog.Default())

	err = l.TTLLock(context.Background(), 1)
	require.NoError(t, err)

	ok, _, err := conn.Exists("/locks/1")
	require.NoError(t, err)
	assert.True(t, ok)

	children, _, err := conn.Children("/locks/1")
	require.NoError(t, err)
	assert.NotEmpty(t, children)

	<-time.After(time.Second * 6)

	ok, _, err = conn.Exists("/locks/1")
	require.NoError(t, err)
	assert.True(t, ok)

	children, _, err = conn.Children("/locks/1")
	require.NoError(t, err)
	assert.Empty(t, children)
}

func TestZookeeperLocker_ConcurrentLock(t *testing.T) {
	conn1, _, err := zk.Connect([]string{"127.0.0.1:9181"}, time.Second*10)
	require.NoError(t, err)

	conn2, _, err := zk.Connect([]string{"127.0.0.1:9182"}, time.Second*10)
	require.NoError(t, err)

	results := [2]time.Time{}

	var wg sync.WaitGroup
	wg.Add(1)

	l1 := NewZookeeperTTLLocker(conn1, "/locks", time.Hour, slog.Default())
	err = l1.TTLLock(context.Background(), 1)
	require.NoError(t, err)

	go func() {
		defer wg.Done()
		l2 := NewZookeeperTTLLocker(conn2, "/locks", time.Hour, slog.Default())
		err := l2.TTLLock(context.Background(), 1)
		require.NoError(t, err)
		results[1] = time.Now()
		err = l2.Unlock(context.Background(), 1)
		require.NoError(t, err)
	}()

	<-time.After(5 * time.Second)
	results[0] = time.Now()
	err = l1.Unlock(context.Background(), 1)
	require.NoError(t, err)

	wg.Wait()

	assert.Greater(t, results[1], results[0])
	_, err = conn1.Sync("/locks/1")
	require.NoError(t, err)
	children, _, err := conn1.Children("/locks/1")
	require.NoError(t, err)
	assert.Empty(t, children)
}

func TestZookeeperLocker_ConcurrentTTLLock(t *testing.T) {
	conn1, _, err := zk.Connect([]string{"127.0.0.1:9181"}, time.Second*10)
	require.NoError(t, err)

	conn2, _, err := zk.Connect([]string{"127.0.0.1:9182"}, time.Second*10)
	require.NoError(t, err)

	results := [2]time.Time{}

	var wg sync.WaitGroup
	wg.Add(1)

	l1 := NewZookeeperTTLLocker(conn1, "/locks", time.Second*2, slog.Default())
	err = l1.TTLLock(context.Background(), 1)
	require.NoError(t, err)

	go func() {
		defer wg.Done()
		l2 := NewZookeeperTTLLocker(conn2, "/locks", time.Second*2, slog.Default())
		err := l2.TTLLock(context.Background(), 1)
		require.NoError(t, err)
		results[1] = time.Now()
	}()

	results[0] = time.Now()

	wg.Wait()

	assert.Greater(t, results[1], results[0])
	assert.InDelta(t, time.Second*2, results[1].Sub(results[0]), float64(time.Millisecond*50))

	<-time.After(time.Second * 4)
	children, _, err := conn1.Children("/locks/1")
	require.NoError(t, err)
	assert.Empty(t, children)
}
