package keeper

import (
	offsetmanager "delta_aggregator/internal/offset_manager"
	"encoding/binary"
	"math/rand"
	"path"
	"testing"
	"time"

	"github.com/samuel/go-zookeeper/zk"
	"github.com/stretchr/testify/require"
)

func TestStoreGetRangeOffsetState(t *testing.T) {
	conn, _, err := zk.Connect([]string{"127.0.0.1:9182", "127.0.0.1:9181", "127.0.0.1:9183"}, time.Second*10)
	require.NoError(t, err)
	defer conn.Close()

	manager, err := NewKeeperOffsetManager(conn, "/offsets")
	require.NoError(t, err)

	minOffset := rand.Uint64() % 10
	maxOffset := rand.Uint64()%10 + 10

	rs := offsetmanager.RangeOffsetState{
		RangeOffset: offsetmanager.RangeOffset{
			MinOffset: minOffset,
			MaxOffset: maxOffset,
		},
		State: offsetmanager.IN_PROGRESS,
	}

	// Store offset range state
	err = manager.StoreRangeOffsetState(1, rs)
	require.NoError(t, err)

	expectedData := make([]byte, 17)
	binary.BigEndian.PutUint64(expectedData[0:8], minOffset)
	binary.BigEndian.PutUint64(expectedData[8:16], maxOffset)
	expectedData[16] = byte(offsetmanager.IN_PROGRESS)
	rsBytes, _, err := conn.Get("/offsets/1")
	require.NoError(t, err)
	require.Equal(t, expectedData, rsBytes)

	// Get offset range state
	state, err := manager.GetRangeOffsetState(1)
	require.NoError(t, err)
	require.Equal(t, rs, state)

	// Store new offset range state
	state.State = offsetmanager.COMPLETED
	err = manager.StoreRangeOffsetState(1, rs)
	require.NoError(t, err)

	// Get new offset range state
	state, err = manager.GetRangeOffsetState(1)
	require.NoError(t, err)
	require.Equal(t, rs, state)

	err = deleteRecursive(conn, "/offsets")
	require.NoError(t, err)
}

func deleteRecursive(conn *zk.Conn, zNodePath string) error {
	children, _, err := conn.Children(zNodePath)
	if err != nil {
		return err
	}
	for _, child := range children {
		childPath := path.Join(zNodePath, child)
		if err := deleteRecursive(conn, childPath); err != nil {
			return err
		}
	}
	return conn.Delete(zNodePath, -1)
}
