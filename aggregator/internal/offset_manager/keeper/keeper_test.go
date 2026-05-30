package keeper

import (
	offsetmanager "delta_aggregator/internal/offset_manager"
	"encoding/binary"
	"fmt"
	"math/rand"
	"path"
	"testing"
	"time"

	"github.com/samuel/go-zookeeper/zk"
	"github.com/stretchr/testify/require"
)

func TestPrepareForRangeStore(t *testing.T) {
	for i := range 3 {
		t.Run(fmt.Sprintf("Host_918%d", i), func(t *testing.T) {
			var conns []*zk.Conn
			for i := 1; i <= 3; i++ {
				conn, _, err := zk.Connect([]string{fmt.Sprintf("127.0.0.1:918%d", i)}, time.Second*10)
				require.NoError(t, err)
				defer conn.Close()

				conns = append(conns, conn)
			}

			var managers []*KeeperOffsetManager
			for i := 0; i < 3; i++ {
				manager, err := NewKeeperOffsetManager(conns[i], "/offsets")
				require.NoError(t, err)
				managers = append(managers, manager)
			}

			minOffset := rand.Uint64() % 10
			maxOffset := rand.Uint64()%10 + 10

			rs := offsetmanager.RangeOffsetState{
				RangeOffset: offsetmanager.RangeOffset{
					MinOffset: minOffset,
					MaxOffset: maxOffset,
				},
				State: offsetmanager.IN_PROGRESS,
			}

			// Check bot
			coreManager := managers[i]
			nonCoreManagers := []*KeeperOffsetManager{managers[(i+1)%3], managers[(i+2)%3]}

			err := coreManager.Prepare(1)
			require.NoError(t, err)

			err = coreManager.StoreRangeOffsetState(1, rs)
			require.NoError(t, err)

			for _, manager := range nonCoreManagers {
				err := manager.Prepare(1)
				require.NoError(t, err)

				rsGet, err := manager.GetRangeOffsetState(1)
				require.NoError(t, err)

				require.Equal(t, rs, rsGet)
			}

			err = deleteRecursive(conns[0], "/offsets")
			require.NoError(t, err)

			// Check prepare non core managers separately
			for _, manager := range nonCoreManagers {
				err := coreManager.Prepare(1)
				require.NoError(t, err)

				err = coreManager.StoreRangeOffsetState(1, rs)
				require.NoError(t, err)

				err = manager.Prepare(1)
				require.NoError(t, err)

				rsGet, err := manager.GetRangeOffsetState(1)
				require.NoError(t, err)

				require.Equal(t, rs, rsGet)

				err = deleteRecursive(conns[0], "/offsets")
				require.NoError(t, err)
			}
		})
	}
}

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
