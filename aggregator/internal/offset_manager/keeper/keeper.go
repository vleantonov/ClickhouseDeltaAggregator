package keeper

import (
	offsetmanager "delta_aggregator/internal/offset_manager"
	"encoding/binary"
	"errors"
	"fmt"
	"path"
	"strconv"

	"github.com/samuel/go-zookeeper/zk"
)

type KeeperOffsetManager struct {
	conn *zk.Conn
	root string
}

func NewKeeperOffsetManager(conn *zk.Conn, rootPath string) (*KeeperOffsetManager, error) {
	manager := &KeeperOffsetManager{conn: conn, root: rootPath}

	exists, _, err := conn.Exists(rootPath)
	if err != nil {
		return nil, err
	}
	if !exists {
		_, err := conn.Create(rootPath, []byte{}, 0, zk.WorldACL(zk.PermAll))
		if err != nil && err != zk.ErrNodeExists {
			return nil, err
		}
	}
	return manager, nil
}

func (m *KeeperOffsetManager) getPartitionPath(partition int64) string {
	return path.Join(m.root, strconv.Itoa(int(partition)))
}

func (m KeeperOffsetManager) StoreRangeOffsetState(partition int64, r offsetmanager.RangeOffsetState) error {

	exists, _, err := m.conn.Exists(m.root)
	if err != nil {
		return err
	}
	if !exists {
		_, err := m.conn.Create(m.root, []byte{}, 0, zk.WorldACL(zk.PermAll))
		if err != nil && err != zk.ErrNodeExists {
			return err
		}
	}

	partPath := m.getPartitionPath(partition)
	data := make([]byte, 17)
	binary.BigEndian.PutUint64(data[0:8], r.MinOffset)
	binary.BigEndian.PutUint64(data[8:16], r.MaxOffset)
	data[16] = byte(r.State)

	exists, stat, err := m.conn.Exists(partPath)
	if err != nil {
		return err
	}
	if exists {
		_, err = m.conn.Set(partPath, data, stat.Version)
		return err
	}
	_, err = m.conn.Create(partPath, data, 0, zk.WorldACL(zk.PermAll))
	return err
}

func (m *KeeperOffsetManager) GetRangeOffsetState(partition int64) (
	offsetmanager.RangeOffsetState,
	error,
) {
	data, _, err := m.conn.Get(m.getPartitionPath(partition))
	if err != nil {
		if errors.Is(err, zk.ErrNoNode) {
			return offsetmanager.RangeOffsetState{}, nil // no offset committed yet
		}
		return offsetmanager.RangeOffsetState{}, err
	}
	if len(data) < 17 {
		return offsetmanager.RangeOffsetState{}, fmt.Errorf("invalid offset data length")
	}

	return offsetmanager.RangeOffsetState{
		RangeOffset: offsetmanager.RangeOffset{
			MinOffset: binary.BigEndian.Uint64(data[0:8]),
			MaxOffset: binary.BigEndian.Uint64(data[8:16]),
		},
		State: offsetmanager.OffsetState(data[16]),
	}, nil
}
