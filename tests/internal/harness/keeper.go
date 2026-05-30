package harness

import (
	"errors"
	"fmt"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

// keeperRoots are the znode roots the aggregator persists its exactly-once state
// under (see offset_manager/keeper and lockers/zookeeper). They MUST be cleared
// between scenarios: a fresh topic restarts offsets at 0, but stale COMPLETED
// ranges left in Keeper would make the aggregator treat the new run's messages
// as an already-processed replay and silently skip them.
var keeperRoots = []string{"/offsets", "/locks"}

// ResetKeeperState recursively removes the aggregator's offset and lock znodes so
// the next run starts from a clean slate. The aggregator recreates the roots on
// startup, so deleting them outright is fine.
func ResetKeeperState(cfg Config) error {
	conn, _, err := zk.Connect(cfg.KeeperAddrs, 10*time.Second, zk.WithLogger(silentLogger{}))
	if err != nil {
		return fmt.Errorf("connect keeper: %w", err)
	}
	defer conn.Close()

	// Give the session a moment to reach the Connected state before issuing ops.
	deadline := time.Now().Add(10 * time.Second)
	for conn.State() != zk.StateHasSession {
		if time.Now().After(deadline) {
			return errors.New("keeper session not established")
		}
		time.Sleep(100 * time.Millisecond)
	}

	for _, root := range keeperRoots {
		if err := deleteRecursive(conn, root); err != nil {
			return fmt.Errorf("delete %s: %w", root, err)
		}
	}
	return nil
}

func deleteRecursive(conn *zk.Conn, path string) error {
	children, _, err := conn.Children(path)
	if errors.Is(err, zk.ErrNoNode) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, child := range children {
		childPath := path + "/" + child
		if path == "/" {
			childPath = "/" + child
		}
		if err := deleteRecursive(conn, childPath); err != nil {
			return err
		}
	}
	_, stat, err := conn.Exists(path)
	if errors.Is(err, zk.ErrNoNode) {
		return nil
	}
	if err != nil {
		return err
	}
	if err := conn.Delete(path, stat.Version); err != nil && !errors.Is(err, zk.ErrNoNode) {
		return err
	}
	return nil
}

// silentLogger discards the go-zookeeper client's chatty connection logs.
type silentLogger struct{}

func (silentLogger) Printf(string, ...interface{}) {}
