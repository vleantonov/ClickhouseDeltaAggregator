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
//
// Deletion is issued through whichever node holds the Raft leadership (go-zookeeper
// iterates the address list until it gets a session). After the write is committed
// Raft replicates it to all followers, but the followers apply the log entry
// asynchronously. To guarantee every replica is clean before the next scenario
// starts we verify absence of the roots on each keeper node individually, retrying
// until either all nodes confirm the delete or the deadline is exceeded.
func ResetKeeperState(cfg Config) error {
	// Step 1: delete through the leader (any node in the ensemble will do;
	// go-zookeeper's DNSHostProvider round-robins until it reaches the leader).
	conn, err := keeperConnect(cfg.KeeperAddrs)
	if err != nil {
		return fmt.Errorf("connect keeper: %w", err)
	}
	defer conn.Close()

	for _, root := range keeperRoots {
		if err := deleteRecursive(conn, root); err != nil {
			return fmt.Errorf("delete %s: %w", root, err)
		}
	}

	// Step 2: confirm every keeper replica has applied the delete.
	// Raft replication is fast (sub-millisecond on a local compose network), but
	// we give it up to 10 s so a briefly-paused follower recovered by a prior
	// scenario still passes.
	return verifyDeletedOnAllReplicas(cfg.KeeperAddrs, keeperRoots, 10*time.Second)
}

// keeperConnect opens a ZooKeeper session to the ensemble and waits until the
// session is fully established.
func keeperConnect(addrs []string) (*zk.Conn, error) {
	conn, _, err := zk.Connect(addrs, 10*time.Second, zk.WithLogger(silentLogger{}))
	if err != nil {
		return nil, err
	}
	deadline := time.Now().Add(10 * time.Second)
	for conn.State() != zk.StateHasSession {
		if time.Now().After(deadline) {
			conn.Close()
			return nil, errors.New("keeper session not established")
		}
		time.Sleep(100 * time.Millisecond)
	}
	return conn, nil
}

// verifyDeletedOnAllReplicas opens a dedicated connection to each keeper address
// and polls until none of the given paths exist on that node, or deadline expires.
// Connecting directly to each follower (bypassing the leader election layer) lets
// us observe the follower's own local state rather than routing through the leader.
func verifyDeletedOnAllReplicas(addrs []string, paths []string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for _, addr := range addrs {
		conn, err := keeperConnect([]string{addr})
		if err != nil {
			// A keeper node may still be recovering from a prior fault; treat a
			// failed connect as "not yet clean" and retry until the deadline.
			if time.Now().After(deadline) {
				return fmt.Errorf("keeper %s unreachable after %s: %w", addr, timeout, err)
			}
		}
		if conn != nil {
			err = waitPathsAbsent(conn, paths, deadline)
			conn.Close()
		}
		if err != nil {
			return fmt.Errorf("keeper %s still has stale state: %w", addr, err)
		}
	}
	return nil
}

// waitPathsAbsent polls each path on conn until all are gone or the deadline passes.
func waitPathsAbsent(conn *zk.Conn, paths []string, deadline time.Time) error {
	for {
		allGone := true
		for _, p := range paths {
			exists, _, err := conn.Exists(p)
			if err != nil && !errors.Is(err, zk.ErrNoNode) {
				return err
			}
			if exists {
				allGone = false
				break
			}
		}
		if allGone {
			return nil
		}
		if time.Now().After(deadline) {
			return errors.New("paths still present after deadline")
		}
		time.Sleep(100 * time.Millisecond)
	}
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
