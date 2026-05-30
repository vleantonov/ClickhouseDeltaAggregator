package clickhouseclusterpool

import (
	"context"
	"maps"
	"os/exec"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

func TestClusterPoolSuite(t *testing.T) {
	suite.Run(t, new(ClusterPoolSuite))
}

type ClusterPoolSuite struct {
	suite.Suite
	shardsReplicaHosts     [][]string
	shardsReplicaHostNames [][]string
}

func (s *ClusterPoolSuite) SetupSuite() {
	s.T().Log("SetupSuite")
	s.shardsReplicaHostNames = [][]string{
		{"clickhouse-01-01", "clickhouse-01-02", "clickhouse-01-03"},
		{"clickhouse-02-01", "clickhouse-02-02", "clickhouse-02-03"},
	}
	s.shardsReplicaHosts = [][]string{
		{"localhost:9011", "localhost:9012", "localhost:9013"},
		{"localhost:9021", "localhost:9022", "localhost:9023"},
	}

}

func (s *ClusterPoolSuite) BeforeTest(suiteName, testName string) {
	s.T().Logf("Before test %s.%s", suiteName, testName)
	cmd := exec.Command("make", "infra")
	out, err := cmd.CombinedOutput()
	if err != nil {
		s.T().Fatalf("%s, %v", out, err)
	}
	<-time.After(5 * time.Second)
}

func (s *ClusterPoolSuite) AfterTest(suiteName, testName string) {
	s.T().Logf("After test %s.%s", suiteName, testName)
	cmd := exec.Command("make", "drop_infra")
	out, err := cmd.CombinedOutput()
	if err != nil {
		s.T().Fatalf("%s, %v", out, err)
	}
}

func (s *ClusterPoolSuite) TestNewClusterPool() {
	ctx := context.Background()

	c := ClusterConfig{
		ConnConfig: ConnConfig{
			Database: "default",
			Username: "default",
			Password: "",
		},
		ShardsReplicaHosts: s.shardsReplicaHosts,
	}

	cp, err := NewClusterPool(c)
	s.Require().NoError(err)

	shardsHostNames := make([][]string, len(cp.shardsConn))
	for idx, shardConn := range cp.shardsConn {
		for _, conn := range shardConn {
			s.Require().NotNil(conn)
			s.Require().NoError(conn.Ping(ctx))

			var hostName string
			row := conn.QueryRow(ctx, "SELECT hostName()")
			row.Scan(&hostName)

			shardsHostNames[idx] = append(shardsHostNames[idx], hostName)
		}
	}

	for idx, shardHostNames := range shardsHostNames {
		s.Require().ElementsMatch(s.shardsReplicaHostNames[idx], shardHostNames)
	}
}

func (s *ClusterPoolSuite) TestGetShardConn() {
	ctx := context.Background()

	c := ClusterConfig{
		ConnConfig: ConnConfig{
			Database: "default",
			Username: "default",
			Password: "",
		},
		ShardsReplicaHosts: s.shardsReplicaHosts,
	}

	cp, err := NewClusterPool(c)
	s.Require().NoError(err)

	selectedShardHostNames := make([]map[string]struct{}, len(s.shardsReplicaHostNames))
	for shard := 0; shard < len(s.shardsReplicaHostNames); shard++ {
		selectedShardHostNames[shard] = map[string]struct{}{}
		for i := 0; i < 20; i++ {
			conn, err := cp.GetShardConn(ctx, shard)

			s.Require().NoError(err)
			s.Require().NoError(conn.Ping(ctx))

			var hostName string
			row := conn.QueryRow(ctx, "SELECT hostName()")
			row.Scan(&hostName)

			selectedShardHostNames[shard][hostName] = struct{}{}
		}
	}

	for shard := 0; shard < len(s.shardsReplicaHostNames); shard++ {
		s.Require().ElementsMatch(
			s.shardsReplicaHostNames[shard],
			slices.Collect(maps.Keys(selectedShardHostNames[shard])),
		)
	}
}

func (s *ClusterPoolSuite) TestGetShardConn_DisabledReplicas() {
	ctx := context.Background()

	c := ClusterConfig{
		ConnConfig: ConnConfig{
			Database: "default",
			Username: "default",
			Password: "",
		},
		ShardsReplicaHosts: s.shardsReplicaHosts,
	}

	cp, err := NewClusterPool(c)
	s.Require().NoError(err)

	selectedShardHostNames := make([]map[string]struct{}, len(s.shardsReplicaHostNames))
	for shard := 0; shard < len(s.shardsReplicaHostNames); shard++ {
		selectedShardHostNames[shard] = map[string]struct{}{}
		for i := 0; i < 20; i++ {
			conn, err := cp.GetShardConn(ctx, shard)

			s.Require().NoError(err)
			s.Require().NoError(conn.Ping(ctx))

			var hostName string
			row := conn.QueryRow(ctx, "SELECT hostName()")
			row.Scan(&hostName)

			selectedShardHostNames[shard][hostName] = struct{}{}
		}
	}

	for shard := 0; shard < len(s.shardsReplicaHostNames); shard++ {
		s.Require().ElementsMatch(
			s.shardsReplicaHostNames[shard],
			slices.Collect(maps.Keys(selectedShardHostNames[shard])),
		)
	}

	// Disable one shard replica
	cmd := exec.CommandContext(ctx, "make", "drop_replica")
	msg, err := cmd.CombinedOutput()
	s.Require().NoError(err, string(msg))

	selectedShardHostNames = make([]map[string]struct{}, len(s.shardsReplicaHostNames))
	for shard := 0; shard < 1; shard++ {
		selectedShardHostNames[shard] = map[string]struct{}{}
		for i := 0; i < 20; i++ {
			conn, err := cp.GetShardConn(ctx, shard)

			s.Require().NoError(err)
			s.Require().NoError(conn.Ping(ctx))

			var hostName string
			row := conn.QueryRow(ctx, "SELECT hostName()")
			row.Scan(&hostName)

			selectedShardHostNames[shard][hostName] = struct{}{}
		}
	}

	incompleteShardsReplicaHostNames := slices.Clone(s.shardsReplicaHostNames[0])
	incompleteShardsReplicaHostNames = slices.DeleteFunc(incompleteShardsReplicaHostNames, func(s string) bool {
		if s == "clickhouse-01-01" {
			return true
		}
		return false
	})

	s.Require().ElementsMatch(
		incompleteShardsReplicaHostNames,
		slices.Collect(maps.Keys(selectedShardHostNames[0])),
	)
}

func (s *ClusterPoolSuite) TestGetShardMultipleConn() {
	ctx := context.Background()

	c := ClusterConfig{
		ConnConfig: ConnConfig{
			Database: "default",
			Username: "default",
			Password: "",
		},
		ShardsReplicaHosts: s.shardsReplicaHosts,
	}

	cp, err := NewClusterPool(c)
	s.Require().NoError(err)

	selectedShardHostNames := make([]map[string]struct{}, len(s.shardsReplicaHostNames))
	for shard := 0; shard < len(s.shardsReplicaHostNames); shard++ {
		selectedShardHostNames[shard] = map[string]struct{}{}
		for i := 0; i < 20; i++ {
			conns, err := cp.GetMultipleShardConn(ctx, shard, 2)
			s.Require().NoError(err)

			multipleConnHostNames := make([]string, 0, len(conns))
			for _, conn := range conns {
				s.Require().NoError(conn.Ping(ctx))
				var hostName string
				row := conn.QueryRow(ctx, "SELECT hostName()")
				row.Scan(&hostName)
				selectedShardHostNames[shard][hostName] = struct{}{}

				s.Require().NotContains(multipleConnHostNames, hostName)
				multipleConnHostNames = append(multipleConnHostNames, hostName)
			}
		}
	}

	for shard := 0; shard < len(s.shardsReplicaHostNames); shard++ {
		s.Require().ElementsMatch(
			s.shardsReplicaHostNames[shard],
			slices.Collect(maps.Keys(selectedShardHostNames[shard])),
		)
	}
}

func (s *ClusterPoolSuite) TestGetShardMultipleConn_DisableReplicas() {
	ctx := context.Background()

	c := ClusterConfig{
		ConnConfig: ConnConfig{
			Database: "default",
			Username: "default",
			Password: "",
		},
		ShardsReplicaHosts: s.shardsReplicaHosts,
	}

	cp, err := NewClusterPool(c)
	s.Require().NoError(err)

	selectedShardHostNames := make([]map[string]struct{}, len(s.shardsReplicaHostNames))
	for shard := 0; shard < len(s.shardsReplicaHostNames); shard++ {
		selectedShardHostNames[shard] = map[string]struct{}{}
		for i := 0; i < 20; i++ {
			conns, err := cp.GetMultipleShardConn(ctx, shard, 2)
			s.Require().NoError(err)

			multipleConnHostNames := make([]string, 0, len(conns))
			for _, conn := range conns {
				s.Require().NoError(conn.Ping(ctx))
				var hostName string
				row := conn.QueryRow(ctx, "SELECT hostName()")
				row.Scan(&hostName)
				selectedShardHostNames[shard][hostName] = struct{}{}

				s.Require().NotContains(multipleConnHostNames, hostName)
				multipleConnHostNames = append(multipleConnHostNames, hostName)
			}
		}
	}

	for shard := 0; shard < len(s.shardsReplicaHostNames); shard++ {
		s.Require().ElementsMatch(
			s.shardsReplicaHostNames[shard],
			slices.Collect(maps.Keys(selectedShardHostNames[shard])),
		)
	}

	// Disable one shard replica
	cmd := exec.CommandContext(ctx, "make", "drop_replica")
	msg, err := cmd.CombinedOutput()
	s.Require().NoError(err, string(msg))

	selectedShardHostNames = make([]map[string]struct{}, len(s.shardsReplicaHostNames))
	for shard := 0; shard < 1; shard++ {
		selectedShardHostNames[shard] = map[string]struct{}{}
		for i := 0; i < 20; i++ {
			conns, err := cp.GetMultipleShardConn(ctx, shard, 2)

			for _, conn := range conns {
				s.Require().NoError(err)
				s.Require().NoError(conn.Ping(ctx))

				var hostName string
				row := conn.QueryRow(ctx, "SELECT hostName()")
				row.Scan(&hostName)

				selectedShardHostNames[shard][hostName] = struct{}{}
			}
		}
	}

	incompleteShardsReplicaHostNames := slices.Clone(s.shardsReplicaHostNames[0])
	incompleteShardsReplicaHostNames = slices.DeleteFunc(incompleteShardsReplicaHostNames, func(s string) bool {
		if s == "clickhouse-01-01" {
			return true
		}
		return false
	})

	s.Require().ElementsMatch(
		incompleteShardsReplicaHostNames,
		slices.Collect(maps.Keys(selectedShardHostNames[0])),
	)
}
