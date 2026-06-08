infra:
	docker-compose -f ./deployments/docker-compose.yaml up --build -d

toxic-connectors:
	toxiproxy-cli create -l toxiproxy:29000 -u 192.168.7.4:9000 click1
	toxiproxy-cli create -l toxiproxy:22135 -u 192.168.8.1:2135 ydb1

drop_infra:
	docker-compose -f ./deployments/docker-compose.yaml down

drop_replica:
	docker-compose -f ./deployments/docker-compose.yaml stop clickhouse-01-01
	
generator-run:
	go run generator/main.go

aggregator-run:
	go run ./aggregator

aggregator-logs:
	docker logs -f deployments-aggregator-1

clean_db:
	clickhouse client --port 9011 --host localhost --database accounting --queries-file migrations/clean.sql

create_db:
	clickhouse client --port 9011 --host localhost -q "CREATE DATABASE IF NOT EXISTS accounting ON CLUSTER '{cluster}' ENGINE = Replicated('/databases/accounting', '{shard}', '{replica}')"

init_db: create_db clean_db
	clickhouse client --port 9011 --host localhost --database accounting --queries-file migrations/init.sql

infra_with_db: infra init_db
	echo "Ready for testing"

connect_db:
	clickhouse client --port 9011 --host localhost --database accounting

# Run the full exactly-once acceptance suite against the live cluster.
# Requires `make infra_with_db` first. Scenarios are slow (random startup
# sleeps), hence the long timeout. Override TEST=Name to run one scenario.
acceptance-test:
	cd tests && go test -tags acceptance -v -timeout 90m -count=1 -run "$(or $(TEST),.*)" ./...

# Long-running soak/chaos test: continuously produces while periodically
# injecting every fault, then compares ClickHouse content against the recorded
# input record-by-record. Tune via SOAK_* env vars (see tests/README.md).
soak-test:
	cd tests && go test -tags soak -v -timeout 90m -count=1 -run TestSoak_ChaosExactlyOnce ./... -v
