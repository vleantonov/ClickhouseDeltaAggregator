#!/usr/bin/env bash
# Create the `accounting` Replicated database and apply the schema, mirroring
# `make init_db` but driven through `docker exec` so the runner needs no host
# clickhouse-client install. Run after wait-clickhouse.sh.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
NODE=clickhouse-01-01
DB=accounting

# CREATE DATABASE ... ON CLUSTER can briefly race replica discovery on a cold
# start, so retry a few times.
for attempt in $(seq 1 10); do
  if docker exec "${NODE}" clickhouse-client -q \
    "CREATE DATABASE IF NOT EXISTS ${DB} ON CLUSTER '{cluster}' ENGINE = Replicated('/databases/${DB}', '{shard}', '{replica}')"; then
    break
  fi
  echo "create database attempt ${attempt} failed, retrying..."
  sleep 3
done

echo "applying clean.sql"
docker exec -i "${NODE}" clickhouse-client -d "${DB}" --multiquery < "${REPO_ROOT}/migrations/clean.sql"

echo "applying init.sql"
docker exec -i "${NODE}" clickhouse-client -d "${DB}" --multiquery < "${REPO_ROOT}/migrations/init.sql"

echo "schema applied"
docker exec "${NODE}" clickhouse-client -d "${DB}" -q "SHOW TABLES"
