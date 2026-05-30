#!/usr/bin/env bash
# Wait until every ClickHouse node in the 2-shard x 3-replica cluster answers a
# trivial query. Distributed inserts + the Replicated database engine need all
# six nodes (and their Keeper quorum) up before the schema can be applied.
set -euo pipefail

NODES=(
  clickhouse-01-01 clickhouse-01-02 clickhouse-01-03
  clickhouse-02-01 clickhouse-02-02 clickhouse-02-03
)
DEADLINE=$(( $(date +%s) + ${WAIT_TIMEOUT:-180} ))

for node in "${NODES[@]}"; do
  echo "::group::waiting for ${node}"
  until docker exec "${node}" clickhouse-client -q "SELECT 1" >/dev/null 2>&1; do
    if (( $(date +%s) > DEADLINE )); then
      echo "timed out waiting for ${node}"
      docker logs --tail 50 "${node}" || true
      exit 1
    fi
    sleep 2
  done
  echo "${node} is up"
  echo "::endgroup::"
done

echo "all ClickHouse nodes are ready"
