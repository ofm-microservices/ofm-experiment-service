#!/usr/bin/env bash
set -euo pipefail

broker="${KAFKA_CONTAINER:-ofm-migration-kafka}"
db_container="${MONOLITH_PROJECTION_DB_CONTAINER:-ofm-user-service-yugabyte}"
db_host="${MONOLITH_PROJECTION_DB_HOST:-172.22.0.1}"
db_port="${MONOLITH_PROJECTION_DB_PORT:-5433}"
db_name="${MONOLITH_PROJECTION_DB_NAME:-ofm_monolith}"

groups="$(docker exec "$broker" /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --all-groups --describe 2>/dev/null)"

max_lag="$(awk '$6 ~ /^[0-9]+$/ && $6 > max {max=$6} END {print max+0}' <<<"$groups")"
echo "kafka_lag=$max_lag"

check_group_lag() {
  local pattern="$1"
  local violations
  violations="$(awk -v pattern="$pattern" '$1 ~ pattern && $6 ~ /^[0-9]+$/ && $6 > 0 {print $1 ":" $2 ":" $6}' <<<"$groups")"
  if [[ -n "$violations" ]]; then
    echo "kafka $pattern: FAIL"
    echo "$violations"
    return 1
  fi
  echo "kafka $pattern: PASS (lag=0)"
}

check_group_lag '^migration-bridge-all$'
check_group_lag '^monolith-projection-'

sql() {
  docker exec "$db_container" ysqlsh -h "$db_host" -p "$db_port" -U admin -d "$db_name" \
    -At -v ON_ERROR_STOP=1 -c "$1"
}

pending="$(sql 'SELECT count(*) FROM migration_pending_projections')"
failures="$(sql 'SELECT count(*) FROM migration_projection_failures')"
# The CDC bridge publishes the immutable outbox INSERT but does not mutate the
# source row. Pending/retrying is therefore diagnostic source state, not proof
# that Kafka delivery failed; Kafka consumer lag and projection state are the
# authoritative delivery gates below.
active_outbox="$(sql "SELECT count(*) FROM migration_fallback_outbox WHERE status IN ('pending','retrying')")"
duplicates="$(sql "SELECT count(*) - count(DISTINCT concat(entity_type, ':', uuid_id::text)) FROM migration_id_mapping")"

echo "projection_pending=$pending"
echo "projection_failures=$failures"
echo "fallback_outbox_active=$active_outbox"
echo "id_mapping_duplicates=$duplicates"

if [[ "$pending" != 0 || "$failures" != 0 || "$duplicates" != 0 ]]; then
  echo "focused Kafka/projection verification: FAIL" >&2
  exit 1
fi
echo "focused Kafka/projection verification: PASS"
