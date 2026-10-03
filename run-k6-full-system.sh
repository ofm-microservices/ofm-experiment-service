#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SCRIPT="$ROOT_DIR/k6/full-system.js"
export K6_BASE_URL="${K6_BASE_URL:-http://api.ofm.local/api/v2}"
export K6_PAYMENT_URL="${K6_PAYMENT_URL:-http://payment.ofm.local/v1}"
export OFM_FULL_K6_VUS="${OFM_FULL_K6_VUS:-1}"
export OFM_FULL_K6_ITERATIONS="${OFM_FULL_K6_ITERATIONS:-1}"
export OFM_FULL_K6_MAX_DURATION="${OFM_FULL_K6_MAX_DURATION:-5m}"
export OFM_FULL_K6_RATE="${OFM_FULL_K6_RATE:-0}"
export OFM_FULL_K6_DURATION="${OFM_FULL_K6_DURATION:-5m}"
export K6_CREATE_USERS="${K6_CREATE_USERS:-false}"
export K6_TEST_RUN_ID="${K6_TEST_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)-${RANDOM}}"
export K6_SCENARIO="${K6_SCENARIO:-full-system-${K6_TEST_RUN_ID}}"
export OFM_TEST_ENVIRONMENT="${OFM_TEST_ENVIRONMENT:-local}"
export OFM_TEST_PROFILE="${OFM_TEST_PROFILE:-smoke}"
export OFM_TEST_ARCHITECTURE="${OFM_TEST_ARCHITECTURE:-microservice}"
export OFM_CLICKHOUSE_URL="${OFM_CLICKHOUSE_URL:-http://127.0.0.1:9097}"
export OFM_CLICKHOUSE_DATABASE="${OFM_CLICKHOUSE_DATABASE:-default}"
export OFM_CLICKHOUSE_USER="${OFM_CLICKHOUSE_USER:-admin}"
export OFM_CLICKHOUSE_PASSWORD="${OFM_CLICKHOUSE_PASSWORD:-admin}"
export OFM_VERIFY_PROJECTION="${OFM_VERIFY_PROJECTION:-true}"

make_jwt() {
  local subject="$1" username="$2" jwt_secret now header payload input signature
  jwt_secret="${JWT_ACCESS_SECRET:-aa96fae1a6eee39b879dad6b6bb372e63278257bf9f94010bc7d25693f61e38c}"
  now="$(date +%s)"
  header="$(printf '%s' '{"alg":"HS256","typ":"JWT"}' | openssl base64 -A | tr '+/' '-_' | tr -d '=')"
  payload="$(jq -nc --arg sub "$subject" --arg username "$username" --arg email "$username@example.com" --argjson iat "$now" --argjson exp "$((now+3600))" '{sub:$sub,username:$username,email:$email,iat:$iat,exp:$exp}')"
  payload="$(printf '%s' "$payload" | openssl base64 -A | tr '+/' '-_' | tr -d '=')"
  input="$header.$payload"
  signature="$(printf '%s' "$input" | openssl dgst -binary -sha256 -hmac "$jwt_secret" | openssl base64 -A | tr '+/' '-_' | tr -d '=')"
  printf '%s.%s' "$input" "$signature"
}

export K6_TOKEN="${K6_TOKEN:-$(make_jwt aeac656a-e617-45f2-b1ce-b922d8bd03fa alex1)}"
export K6_FREELANCER_TOKEN="${K6_FREELANCER_TOKEN:-$K6_TOKEN}"
export K6_BUYER_TOKEN="${K6_BUYER_TOKEN:-$(make_jwt 7c1d1af1-6be8-4e77-8e57-0b1f2d12e9aa buyer1)}"

RESULT_DIR="$(mktemp -d)"
SUMMARY_FILE="$RESULT_DIR/k6-summary.json"
STARTED_AT="$(date -u '+%Y-%m-%d %H:%M:%S.%3N')"
chmod 777 "$RESULT_DIR"
cleanup() { rm -rf "$RESULT_DIR"; }
trap cleanup EXIT

record_clickhouse() {
  local exit_status="$1" finished_at status row projection_output projection_status kafka_lag projection_pending projection_failures dlq_count recovery_time_ms
  finished_at="$(date -u '+%Y-%m-%d %H:%M:%S.%3N')"
  status="passed"
  [[ "$exit_status" == 0 ]] || status="failed"
  [[ -s "$SUMMARY_FILE" ]] || { echo "warning: k6 summary was not produced; skipping ClickHouse result" >&2; return 0; }
  kafka_lag=0
  projection_pending=0
  projection_failures=0
  dlq_count=0
  recovery_time_ms=0
  projection_status="not_checked"
  if [[ "$OFM_VERIFY_PROJECTION" == "true" ]] && [[ -x "$ROOT_DIR/verify-kafka-projection.sh" ]]; then
    projection_output="$(mktemp)"
    if "$ROOT_DIR/verify-kafka-projection.sh" >"$projection_output" 2>&1; then
      projection_status="passed"
    else
      projection_status="failed"
      [[ "$status" == "passed" ]] && status="degraded"
    fi
    kafka_lag="$(awk -F= '/^kafka_lag=/{print $2; exit}' "$projection_output" | tr -d '[:space:]')"
    projection_pending="$(awk -F= '/^projection_pending=/{print $2; exit}' "$projection_output" | tr -d '[:space:]')"
    projection_failures="$(awk -F= '/^projection_failures=/{print $2; exit}' "$projection_output" | tr -d '[:space:]')"
    rm -f "$projection_output"
    kafka_lag="${kafka_lag:-0}"
    projection_pending="${projection_pending:-0}"
    projection_failures="${projection_failures:-0}"
  fi
  row="$(jq -cn \
    --arg started "$STARTED_AT" --arg finished "$finished_at" --arg run "$K6_TEST_RUN_ID" \
    --arg scenario "$K6_SCENARIO" --arg environment "$OFM_TEST_ENVIRONMENT" \
    --arg profile "$OFM_TEST_PROFILE" --arg architecture "$OFM_TEST_ARCHITECTURE" \
    --arg status "$status" --arg projection_status "$projection_status" --slurpfile result "$SUMMARY_FILE" \
    --argjson vus "$OFM_FULL_K6_VUS" --argjson rate "$OFM_FULL_K6_RATE" \
    --argjson kafka_lag "$kafka_lag" --argjson projection_pending "$projection_pending" --argjson projection_failures "$projection_failures" --argjson dlq_count "$dlq_count" --argjson recovery_time_ms "$recovery_time_ms" \
    '{started_at:$started,finished_at:$finished,run_id:$run,scenario_id:$scenario,environment:$environment,profile:$profile,architecture:$architecture,vus:$vus,rate:$rate,duration_seconds:((($result[0].state.testRunDurationMs//0)/1000)|floor),status:$status,checks_total:(($result[0].metrics.checks.passes//0)+($result[0].metrics.checks.fails//0)),checks_failed:($result[0].metrics.checks.fails//0),http_requests:($result[0].metrics.http_reqs.count//0),http_failed:($result[0].metrics.http_req_failed.fails//0),p95_duration_ms:($result[0].metrics.http_req_duration["p(95)"]//0),kafka_lag:$kafka_lag,projection_pending:$projection_pending,projection_failures:$projection_failures,dlq_count:$dlq_count,recovery_time_ms:$recovery_time_ms,result_json:(($result[0]|. + {projection_status:$projection_status})|tojson)}')"
  curl -fsS -u "$OFM_CLICKHOUSE_USER:$OFM_CLICKHOUSE_PASSWORD" \
    "$OFM_CLICKHOUSE_URL/?database=$OFM_CLICKHOUSE_DATABASE&query=INSERT%20INTO%20ofm_load_test_runs%20FORMAT%20JSONEachRow" \
    -H 'Content-Type: application/json' --data-binary "$row" >/dev/null \
    || echo "warning: unable to persist load-test result in ClickHouse at $OFM_CLICKHOUSE_URL" >&2
}

set +e
if command -v k6 >/dev/null 2>&1; then
  k6 run --summary-export "$SUMMARY_FILE" "$SCRIPT"
  K6_EXIT=$?
else
  docker run --rm --network host \
    --add-host api.ofm.local:172.22.0.2 \
    --add-host payment.ofm.local:172.22.0.2 \
    -e K6_BASE_URL -e K6_PAYMENT_URL -e K6_TEST_RUN_ID -e K6_SCENARIO -e OFM_FULL_K6_VUS -e OFM_FULL_K6_ITERATIONS -e OFM_FULL_K6_MAX_DURATION -e OFM_FULL_K6_RATE -e OFM_FULL_K6_DURATION -e K6_CREATE_USERS -e K6_VERIFICATION_CODE -e K6_TEST_PASSWORD \
    -e K6_TOKEN -e K6_FREELANCER_TOKEN -e K6_BUYER_TOKEN -e K6_GIG_ID -e K6_PACKAGE_ID -e K6_ORDER_ID -e K6_PAYMENT_ID \
    -e FAULT_PROFILE -e FAULT_TARGET -e FAULT_RATE -e FAULT_DELAY -e FAULT_MAX_FAILURES -e FAULT_TTL -e FAULT_TEST_TOKEN \
    -v "$ROOT_DIR:/work:ro" -v "$RESULT_DIR:/results" grafana/k6:latest run --summary-export /results/k6-summary.json /work/k6/full-system.js
  K6_EXIT=$?
fi
set -e
record_clickhouse "$K6_EXIT"
exit "$K6_EXIT"
