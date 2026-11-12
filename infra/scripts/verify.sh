#!/usr/bin/env bash
# Six checks that the Week 1 pipeline is healthy. Runs every check and reports; exits 1 if any failed.
set -uo pipefail

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
OPENSEARCH_URL="${OPENSEARCH_URL:-http://localhost:9200}"
CONNECTOR_NAME="${CONNECTOR_NAME:-opensearch-sink-logs-raw}"
TEMPLATE_NAME="${TEMPLATE_NAME:-logs-raw-template}"
INDEX_NAME="${INDEX_NAME:-logs.raw}"
GROUP="connect-$CONNECTOR_NAME"
failures=0

pass() { echo "PASS  $1"; }
fail() { echo "FAIL  $1"; failures=$((failures + 1)); }

check_plugin_listed() {
  if curl -fsS "$CONNECT_URL/connector-plugins" 2>/dev/null \
      | jq -e '.[] | select(.class | test("opensearch"; "i"))' >/dev/null 2>&1; then
    pass "OpenSearch sink plugin is installed"
  else
    fail "OpenSearch sink plugin not found in /connector-plugins"
  fi
}

check_connector_running() {
  local states
  states=$(curl -fsS "$CONNECT_URL/connectors/$CONNECTOR_NAME/status" 2>/dev/null \
    | jq -r '[.connector.state] + [.tasks[].state] | unique | join(",")' 2>/dev/null)
  if [ "$states" = "RUNNING" ]; then
    pass "connector and tasks RUNNING"
  else
    fail "connector state: ${states:-unreachable}"
  fi
}

check_template_applied() {
  if curl -fsS "$OPENSEARCH_URL/_index_template/$TEMPLATE_NAME" >/dev/null 2>&1; then
    pass "index template $TEMPLATE_NAME present"
  else
    fail "index template $TEMPLATE_NAME missing"
  fi
}

check_mapping_from_template() {
  local type
  type=$(curl -fsS "$OPENSEARCH_URL/$INDEX_NAME/_mapping" 2>/dev/null \
    | jq -r ".[\"$INDEX_NAME\"].mappings.properties.level.type" 2>/dev/null)
  if [ "$type" = "keyword" ]; then
    pass "level is mapped as keyword"
  else
    fail "level mapped as ${type:-missing}; index absent, or created before the template"
  fi
}

check_index_count() {
  local count
  count=$(curl -fsS "$OPENSEARCH_URL/$INDEX_NAME/_count" 2>/dev/null | jq -r '.count' 2>/dev/null)
  if [ "${count:-0}" -gt 0 ] 2>/dev/null; then
    pass "$INDEX_NAME holds $count documents"
  else
    fail "$INDEX_NAME has no documents yet; is the generator and shipper running?"
  fi
}

check_consumer_lag() {
  if ! command -v docker >/dev/null 2>&1; then
    echo "SKIP  consumer lag (docker not available here)"
    return
  fi
  local lag
  lag=$(docker exec kafka kafka-consumer-groups --bootstrap-server kafka:29092 --describe --group "$GROUP" 2>/dev/null \
    | awk '$6 ~ /^[0-9]+$/ { sum += $6 } END { print sum + 0 }')
  if [ "$lag" -le 100 ]; then
    pass "consumer lag is $lag"
  else
    fail "consumer lag is $lag"
  fi
}

main() {
  check_plugin_listed
  check_connector_running
  check_template_applied
  check_mapping_from_template
  check_index_count
  check_consumer_lag
  echo
  if [ "$failures" -eq 0 ]; then
    echo "all checks passed"
  else
    echo "$failures check(s) failed"
    exit 1
  fi
}

main "$@"