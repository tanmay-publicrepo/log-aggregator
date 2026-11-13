#!/usr/bin/env bash
# Manage the OpenSearch sink: plugin install, index template, connector lifecycle.
set -euo pipefail

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
OPENSEARCH_URL="${OPENSEARCH_URL:-http://localhost:9200}"
CONNECTOR_NAME="${CONNECTOR_NAME:-opensearch-sink-logs-raw}"
TEMPLATE_NAME="${TEMPLATE_NAME:-logs-raw-template}"
INDEX_NAME="${INDEX_NAME:-logs.raw}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PLUGIN_DIR="$SCRIPT_DIR/../plugins/opensearch-sink"
CONFIG_FILE="${CONFIG_FILE:-$SCRIPT_DIR/../connectors/opensearch-sink.json}"
TEMPLATE_FILE="$SCRIPT_DIR/../opensearch/logs-raw-template.json"

usage() {
  cat <<EOF
Usage: $(basename "$0") <command>
  plugin     download the OpenSearch sink plugin into $PLUGIN_DIR
  template   create or update the index template $TEMPLATE_NAME
  up         wait for services, apply template, register connector, wait for RUNNING
  status     show connector and task state
  down       delete the connector (the index is kept)
EOF
}

wait_for_http() {
  local url="$1" name="$2" attempts="${3:-30}"
  for ((i = 1; i <= attempts; i++)); do
    if curl -fsS "$url" >/dev/null 2>&1; then
      echo "$name is up"
      return 0
    fi
    echo "waiting for $name ($i/$attempts)"
    sleep 2
  done
  echo "$name did not answer at $url" >&2
  return 1
}

install_opensearch_plugin() {
  mkdir -p "$PLUGIN_DIR"
  if find "$PLUGIN_DIR" -name '*.jar' | grep -q .; then
    echo "plugin already present in $PLUGIN_DIR"
    return 0
  fi
  local url
  url=$(curl -fsSL https://api.github.com/repos/Aiven-Open/opensearch-connector-for-apache-kafka/releases/latest \
        | jq -r '.assets[] | select(.name | endswith(".zip")) | .browser_download_url' | head -n 1)
  if [ -z "$url" ]; then
    echo "could not find a .zip asset in the latest release" >&2
    return 1
  fi
  echo "downloading $url"
  local tmp
  tmp=$(mktemp -d)
  curl -fsSL "$url" -o "$tmp/plugin.zip"
  unzip -q "$tmp/plugin.zip" -d "$PLUGIN_DIR"
  rm -rf "$tmp"
  echo "unpacked into $PLUGIN_DIR; run 'docker compose restart connect' to load it"
}

apply_template() {
  curl -fsS -X PUT "$OPENSEARCH_URL/_index_template/$TEMPLATE_NAME" \
    -H "Content-Type: application/json" \
    --data-binary "@$TEMPLATE_FILE"
  echo
  echo "template $TEMPLATE_NAME applied"
}

register_connector() {
  curl -fsS -X PUT "$CONNECT_URL/connectors/$CONNECTOR_NAME/config" \
    -H "Content-Type: application/json" \
    --data-binary "@$CONFIG_FILE" | jq .
}

connector_status() {
  curl -fsS "$CONNECT_URL/connectors/$CONNECTOR_NAME/status" | jq .
}

wait_for_running() {
  local attempts=30 state
  for ((i = 1; i <= attempts; i++)); do
    state=$(curl -fsS "$CONNECT_URL/connectors/$CONNECTOR_NAME/status" 2>/dev/null \
      | jq -r 'if (.tasks | length) == 0 then "NO_TASKS"
               else ([.connector.state] + [.tasks[].state] | unique | join(",")) end' 2>/dev/null \
      || echo "UNKNOWN")
    case "$state" in
      RUNNING)
        echo "connector and tasks RUNNING"
        return 0 ;;
      *FAILED*)
        echo "connector or a task FAILED:" >&2
        connector_status >&2
        return 1 ;;
      *)
        echo "state: $state ($i/$attempts)"
        sleep 2 ;;
    esac
  done
  echo "connector did not reach RUNNING" >&2
  return 1
}

delete_connector() {
  curl -fsS -X DELETE "$CONNECT_URL/connectors/$CONNECTOR_NAME"
  echo "connector $CONNECTOR_NAME deleted"
}

main() {
  case "${1:-}" in
    plugin)   install_opensearch_plugin ;;
    template) wait_for_http "$OPENSEARCH_URL" "OpenSearch"; apply_template ;;
    up)
      wait_for_http "$CONNECT_URL/connectors" "Kafka Connect"
      wait_for_http "$OPENSEARCH_URL" "OpenSearch"
      apply_template
      register_connector
      wait_for_running
      ;;
       register)
      wait_for_http "$CONNECT_URL/connectors" "Kafka Connect"
      register_connector
      wait_for_running
      ;;
    status)   connector_status ;;
    down)     delete_connector ;;
    *)        usage; exit 2 ;;
  esac
}

main "$@"