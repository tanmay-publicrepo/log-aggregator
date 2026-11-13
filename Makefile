PY ?= python3 
LOG_DIR ?= /tmp/logs
Rate ?= 1000
COMPOSE ?= docker compose -f infra/compose.yaml
CONNECT_SH ?= infra/scripts/connect.sh
PG_CONNECTOR = postgres-sink-logs-parsed
PG_CONFIG    = infra/connectors/postgres-sink.json

.PHONY: gen gen-load gen-test build test up down logs connect-plugins os-template connector-up connector-status connector-down os-count

gen:
	$(PY) generator/generate.py --services 4 --rate 5 --malformed-pct 2 --log-dir $(LOG_DIR)

gen-load:
	$(PY) generator/generate.py --services 8 --rate $(RATE) --malformed-pct 1 --burst --log-dir $(LOG_DIR)

gen-test:
	$(PY) -m pytest generator -q

build:
	./gradlew build

test:
	./gradlew test

up:
	$(COMPOSE) up -d --build

down:
	$(COMPOSE) down

logs:
	$(COMPOSE) logs -f $(S)

connect-plugins:
	$(CONNECT_SH) plugin

os-template:
	$(CONNECT_SH) template

connector-up:
	$(CONNECT_SH) up

connector-status:
	$(CONNECT_SH) status

connector-down:
	$(CONNECT_SH) down

os-count:
	curl -s localhost:9200/logs.raw/_count | jq .count

verify:
	infra/scripts/verify.sh

clean:
	$(COMPOSE) down -v

pg-plugin:
	./gradlew :postgres-sink-connector:installPlugin
	$(COMPOSE) restart connect

pg-connector-up:
	CONNECTOR_NAME=$(PG_CONNECTOR) CONFIG_FILE=$(PG_CONFIG) $(CONNECT_SH) register

pg-connector-status:
	CONNECTOR_NAME=$(PG_CONNECTOR) $(CONNECT_SH) status

pg-connector-down:
	CONNECTOR_NAME=$(PG_CONNECTOR) $(CONNECT_SH) down