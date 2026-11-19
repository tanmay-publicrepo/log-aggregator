PY ?= python3 
LOG_DIR ?= data/logs
Rate ?= 1000
COMPOSE ?= docker compose -f infra/compose.yaml
CONNECT_SH ?= infra/scripts/connect.sh
PG_CONNECTOR = postgres-sink-logs-parsed
PG_CONFIG    = infra/connectors/postgres-sink.json
S3_CONNECTOR = s3-sink-logs-parsed
S3_CONFIG    = infra/connectors/s3-sink.json
JMX_VERSION = 1.3.0
JMX_DIR     = infra/jmx
JMX_BASE    = https://github.com/prometheus/jmx_exporter/releases/download
JMX_RULES   = https://raw.githubusercontent.com/prometheus/jmx_exporter/$(JMX_VERSION)/examples

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

pg-shell:
	$(COMPOSE) exec postgres psql -U logs -d logs

pg-count:
	$(COMPOSE) exec postgres psql -U logs -d logs -c "SELECT count(*) AS rows, count(DISTINCT (kafka_partition, kafka_offset)) AS distinct_records FROM parsed_logs"

s3-connector-up:
	CONNECTOR_NAME=$(S3_CONNECTOR) CONFIG_FILE=$(S3_CONFIG) $(CONNECT_SH) register

s3-connector-status:
	CONNECTOR_NAME=$(S3_CONNECTOR) $(CONNECT_SH) status

s3-connector-down:
	CONNECTOR_NAME=$(S3_CONNECTOR) $(CONNECT_SH) down

s3-ls:
	$(COMPOSE) exec minio mc ls --recursive local/logs

s3-cat:
	$(COMPOSE) exec -T minio mc cat local/logs/$(KEY) > $(notdir $(KEY))

jmx-agent:
	mkdir -p $(JMX_DIR)
	curl -fsSL -o $(JMX_DIR)/jmx_prometheus_javaagent.jar $(JMX_BASE)/$(JMX_VERSION)/jmx_prometheus_javaagent-$(JMX_VERSION).jar
	curl -fsSL -o $(JMX_DIR)/kafka.yml $(JMX_RULES)/kafka-2_0_0.yml
	curl -fsSL -o $(JMX_DIR)/connect.yml $(JMX_RULES)/kafka-connect.yml