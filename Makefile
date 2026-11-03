PY ?= python3 
LOG_DIR ?= /tmp/logs
Rate ?= 1000
COMPOSE ?= docker compose -f infra/compose.yaml

.PHONY: gen gen-load gen-test build test up down logs

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
	$(COMPOSE) up -d

down:
	$(COMPOSE) down

logs:
	$(COMPOSE) logs -f $(S)