SHELL := /bin/bash
PY    ?= python3
START ?= 2026-10-04
DAYS  ?= 6
export PYTHONPATH := $(CURDIR)

.PHONY: help install test local-run dbt-build dbt-docs up down logs stream stream-down spark clean

help:            ## list targets
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-12s %s\n", $$1, $$2}'

install:         ## install local (no-Docker) dependencies
	$(PY) -m pip install -r requirements.txt

test:            ## Python unit tests
	$(PY) -m pytest -q

local-run:       ## no Docker: generate -> load DuckDB -> dbt build, one day at a time (START, DAYS)
	@for i in $$(seq 0 $$(($(DAYS)-1))); do \
	  d=$$($(PY) -c "import datetime as t;print(t.date.fromisoformat('$(START)')+t.timedelta($$i))"); echo "=== $$d ==="; \
	  $(PY) -m generator.generate_data --date $$d && \
	  STORAGE_BACKEND=local $(PY) -m ingestion.load_warehouse --date $$d && \
	  (cd dbt && dbt build --profiles-dir . --quiet) || exit 1; \
	done

dbt-build:       ## dbt build against the default (DuckDB) target
	cd dbt && dbt build --profiles-dir .

dbt-docs:        ## generate and serve dbt docs (lineage graph) on :8081
	cd dbt && dbt docs generate --profiles-dir . && dbt docs serve --profiles-dir . --port 8081

up:              ## Airflow + Postgres + MinIO in Docker (UI on :8080)
	AIRFLOW_UID=$$(id -u) docker compose up -d --build

down:            ## stop the stack
	docker compose --profile streaming down

logs:
	docker compose logs -f airflow

stream:          ## Kafka + producer + consumer (needs `make up` first)
	AIRFLOW_UID=$$(id -u) docker compose --profile streaming up -d kafka event-producer event-consumer

stream-down:
	docker compose stop event-producer event-consumer kafka

spark:           ## PySpark: build a 5M-row file, aggregate it, register result in DuckDB
	$(PY) spark/process_large_orders.py make-sample --rows 5000000
	$(PY) spark/process_large_orders.py process --load-duckdb

clean:           ## remove generated data, warehouse file and dbt artefacts
	rm -rf data warehouse/*.duckdb* dbt/target dbt/logs
