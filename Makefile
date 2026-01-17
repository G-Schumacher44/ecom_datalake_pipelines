# =============================================================================
# Makefile - Common commands for local development and deployment
# =============================================================================

.PHONY: help build up down restart logs shell test lint format type-check clean clean-data
.PHONY: dbt-deps dbt-build dbt-test local-silver push-image
.PHONY: strict-mode easy-mode run-sample run-sample-strict run-sample-bq backfill-easy backfill-strict run-dims backfill-dims

RUN_ID_OPT = $(if $(RUN_ID),--run-id $(RUN_ID),)

# Default target
help:
	@echo "E-commerce Data Pipeline - Available Commands"
	@echo ""
	@echo "Local Development:"
	@echo "  make build       - Build custom Airflow Docker image"
	@echo "  make up          - Start Airflow services (webserver + scheduler)"
	@echo "  make down        - Stop Airflow services"
	@echo "  make restart     - Restart Airflow services"
	@echo "  make logs        - Tail Airflow scheduler logs"
	@echo "  make shell       - Open bash shell in scheduler container"
	@echo "  make clean       - Stop services and remove volumes/images/data"
	@echo "  make clean-data  - Remove contents of data/silver and data/enriched"
	@echo ""
	@echo "Testing & Quality:"
	@echo "  make test        - Run pytest unit tests"
	@echo "  make lint        - Run ruff linter"
	@echo "  make format      - Format code with black + isort"
	@echo "  make type-check  - Run mypy type checking"
	@echo ""
	@echo "dbt Commands:"
	@echo "  make dbt-deps    - Install dbt packages (dbt-utils, etc.)"
	@echo "  make dbt-test    - Run dbt tests on Base Silver models"
	@echo "  make dbt-build   - Build all Base Silver models"
	@echo "  make local-silver - Build Silver locally (no Docker)"
	@echo ""
	@echo "Deployment:"
	@echo "  make push-image  - Build and push to Artifact Registry (set PROJECT_ID)"
	@echo ""
	@echo "Pipeline Runs:"
	@echo "  make strict-mode - Restart Airflow in strict validation mode (prod-like)"
	@echo "  make easy-mode   - Restart Airflow in easy validation mode (local)"
	@echo "  make run-sample DATE=YYYY-MM-DD        - Trigger DAG for a single date"
	@echo "  make run-sample-strict DATE=YYYY-MM-DD - Strict run + single date trigger"
	@echo "  make run-sample-bq DATE=YYYY-MM-DD     - Enable BQ load + single date trigger"
	@echo "  make run-dims DATE=YYYY-MM-DD          - Trigger dimension refresh DAG"
	@echo "  make backfill-dims START=YYYY-MM-DD END=YYYY-MM-DD - Backfill dimension refresh DAG"
	@echo "  make backfill-easy START=YYYY-MM-DD END=YYYY-MM-DD   - Backfill in easy mode"
	@echo "  make backfill-strict START=YYYY-MM-DD END=YYYY-MM-DD - Backfill in strict mode"
	@echo ""
	@echo "Access Airflow UI: http://localhost:8080 (airflow/airflow)"

# ==============================================================================
# Docker Commands
# ==============================================================================

build:
	docker-compose build

up:
	@echo "Starting Airflow..."
	@echo "Webserver will be available at http://localhost:8080"
	@echo "Username: airflow | Password: airflow"
	docker-compose up airflow-init
	docker-compose up -d

down:
	docker-compose down

restart:
	docker-compose restart airflow-scheduler airflow-webserver

logs:
	docker-compose logs -f airflow-scheduler

shell:
	docker-compose exec airflow-scheduler bash

clean-data:
	@echo "Cleaning local data directories..."
	rm -rf data/silver/base/*
	rm -rf data/silver/enriched/*
	rm -rf data/metrics/*
	@echo "Local data cleared!"

clean:
	@echo "WARNING: This will remove all containers, volumes, custom image, AND local data!"
	@read -p "Are you sure? [y/N] " -n 1 -r; \
	echo; \
	if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
		docker-compose down -v; \
		docker rmi ecom-datalake-pipeline:latest 2>/dev/null || true; \
		$(MAKE) clean-data; \
		echo "Cleanup complete!"; \
	fi

# ==============================================================================
# Pipeline Runs
# ==============================================================================

strict-mode:
	PIPELINE_ENV=prod \
	OBSERVABILITY_ENV=local \
	BRONZE_QA_FAIL=true \
	BQ_LOAD_ENABLED=false \
	GOLD_PIPELINE_ENABLED=false \
	docker-compose up -d --force-recreate airflow-scheduler airflow-webserver

easy-mode:
	PIPELINE_ENV=local \
	BRONZE_QA_FAIL=false \
	BQ_LOAD_ENABLED=false \
	GOLD_PIPELINE_ENABLED=false \
	docker-compose up -d --force-recreate airflow-scheduler airflow-webserver

run-sample:
ifndef DATE
	@echo "ERROR: DATE not set. Usage: make run-sample DATE=YYYY-MM-DD"
	@exit 1
endif
	docker-compose exec airflow-scheduler \
		airflow dags trigger ecom_silver_to_gold_pipeline --exec-date $(DATE) $(RUN_ID_OPT)

run-sample-strict: strict-mode run-sample

run-sample-bq:
ifndef DATE
	@echo "ERROR: DATE not set. Usage: make run-sample-bq DATE=YYYY-MM-DD"
	@exit 1
endif
ifndef GOOGLE_CLOUD_PROJECT
	@echo "ERROR: GOOGLE_CLOUD_PROJECT not set"
	@exit 1
endif
ifndef GCS_BUCKET
	@echo "ERROR: GCS_BUCKET not set"
	@exit 1
endif
	PIPELINE_ENV=prod \
	OBSERVABILITY_ENV=local \
	BRONZE_QA_FAIL=true \
	BQ_LOAD_ENABLED=true \
	GOLD_PIPELINE_ENABLED=false \
	docker-compose up -d --force-recreate airflow-scheduler airflow-webserver
	docker-compose exec airflow-scheduler \
		airflow dags trigger ecom_silver_to_gold_pipeline --exec-date $(DATE)

run-dims:
ifndef DATE
	@echo "ERROR: DATE not set. Usage: make run-dims DATE=YYYY-MM-DD"
	@exit 1
endif
	docker-compose exec airflow-scheduler \
		airflow dags trigger ecom_dim_refresh_pipeline --exec-date $(DATE) $(RUN_ID_OPT)

backfill-dims:
ifndef START
	@echo "ERROR: START not set. Usage: make backfill-dims START=YYYY-MM-DD END=YYYY-MM-DD"
	@exit 1
endif
ifndef END
	@echo "ERROR: END not set. Usage: make backfill-dims START=YYYY-MM-DD END=YYYY-MM-DD"
	@exit 1
endif
	docker-compose exec airflow-scheduler \
		airflow dags backfill ecom_dim_refresh_pipeline -s $(START) -e $(END)

backfill-easy: easy-mode
ifndef START
	@echo "ERROR: START not set. Usage: make backfill-easy START=YYYY-MM-DD END=YYYY-MM-DD"
	@exit 1
endif
ifndef END
	@echo "ERROR: END not set. Usage: make backfill-easy START=YYYY-MM-DD END=YYYY-MM-DD"
	@exit 1
endif
	docker-compose exec airflow-scheduler \
		airflow dags backfill ecom_silver_to_gold_pipeline -s $(START) -e $(END)

backfill-strict: strict-mode
ifndef START
	@echo "ERROR: START not set. Usage: make backfill-strict START=YYYY-MM-DD END=YYYY-MM-DD"
	@exit 1
endif
ifndef END
	@echo "ERROR: END not set. Usage: make backfill-strict START=YYYY-MM-DD END=YYYY-MM-DD"
	@exit 1
endif
	docker-compose exec airflow-scheduler \
		airflow dags backfill ecom_silver_to_gold_pipeline -s $(START) -e $(END)

# ==============================================================================
# Testing & Code Quality
# ==============================================================================

test:
	pytest tests/unit/ -v

lint:
	ruff check src/ tests/

format:
	black src/ tests/
	isort src/ tests/

type-check:
	mypy src/

# ==============================================================================
# dbt Commands
# ==============================================================================

dbt-deps:
	cd dbt_duckdb && dbt deps --project-dir . --profiles-dir .

dbt-build:
	cd dbt_duckdb && dbt build --project-dir . --profiles-dir .

dbt-test:
	cd dbt_duckdb && dbt test --project-dir . --profiles-dir .

# Build Silver locally (no Docker, for quick testing)
local-silver:
	PIPELINE_ENV=local \
	BRONZE_BASE_PATH="$(PWD)/samples/bronze" \
	SILVER_BASE_PATH="$(PWD)/data/silver/base" \
	dbt build --project-dir dbt_duckdb --profiles-dir dbt_duckdb --select "base_silver.*"
	python -m src.validation.silver \
		--bronze-path samples/bronze \
		--silver-path data/silver/base \
		--quarantine-path data/silver/base/quarantine \
		--output-report docs/validation_reports/SILVER_QUALITY.md

# ==============================================================================
# Production Deployment (requires gcloud CLI + PROJECT_ID env var)
# ==============================================================================

push-image:
ifndef PROJECT_ID
	@echo "ERROR: PROJECT_ID not set. Usage: make push-image PROJECT_ID=my-project-123"
	@exit 1
endif
	@echo "Building and pushing image to Artifact Registry..."
	docker build -t ecom-datalake-pipeline:latest .
	docker tag ecom-datalake-pipeline:latest \
		us-central1-docker.pkg.dev/$(PROJECT_ID)/airflow-images/ecom-datalake-pipeline:latest
	gcloud auth configure-docker us-central1-docker.pkg.dev
	docker push us-central1-docker.pkg.dev/$(PROJECT_ID)/airflow-images/ecom-datalake-pipeline:latest
	@echo "Image pushed successfully!"
