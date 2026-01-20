# =============================================================================
# Makefile - Common commands for local development and deployment
# =============================================================================

.PHONY: help build up down restart logs shell log-task test lint format type-check clean clean-data
.PHONY: dbt-deps dbt-build dbt-test local-silver push-image
.PHONY: strict-mode easy-mode run-sample run-sample-strict run-sample-bq backfill-easy backfill-strict run-dims backfill-dims
.PHONY: run-dev-gcs dev-mode run-dev-docker

RUN_ID_OPT = $(if $(RUN_ID),--run-id $(RUN_ID),)

# Default target
help:
	@echo "======================================================================"
	@echo "   E-commerce Data Pipeline - CLI & Make Commands"
	@echo "======================================================================"
	@echo ""
	@echo "Environment Control:"
	@echo "  make up              Start local Airflow (Webserver: http://localhost:8080)"
	@echo "  make down            Stop Airflow services"
	@echo "  make restart         Restart Scheduler & Webserver"
	@echo "  make logs            Tail Scheduler logs"
	@echo "  make log-task        Tail a specific task log"
	@echo "      Required: DAG=<dag_id> RUN_ID=<run_id> TASK=<task_id>"
	@echo "      Optional: TRY=<n> (default: 1) LINES=<n> (default: 200)"
	@echo "  make shell           Open Bash in Scheduler container"
	@echo "  make clean           Destroy containers, images, volumes AND local data"
	@echo "  make clean-data      Wipe local 'data/silver' and 'data/metrics' only"
	@echo "  make strict-mode     Restart Airflow in PROD mode (Hard Validation Fails)"
	@echo "  make easy-mode       Restart Airflow in LOCAL mode (Soft Validation Fails)"
	@echo ""
	@echo "Pipeline Execution:"
	@echo "  make run-sample        Trigger Main Pipeline (Soft Mode)"
	@echo "      Required: DATE=YYYY-MM-DD"
	@echo "      Optional: RUN_ID=<custom_id>"
	@echo ""
	@echo "  make run-sample-strict Trigger Main Pipeline (Strict Mode)"
	@echo "      Required: DATE=YYYY-MM-DD"
	@echo ""
	@echo "  make run-sample-bq     Trigger Main Pipeline + BigQuery Load (Strict Mode)"
	@echo "      Required: DATE=YYYY-MM-DD"
	@echo "      Env Vars: GOOGLE_CLOUD_PROJECT, GCS_BUCKET must be set"
	@echo ""
	@echo "  make run-dims          Trigger Dimension Refresh Pipeline"
	@echo "      Required: DATE=YYYY-MM-DD"
	@echo ""
	@echo "  make backfill-easy     Backfill Main Pipeline (Soft Mode)"
	@echo "      Required: START=YYYY-MM-DD END=YYYY-MM-DD"
	@echo ""
	@echo "  make backfill-strict   Backfill Main Pipeline (Strict Mode)"
	@echo "      Required: START=YYYY-MM-DD END=YYYY-MM-DD"
	@echo ""
	@echo "  make run-dev-gcs       Run Pipeline with GCS (Native, No Docker)"
	@echo "      Required: DATE=YYYY-MM-DD"
	@echo ""
	@echo "  make run-dev-docker    Run Pipeline with GCS (Docker + Airflow)"
	@echo "      Required: DATE=YYYY-MM-DD"
	@echo "      Uses: gs://acme-analytics-raw (Bronze)"
	@echo "            gs://acme-analytics-silver (Silver)"
	@echo ""
	@echo "Development & Testing:"
	@echo "  make test            Run Unit Tests (pytest)"
	@echo "  make lint            Run Linter (ruff)"
	@echo "  make format          Auto-format Code (black + isort)"
	@echo "  make type-check      Run Type Checker (mypy)"
	@echo "  make local-silver    Run dbt + Validation locally (No Docker)"
	@echo "  make local-enriched  Run enriched transforms locally (optional: DATE=YYYY-MM-DD)"
	@echo "  make local-dims      Run customer + product catalog dims locally"
	@echo ""
	@echo "dbt Utilities:"
	@echo "  make dbt-deps        Install dbt packages"
	@echo "  make dbt-build       Build all Base Silver models"
	@echo "  make dbt-test        Run dbt data tests"
	@echo ""
	@echo "Deployment:"
	@echo "  make push-image      Build & Push Docker Image"
	@echo "      Required: PROJECT_ID=<gcp_project_id>"
	@echo ""


# ==============================================================================
# Docker Commands
# ==============================================================================

build:
	@echo "Building Docker image (building scheduler service only, others will reuse)..."
	docker-compose build airflow-scheduler

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

log-task:
ifndef DAG
	@echo "ERROR: DAG not set. Usage: make log-task DAG=<dag_id> RUN_ID=<run_id> TASK=<task_id> [TRY=N] [LINES=N]"
	@exit 1
endif
ifndef RUN_ID
	@echo "ERROR: RUN_ID not set. Usage: make log-task DAG=<dag_id> RUN_ID=<run_id> TASK=<task_id> [TRY=N] [LINES=N]"
	@exit 1
endif
ifndef TASK
	@echo "ERROR: TASK not set. Usage: make log-task DAG=<dag_id> RUN_ID=<run_id> TASK=<task_id> [TRY=N] [LINES=N]"
	@exit 1
endif
	@LINES=$${LINES:-200}; TRY=$${TRY:-1}; \
	docker-compose exec airflow-scheduler tail -n $$LINES \
	"/opt/airflow/logs/dag_id=$${DAG}/run_id=$${RUN_ID}/task_id=$${TASK}/attempt=$${TRY}.log"

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

# Run pipeline in dev mode with GCS buckets (No Docker, No BQ)
run-dev-gcs:
ifndef DATE
	@echo "ERROR: DATE not set. Usage: make run-dev-gcs DATE=2025-10-04"
	@exit 1
endif
	@echo "=========================================="
	@echo "Running Dev Pipeline with GCS (Native)"
	@echo "Date: $(DATE)"
	@echo "Bronze: gs://acme-analytics-raw/data/bronze"
	@echo "Silver: gs://acme-analytics-silver/data/silver"
	@echo "=========================================="
	@./scripts/run_dev_pipeline.sh $(DATE)

# Start Airflow in dev mode (GCS buckets, no BQ load)
dev-mode:
	@echo "Starting Airflow in DEV mode..."
	@echo "  - Environment: dev"
	@echo "  - Bronze: gs://acme-analytics-raw"
	@echo "  - Silver: gs://acme-analytics-silver"
	@echo "  - BigQuery: DISABLED"
	@echo "  - Gold: DISABLED"
	PIPELINE_ENV=dev \
	BQ_LOAD_ENABLED=false \
	GOLD_PIPELINE_ENABLED=false \
	docker-compose up -d --force-recreate airflow-scheduler airflow-webserver
	@echo ""
	@echo "Airflow UI: http://localhost:8080"
	@echo "Username: airflow | Password: airflow"

# Run pipeline in Docker with GCS buckets
run-dev-docker: dev-mode
ifndef DATE
	@echo "ERROR: DATE not set. Usage: make run-dev-docker DATE=2025-10-04"
	@exit 1
endif
	@echo "=========================================="
	@echo "Triggering DAG in Airflow (Docker)"
	@echo "Date: $(DATE)"
	@echo "=========================================="
	@sleep 5  # Give Airflow time to start
	docker-compose exec airflow-scheduler \
		airflow dags trigger ecom_silver_to_gold_pipeline --exec-date $(DATE)
	@echo ""
	@echo "DAG triggered! Monitor at http://localhost:8080"

# ==============================================================================
# Testing & Code Quality
# ==============================================================================

test:
	pytest tests/unit/ -v

lint:
	ruff check src/ tests/ airflow/
	yamllint .

format:
	black src/ tests/ airflow/
	isort src/ tests/ airflow/

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
		python -m src.runners.base_silver --select "base_silver.*"
	python -m src.validation.silver \
		--bronze-path samples/bronze \
		--silver-path data/silver/base \
		--quarantine-path data/silver/base/quarantine \
		--output-report docs/validation_reports/SILVER_QUALITY_FULL.md

local-silver-strict:
	PIPELINE_ENV=local \
		BRONZE_BASE_PATH="$(PWD)/samples/bronze" \
		SILVER_BASE_PATH="$(PWD)/data/silver/base" \
		python -m src.runners.base_silver --select "base_silver.*"
	python -m src.validation.silver \
		--bronze-path samples/bronze \
		--silver-path data/silver/base \
		--quarantine-path data/silver/base/quarantine \
		--output-report docs/validation_reports/SILVER_QUALITY_FULL.md \
		--enforce-quality

local-enriched:
	PIPELINE_ENV=local \
		BRONZE_BASE_PATH="$(PWD)/samples/bronze" \
		SILVER_BASE_PATH="$(PWD)/data/silver/base" \
		SILVER_ENRICHED_PATH="$(PWD)/data/silver/enriched" \
		python scripts/run_enriched_all_samples.py \
		--base-path "$(PWD)/data/silver/base" \
		--output-path "$(PWD)/data/silver/enriched" \
		--per-date \
		$(if $(DATE),--ingest-dt $(DATE),)

local-enriched-strict:
	PIPELINE_ENV=local \
		BRONZE_BASE_PATH="$(PWD)/samples/bronze" \
		SILVER_BASE_PATH="$(PWD)/data/silver/base" \
		SILVER_ENRICHED_PATH="$(PWD)/data/silver/enriched" \
		python scripts/run_enriched_all_samples.py \
		--base-path "$(PWD)/data/silver/base" \
		--output-path "$(PWD)/data/silver/enriched" \
		--per-date \
		--enforce-quality \
		$(if $(DATE),--ingest-dt $(DATE),)

local-dims:
	PIPELINE_ENV=local \
		BRONZE_BASE_PATH="$(PWD)/samples/bronze" \
		SILVER_BASE_PATH="$(PWD)/data/silver/base" \
		python -m src.runners.base_silver \
		--select stg_ecommerce__customers \
		stg_ecommerce__customers_quarantine \
		stg_ecommerce__product_catalog \
		stg_ecommerce__product_catalog_quarantine
	python -m src.validation.silver \
		--bronze-path samples/bronze \
		--silver-path data/silver/base \
		--quarantine-path data/silver/base/quarantine \
		--output-report docs/validation_reports/SILVER_QUALITY.md

local-dims-strict:
	PIPELINE_ENV=local \
		BRONZE_BASE_PATH="$(PWD)/samples/bronze" \
		SILVER_BASE_PATH="$(PWD)/data/silver/base" \
		python -m src.runners.base_silver \
		--select stg_ecommerce__customers \
		stg_ecommerce__customers_quarantine \
		stg_ecommerce__product_catalog \
		stg_ecommerce__product_catalog_quarantine
	python -m src.validation.silver \
		--bronze-path samples/bronze \
		--silver-path data/silver/base \
		--quarantine-path data/silver/base/quarantine \
		--output-report docs/validation_reports/SILVER_QUALITY.md \
		--enforce-quality

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
