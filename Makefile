# =============================================================================
# Makefile - Common commands for local development and deployment
# =============================================================================

.PHONY: help build up down restart logs shell test lint format type-check clean
.PHONY: dbt-deps dbt-build dbt-test local-silver push-image

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
	@echo "  make clean       - Stop services and remove volumes/images"
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

clean:
	@echo "WARNING: This will remove all containers, volumes, and the custom image!"
	@read -p "Are you sure? [y/N] " -n 1 -r; \
	echo; \
	if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
		docker-compose down -v; \
		docker rmi ecom-datalake-pipeline:latest 2>/dev/null || true; \
		echo "Cleanup complete!"; \
	fi

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
	python -m src.validation.silver_quality \
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
