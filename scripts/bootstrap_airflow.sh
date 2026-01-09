#!/usr/bin/env bash
set -euo pipefail

mkdir -p airflow/dags airflow/logs airflow/plugins

echo "Initializing Airflow services..."
docker compose up airflow-init

echo "Starting Airflow webserver and scheduler..."
docker compose up -d airflow-webserver airflow-scheduler
