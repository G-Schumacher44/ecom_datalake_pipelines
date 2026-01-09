# Ecom Datalake Pipelines

Modern lakehouse pipeline showcasing Bronze to Rich Silver to BigQuery Gold marts.

## Quickstart

```bash
conda env create -f environment.yml
conda activate ecom-datalake-pipelines
pre-commit install
```

## Repo Layout

- `src/` - Polars transforms, validation schemas, audit helpers
- `dbt_duckdb/` - Base Silver dbt project (DuckDB)
- `dbt_bigquery/` - Enriched Silver + Gold dbt project (BigQuery)
- `airflow/` - Local Airflow DAGs and config
- `config/` - Pipeline config (YAML)
- `scripts/` - Utilities for pulling samples and bootstrapping Airflow
- `docs/planning/` - Planning docs and schema samples

## Configuration

- `.env` for secrets and local overrides (see `.env.example`).
- `config/config.yml` for pipeline settings.

## Observability

- Bronze manifests validate ingestion completeness.
- Silver and Enriched Silver emit audit JSON per table + partition.
- Audit records can be loaded into BigQuery for SLA dashboards and alerting.
- Canonical schema: `docs/planning/planning/AUDIT_SCHEMA.md`.

## Airflow (Docker)

```bash
make airflow-init
```

## Tests

```bash
make test
```
