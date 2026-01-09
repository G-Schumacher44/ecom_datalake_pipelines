# Silver Transform Framework (Hybrid)

## Purpose
Define how Base Silver and Enriched Silver transformations are organized and executed.
This document reflects the hybrid dbt + Polars approach used in this repo.

## Design Goals
- Keep orchestration in Airflow with clear task boundaries.
- Use dbt-duckdb for Base Silver cleaning and conformance.
- Use reusable Polars modules for enrichment logic, wrapped by dbt Python models.
- Keep data quality checks visible and traceable.

## High-Level Flow
1) Validate bronze partition(s) and required schema.
2) Base Silver (dbt-duckdb) cleans and standardizes bronze tables.
3) Enriched Silver (dbt-bigquery Python models) calls `src/transforms`.
4) Load enriched parquet to BigQuery `silver` dataset.
5) Build Gold marts (dbt-bigquery SQL models).
6) Emit audit metrics per table and partition.

## Module Layout
```
src/
  transforms/
    cart_attribution.py
    inventory_risk.py
    churn_detection.py
    sales_velocity.py
    regional_financials.py
  validation/
    schemas.py
  observability/
    audit.py

dbt_duckdb/
  models/base_silver/
    stg_ecommerce__*.sql

dbt_bigquery/
  models/enriched_silver/
    int_*.py
  models/gold_marts/
    fct_*.sql
```

## Base Silver (dbt-duckdb)
- Bronze tables are referenced as external Parquet sources.
- Base Silver models apply schema casting, deduplication, and integrity rules.
- Output tables remain 1:1 with bronze entities.

## Enriched Silver (dbt-bigquery + Polars)
- dbt Python models read Base Silver tables.
- Enrichment logic lives in `src/transforms` and returns Polars DataFrames.
- Output tables are business-aligned (attribution, risk, retention, velocity).

## Validation and Quality
- Pydantic schemas in `src/validation/schemas.py` validate key fields.
- SLA and quality thresholds live in `docs/planning/planning/SLA_AND_QUALITY.md`.
- Audit logs written via `src/observability/audit.py`.

## Outputs
- Base Silver parquet: `gs://<silver-bucket>/silver/base/<table>/...`
- Enriched Silver parquet: `gs://<silver-bucket>/silver/enriched/<table>/...`
- Audit logs: `gs://<silver-bucket>/silver/_audit/run_id=.../summary.json`

## Notes
- Keep table list aligned with `docs/planning/planning/DATA_CONTRACT.md`.
- Keep quality checks aligned with `docs/planning/planning/SLA_AND_QUALITY.md`.
- dbt lineage covers both Base and Enriched Silver outputs.
