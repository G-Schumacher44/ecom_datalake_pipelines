# Contributing

Thanks for contributing! This repo uses dbt for base silver and Polars for enriched silver.

## Setup

```bash
python -m pip install -e ".[dev]"
```

## Common Commands

```bash
# dbt base silver
dbt build --project-dir dbt_duckdb --profiles-dir dbt_duckdb --select "base_silver.*"

# unit tests (polars transforms)
pytest -q

# type checks
mypy src

# lint/format
ruff check --fix
black .
```

## Pre-commit

```bash
pre-commit install
pre-commit run --all-files
```

## CI Checks

CI runs `mypy` + `pytest` on Python 3.12 via `.github/workflows/ci.yml`.

```bash
# match CI locally
pytest -q
mypy src
```

## Project Layout

- `dbt_duckdb/`: Base Silver models and tests (DuckDB).
- `src/transforms/`: Enriched Silver transforms (Polars).
- `src/validation/`: Bronze/Silver quality checks.
- `airflow/dags/`: Pipeline orchestration.

## PR Expectations

- Run dbt build on base silver.
- Run pytest + mypy.
- Update docs when changing data contracts or pipeline behavior.

---

<p align="center">
  <a href="README.md">🏠 <b>Home</b></a>
  &nbsp;·&nbsp;
  <a href="RESOURCE_HUB.md">📚 <b>Resource Hub</b></a>
</p>

<p align="center">
  <sub>Last updated: 2026-01-24</sub><br>
  <sub>✨ Transform the data. Tell the story. Build the future. ✨</sub>
</p>
