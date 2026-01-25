# Contributing Guide

Thanks for contributing to ecom-datalake-pipelines! This guide will help you understand the codebase, set up your development environment, and submit high-quality pull requests.

---

## Table of Contents

1. [Development Setup](#development-setup)
2. [Project Architecture](#project-architecture)
3. [Making Changes](#making-changes)
4. [Testing & Validation](#testing--validation)
5. [CI/CD Pipeline](#cicd-pipeline)
6. [PR Guidelines](#pr-guidelines)
7. [Common Workflows](#common-workflows)

---

## Development Setup

### Prerequisites

- Python 3.12+
- Docker & Docker Compose (for Airflow testing)
- Git
- 8GB RAM minimum

### Quick Setup

```bash
# 1. Clone the repo
git clone https://github.com/YOUR_USERNAME/ecom-datalake-pipelines.git
cd ecom-datalake-pipelines

# 2. Install dependencies
python -m pip install -e ".[dev,dbt]"

# 3. Install pre-commit hooks
pre-commit install

# 4. Unzip sample data
unzip bronze_samples.zip

# 5. Install dbt packages
make dbt-deps

# 6. Run the demo to verify setup
make local-demo-fast
```

### Development Dependencies

The project uses:

- **dbt-duckdb**: Base Silver transformations
- **Polars**: Enriched Silver transformations
- **pytest**: Unit and integration testing
- **mypy**: Type checking
- **ruff + black**: Linting and formatting
- **pre-commit**: Git hooks for quality checks

---

## Project Architecture

### Layer Structure

```
Bronze (Parquet)
    ↓
Base Silver (dbt + DuckDB)
    ↓
Enriched Silver (Polars)
    ↓
Gold (dbt + BigQuery)
```

### Key Directories

```
ecom-datalake-pipelines/
├── src/
│   ├── transforms/          # Polars transforms for Enriched Silver
│   ├── validation/          # Quality validation frameworks
│   ├── runners/             # Pipeline orchestration runners
│   └── settings.py          # Configuration management
├── dbt_duckdb/
│   └── models/base_silver/  # dbt models for Base Silver
├── dbt_bigquery/
│   └── models/gold/         # dbt models for Gold marts
├── airflow/dags/            # Airflow DAG definitions
├── tests/
│   ├── unit/                # Unit tests (transforms, validation)
│   └── integration/         # Integration tests (E2E)
├── config/
│   └── specs/               # YAML specs for table configs
└── samples/                 # Sample Bronze data + pre-cooked dims
```

### Data Contracts

Tables are defined in YAML specs under `config/specs/`:

- `bronze.yml` - Bronze table schemas and partitions
- `silver_base.yml` - Base Silver tables and quality SLAs
- `enriched.yml` - Enriched Silver business tables
- `dims.yml` - Dimension snapshot tables

---

## Making Changes

### Branch Naming

```bash
# Feature branches
git checkout -b feature/add-customer-segmentation

# Bug fixes
git checkout -b fix/dims-snapshot-fallback

# Documentation
git checkout -b docs/update-testing-guide
```

### Code Style

The project enforces strict code quality:

```bash
# Auto-format code
black src/ tests/
ruff check --fix src/ tests/

# Type checking
mypy src/

# Run all pre-commit checks
pre-commit run --all-files
```

### Adding a New Transform

**Example: Adding a new enriched transform**

1. **Create the transform function** in `src/transforms/your_transform.py`:

```python
import polars as pl

def compute_new_metric(
    orders: pl.LazyFrame,
    customers: pl.LazyFrame,
) -> pl.LazyFrame:
    """Compute new business metric."""
    return orders.join(
        customers, on="customer_id", how="left"
    ).select([
        pl.col("customer_id"),
        # ... your logic
    ])
```

2. **Add the runner** in `src/runners/enriched/your_module.py`:

```python
from src.transforms.your_transform import compute_new_metric
from .shared import enriched_runner

@enriched_runner(
    output_table="int_new_metric",
    input_tables=["orders", "customers"],
)
def run_new_metric(
    tables: dict[str, pl.LazyFrame],
    settings: PipelineConfig,
    ingest_dt: str,
) -> pl.LazyFrame:
    return compute_new_metric(
        orders=tables["orders"],
        customers=tables["customers"],
    )
```

3. **Add spec entry** in `config/specs/enriched.yml`:

```yaml
- name: "int_new_metric"
  partition_key: "ingest_dt"
  inputs: ["silver_base.orders", "silver_base.customers"]
  min_rows: 1
```

4. **Write unit tests** in `tests/unit/test_transforms.py`:

```python
def test_compute_new_metric():
    orders = pl.LazyFrame({"customer_id": [1, 2]})
    customers = pl.LazyFrame({"customer_id": [1, 2]})
    result = compute_new_metric(orders, customers).collect()
    assert len(result) == 2
```

### Adding a New dbt Model

**Example: Adding a Base Silver model**

1. **Create the model** in `dbt_duckdb/models/base_silver/stg_ecommerce__new_table.sql`:

```sql
{{ config(materialized='external', location='{{ env_var("SILVER_BASE_PATH") }}/new_table') }}

select
    id,
    created_at,
    -- ... columns
    '{{ var("run_date") }}' as ingestion_dt
from {{ source('bronze', 'new_table') }}
where ingest_dt = '{{ var("run_date") }}'
```

2. **Add tests** in `dbt_duckdb/models/base_silver/schema.yml`:

```yaml
- name: stg_ecommerce__new_table
  columns:
    - name: id
      tests:
        - unique
        - not_null
```

3. **Add spec entry** in `config/specs/silver_base.yml`:

```yaml
- name: "new_table"
  partition_key: "ingestion_dt"
  source: "bronze.new_table"
  dbt_model: "stg_ecommerce__new_table"
  quality:
    sla: 0.95
    min_rows: 1
```

---

## Testing & Validation

### Unit Tests

```bash
# Run all unit tests
pytest tests/unit/ -v

# Run specific test file
pytest tests/unit/test_transforms.py -v

# Run with coverage
pytest tests/unit/ --cov=src --cov-report=term-missing

# Run tests matching a pattern
pytest tests/unit/ -k "cart_attribution"
```

### Integration Tests

```bash
# Run E2E pipeline test
pytest tests/integration/test_gold_logic_duckdb.py -v

# Manual E2E test
make local-demo-fast
```

### dbt Tests

```bash
# Test all models
make dbt-test

# Test specific model
cd dbt_duckdb
dbt test --select stg_ecommerce__orders
```

### Validation Reports

Quality validation runs automatically in CI and generates reports:

```bash
# Silver validation
python -m src.validation.silver \
  --partition-date 2023-01-01 \
  --output-report docs/validation_reports/SILVER_QUALITY.md

# Enriched validation
python -m src.validation.enriched \
  --ingest-dt 2023-01-01 \
  --output-report docs/validation_reports/ENRICHED_QUALITY.md

# Dims validation
python -m src.validation.dims_snapshot \
  --run-date 2024-01-01 \
  --run-id "manual_test"
```

---

## CI/CD Pipeline

### GitHub Actions Workflows

The project has 4 CI workflows:

1. **Pipeline E2E** (`.github/workflows/pipeline-e2e.yml`)
   - Validates Bronze → Silver → Enriched → Gold
   - Runs on every push to `main` and PRs
   - Tests: dbt models, Polars transforms, validation frameworks

2. **Python Quality** (`.github/workflows/python-quality.yml`)
   - Runs pytest, mypy, ruff, black
   - Enforces code quality standards

3. **dbt Validation** (`.github/workflows/dbt-validation.yml`)
   - Validates dbt project structure
   - Runs dbt compile and test

4. **Docker Build** (`.github/workflows/docker-build.yml`)
   - Builds and publishes Docker images
   - Only on releases

### Local CI Simulation

Match CI environment locally:

```bash
# Run Python quality checks (matches python-quality.yml)
pytest tests/unit/ -v
mypy src/
ruff check src/ tests/
black --check src/ tests/

# Run E2E pipeline (matches pipeline-e2e.yml)
make local-demo-fast

# Run dbt checks (matches dbt-validation.yml)
make dbt-deps
make dbt-test
```

---

## PR Guidelines

### Checklist

Before submitting a PR, ensure:

- [ ] All tests pass locally (`pytest tests/`)
- [ ] Type checks pass (`mypy src/`)
- [ ] Code is formatted (`black src/ tests/`)
- [ ] Linter passes (`ruff check src/ tests/`)
- [ ] dbt tests pass (if modifying dbt models) (`make dbt-test`)
- [ ] Demo runs successfully (`make local-demo-fast`)
- [ ] Documentation updated (if changing behavior)
- [ ] CHANGELOG.md updated (for notable changes)

### PR Description Template

```markdown
## Summary
Brief description of changes

## Changes
- Added X
- Fixed Y
- Updated Z

## Testing
- [ ] Unit tests added/updated
- [ ] Integration tests pass
- [ ] Manual testing completed

## Documentation
- [ ] README updated (if needed)
- [ ] Docstrings added/updated
- [ ] CHANGELOG updated
```

### Review Process

1. CI checks must pass
2. At least 1 approving review required
3. Merge to `main` triggers Docker build (if release)

---

## Common Workflows

### Running the Demo

```bash
# Fast demo (recommended)
make local-demo-fast

# Full demo (3-day dims + 3-day silver)
make local-demo
```

### Testing a Specific Date

```bash
# Generate dims for a date
make local-dims DATE=2024-01-15

# Run Base Silver for a date
make local-silver DATE=2024-01-15

# Run Enriched for a date
make local-enriched DATE=2024-01-15
```

### Debugging Pipeline Issues

```bash
# Check dbt logs
cat /tmp/dbt_logs/dbt.log

# Check validation reports
cat docs/validation_reports/SILVER_QUALITY_FULL.md
cat docs/validation_reports/ENRICHED_QUALITY_*.md

# Inspect parquet files
python -c "import polars as pl; print(pl.read_parquet('data/silver/base/orders/ingestion_dt=2023-01-01/*.parquet').head())"

# Check dims snapshots
ls -la data/silver/dims/customers/
ls -la data/silver/dims/product_catalog/
```

### Updating Specs

When modifying table specs in `config/specs/`:

1. Update the YAML spec file
2. Run validation to test the change
3. Update docs if the change affects behavior
4. Test with `make local-demo-fast`

### Adding Test Coverage

```bash
# Check current coverage
pytest tests/unit/ --cov=src --cov-report=html

# Open coverage report
open htmlcov/index.html

# Add tests for uncovered code
pytest tests/unit/test_new_module.py -v
```

---

## Environment Variables

Key environment variables used in development:

```bash
# Pipeline environment
PIPELINE_ENV=local              # local, dev, prod

# Data paths
BRONZE_BASE_PATH=samples/bronze
SILVER_BASE_PATH=data/silver/base
SILVER_DIMS_PATH=data/silver/dims
SILVER_ENRICHED_PATH=data/silver/enriched

# dbt paths (to avoid file locking on macOS)
DBT_LOG_PATH=/tmp/dbt_logs
DBT_TARGET_PATH=/tmp/dbt_target

# Configuration
ECOM_CONFIG_PATH=config/config.yml
```

---

## Getting Help

- **Questions?** Open a [Discussion](https://github.com/YOUR_USERNAME/ecom-datalake-pipelines/discussions)
- **Bug report?** Open an [Issue](https://github.com/YOUR_USERNAME/ecom-datalake-pipelines/issues)
- **Documentation:** Check [docs/resources/](docs/resources/) for detailed guides

---

<p align="center">
  <a href="README.md">🏠 <b>Home</b></a>
  &nbsp;·&nbsp;
  <a href="RESOURCE_HUB.md">📚 <b>Resource Hub</b></a>
  &nbsp;·&nbsp;
  <a href="docs/resources/TESTING_GUIDE.md">🧪 <b>Testing Guide</b></a>
</p>

<p align="center">
  <sub>Last updated: 2026-01-24</sub><br>
  <sub>✨ Transform the data. Tell the story. Build the future. ✨</sub>
</p>
