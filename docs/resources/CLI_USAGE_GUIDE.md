# CLI Usage Guide

## Overview

This project includes comprehensive CLI tools for pipeline execution, data validation, profiling, and infrastructure management. The toolkit supports both local development and production workflows with a focus on automation and self-documenting systems.

**Quick Navigation**:
- [Makefile Commands](#-makefile-commands) - Common development tasks
- [Validation Modules](#-validation-modules) - Three-layer quality framework
- [Bronze Profiling](#-bronze-profiling-describe_parquet_samplespy) - Self-documenting profiling system
- [Pipeline Scripts](#-pipeline-scripts) - Execution and orchestration
- [Workflow Examples](#workflow-examples) - End-to-end scenarios

---

## Quick Reference

### Core Scripts

| Script | Purpose | Key Output |
|--------|---------|-----------|
| `describe_parquet_samples.py` | Profile Bronze Parquet samples | Quality report, schema map JSON, data contract updates, data dictionary |
| `report_bronze_sizes.sh` | Generate bucket size report | Markdown report with table-level storage metrics |
| `pull_bronze_sample.sh` | Pull sample partitions from GCS | Local Bronze samples for profiling |
| `bootstrap_airflow.sh` | Initialize Airflow environment | Airflow directories and Docker containers |
| `run_dev_pipeline.sh` | Run dev pipeline (GCS native) | Full Bronze→Silver→Enriched execution |
| `run_sim_prod_gcs.sh` | Run sim-prod pipeline (GCS native) | Production simulation against GCS |
| `run_dims_from_spec.py` | Run dimension snapshots from spec | Customer and product catalog snapshots |
| `run_enriched_all_samples.py` | Run all enriched transforms | Enriched Silver layer outputs |
| `validate_bronze_samples.py` | Quick Bronze validation | Validation report for samples |
| `test_gold_logic_on_duckdb.py` | Test Gold mart logic locally | Local Gold validation |

### Python Modules (Validation)

| Module | Purpose | Use Case |
|--------|---------|----------|
| `src.validation.bronze_quality` | Bronze layer validation | Manifest checks, schema validation, partition coverage |
| `src.validation.silver` | Silver layer validation | Row reconciliation, PK/FK integrity, quarantine analysis |
| `src.validation.enriched` | Enriched layer validation | Business rule enforcement, schema consistency |
| `src.validation.dims_snapshot` | Dimension snapshot validation | Lightweight quality gate for dimension tables |

### Python Modules (Runners)

| Module | Purpose | Use Case |
|--------|---------|----------|
| `src.runners.base_silver` | Base Silver transformations | Run dbt-duckdb models programmatically |
| `src.runners.dims_snapshot` | Dimension snapshot runner | Create daily dimension snapshots |
| `src.runners.enriched.*` | Enriched transform runners | Polars-based enriched transforms |
| `src.runners.mock_bq_load` | Mock BigQuery load testing | Validate parquet schema/format before BQ load |

---

## 📋 Makefile Commands

The Makefile provides convenient shortcuts for common development tasks. Run `make help` to see all available commands.

### Environment Control

```bash
# Start local Airflow (Webserver: http://localhost:8080)
make up

# Stop Airflow services
make down

# Restart Scheduler & Webserver
make restart

# Tail Scheduler logs
make logs

# Tail specific task log
make log-task DAG=ecom_silver_to_gold_pipeline RUN_ID=manual_20260123 TASK=validate_bronze

# Open Bash in Scheduler container
make shell

# Wipe local data only (preserves Docker containers)
make clean-data

# Destroy containers, images, volumes AND local data
make clean
```

### Pipeline Execution (Airflow)

```bash
# Trigger Main Pipeline (Soft Mode - warnings only)
make run-sample DATE=2024-01-03

# Trigger Main Pipeline (Strict Mode - fail on quality issues)
make run-sample-strict DATE=2024-01-03

# Trigger Main Pipeline + BigQuery Load
make run-sample-bq DATE=2024-01-03

# Trigger Dimension Refresh Pipeline
make run-dims DATE=2024-01-03

# Backfill Main Pipeline (Soft Mode)
make backfill-easy START=2025-10-01 END=2025-10-31

# Backfill Main Pipeline (Strict Mode)
make backfill-strict START=2025-10-01 END=2025-10-31
```

### Local Development (No Docker)

```bash
# Run dbt + Validation locally
make local-silver DATE=2024-01-03

# Run dbt + Validation locally (Strict)
make local-silver-strict DATE=2024-01-03

# Run enriched transforms locally
make local-enriched DATE=2024-01-03

# Run enriched transforms locally (Strict)
make local-enriched-strict DATE=2024-01-03

# Run customer + product catalog dims locally
make local-dims DATE=2024-01-03

# Run customer + product catalog dims locally (Strict)
make local-dims-strict DATE=2024-01-03
```

### GCS Pipeline Execution (Native)

```bash
# Run Pipeline with GCS (Native, No Docker)
make run-dev-gcs DATE=2024-01-03

# Simulate prod run against GCS (Native, No Docker)
make run-sim-prod-gcs DATE=2024-01-03

# Run Pipeline with GCS (Docker + Airflow, dev)
make run-dev-docker DATE=2024-01-03

# Run Pipeline with GCS (Docker + Airflow, prod-sim)
make run-prod-sim-docker DATE=2024-01-03
```

### Development & Testing

```bash
# Run Unit Tests (pytest)
make test

# Run Unit Tests with coverage report
pytest tests/unit/ --cov=src --cov-report=term-missing

# Run Linter (ruff)
make lint

# Auto-format Code (ruff format)
make format

# Run Type Checker (mypy)
make type-check
```

### dbt Utilities

```bash
# Install dbt packages
make dbt-deps

# Build all Base Silver models
make dbt-build

# Run dbt data tests
make dbt-test
```

---

## 🔍 Validation Modules

The three-layer validation framework ensures data quality at each stage of the pipeline.

### Bronze Quality Validation: `src.validation.bronze_quality`

Validates Bronze layer inputs before Silver transformations.

#### Core Functionality

- **Manifest verification**: Ensures `_MANIFEST.json` exists and row counts match actual data
- **Schema validation**: Confirms all required columns are present with expected types
- **Partition coverage**: Checks for missing or incomplete partitions
- **File integrity**: Validates Parquet file structure and readability
- **Spec-driven validation**: Uses spec YAML to determine which tables to validate

#### Basic Usage

```bash
# Validate Bronze partition for specific date
python -m src.validation.bronze_quality \
  --bronze-path samples/bronze \
  --partition-date 2024-01-03 \
  --lookback-days 0 \
  --output-report docs/validation_reports/BRONZE_QUALITY.md

# Enforce quality gates (fail on issues)
python -m src.validation.bronze_quality \
  --bronze-path samples/bronze \
  --partition-date 2024-01-03 \
  --enforce-quality

# Validate specific tables only
python -m src.validation.bronze_quality \
  --bronze-path samples/bronze \
  --partition-date 2024-01-03 \
  --tables orders,customers,product_catalog

# Use spec file for table list
python -m src.validation.bronze_quality \
  --bronze-path samples/bronze \
  --partition-date 2024-01-03 \
  --spec-path config/specs/base.yml
```

#### Key Arguments

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `--bronze-path` | string | `samples/bronze` | Path to Bronze layer root |
| `--tables` | string | None | Comma-separated table names to validate (overrides spec) |
| `--partition-date` | string | None | Partition date (YYYY-MM-DD) to validate |
| `--lookback-days` | int | 0 | Number of days to look back for Bronze partitions |
| `--fail-on-issues` | flag | False | Exit non-zero on validation issues |
| `--enforce-quality` | flag | False | Exit non-zero on quality failures |
| `--run-id` | string | Auto-generated | Run ID for this validation |
| `--config` | string | None | Path to config YAML |
| `--output-report` | string | `docs/validation_reports/BRONZE_QUALITY.md` | Report output path |
| `--spec-path` | string | None | Path to spec YAML for table list |

#### Quality Checks Performed

- ✅ Manifest exists and is valid JSON
- ✅ Row count in manifest matches actual row count
- ✅ All required columns present
- ✅ No unexpected columns (schema drift)
- ✅ Partition directories follow naming convention
- ✅ Parquet files are readable

---

### Silver Quality Validation: `src.validation.silver`

Validates Silver layer transformation quality and data integrity.

#### Core Functionality

- **Bronze-to-Silver reconciliation**: Compares row counts between Bronze input and Silver output
- **Primary key uniqueness**: Validates PK constraints on customer_id, order_id, product_id
- **Foreign key integrity**: Checks FK relationships across tables
- **Quarantine analysis**: Analyzes rejected rows and quarantine reasons
- **Schema consistency**: Ensures Silver schema matches expected contract
- **Partition lookback**: Supports validation across multiple partitions for incremental processing

#### Basic Usage

```bash
# Validate Silver transformation for specific partition
python -m src.validation.silver \
  --bronze-path samples/bronze \
  --silver-path data/silver/base \
  --partition-date 2024-01-03 \
  --tables orders,customers,product_catalog \
  --output-report docs/validation_reports/SILVER_QUALITY.md

# Enforce quality gates (fail on SLA breach)
python -m src.validation.silver \
  --bronze-path samples/bronze \
  --silver-path data/silver/base \
  --partition-date 2024-01-03 \
  --enforce-quality

# Validate with 7-day lookback (for incremental processing)
python -m src.validation.silver \
  --bronze-path samples/bronze \
  --silver-path data/silver/base \
  --partition-date 2024-01-03 \
  --lookback-days 7 \
  --enforce-quality

# Use spec file for table list
python -m src.validation.silver \
  --bronze-path samples/bronze \
  --silver-path data/silver/base \
  --partition-date 2024-01-03 \
  --spec-path config/specs/base.yml
```

#### Key Arguments

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `--config` | string | `config/config.yml` | Path to pipeline config YAML |
| `--bronze-path` | string | From config | Path to Bronze layer root |
| `--silver-path` | string | From config | Path to Silver layer root |
| `--quarantine-path` | string | From config | Path to quarantine data |
| `--run-id` | string | Auto-generated | Run ID for this validation |
| `--tables` | string | None | Comma-separated table names to validate |
| `--partition-date` | string | None | Partition date (YYYY-MM-DD) to validate |
| `--lookback-days` | int | 0 | Number of days to look back for Bronze partitions |
| `--fail-on-sla-breach` | flag | False | Exit non-zero on SLA breach |
| `--enforce-quality` | flag | False | Exit non-zero on quality failures |
| `--output-report` | string | `docs/validation_reports/SILVER_QUALITY.md` | Report output path |
| `--spec-path` | string | None | Path to spec YAML for table list |

#### Quality Checks Performed

- ✅ Row loss < 1% (Bronze → Silver, excluding quarantine)
- ✅ Quarantine rate < 5%
- ✅ Primary key uniqueness 100%
- ✅ Foreign key integrity > 99%
- ✅ Required columns present
- ✅ Null rates within acceptable thresholds
- ✅ `ingestion_dt` metadata present

---

### Enriched Quality Validation: `src.validation.enriched`

Validates Enriched Silver layer business rules and schema consistency.

#### Core Functionality

- **Business rule enforcement**: Validates domain-specific logic (e.g., CLV calculations, attribution windows)
- **Required column presence**: Ensures all expected enriched columns exist
- **Minimum row thresholds**: Checks for suspiciously low row counts
- **Schema snapshot consistency**: Validates against expected enriched schemas
- **Null rate analysis**: Flags unexpected null patterns in computed fields

#### Basic Usage

```bash
# Validate Enriched layer for specific partition
python -m src.validation.enriched \
  --enriched-path data/silver/enriched \
  --ingest-dt 2024-01-03 \
  --output-report docs/validation_reports/ENRICHED_QUALITY.md

# Enforce quality gates
python -m src.validation.enriched \
  --enriched-path data/silver/enriched \
  --ingest-dt 2024-01-03 \
  --enforce-quality

# Validate with custom config
python -m src.validation.enriched \
  --config config/config.yml \
  --enriched-path data/silver/enriched \
  --ingest-dt 2024-01-03
```

#### Key Arguments

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `--config` | string | `config/config.yml` | Path to pipeline config YAML |
| `--enriched-path` | string | From config | Path to Enriched Silver layer root |
| `--ingest-dt` | string | None | Ingestion date (YYYY-MM-DD) to validate |
| `--enforce-quality` | flag | False | Exit non-zero on quality failures |
| `--output-report` | string | `docs/validation_reports/ENRICHED_QUALITY.md` | Report output path |
| `--run-id` | string | Auto-generated | Run ID for this validation |

#### Quality Checks Performed

- ✅ All expected enriched tables present
- ✅ Required columns exist (e.g., `customer_ltv`, `cart_attribution_status`)
- ✅ Minimum row thresholds met (e.g., > 100 rows for customer_lifetime_value)
- ✅ Schema matches enriched schema specifications
- ✅ No excessive nulls in computed fields
- ✅ `ingestion_dt` metadata present

---

### Dimension Snapshot Validation: `src.validation.dims_snapshot`

A lightweight quality gate designed to validate dimension snapshots (`customers`, `product_catalog`) without the overhead of scanning historical Bronze data.

#### Core Functionality

- **Partition verification**: Ensures the `snapshot_dt` partition exists and is readable
- **Schema validation**: Confirms all required columns defined in `base_silver_schemas.py` are present
- **Integrity checks**: Performs null checks on primary keys (e.g., `customer_id`, `product_id`)
- **GCS optimized**: Uses `_MANIFEST.json` for near-instant file discovery on cloud storage
- **No Bronze comparison**: Validates dimensions independently without historical Bronze scans

#### Basic Usage

```bash
# Validate today's dimension snapshots
python -m src.validation.dims_snapshot \
  --run-date 2025-10-06 \
  --run-id "manual_run_123"

# Enforce quality gates
python -m src.validation.dims_snapshot \
  --run-date 2025-10-06 \
  --enforce-quality

# Validate with custom dims path
python -m src.validation.dims_snapshot \
  --run-date 2025-10-06 \
  --dims-path data/silver/dims
```

#### Key Arguments

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `--run-date` | string | Required | The snapshot date (YYYY-MM-DD) to validate |
| `--run-id` | string | Auto-generated | Airflow run ID (used for GCS report organization) |
| `--enforce-quality` | flag | False | Exit non-zero on any validation failure |
| `--dims-path` | string | From config | Path to dimension snapshots |
| `--config` | string | `config/config.yml` | Path to pipeline config YAML |

#### Quality Checks Performed

- ✅ Dimension snapshot partitions exist
- ✅ Primary keys non-null (customer_id, product_id)
- ✅ Required columns present
- ✅ Manifest row counts match actual
- ✅ `_latest.json` pointer valid

---

## 🔍 Bronze Profiling: `describe_parquet_samples.py`

The self-documenting profiling script that analyzes Parquet samples and generates multiple documentation artifacts in one pass.

### Core Functionality

- **Schema analysis**: Detects data types, nullability, cardinality, and value distributions
- **Quality checks**: Flags schema drift, high null percentages, duplicate keys, cardinality anomalies
- **Documentation generation**: Auto-creates quality reports, schema maps, data contracts, and dictionaries
- **Multi-partition support**: Analyzes date ranges, specific months, or individual partitions
- **Stratified temporal sampling**: Sample 3 representative months across dataset lifecycle

### Basic Usage

```bash
# Profile January 2020 samples (generates quality report)
python scripts/describe_parquet_samples.py \
  --date-range 2020-01-01..2020-01-31

# Profile specific tables only
python scripts/describe_parquet_samples.py \
  --tables orders,customers,product_catalog \
  --months 2020-01,2020-02

# Profile multiple specific dates
python scripts/describe_parquet_samples.py \
  --ingest-dts 2020-01-15,2020-02-15,2020-03-15

# Profile all samples (stratified sampling recommended)
python scripts/describe_parquet_samples.py \
  --months 2020-03,2023-01,2025-10
```

### Self-Documenting Outputs

The profiling script generates **four documentation artifacts** from a single run:

#### 1. Quality Report (Markdown)

**Default output**: `docs/data/BRONZE_PROFILE_REPORT.md`

```bash
# Generate quality report with schema drift detection
python scripts/describe_parquet_samples.py \
  --date-range 2020-01-01..2020-12-31 \
  --output docs/data/BRONZE_PROFILE_REPORT.md
```

**What it includes**:
- Table and partition summaries
- Total row counts and file counts
- Schema consistency checks (drift detection)
- Column-level statistics (nulls, cardinality, min/max, percentiles)
- Quality flags (high nulls, duplicate keys, cardinality mismatches)
- Top values for categorical fields

#### 2. Schema Map (JSON)

**Programmatic schema export** for validation pipelines and automation:

```bash
# Generate JSON schema map for programmatic use
python scripts/describe_parquet_samples.py \
  --date-range 2020-01-01..2020-12-31 \
  --schema-json docs/data/BRONZE_SCHEMA_MAP.json
```

**Format**:
```json
{
  "orders": {
    "order_id": "string",
    "customer_id": "string",
    "order_date": "string",
    "total_items": "int64",
    "gross_total": "float64"
  },
  "customers": {
    "customer_id": "string",
    "email": "string",
    "signup_date": "string"
  }
}
```

#### 3. Data Contract Updates

**Auto-updates Bronze → Base Silver type mappings** in your data contract:

```bash
# Update data contract with observed Bronze types
python scripts/describe_parquet_samples.py \
  --date-range 2020-01-01..2020-12-31 \
  --update-contract docs/resources/DATA_CONTRACT.md
```

**What it does**:
- Reads observed Bronze types from Parquet samples
- Updates the "Observed Bronze Types vs Base Silver Targets" section
- Preserves Base Silver target types (timestamp casting, etc.)
- Documents required type conversions

**Before**:
```markdown
### orders
- order_id: TBD -> Base Silver `string`
- order_date: TBD -> Base Silver `timestamp`
```

**After**:
```markdown
### orders
- order_id: Bronze `string` -> Base Silver `string`
- order_date: Bronze `string` -> Base Silver `timestamp`
```

#### 4. Data Dictionary

**Auto-generates field-level documentation** from schema analysis:

```bash
# Generate data dictionary with field descriptions
python scripts/describe_parquet_samples.py \
  --date-range 2020-01-01..2020-12-31 \
  --data-dictionary docs/data/DATA_DICTIONARY.md
```

**What it includes**:
- Table-level summaries
- Field names, types, nullability
- Observed value ranges (min/max)
- Cardinality stats (distinct counts)
- Placeholder descriptions (can be manually enhanced)

### Combined Workflow (All Artifacts)

```bash
# Generate ALL documentation artifacts in one pass
python scripts/describe_parquet_samples.py \
  --date-range 2020-01-01..2020-12-31 \
  --output docs/data/BRONZE_PROFILE_REPORT.md \
  --schema-json docs/data/BRONZE_SCHEMA_MAP.json \
  --update-contract docs/resources/DATA_CONTRACT.md \
  --data-dictionary docs/data/DATA_DICTIONARY.md
```

**Result**: Quality report + schema JSON + updated contract + data dictionary, all from live data.

### Advanced Options

#### Custom Sample Directory

```bash
# Profile samples from non-default location
python scripts/describe_parquet_samples.py \
  --root /path/to/custom/samples \
  --date-range 2020-01-01..2020-01-31
```

#### Limit Files and Rows Per Partition

```bash
# Profile more files per partition for better coverage
python scripts/describe_parquet_samples.py \
  --date-range 2020-01-01..2020-12-31 \
  --max-files 5 \
  --max-rows 500000
```

#### Filter by Tables

```bash
# Profile only transactional tables (exclude catalog/returns)
python scripts/describe_parquet_samples.py \
  --tables orders,order_items,customers \
  --date-range 2020-01-01..2020-12-31
```

### CLI Arguments Reference

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `--root` | string | `samples/bronze` | Sample root directory |
| `--max-files` | int | `1` | Max Parquet files per partition to analyze |
| `--max-rows` | int | `100000` | Max rows per file sample (0=all) |
| `--tables` | string | `""` | Comma-separated table names to include |
| `--ingest-dts` | string | `""` | Comma-separated dates (YYYY-MM-DD) |
| `--months` | string | `""` | Comma-separated months (YYYY-MM) |
| `--date-range` | string | `""` | Date range (YYYY-MM-DD..YYYY-MM-DD) |
| `--output` | string | `docs/data/BRONZE_PROFILE_REPORT.md` | Markdown report output path |
| `--schema-json` | string | `""` | Optional JSON schema map output path |
| `--update-contract` | string | `""` | Optional DATA_CONTRACT.md path to update |
| `--data-dictionary` | string | `""` | Optional DATA_DICTIONARY.md output path |

### Quality Checks Performed

The profiling script automatically detects and flags:

- **Schema drift**: Inconsistent schemas across partitions
- **High nulls**: Columns with >50% null values
- **Low cardinality IDs**: Primary keys with suspiciously few distinct values (<10 for entity IDs)
- **Duplicate keys**: Tables where primary key uniqueness is violated
- **Cardinality mismatches**: Product names > product IDs (data quality issue)
- **Volume spikes**: Partitions with row counts 50% above average

---

## 🚀 Pipeline Scripts

### Sample Extraction: `pull_bronze_sample.sh`

Pull sample Bronze partitions from GCS for local profiling and development.

#### Basic Usage

```bash
# Pull samples for specific months (default: Jun 2020, Jan 2023, Dec 2025)
./scripts/pull_bronze_sample.sh

# Pull single month
./scripts/pull_bronze_sample.sh 2020-01

# Pull multiple months
./scripts/pull_bronze_sample.sh "2020-01,2020-02,2020-03"

# Pull specific dates
./scripts/pull_bronze_sample.sh "2020-01-15,2020-02-15"
```

#### Custom Destination

```bash
# Pull to custom directory
./scripts/pull_bronze_sample.sh 2020-01 /tmp/bronze_samples
```

#### Limit Days Per Month

```bash
# Pull only first 3 days of each month (faster sampling)
MAX_DAYS=3 ./scripts/pull_bronze_sample.sh "2020-01,2020-02"
```

#### What Gets Pulled

Per partition:
- `_MANIFEST.json` (if exists)
- First 3 Parquet files (configurable in script)

---

### Bucket Size Reporting: `report_bronze_sizes.sh`

Generate Markdown reports of GCS bucket and per-table storage metrics.

#### Basic Usage

```bash
# Generate report using config.yml defaults
./scripts/report_bronze_sizes.sh

# Specify bucket and prefix
./scripts/report_bronze_sizes.sh gcs-automation-project-raw ecom/raw

# Custom output path
./scripts/report_bronze_sizes.sh \
  gcs-automation-project-raw \
  ecom/raw \
  docs/data/BRONZE_SIZES_2026_Q1.md
```

#### Output Format

```markdown
# Bronze Bucket Size Report

Generated: 2026-01-10T15:30:00Z

## Totals

- Bucket: `gs://gcs-automation-project-raw`
  - Total: 14.01 GiB
- Prefix: `gs://gcs-automation-project-raw/ecom/raw`
  - Total: 14.01 GiB

## Per-Table Sizes

| Table | Size |
| --- | --- |
| cart_items | 8.95 GiB |
| orders | 2.56 GiB |
| order_items | 1.78 GiB |
| customers | 80.19 MiB |
```

---

### Airflow Setup: `bootstrap_airflow.sh`

Initialize local Airflow environment with Docker Compose.

#### Usage

```bash
# Initialize and start Airflow
./scripts/bootstrap_airflow.sh
```

#### What It Does

1. Creates required directories: `airflow/dags/`, `airflow/logs/`, `airflow/plugins/`
2. Runs `docker compose up airflow-init` (database setup, user creation)
3. Starts services: `airflow-webserver` (http://localhost:8080), `airflow-scheduler`

#### Post-Setup

Access Airflow UI at `http://localhost:8080`:
- Default user: `airflow`
- Default password: `airflow`

#### Stop Airflow

```bash
docker compose down
```

#### Clean Slate

```bash
# Remove all Airflow data and restart
rm -rf airflow/logs/* airflow/plugins/*
docker compose down -v
./scripts/bootstrap_airflow.sh
```

---

### Pipeline Execution: `run_dev_pipeline.sh`

Run the full pipeline (Bronze → Silver → Enriched) in development mode against GCS buckets, without using Docker/Airflow.

#### Usage

```bash
# Run for a specific date
./scripts/run_dev_pipeline.sh 2025-10-04
```

**What it does**:
- Sets `PIPELINE_ENV=dev`
- Runs Bronze validation
- Runs Base Silver transformations (dbt-duckdb)
- Runs Silver validation
- Runs dimension snapshots
- Runs enriched transforms
- Runs enriched validation

---

### Production Simulation: `run_sim_prod_gcs.sh`

Simulate a production run against GCS buckets, including "Prod" specific gates and configurations.

#### Usage

```bash
# Run simulation for a specific date
./scripts/run_sim_prod_gcs.sh 2025-10-04
```

**What it does**:
- Sets `PIPELINE_ENV=prod-sim`
- Enforces strict quality gates
- Publishes validation reports to GCS
- Uses staging prefix pattern for atomic publishes

---

## Workflow Examples

### 1. Initial Bronze Profiling (Full Documentation)

```bash
# Step 1: Pull representative samples (stratified temporal sampling)
./scripts/pull_bronze_sample.sh "2020-03,2023-01,2025-10"

# Step 2: Generate all documentation artifacts
python scripts/describe_parquet_samples.py \
  --months 2020-03,2023-01,2025-10 \
  --output docs/data/BRONZE_PROFILE_REPORT.md \
  --schema-json docs/data/BRONZE_SCHEMA_MAP.json \
  --update-contract docs/resources/DATA_CONTRACT.md \
  --data-dictionary docs/data/DATA_DICTIONARY.md

# Step 3: Review generated docs
cat docs/data/BRONZE_PROFILE_REPORT.md | grep "⚠️"
```

---

### 2. Full E2E Pipeline Test (Local)

```bash
# Step 1: Validate Bronze samples
python -m src.validation.bronze_quality \
  --bronze-path samples/bronze \
  --partition-date 2024-01-03 \
  --enforce-quality

# Step 2: Run Base Silver transformations
make local-silver DATE=2024-01-03

# Step 3: Validate Silver outputs
python -m src.validation.silver \
  --bronze-path samples/bronze \
  --silver-path data/silver/base \
  --partition-date 2024-01-03 \
  --enforce-quality

# Step 4: Run dimension snapshots
make local-dims DATE=2024-01-03

# Step 5: Validate dimension snapshots
python -m src.validation.dims_snapshot \
  --run-date 2024-01-03 \
  --enforce-quality

# Step 6: Run Enriched transforms
make local-enriched DATE=2024-01-03

# Step 7: Validate Enriched outputs
python -m src.validation.enriched \
  --enriched-path data/silver/enriched \
  --ingest-dt 2024-01-03 \
  --enforce-quality
```

---

### 3. Schema Drift Detection (Incremental)

```bash
# Pull latest month samples
./scripts/pull_bronze_sample.sh 2026-01

# Profile and generate new schema map
python scripts/describe_parquet_samples.py \
  --months 2026-01 \
  --schema-json /tmp/BRONZE_SCHEMA_MAP_NEW.json

# Compare schemas
diff docs/data/BRONZE_SCHEMA_MAP.json /tmp/BRONZE_SCHEMA_MAP_NEW.json

# If differences found, regenerate full profile
python scripts/describe_parquet_samples.py \
  --months 2026-01 \
  --output docs/data/BRONZE_PROFILE_2026_01.md
```

---

### 4. CI/CD Validation Workflow

```bash
# Run validation in CI/CD pipeline
set -e  # Exit on first error

# Validate Bronze
python -m src.validation.bronze_quality \
  --partition-date ${RUN_DATE} \
  --enforce-quality

# Run transformations
make local-silver DATE=${RUN_DATE}

# Validate Silver
python -m src.validation.silver \
  --partition-date ${RUN_DATE} \
  --enforce-quality

# Run enriched
make local-enriched DATE=${RUN_DATE}

# Validate Enriched
python -m src.validation.enriched \
  --ingest-dt ${RUN_DATE} \
  --enforce-quality

echo "✅ All validations passed"
```

---

### 5. Production Deployment Workflow

```bash
# Step 1: Validate Bronze inputs
python -m src.validation.bronze_quality \
  --bronze-path gs://gcs-automation-project-raw/ecom/raw \
  --partition-date 2024-01-03 \
  --enforce-quality \
  --run-id airflow_prod_20251015_123456

# Step 2: Run Base Silver (dbt-duckdb)
make local-silver-strict DATE=2024-01-03

# Step 3: Validate Silver outputs
python -m src.validation.silver \
  --bronze-path gs://gcs-automation-project-raw/ecom/raw \
  --silver-path gs://gcs-automation-project-silver/base \
  --partition-date 2024-01-03 \
  --enforce-quality

# Step 4: Run dimension snapshots
make local-dims-strict DATE=2024-01-03

# Step 5: Run enriched transforms
make local-enriched-strict DATE=2024-01-03

# Step 6: Publish to GCS with staging prefix
# (handled by runners with SILVER_PUBLISH_MODE=staging)

# Step 7: Load to BigQuery
# (handled by Airflow DAG)
```

---

## Best Practices

### Profiling Cadence

- **Initial setup**: Profile 3-5 representative months across the full date range (stratified temporal sampling)
- **Post-generation**: Profile all newly generated partitions before Silver pipeline
- **Monthly**: Run profiling on latest month to detect schema drift
- **Pre-deployment**: Profile sample partitions after any generator changes

### Sampling Strategy

- **Small datasets (<1M rows/table)**: Use `--max-files 3` for better coverage
- **Large datasets (>10M rows/table)**: Use `--max-files 1 --max-rows 100000` for speed
- **Schema validation**: Single file per partition is sufficient
- **Quality analysis**: Multiple files per partition recommended

### Validation Workflow

```bash
# Always validate in sequence: Bronze → Silver → Enriched
# Use --enforce-quality in CI/CD, omit for exploratory analysis

# Development (warnings only)
python -m src.validation.bronze_quality --partition-date 2024-01-03
python -m src.validation.silver --partition-date 2024-01-03
python -m src.validation.enriched --ingest-dt 2024-01-03

# CI/CD (fail on issues)
python -m src.validation.bronze_quality --partition-date 2024-01-03 --enforce-quality
python -m src.validation.silver --partition-date 2024-01-03 --enforce-quality
python -m src.validation.enriched --ingest-dt 2024-01-03 --enforce-quality
```

### Output Organization

```
docs/data/
  BRONZE_PROFILE_REPORT.md          # Latest full profile
  BRONZE_SCHEMA_MAP.json            # Current schema baseline
  DATA_DICTIONARY.md                # Field-level documentation
  BRONZE_SIZES.md                   # Current bucket size report
  archive/
    BRONZE_PROFILE_2025_12.md       # Historical snapshots
    BRONZE_PROFILE_2025_06.md
docs/resources/
  DATA_CONTRACT.md                  # Updated with observed types
docs/validation_reports/
  BRONZE_QUALITY.md                 # Latest Bronze validation
  SILVER_QUALITY.md                 # Latest Silver validation
  ENRICHED_QUALITY.md               # Latest Enriched validation
  DIMS_SNAPSHOT_QUALITY.md          # Latest dimension validation
```

---

## Troubleshooting

### Issue: `ModuleNotFoundError: No module named 'polars'`

**Solution**: Activate conda environment or install dependencies:

```bash
conda env create -f environment.yml
conda activate ecom_pipelines

# Or with pip
pip install polars pyarrow pyyaml pydantic
```

---

### Issue: `gcloud: command not found`

**Solution**: Install Google Cloud SDK:

```bash
# macOS
brew install google-cloud-sdk

# Linux
curl https://sdk.cloud.google.com | bash
exec -l $SHELL

# Authenticate
gcloud auth login
```

---

### Issue: Empty profile report (no tables found)

**Checklist**:
1. Verify sample directory exists: `ls samples/bronze/`
2. Check partition naming: Must be `ingest_dt=YYYY-MM-DD/`
3. Verify Parquet files exist: `find samples/bronze/ -name "*.parquet"`
4. Check date filters match actual partitions

---

### Issue: Validation module exits with non-zero code

**Explanation**: When using `--enforce-quality` flag, validation modules exit with non-zero code on quality failures. This is intentional for CI/CD integration. Remove the flag for warning-only mode.

```bash
# Warning-only mode (exit 0 even on failures)
python -m src.validation.silver --partition-date 2024-01-03

# Enforce mode (exit non-zero on failures)
python -m src.validation.silver --partition-date 2024-01-03 --enforce-quality
```

---

### Issue: Schema map shows unexpected types

**Explanation**: The profiler shows **observed types** from Parquet. String-heavy schemas are common in Bronze (especially for timestamps/dates). Base Silver transformations cast these to proper types—that's documented in the data contract.

**Example**:
- Bronze: `order_date: string`
- Silver: `order_date: timestamp` (after dbt casting)

---

### Issue: Quality flags on returns tables (low cardinality)

**Expected behavior**: Low-volume tables (returns, cart abandonments) naturally have lower cardinality. Quality flags help you spot anomalies, but not all flags require action.

---

## Next Steps

After running the profiling and validation scripts:

1. **Review quality flags**: Address any ⚠️ warnings in profile and validation reports
2. **Validate data contract**: Ensure Bronze → Silver type mappings are correct
3. **Run Base Silver transforms**: Process profiled partitions with dbt-duckdb
4. **Monitor schema drift**: Compare new profiles against baseline schema map
5. **Update documentation**: Enhance auto-generated data dictionary with business context
6. **Set up CI/CD**: Integrate validation modules into your deployment pipeline
7. **Enable GCS reports**: Configure `GCS_REPORTS_BUCKET` for production observability

---

## Related Documentation

- **[Testing Guide](TESTING_GUIDE.md)** - Comprehensive testing strategies (unit, integration, E2E)
- **[Validation Guide](VALIDATION_GUIDE.md)** - Three-layer validation framework details
- **[Architecture Overview](ARCHITECTURE.md)** - Complete system architecture
- **[Bronze Profile Report](../data/BRONZE_PROFILE_REPORT.md)** - Latest quality analysis
- **[Data Contract](DATA_CONTRACT.md)** - Bronze → Silver type mappings
- **[Data Dictionary](../data/DATA_DICTIONARY.md)** - Field-level documentation
- **[SLA & Quality Gates](SLA_AND_QUALITY.md)** - Quality thresholds and acceptance criteria

---

**Last Updated**: 2026-01-23  
**CLI Version**: 1.0.0  
**Pipeline Version**: 1.0.0

---

<p align="center">
  <a href="../../README.md">🏠 <b>Home</b></a>
  &nbsp;·&nbsp;
  <a href="../../RESOURCE_HUB.md">📚 <b>Resource Hub</b></a>
</p>

<p align="center">
  <sub>Last updated: 2026-01-24</sub><br>
  <sub>✨ Transform the data. Tell the story. Build the future. ✨</sub>
</p>
