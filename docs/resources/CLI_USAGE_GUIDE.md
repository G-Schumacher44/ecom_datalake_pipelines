# CLI Usage Guide: Self-Documenting Scripts

## Overview

This project includes a suite of CLI scripts designed to automate Bronze layer profiling, documentation generation, and infrastructure setup. The star of the show is the **self-documenting profiling system** that analyzes live Parquet samples and auto-generates comprehensive documentation artifacts—no manual spreadsheet work required.

## Quick Reference

| Script | Purpose | Auto-Generated Output |
|--------|---------|----------------------|
| `describe_parquet_samples.py` | Profile Bronze Parquet samples | Quality report, schema map JSON, data contract updates, data dictionary |
| `pull_bronze_sample.sh` | Pull sample partitions from GCS | Local Bronze samples for profiling |
| `report_bronze_sizes.sh` | Generate bucket size report | Markdown report with table-level storage metrics |
| `bootstrap_airflow.sh` | Initialize Airflow environment | Airflow directories and Docker containers |
| `dims_snapshot.py` | Validate Dimension snapshots | Quality report for dimension tables |
| `run_dev_pipeline.sh` | Run dev pipeline (GCS native) | Full pipeline execution without Docker |
| `run_sim_prod_gcs.sh` | Run sim-prod pipeline (GCS native) | Production simulation against GCS |

---

## 🔍 Dimension Validation: `src.validation.dims_snapshot`

A lightweight quality gate designed to validate dimension snapshots (`customers`, `product_catalog`) without the overhead of scanning historical Bronze data.

### Core Functionality

- **Partition Verification**: Ensures the `snapshot_dt` partition exists and is readable.
- **Schema Validation**: Confirms all required columns defined in `base_silver_schemas.py` are present.
- **Integrity Checks**: Performs null checks on primary keys (e.g., `customer_id`).
- **GCS Optimized**: Uses `_MANIFEST.json` for near-instant file discovery on cloud storage.

### Basic Usage

```bash
# Validate today's dimension snapshots
python -m src.validation.dims_snapshot --run-date 2025-10-06 --run-id "manual_run_123"
```

### Key Arguments

| Argument | Description |
|----------|-------------|
| `--run-date` | The snapshot date (YYYY-MM-DD) to validate. |
| `--run-id` | Airflow run ID (used for GCS report organization). |
| `--enforce-quality` | Exit non-zero on any validation failure. |

---

## 🔍 Bronze Profiling: `describe_parquet_samples.py`

The self-documenting profiling script that analyzes Parquet samples and generates multiple documentation artifacts in one pass.

### Core Functionality

- **Schema analysis**: Detects data types, nullability, cardinality, and value distributions
- **Quality checks**: Flags schema drift, high null percentages, duplicate keys, cardinality anomalies
- **Documentation generation**: Auto-creates quality reports, schema maps, data contracts, and dictionaries
- **Multi-partition support**: Analyzes date ranges, specific months, or individual partitions

### Basic Usage

```bash
# Profile January 2020 samples (generates quality report)
python scripts/describe_parquet_samples.py \
  --date-range 2020-01-01..2020-01-31

# Profile specific tables only
python scripts/describe_parquet_samples.py \
  --tables orders,customers,products \
  --months 2020-01,2020-02

# Profile multiple specific dates
python scripts/describe_parquet_samples.py \
  --ingest-dts 2020-01-15,2020-02-15,2020-03-15
```

### Self-Documenting Outputs

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
- **Low cardinality IDs**: Primary keys with suspiciously few distinct values
- **Duplicate keys**: Tables where primary key uniqueness is violated
- **Cardinality mismatches**: Product names > product IDs (data quality issue)
- **Volume spikes**: Partitions with row counts 50% above average

---

## 📦 Sample Extraction: `pull_bronze_sample.sh`

Pull sample Bronze partitions from GCS for local profiling and development.

### Basic Usage

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

### Custom Destination

```bash
# Pull to custom directory
./scripts/pull_bronze_sample.sh 2020-01 /tmp/bronze_samples
```

### Limit Days Per Month

```bash
# Pull only first 3 days of each month (faster sampling)
MAX_DAYS=3 ./scripts/pull_bronze_sample.sh "2020-01,2020-02"
```

### What Gets Pulled

Per partition:
- `_MANIFEST.json` (if exists)
- First 3 Parquet files (configurable in script)

### Directory Structure

```
samples/bronze/
  orders/
    ingest_dt=2020-01-01/
      batch_20200101_120000.parquet
      batch_20200101_130000.parquet
      _MANIFEST.json
    ingest_dt=2020-01-02/
      ...
  customers/
    ingest_dt=2020-01-01/
      ...
```

### GCS Source Paths

Edit the `TABLE_PATHS` array in the script to customize source buckets/prefixes:

```bash
TABLE_PATHS=(
  "your-bucket/your-prefix/orders"
  "your-bucket/your-prefix/customers"
  # Add more tables as needed
)
```

---

## 📊 Bucket Size Reporting: `report_bronze_sizes.sh`

Generate Markdown reports of GCS bucket and per-table storage metrics.

### Basic Usage

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

### Output Format

```markdown
# Bronze Bucket Size Report

Generated: 2026-01-10T15:30:00Z

## Totals

- Bucket: `gs://gcs-automation-project-raw`
  - Total: 5.2GiB (5586739200 bytes)
- Prefix: `gs://gcs-automation-project-raw/ecom/raw`
  - Total: 3.8GiB (4080218931 bytes)

## Per-Table Sizes

| Table | Size (human) | Size (bytes) |
| --- | --- | --- |
| orders | 1.2GiB | 1288490188 |
| order_items | 980MiB | 1027604480 |
| customers | 512MiB | 536870912 |
| product_catalog | 128MiB | 134217728 |
```

### Configuration

The script reads `config/config.yml` for defaults:

```yaml
pipeline:
  bronze_bucket: "gcs-automation-project-raw"
  bronze_prefix: "ecom/raw"
```

### Requirements

- `gsutil` CLI installed and authenticated
- `numfmt` for human-readable sizes (optional, falls back to bytes)

---

## 🚁 Airflow Setup: `bootstrap_airflow.sh`

Initialize local Airflow environment with Docker Compose.

### Usage

```bash
# Initialize and start Airflow
./scripts/bootstrap_airflow.sh
```

### What It Does

1. Creates required directories:
   - `airflow/dags/`
   - `airflow/logs/`
   - `airflow/plugins/`

2. Runs `docker compose up airflow-init` (database setup, user creation)

3. Starts services:
   - `airflow-webserver` (http://localhost:8080)
   - `airflow-scheduler`

### Post-Setup

Access Airflow UI at `http://localhost:8080`:
- Default user: `airflow`
- Default password: `airflow`

### Stop Airflow

```bash
docker compose down
```

### Clean Slate

```bash
# Remove all Airflow data and restart
rm -rf airflow/logs/* airflow/plugins/*
docker compose down -v
./scripts/bootstrap_airflow.sh
```

## 🚀 Pipeline Execution: `run_dev_pipeline.sh`

Run the full pipeline (Bronze -> Silver -> Enriched) in development mode against GCS buckets, without using Docker/Airflow. Ideal for fast feedback loops.

### Usage

```bash
# Run for a specific date
./scripts/run_dev_pipeline.sh 2025-10-04
```

## 🏭 Production Simulation: `run_sim_prod_gcs.sh`

Simulate a production run against GCS buckets, including "Prod" specific gates and configurations.

### Usage

```bash
# Run simulation for a specific date
./scripts/run_sim_prod_gcs.sh 2025-10-04
```

---

## Workflow Examples

### 1. Initial Bronze Profiling (Full Documentation)

```bash
# Step 1: Pull representative samples
./scripts/pull_bronze_sample.sh "2020-01,2020-06,2025-12"

# Step 2: Generate all documentation artifacts
python scripts/describe_parquet_samples.py \
  --date-range 2020-01-01..2025-12-31 \
  --output docs/data/BRONZE_PROFILE_REPORT.md \
  --schema-json docs/data/BRONZE_SCHEMA_MAP.json \
  --update-contract docs/resources/DATA_CONTRACT.md \
  --data-dictionary docs/data/DATA_DICTIONARY.md

# Step 3: Review generated docs
cat docs/data/BRONZE_PROFILE_REPORT.md
```

### 2. Schema Drift Detection (Incremental)

```bash
# Pull latest month samples
./scripts/pull_bronze_sample.sh 2026-01

# Profile and check for drift vs baseline
python scripts/describe_parquet_samples.py \
  --months 2026-01 \
  --output docs/data/BRONZE_PROFILE_2026_01.md

# Compare schemas
diff docs/data/BRONZE_SCHEMA_MAP.json docs/data/BRONZE_SCHEMA_MAP_2026_01.json
```

### 3. Quality Validation Before Silver Pipeline

```bash
# Profile partitions you're about to process
python scripts/describe_parquet_samples.py \
  --date-range 2020-01-01..2020-01-31 \
  --tables orders,order_items,customers

# Check report for quality flags before proceeding
grep "⚠️" docs/data/BRONZE_PROFILE_REPORT.md
```

### 4. Ad-Hoc Table Investigation

```bash
# Deep dive into single table with more samples
python scripts/describe_parquet_samples.py \
  --tables orders \
  --date-range 2020-01-01..2020-12-31 \
  --max-files 10 \
  --max-rows 500000 \
  --output docs/planning/ORDERS_DEEP_PROFILE.md
```

### 5. Bucket Cost Analysis

```bash
# Generate size report for cost estimation
./scripts/report_bronze_sizes.sh \
  gcs-automation-project-raw \
  ecom/raw \
  docs/data/BRONZE_SIZES_CURRENT.md

# Review per-table costs
cat docs/data/BRONZE_SIZES_CURRENT.md
```

---

## Best Practices

### Profiling Cadence

- **Initial setup**: Profile 3-5 representative months across the full date range
- **Post-generation**: Profile all newly generated partitions before Silver pipeline
- **Monthly**: Run profiling on latest month to detect schema drift
- **Pre-deployment**: Profile sample partitions after any generator changes

### Sampling Strategy

- **Small datasets (<1M rows/table)**: Use `--max-files 3` for better coverage
- **Large datasets (>10M rows/table)**: Use `--max-files 1 --max-rows 100000` for speed
- **Schema validation**: Single file per partition is sufficient
- **Quality analysis**: Multiple files per partition recommended

### Output Organization

```
docs/data/
  BRONZE_PROFILE_REPORT.md          # Latest full profile
  BRONZE_SCHEMA_MAP.json            # Current schema baseline
  DATA_DICTIONARY.md                # Field-level documentation
  archive/
    BRONZE_PROFILE_2025_12.md       # Historical snapshots
    BRONZE_PROFILE_2025_06.md
docs/resources/
  DATA_CONTRACT.md                  # Updated with observed types
```

### Automation Integration

```python
# Example: Pre-Silver pipeline validation
import subprocess
import json

# Profile latest partition
result = subprocess.run([
    "python", "scripts/describe_parquet_samples.py",
    "--date-range", "2020-01-01..2020-01-01",
    "--schema-json", "/tmp/schema_check.json"
], check=True)

# Compare to baseline
with open("docs/data/BRONZE_SCHEMA_MAP.json") as f:
    baseline = json.load(f)

with open("/tmp/schema_check.json") as f:
    current = json.load(f)

if baseline != current:
    raise ValueError("Schema drift detected! Review before proceeding.")
```

---

## Troubleshooting

### Issue: `ModuleNotFoundError: No module named 'polars'`

**Solution**: Activate conda environment or install dependencies:

```bash
conda env create -f environment.yml
conda activate ecom_pipelines

# Or with pip
pip install polars pyarrow pyyaml
```

### Issue: `gsutil: command not found`

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

### Issue: Empty profile report (no tables found)

**Checklist**:
1. Verify sample directory exists: `ls samples/bronze/`
2. Check partition naming: Must be `ingest_dt=YYYY-MM-DD/`
3. Verify Parquet files exist: `find samples/bronze/ -name "*.parquet"`
4. Check date filters match actual partitions

### Issue: Schema map shows unexpected types

**Explanation**: The profiler shows **observed types** from Parquet. String-heavy schemas are common in Bronze (especially for timestamps/dates). Base Silver transformations cast these to proper types—that's documented in the data contract.

### Issue: Quality flags on returns tables (low cardinality)

**Expected behavior**: Low-volume tables (returns, cart abandonments) naturally have lower cardinality. Quality flags help you spot anomalies, but not all flags require action.

---

## Next Steps

After running the profiling scripts:

1. **Review quality flags**: Address any ⚠️ warnings in the profile report
2. **Validate data contract**: Ensure Bronze → Silver type mappings are correct
3. **Run Base Silver transforms**: Process profiled partitions with dbt-duckdb
4. **Monitor schema drift**: Compare new profiles against baseline schema map
5. **Update documentation**: Enhance auto-generated data dictionary with business context

---

## Related Documentation

- [Bronze Profile Report](data/BRONZE_PROFILE_REPORT.md) - Latest quality analysis
- [Data Contract](resources/DATA_CONTRACT.md) - Bronze → Silver type mappings
- [Data Dictionary](data/DATA_DICTIONARY.md) - Field-level documentation
- [Silver Framework](planning/SILVER_FRAMEWORK.md) - Transformation architecture
- [Testing Runbook](planning/TESTING_RUNBOOK.md) - Quality validation procedures
