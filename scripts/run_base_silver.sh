#!/bin/bash
set -e

# Default paths if not set
: "${BRONZE_BASE_PATH:=samples/bronze}"
: "${SILVER_BASE_PATH:=data/silver/base}"
# Derive quarantine path if not set, assuming standard structure
: "${SILVER_QUARANTINE_PATH:=$SILVER_BASE_PATH/quarantine}"

echo "Starting Base Silver run..."
echo "Bronze Source: $BRONZE_BASE_PATH"
echo "Silver Target: $SILVER_BASE_PATH"

# Workaround for macOS Docker VirtioFS deadlock (Errno 35)
# If reading from a mounted path that might have locking issues, copy to /tmp first.
# This uses dd with direct I/O to avoid VirtioFS caching issues.
if [[ "$BRONZE_BASE_PATH" == *"/opt/airflow/samples"* ]] && [[ -d "$BRONZE_BASE_PATH" ]]; then
    # Test if we can read files without deadlock
    if ! find "$BRONZE_BASE_PATH" -maxdepth 2 -type f -name "*.parquet" -print -quit 2>/dev/null | head -1 > /dev/null; then
        echo "⚠️  VirtioFS read issue detected. Try: Docker Desktop → Settings → Change VirtioFS to gRPC FUSE"
        exit 1
    fi
fi

# Ensure directories exist for local/container paths (DuckDB won't auto-create)
if [[ "$SILVER_BASE_PATH" != gs://* ]]; then
    echo "Ensuring local directory structure exists..."
    # Create standard table directories
    TABLES="customers product_catalog orders shopping_carts cart_items order_items returns return_items"
    for table in $TABLES; do
        mkdir -p "$SILVER_BASE_PATH/$table"
        mkdir -p "$SILVER_QUARANTINE_PATH/$table"
    done
fi

# Set up dbt temp paths to avoid filesystem locking on mounts
mkdir -p /tmp/dbt_target /tmp/dbt_logs /tmp/dbt_duckdb

# dbt environment variables for safety
export DBT_TARGET_PATH="/tmp/dbt_target"
export DBT_LOG_PATH="/tmp/dbt_logs"
export DBT_PARTIAL_PARSE="false"

# Run dbt, passing through all arguments (e.g. --select ...)
exec dbt run --project-dir dbt_duckdb --profiles-dir dbt_duckdb --no-partial-parse "$@"