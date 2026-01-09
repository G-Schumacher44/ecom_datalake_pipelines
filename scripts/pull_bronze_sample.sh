#!/usr/bin/env bash
set -euo pipefail

# Pull one sample partition per table for schema discovery.
# Usage: ./scripts/pull_bronze_sample.sh [INGEST_DT] [DEST_DIR]

INGEST_DT="${1:-2020-01-01}"
DEST_DIR="${2:-samples/bronze}"

TABLE_PATHS=(
  "gcs-automation-project-raw/ecom/raw/cart_items"
  "gcs-automation-project-raw/ecom/raw/customers"
  "gcs-automation-project-raw/ecom/raw/order_items"
  "gcs-automation-project-raw/ecom/raw/orders"
  "gcs-automation-project-raw/ecom/raw/product_catalog"
  "gcs-automation-project-raw/ecom/raw/return_items"
  "gcs-automation-project-raw/ecom/raw/returns"
  "gcs-automation-project-raw/ecom/raw/shopping_carts"
)

mkdir -p "${DEST_DIR}"

for table_path in "${TABLE_PATHS[@]}"; do
  table_name="${table_path##*/}"
  partition_path="gs://${table_path}/ingest_dt=${INGEST_DT}"
  local_dir="${DEST_DIR}/${table_name}/ingest_dt=${INGEST_DT}"

  echo "==> ${table_name}"
  mkdir -p "${local_dir}"

  # Pull manifest (if present).
  gsutil -q cp "${partition_path}/_MANIFEST.json" "${local_dir}/" || true

  # Pull up to 3 parquet files for quick inspection.
  gsutil -q ls "${partition_path}/*.parquet" | head -n 3 | \
    xargs -I {} gsutil -q cp {} "${local_dir}/" || true

done

echo "Done. Samples in ${DEST_DIR}."
