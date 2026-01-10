#!/usr/bin/env bash
set -euo pipefail

# Pull sample partitions per table for schema discovery.
# Usage: ./scripts/pull_bronze_sample.sh [INGEST_DTS] [DEST_DIR]
# INGEST_DTS can be a single date (YYYY-MM-DD), a month (YYYY-MM),
# or a comma-separated list of dates/months.
# Set MAX_DAYS to limit how many days are pulled per month.

INGEST_DTS="${1:-2020-06,2023-01,2025-12}"
DEST_DIR="${2:-samples/bronze}"
MAX_DAYS="${MAX_DAYS:-0}"

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

IFS=',' read -r -a ingest_dates <<< "${INGEST_DTS}"

for ingest_dt in "${ingest_dates[@]}"; do
  ingest_dt="$(echo "${ingest_dt}" | xargs)"
  if [[ "${ingest_dt}" =~ ^[0-9]{4}-[0-9]{2}$ ]]; then
    echo "==> Sampling month=${ingest_dt}"
  else
    echo "==> Sampling ingest_dt=${ingest_dt}"
  fi

  for table_path in "${TABLE_PATHS[@]}"; do
    table_name="${table_path##*/}"

    if [[ "${ingest_dt}" =~ ^[0-9]{4}-[0-9]{2}$ ]]; then
      # Month mode: discover matching partition directories for this table.
      partitions=$(gsutil ls -d "gs://${table_path}/ingest_dt=${ingest_dt}-*/" 2>/dev/null || true)
      if [[ -z "${partitions}" ]]; then
        echo "  -> ${table_name} (no partitions for ${ingest_dt})"
        continue
      fi
      if [[ "${MAX_DAYS}" -gt 0 ]]; then
        partitions=$(echo "${partitions}" | head -n "${MAX_DAYS}")
      fi
      while read -r partition_path; do
        [[ -z "${partition_path}" ]] && continue
        day_value="${partition_path##*ingest_dt=}"
        day_value="${day_value%/}"
        local_dir="${DEST_DIR}/${table_name}/ingest_dt=${day_value}"

        echo "  -> ${table_name} (${day_value})"
        mkdir -p "${local_dir}"

        gsutil -q cp "${partition_path}_MANIFEST.json" "${local_dir}/" || true
        gsutil -q ls "${partition_path}*.parquet" | head -n 3 | \
          xargs -I {} gsutil -q cp {} "${local_dir}/" || true
      done <<< "${partitions}"
    else
      partition_path="gs://${table_path}/ingest_dt=${ingest_dt}"
      local_dir="${DEST_DIR}/${table_name}/ingest_dt=${ingest_dt}"

      echo "  -> ${table_name}"
      mkdir -p "${local_dir}"

      gsutil -q cp "${partition_path}/_MANIFEST.json" "${local_dir}/" || true
      gsutil -q ls "${partition_path}/*.parquet" | head -n 3 | \
        xargs -I {} gsutil -q cp {} "${local_dir}/" || true
    fi
  done
done

echo "Done. Samples in ${DEST_DIR}."
