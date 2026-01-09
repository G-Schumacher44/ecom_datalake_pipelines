#!/usr/bin/env bash
set -euo pipefail

# Generate a Markdown report of bucket/prefix sizes and per-table sizes.
# Usage: ./scripts/report_bronze_sizes.sh [BUCKET] [PREFIX] [OUTPUT]
# Defaults read from config/config.yml when present.

CONFIG_PATH="${CONFIG_PATH:-config/config.yml}"

default_bucket="gcs-automation-project-raw"
default_prefix="ecom/raw"

if [[ -f "${CONFIG_PATH}" ]]; then
  read -r cfg_bucket cfg_prefix < <(
    python - <<'PY'
import yaml
from pathlib import Path

cfg = yaml.safe_load(Path("config/config.yml").read_text()) or {}
pipeline = cfg.get("pipeline", {})
print(pipeline.get("bronze_bucket", ""))
print(pipeline.get("bronze_prefix", ""))
PY
  )
  if [[ -n "${cfg_bucket}" ]]; then
    default_bucket="${cfg_bucket}"
  fi
  if [[ -n "${cfg_prefix}" ]]; then
    default_prefix="${cfg_prefix}"
  fi
fi

BUCKET="${1:-${default_bucket}}"
PREFIX="${2:-${default_prefix}}"
OUTPUT="${3:-docs/planning/planning/BRONZE_BUCKET_SIZES.md}"

bucket_uri="gs://${BUCKET}"
prefix_uri="${bucket_uri}/${PREFIX}"

human_size() {
  local bytes="$1"
  if command -v numfmt >/dev/null 2>&1; then
    numfmt --to=iec --suffix=B "$bytes"
  else
    echo "${bytes}B"
  fi
}

echo "Scanning bucket total: ${bucket_uri}"
bucket_total_bytes=$(gsutil du -s "${bucket_uri}" | awk '{print $1}')

echo "Scanning prefix total: ${prefix_uri}"
prefix_total_bytes=$(gsutil du -s "${prefix_uri}" | awk '{print $1}')

report_date=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

{
  echo "# Bronze Bucket Size Report"
  echo ""
  echo "Generated: ${report_date}"
  echo ""
  echo "## Totals"
  echo ""
  echo "- Bucket: \`${bucket_uri}\`"
  echo "  - Total: $(human_size "${bucket_total_bytes}") (${bucket_total_bytes} bytes)"
  echo "- Prefix: \`${prefix_uri}\`"
  echo "  - Total: $(human_size "${prefix_total_bytes}") (${prefix_total_bytes} bytes)"
  echo ""
  echo "## Per-Table Sizes"
  echo ""
  echo "| Table | Size (human) | Size (bytes) |"
  echo "| --- | --- | --- |"

  gsutil ls "${prefix_uri}/" | while read -r table_path; do
    table_name=$(basename "${table_path}")
    echo "Scanning table: ${table_name}"
    table_bytes=$(gsutil du -s "${table_path}" | awk '{print $1}')
    echo "| ${table_name} | $(human_size "${table_bytes}") | ${table_bytes} |"
  done
} > "${OUTPUT}"

echo "Wrote ${OUTPUT}"
