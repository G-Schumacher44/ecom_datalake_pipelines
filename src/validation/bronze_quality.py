#!/usr/bin/env python3
"""Bronze Layer Quality Validation.

Validates Bronze inputs by:
1. Scanning manifest files per partition
2. Aggregating row counts per table
3. Reporting missing manifests / empty partitions

Outputs:
- JSON metrics: data/metrics/data_quality/bronze_quality_{run_id}.json
- Markdown report: docs/validation_reports/BRONZE_QUALITY.md
"""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from src.observability import get_logger
from src.observability.metrics import write_data_quality_metric

logger = get_logger(__name__)


EXPECTED_TABLES = [
    "orders",
    "order_items",
    "customers",
    "product_catalog",
    "shopping_carts",
    "cart_items",
    "returns",
    "return_items",
]


PARTITION_GLOBS = {
    "customers": ("signup_date", "signup_date=*"),
    "product_catalog": ("category", "category=*"),
}


@dataclass
class TableMetrics:
    table: str
    partitions: int
    manifests: int
    rows: int
    missing_manifests: int
    empty_partitions: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate Bronze layer inputs.")
    parser.add_argument(
        "--bronze-path",
        default="samples/bronze",
        help="Path to Bronze data (local path or gs:// bucket).",
    )
    parser.add_argument(
        "--fail-on-issues",
        action="store_true",
        help="Exit non-zero if missing manifests or empty partitions are found.",
    )
    parser.add_argument(
        "--run-id",
        help="Run ID for this validation (auto-generated if not provided).",
    )
    parser.add_argument(
        "--output-report",
        default="docs/validation_reports/BRONZE_QUALITY.md",
        help="Path to write Markdown report.",
    )
    return parser.parse_args()


def is_gcs_path(path: str) -> bool:
    return path.startswith("gs://")


def bronze_qa_required() -> bool:
    env_override = os.getenv("BRONZE_QA_REQUIRED")
    if env_override is not None:
        return env_override.lower() in {"true", "1", "yes"}
    pipeline_env = os.getenv("PIPELINE_ENV", "local").lower()
    return pipeline_env in {"dev", "prod"}


def bronze_qa_fail() -> bool:
    env_override = os.getenv("BRONZE_QA_FAIL")
    if env_override is not None:
        return env_override.lower() in {"true", "1", "yes"}
    pipeline_env = os.getenv("PIPELINE_ENV", "local").lower()
    return pipeline_env in {"prod"}


def list_tables(root: str) -> list[str]:
    if is_gcs_path(root):
        try:
            import fsspec
        except ModuleNotFoundError as exc:
            if not bronze_qa_required():
                logger.warning("gcsfs not installed; skipping bronze QA for GCS path")
                return []
            raise RuntimeError("gcsfs is required for GCS bronze QA") from exc

        fs = fsspec.filesystem("gcs")
        entries = fs.ls(root, detail=True)
        return sorted(
            [
                e["name"].rstrip("/").split("/")[-1]
                for e in entries
                if e["type"] == "directory"
            ]
        )

    return sorted([p.name for p in Path(root).iterdir() if p.is_dir()])


def list_partitions(root: str, table: str) -> list[str]:
    partition_key, partition_glob = PARTITION_GLOBS.get(
        table, ("ingest_dt", "ingest_dt=*")
    )
    if is_gcs_path(root):
        import fsspec

        fs = fsspec.filesystem("gcs")
        return sorted(fs.glob(f"{root}/{table}/{partition_glob}"))

    return sorted(str(p) for p in Path(root, table).glob(partition_glob) if p.is_dir())


def read_manifest(path: str) -> dict[str, Any] | None:
    manifest_name = "_MANIFEST.json"
    if is_gcs_path(path):
        import fsspec

        fs = fsspec.filesystem("gcs")
        manifest_path = f"{path}/{manifest_name}"
        if not fs.exists(manifest_path):
            return None
        with fs.open(manifest_path) as handle:
            return json.load(handle)

    manifest_file = Path(path) / manifest_name
    if not manifest_file.exists():
        return None
    return json.loads(manifest_file.read_text())


def validate_table(root: str, table: str) -> TableMetrics:
    partitions = list_partitions(root, table)
    manifests = 0
    rows = 0
    missing_manifests = 0
    empty_partitions = 0

    for partition_path in partitions:
        manifest = read_manifest(partition_path)
        if manifest is None:
            missing_manifests += 1
            continue
        manifests += 1
        rows += int(manifest.get("total_rows", 0))
        if int(manifest.get("total_rows", 0)) == 0:
            empty_partitions += 1

    return TableMetrics(
        table=table,
        partitions=len(partitions),
        manifests=manifests,
        rows=rows,
        missing_manifests=missing_manifests,
        empty_partitions=empty_partitions,
    )


def generate_report(
    metrics: list[TableMetrics], output_path: Path, run_id: str
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    lines = [
        "# Bronze Quality Report",
        "",
        f"**Last Updated:** {timestamp}",
        f"**Run ID:** `{run_id}`",
        "",
        "## Summary",
        "",
        (
            "| Table | Partitions | Manifests | Rows | Missing Manifests | "
            "Empty Partitions |"
        ),
        "| --- | --- | --- | --- | --- | --- |",
    ]

    for m in metrics:
        lines.append(
            f"| {m.table} | {m.partitions} | {m.manifests} | {m.rows:,} | "
            f"{m.missing_manifests} | {m.empty_partitions} |"
        )

    total_missing = sum(m.missing_manifests for m in metrics)
    total_empty = sum(m.empty_partitions for m in metrics)
    lines.extend(
        [
            "",
            "## Checks",
            "",
            f"- **Missing manifests:** {total_missing}",
            f"- **Empty partitions:** {total_empty}",
            "",
            "---",
            "",
            "## Metadata",
            "",
            "- **Generated by:** `src/validation/bronze_quality.py`",
            "- **Report Format Version:** 1.0",
            "",
            "<!-- AUTO-GENERATED - DO NOT EDIT MANUALLY -->",
        ]
    )

    output_path.write_text("\n".join(lines))
    logger.info(f"Wrote Markdown report to: {output_path}")


def main() -> int:
    args = parse_args()

    run_id = args.run_id or datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    bronze_root = args.bronze_path

    logger.info(f"Starting Bronze quality validation (run_id={run_id})")

    available_tables = list_tables(bronze_root)
    tables = [t for t in EXPECTED_TABLES if t in available_tables]

    metrics = [validate_table(bronze_root, table) for table in tables]

    # Overall status: WARN if any missing manifests or empty partitions.
    overall_status = "PASS"
    total_missing = sum(m.missing_manifests for m in metrics)
    total_empty = sum(m.empty_partitions for m in metrics)
    if total_missing > 0 or total_empty > 0:
        overall_status = "WARN"

    metric_payloads = [
        {
            "table": m.table,
            "row_count": {
                "actual": m.rows,
                "status": "PASS" if m.rows > 0 else "WARN",
            },
            "manifest_check": {
                "missing": m.missing_manifests,
                "status": "PASS" if m.missing_manifests == 0 else "WARN",
            },
            "empty_partitions": {
                "count": m.empty_partitions,
                "status": "PASS" if m.empty_partitions == 0 else "WARN",
            },
        }
        for m in metrics
    ]

    write_data_quality_metric(
        run_id=run_id,
        validation_type="bronze_quality",
        table_metrics=metric_payloads,
        overall_status=overall_status,
    )

    generate_report(metrics, Path(args.output_report), run_id)
    logger.info("✅ Bronze quality validation completed")

    fail_on_issues = args.fail_on_issues or bronze_qa_fail()
    if fail_on_issues and (total_missing > 0 or total_empty > 0):
        logger.error(
            "Bronze quality validation failed due to missing manifests or empty partitions"
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
