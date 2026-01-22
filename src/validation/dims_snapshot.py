"""Validate dims snapshot partitions."""

from __future__ import annotations

import argparse
import os
from datetime import datetime, timezone
from pathlib import Path

import polars as pl

from src.observability import get_logger
from src.specs import load_spec_safe

logger = get_logger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate dims snapshots.")
    parser.add_argument(
        "--dims-path",
        default=None,
        help="Path to dims snapshot root (overrides env/spec).",
    )
    parser.add_argument(
        "--run-date",
        required=True,
        help="Snapshot date (YYYY-MM-DD) to validate.",
    )
    parser.add_argument(
        "--tables",
        default=None,
        help="Comma-separated list of tables to validate.",
    )
    parser.add_argument(
        "--output-report",
        default="docs/validation_reports/DIMS_SNAPSHOT.md",
        help="Path to write Markdown report.",
    )
    parser.add_argument(
        "--enforce-quality",
        action="store_true",
        help="Exit non-zero when any table fails.",
    )
    parser.add_argument(
        "--spec-path",
        default=None,
        help="Path to spec directory or file (overrides ECOM_SPEC_PATH).",
    )
    return parser.parse_args()


def resolve_dims_path(args: argparse.Namespace) -> str:
    if args.dims_path:
        return args.dims_path
    spec = load_spec_safe()
    if spec and spec.dims.base_path:
        return os.path.expandvars(spec.dims.base_path)
    return os.getenv("SILVER_DIMS_PATH", "data/silver/dims")


def list_tables(args: argparse.Namespace) -> list[tuple[str, str]]:
    if args.tables:
        return [(t.strip(), "snapshot_dt") for t in args.tables.split(",") if t.strip()]
    spec = load_spec_safe()
    if spec:
        return [(t.name, t.partition_key) for t in spec.dims.tables]
    return [("customers", "snapshot_dt"), ("product_catalog", "snapshot_dt")]


def count_rows(path: Path) -> int:
    if not path.exists():
        return 0
    return pl.scan_parquet(str(path / "*.parquet")).select(pl.len()).collect().item()


def main() -> int:
    args = parse_args()
    if args.spec_path:
        os.environ["ECOM_SPEC_PATH"] = args.spec_path
    dims_path = resolve_dims_path(args)
    if dims_path.startswith("gs://"):
        dims_path = os.getenv(
            "SILVER_DIMS_LOCAL_PATH", "/opt/airflow/data/silver/dims"
        )
    dims_path = Path(dims_path)
    tables = list_tables(args)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    rows = []
    failures = []
    for table, partition_key in tables:
        partition_path = dims_path / table / f"{partition_key}={args.run_date}"
        row_count = count_rows(partition_path)
        status = "PASS" if row_count > 0 else "FAIL"
        rows.append((table, status, row_count, str(partition_path)))
        if status == "FAIL":
            failures.append(table)

    report_lines = [
        f"# Dims Snapshot Validation ({run_id})",
        "",
        f"Run date: `{args.run_date}`",
        "",
        "| table | status | rows | path |",
        "| --- | --- | ---: | --- |",
    ]
    for table, status, row_count, path in rows:
        report_lines.append(f"| {table} | {status} | {row_count} | `{path}` |")
    from src.validation.common import resolve_reports_enabled

    if resolve_reports_enabled(args.spec_path):
        Path(args.output_report).write_text("\n".join(report_lines), encoding="utf-8")

    if failures:
        logger.error("Dims snapshot validation failed", tables=failures)
        return 1 if args.enforce_quality else 0
    logger.info("Dims snapshot validation passed", tables=[t for t, _ in tables])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
