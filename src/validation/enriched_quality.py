#!/usr/bin/env python3
"""Enriched Silver Layer Quality Validation.

Validates Enriched Silver outputs by:
1. Checking ingest_dt partition presence per table
2. Counting rows for the target partition
3. Enforcing minimum row counts (optional)

Outputs:
- JSON metrics: data/metrics/enriched_silver/enriched_silver_{run_id}.json
- Markdown report: docs/validation_reports/ENRICHED_QUALITY.md
"""

from __future__ import annotations

import argparse
import os
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import polars as pl
import pyarrow.parquet as pq
from pyarrow.lib import ArrowInvalid, ArrowTypeError

from src.observability import get_logger
from src.observability.metrics import write_enriched_quality_metric
from src.settings import load_settings, PipelineConfig, ValidationConfig
from src.validation.common import (
    ValidationStatus,
    get_overall_status,
    handle_exit,
    resolve_layer_paths,
)

logger = get_logger(__name__)

DEFAULT_ENRICHED_TABLES = [
    "int_attributed_purchases",
    "int_cart_attribution",
    "int_churn_detection",
    "int_customer_lifetime_value",
    "int_daily_business_metrics",
    "int_inventory_risk",
    "int_product_performance",
    "int_regional_financials",
    "int_sales_velocity",
    "int_shipping_economics",
]


@dataclass
class EnrichedTableMetrics:
    table: str
    partition_key: str
    ingest_dt: str | None
    row_count: int
    min_rows: int | None
    prior_row_count: int | None
    row_delta_pct: float | None
    schema_snapshot: dict[str, str]
    null_rates: dict[str, float]
    sanity_issues: list[str]
    semantic_issues: list[str]
    status: str  # PASS, WARN, FAIL
    notes: list[str]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate Enriched Silver outputs.")
    parser.add_argument(
        "--config",
        default="config/config.yml",
        help="Path to pipeline config YAML.",
    )
    parser.add_argument(
        "--enriched-path",
        default=None,
        help="Path to Enriched Silver data (overrides env/config).",
    )
    parser.add_argument(
        "--run-id",
        help="Run ID for this validation (auto-generated if not provided).",
    )
    parser.add_argument(
        "--ingest-dt",
        default=None,
        help="Target partition date (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--fail-on-issues",
        action="store_true",
        help="Legacy: use --enforce-quality instead.",
    )
    parser.add_argument(
        "--enforce-quality",
        action="store_true",
        help="Exit with non-zero code on any failures (standard gate).",
    )
    parser.add_argument(
        "--output-report",
        default="docs/validation_reports/ENRICHED_QUALITY.md",
        help="Path to write Markdown report.",
    )
    return parser.parse_args()


def collect_parquet_files(path: Path) -> list[Path]:
    candidates = list(path.glob("**/*.parquet"))
    if not candidates:
        return []

    valid_files = []
    for file_path in candidates:
        try:
            with file_path.open("rb") as handle:
                header = handle.read(4)
                if header != b"PAR1":
                    continue
                handle.seek(-4, 2)
                if handle.read(4) != b"PAR1":
                    continue
            valid_files.append(file_path)
        except OSError:
            continue

    return valid_files


def count_parquet_rows(path: Path) -> int:
    if not path.exists():
        return 0
    parquet_files = collect_parquet_files(path)
    if not parquet_files:
        return 0

    total_rows = 0
    for file_path in parquet_files:
        try:
            parquet_file = pq.ParquetFile(file_path)
            total_rows += parquet_file.metadata.num_rows
        except (ArrowInvalid, ArrowTypeError, OSError, ValueError) as exc:
            logger.warning(
                "Failed to read parquet",
                path=str(file_path),
                error_type=type(exc).__name__,
                error=str(exc),
            )
            continue
    return total_rows


def list_partitions(path: Path, partition_key: str) -> list[str]:
    partitions = []
    for part_dir in path.glob(f"{partition_key}=*"):
        if part_dir.is_dir():
            partitions.append(part_dir.name.split("=", 1)[-1])
    return sorted(partitions)


def resolve_partition(
    table_path: Path, partition_key: str, ingest_dt: str | None
) -> tuple[str | None, Path | None]:
    if not table_path.exists():
        return ingest_dt, None
    if ingest_dt:
        partition_path = table_path / f"{partition_key}={ingest_dt}"
        return ingest_dt, partition_path if partition_path.exists() else None
    partitions = list_partitions(table_path, partition_key)
    if not partitions:
        return None, None
    latest = partitions[-1]
    return latest, table_path / f"{partition_key}={latest}"


def read_parquet_safe(
    path: Path,
    columns: list[str] | None = None,
    n_rows: int | None = None,
) -> pl.DataFrame | None:
    parquet_files = collect_parquet_files(path)
    if not parquet_files:
        return None
    try:
        return pl.read_parquet(
            parquet_files,
            columns=columns,
            n_rows=n_rows,
            memory_map=False,
            low_memory=True,
            use_pyarrow=False,
        )
    except (ArrowInvalid, ArrowTypeError, OSError, ValueError) as exc:
        logger.warning(
            "Failed to read parquet",
            path=str(path),
            error_type=type(exc).__name__,
            error=str(exc),
        )
        return None


def get_schema_snapshot(path: Path) -> dict[str, str]:
    parquet_files = collect_parquet_files(path)
    if not parquet_files:
        return {}
    try:
        parquet_file = pq.ParquetFile(parquet_files[0])
    except (ArrowInvalid, ArrowTypeError, OSError, ValueError) as exc:
        logger.warning(
            "Failed to read parquet schema",
            path=str(parquet_files[0]),
            error_type=type(exc).__name__,
            error=str(exc),
        )
        return {}
    schema = parquet_file.schema_arrow
    return {field.name: str(field.type) for field in schema}


def compute_null_rates(df: pl.DataFrame, columns: list[str]) -> dict[str, float]:
    null_rates: dict[str, float] = {}
    for column in columns:
        if column not in df.columns:
            continue
        total = df.height
        if total == 0:
            null_rates[column] = 0.0
            continue
        nulls = df.select(pl.col(column).is_null().sum()).item()
        null_rates[column] = round(nulls / total, 6)
    return null_rates


def evaluate_sanity_checks(df: pl.DataFrame, config: ValidationConfig) -> list[str]:
    issues: list[str] = []
    sanity = config.sanity_checks
    for column in sanity.get("non_negative", []):
        if column not in df.columns:
            continue
        negatives = df.select((pl.col(column) < 0).sum()).item()
        if negatives:
            issues.append(f"{column}: {negatives} negative")

    for column in sanity.get("rate_0_1", []):
        if column not in df.columns:
            continue
        out_of_range = df.select(
            ((pl.col(column) < 0) | (pl.col(column) > 1)).sum()
        ).item()
        if out_of_range:
            issues.append(f"{column}: {out_of_range} outside_0_1")
    return issues


def evaluate_semantic_checks(
    df: pl.DataFrame,
    table: str,
    config: ValidationConfig,
    ratio_epsilon: float,
) -> list[str]:
    checks = config.semantic_checks.get(table, [])
    issues: list[str] = []
    for check in checks:
        name = check["name"]
        expr = check["expr"].format(ratio_epsilon=ratio_epsilon)
        try:
            failures = df.filter(pl.sql_expr(expr)).height
        except Exception as exc:
            issues.append(f"{name}: failed_to_evaluate ({type(exc).__name__})")
            continue
        if failures:
            issues.append(f"{name}: {failures} rows")
    return issues


def compute_row_delta(
    table_path: Path,
    partition_key: str,
    ingest_dt: str | None,
    current_rows: int,
) -> tuple[int | None, float | None]:
    partitions = list_partitions(table_path, partition_key)
    if not partitions or ingest_dt not in partitions:
        return None, None
    idx = partitions.index(ingest_dt)
    if idx == 0:
        return None, None
    prior_partition = table_path / f"{partition_key}={partitions[idx - 1]}"
    prior_rows = count_parquet_rows(prior_partition)
    if prior_rows == 0:
        return prior_rows, None
    delta_pct = round(((current_rows - prior_rows) / prior_rows) * 100, 2)
    return prior_rows, delta_pct


def validate_table(
    table: str,
    enriched_path: Path,
    ingest_dt: str | None,
    min_rows: int | None,
    pipeline_env: str,
    settings: PipelineConfig,
) -> EnrichedTableMetrics:
    table_path = enriched_path / table
    notes: list[str] = []
    status = "PASS"
    config = settings.validation
    partition_key = settings.enriched_partitions.get(table, "ingest_dt")
    ratio_epsilon = settings.enriched_ratio_epsilon

    schema_snapshot: dict[str, str] = {}
    null_rates: dict[str, float] = {}
    sanity_issues: list[str] = []
    semantic_issues: list[str] = []
    prior_rows: int | None = None
    row_delta_pct: float | None = None

    resolved_ingest_dt, partition_path = resolve_partition(
        table_path, partition_key, ingest_dt
    )
    if partition_path is None:
        notes.append("missing_ingest_partition")
        status = "FAIL" if pipeline_env == "prod" else "WARN"
        return EnrichedTableMetrics(
            table=table,
            partition_key=partition_key,
            ingest_dt=resolved_ingest_dt,
            row_count=0,
            min_rows=min_rows,
            prior_row_count=prior_rows,
            row_delta_pct=row_delta_pct,
            schema_snapshot=schema_snapshot,
            null_rates=null_rates,
            sanity_issues=sanity_issues,
            semantic_issues=semantic_issues,
            status=status,
            notes=notes,
        )

    row_count = count_parquet_rows(partition_path)
    prior_rows, row_delta_pct = compute_row_delta(
        table_path, partition_key, resolved_ingest_dt, row_count
    )

    schema_snapshot = get_schema_snapshot(partition_path)
    key_fields = config.key_fields.get(table, [])
    if schema_snapshot:
        available = set(schema_snapshot.keys())
        semantic_columns: list[str] = []
        for check in config.semantic_checks.get(table, []):
            tokens = (
                check["expr"]
                .replace("(", " ")
                .replace(")", " ")
                .replace("/", " ")
                .replace("*", " ")
                .replace("+", " ")
                .replace("-", " ")
                .split()
            )
            for token in tokens:
                if token.isidentifier():
                    semantic_columns.append(token)

        check_columns = (
            key_fields
            + config.sanity_checks.get("non_negative", [])
            + config.sanity_checks.get("rate_0_1", [])
            + semantic_columns
        )
        selected = []
        for col in check_columns:
            if col in available and col not in selected:
                selected.append(col)
        if selected:
            df = read_parquet_safe(partition_path, columns=selected)
            if df is not None:
                null_rates = compute_null_rates(df, key_fields)
                sanity_issues = evaluate_sanity_checks(df, config)
                semantic_issues = evaluate_semantic_checks(
                    df, table, config, ratio_epsilon
                )

    if row_count == 0:
        notes.append("empty_partition")
        status = "FAIL" if pipeline_env == "prod" else "WARN"

    if min_rows is not None and row_count < min_rows:
        notes.append(f"below_min_rows:{row_count}<{min_rows}")
        status = "FAIL" if pipeline_env == "prod" else "WARN"

    if sanity_issues:
        notes.append("sanity_checks_failed")
        status = "FAIL" if pipeline_env == "prod" else "WARN"
    if semantic_issues:
        notes.append("semantic_checks_failed")
        status = "FAIL" if pipeline_env == "prod" else "WARN"

    return EnrichedTableMetrics(
        table=table,
        partition_key=partition_key,
        ingest_dt=resolved_ingest_dt,
        row_count=row_count,
        min_rows=min_rows,
        prior_row_count=prior_rows,
        row_delta_pct=row_delta_pct,
        schema_snapshot=schema_snapshot,
        null_rates=null_rates,
        sanity_issues=sanity_issues,
        semantic_issues=semantic_issues,
        status=status,
        notes=notes,
    )


def generate_markdown_report(
    run_id: str,
    timestamp: str,
    table_metrics: list[EnrichedTableMetrics],
    overall_status: str,
    output_path: Path,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    status_emoji = {"PASS": "✅", "WARN": "⚠️", "FAIL": "❌"}

    lines = [
        "# Enriched Silver Quality Report",
        "",
        f"**Last Updated:** {timestamp}",
        f"**Run ID:** `{run_id}`",
        f"**Overall Status:** {status_emoji.get(overall_status, '❓')} {overall_status}",
        "",
        "## Summary",
        "",
        "| Table | Partition | Value | Rows | Min Rows | Δ vs prior | Status | Notes |",
        "|-------|-----------|-------|------|----------|-----------|--------|-------|",
    ]

    for metrics in table_metrics:
        notes = ", ".join(metrics.notes) if metrics.notes else "-"
        min_rows = metrics.min_rows if metrics.min_rows is not None else "-"
        delta_display = "-"
        if metrics.row_delta_pct is not None:
            delta_display = f"{metrics.row_delta_pct:+.2f}%"
        lines.append(
            f"| {metrics.table} | {metrics.partition_key} | {metrics.ingest_dt or '-'} | "
            f"{metrics.row_count:,} | {min_rows} | {delta_display} | "
            f"{status_emoji.get(metrics.status, '❓')} {metrics.status} | {notes} |"
        )

    failing = [m for m in table_metrics if m.status == "FAIL"]
    warning = [m for m in table_metrics if m.status == "WARN"]
    if failing or warning:
        lines.extend(["", "---", "", "## Issues Detected", ""])
        for metrics in failing + warning:
            emoji = status_emoji.get(metrics.status, "❓")
            lines.append(f"### {emoji} {metrics.table}")
            if metrics.ingest_dt:
                lines.append(
                    f"- **Partition:** {metrics.partition_key}={metrics.ingest_dt}"
                )
            lines.append(f"- **Rows:** {metrics.row_count:,}")
            if metrics.min_rows is not None:
                lines.append(f"- **Min Rows:** {metrics.min_rows}")
            if metrics.row_delta_pct is not None:
                lines.append(f"- **Row Delta vs prior:** {metrics.row_delta_pct:+.2f}%")
            if metrics.notes:
                lines.append(f"- **Notes:** {', '.join(metrics.notes)}")
            lines.append("")

    lines.extend(["---", "", "## Schema & Null Rates", ""])
    for metrics in table_metrics:
        lines.append(f"### {metrics.table}")
        if metrics.schema_snapshot:
            lines.append("")
            lines.append("**Schema (column → dtype):**")
            lines.append("")
            lines.append("| Column | Dtype |")
            lines.append("|--------|-------|")
            for col, dtype in metrics.schema_snapshot.items():
                lines.append(f"| {col} | {dtype} |")
        else:
            lines.append("")
            lines.append("- **Schema:** unavailable")
        if metrics.null_rates:
            lines.append("")
            lines.append("**Key Field Null Rates:**")
            lines.append("")
            lines.append("| Column | Null Rate |")
            lines.append("|--------|----------|")
            for col, rate in metrics.null_rates.items():
                lines.append(f"| {col} | {rate:.2%} |")
        else:
            lines.append("")
            lines.append("- **Key Field Null Rates:** unavailable")
        if metrics.sanity_issues:
            lines.append("")
            lines.append(f"**Sanity Issues:** {', '.join(metrics.sanity_issues)}")
        if metrics.semantic_issues:
            lines.append("")
            lines.append(f"**Semantic Issues:** {', '.join(metrics.semantic_issues)}")
        lines.append("")

    lines.extend(
        [
            "---",
            "",
            "## Metadata",
            "",
            "- **Generated by:** `src/validation/enriched_quality.py`",
            "- **Report Format Version:** 1.0",
            "",
            "<!-- AUTO-GENERATED - DO NOT EDIT MANUALLY -->",
        ]
    )

    content = "\n".join(lines)
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        delete=False,
        dir=output_path.parent,
        prefix=f".{output_path.name}.",
        suffix=".tmp",
    ) as handle:
        handle.write(content)
        temp_name = handle.name

    os.replace(temp_name, output_path)
    logger.info(f"Wrote Markdown report to: {output_path}")


def main() -> int:
    args = parse_args()
    settings = load_settings(args.config)
    pipeline_env = os.getenv("PIPELINE_ENV", settings.pipeline.environment).lower()

    run_id = args.run_id or datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    paths = resolve_layer_paths(
        config_path=args.config, enriched_over=args.enriched_path
    )

    enriched_path = paths["enriched"]

    tables = settings.pipeline.enriched_tables or DEFAULT_ENRICHED_TABLES
    min_rows_map = settings.pipeline.enriched_min_table_rows or {}

    table_metrics = [
        validate_table(
            table=table,
            enriched_path=enriched_path,
            ingest_dt=args.ingest_dt,
            min_rows=min_rows_map.get(table),
            pipeline_env=pipeline_env,
            settings=settings.pipeline,
        )
        for table in tables
    ]

    overall_status = "PASS"
    if any(m.status == "FAIL" for m in table_metrics):
        overall_status = "FAIL"
    elif any(m.status == "WARN" for m in table_metrics):
        overall_status = "WARN"

    metrics_payloads = [
        {
            "table": m.table,
            "partition_key": m.partition_key,
            "ingest_dt": m.ingest_dt,
            "row_count": m.row_count,
            "min_rows": m.min_rows,
            "prior_row_count": m.prior_row_count,
            "row_delta_pct": m.row_delta_pct,
            "schema_snapshot": m.schema_snapshot,
            "null_rates": m.null_rates,
            "sanity_issues": m.sanity_issues,
            "semantic_issues": m.semantic_issues,
            "status": m.status,
            "notes": m.notes,
        }
        for m in table_metrics
    ]

    write_enriched_quality_metric(
        run_id=run_id,
        table_metrics=metrics_payloads,
        overall_status=overall_status,
    )

    generate_markdown_report(
        run_id=run_id,
        timestamp=timestamp,
        table_metrics=table_metrics,
        overall_status=overall_status,
        output_path=Path(args.output_report),
    )

    return handle_exit(
        overall_status=overall_status,
        enforce=args.enforce_quality or args.fail_on_issues,
        env=pipeline_env,
    )


if __name__ == "__main__":
    raise SystemExit(main())
