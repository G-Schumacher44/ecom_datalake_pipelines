#!/usr/bin/env python3
"""Silver Layer Quality Validation.

Validates Silver transformation quality by:
1. Comparing Bronze input vs. Silver output vs. Quarantine
2. Calculating pass rates per table
3. Validating against SLA thresholds
4. Analyzing quarantine reason distributions

Outputs:
- JSON metrics: data/metrics/silver_quality/{run_id}.json
- Markdown report: docs/validation_reports/SILVER_QUALITY.md
"""

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from datetime import datetime, timezone, date
from pathlib import Path
from typing import Any

import polars as pl
import pyarrow.parquet as pq
from pyarrow.lib import ArrowInvalid, ArrowTypeError

from src.observability import get_logger
from src.observability.metrics import write_silver_quality_metric
from src.settings import load_settings
from src.validation.common import (
    ValidationStatus,
    resolve_layer_paths,
    get_overall_status,
    handle_exit,
)

logger = get_logger(__name__)


@dataclass
class TableQualityMetrics:
    """Quality metrics for a single table."""

    table: str
    bronze_rows: int
    silver_rows: int
    quarantine_rows: int
    pass_rate: float
    sla_threshold: float
    status: str  # PASS, WARN, FAIL
    quarantine_breakdown: list[dict[str, Any]]
    row_loss: int
    row_loss_pct: float


@dataclass
class SilverQualityReport:
    """Overall Silver layer quality report."""

    run_id: str
    timestamp: str
    table_metrics: list[TableQualityMetrics]
    fk_mismatch_summary: list[dict[str, Any]]
    contract_issues: list[dict[str, Any]]
    overall_status: str
    tables_passing: int
    tables_warning: int
    tables_failing: int
    total_quarantined: int
    total_processed: int



def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Validate Silver layer transformation quality."
    )
    parser.add_argument(
        "--config",
        default="config/config.yml",
        help="Path to pipeline config YAML.",
    )
    parser.add_argument(
        "--bronze-path",
        default=None,
        help="Path to Bronze layer data (overrides env/config).",
    )
    parser.add_argument(
        "--silver-path",
        default=None,
        help="Path to Silver layer data (overrides env/config).",
    )
    parser.add_argument(
        "--quarantine-path",
        default=None,
        help="Path to quarantine data (overrides env/config).",
    )
    parser.add_argument(
        "--run-id",
        help="Run ID for this validation (auto-generated if not provided).",
    )
    parser.add_argument(
        "--tables",
        default=None,
        help="Comma-separated list of tables to validate (default: all).",
    )
    parser.add_argument(
        "--fail-on-sla-breach",
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
        default="docs/validation_reports/SILVER_QUALITY.md",
        help="Path to write Markdown report.",
    )
    return parser.parse_args()


def count_parquet_rows(path: Path) -> int:
    """Count total rows in all Parquet files in a directory.

    Args:
        path: Directory containing Parquet files (may be partitioned)

    Returns:
        Total row count across all files
    """
    if not path.exists():
        logger.warning(f"Path does not exist: {path}")
        return 0

    parquet_files = collect_parquet_files(path)
    if not parquet_files:
        logger.warning(f"No Parquet files found in: {path}")
        return 0

    total_rows = 0
    for file_path in parquet_files:
        try:
            parquet_file = pq.ParquetFile(file_path)
            total_rows += parquet_file.metadata.num_rows
        except (ArrowInvalid, ArrowTypeError, OSError, ValueError) as exc:
            logger.error(
                f"Failed to read {file_path}",
                error_type=type(exc).__name__,
                error=str(exc),
            )
            continue

    return total_rows


def get_quarantine_breakdown(quarantine_path: Path, top_n: int = 5) -> list[dict]:
    """Analyze quarantine reasons and return top failures.

    Args:
        quarantine_path: Path to quarantine Parquet files
        top_n: Number of top reasons to return

    Returns:
        List of dicts with reason, count, percentage
    """
    if not quarantine_path.exists():
        return []

    parquet_files = collect_parquet_files(quarantine_path)
    if not parquet_files:
        return []

    try:
        # Read all quarantine files
        df = pl.read_parquet(
            parquet_files,
            memory_map=False,
            low_memory=True,
            use_pyarrow=True,
        )

        if "invalid_reason" not in df.columns:
            logger.warning(f"No invalid_reason column in {quarantine_path}")
            return []

        # Filter out completely null rows (dbt-duckdb artifact from empty partition writes)
        # These rows have all fields NULL including computed fields like row_num
        if "row_num" in df.columns:
            df = df.filter(pl.col("row_num").is_not_null())

        if df.height == 0:
            return []

        # Count by reason
        reason_counts = (
            df.group_by("invalid_reason")
            .agg(pl.len().alias("count"))
            .sort("count", descending=True)
            .head(top_n)
        )

        total_quarantined = df.height

        # Convert to list of dicts
        breakdown = []
        for row in reason_counts.iter_rows(named=True):
            breakdown.append(
                {
                    "reason": row["invalid_reason"] or "unknown",
                    "count": row["count"],
                    "percentage": round((row["count"] / total_quarantined) * 100, 1),
                }
            )

        return breakdown

    except Exception as e:
        logger.error(
            f"Failed to analyze quarantine for {quarantine_path}",
            error_type=type(e).__name__,
            error=str(e),
        )
        return []


def compute_key_cardinality(table_path: Path, key: str) -> dict[str, float]:
    if not table_path.exists():
        return {
            "total_rows": 0,
            "non_null_rows": 0,
            "distinct_count": 0,
            "distinct_ratio": 0.0,
        }

    parquet_files = collect_parquet_files(table_path)
    if not parquet_files:
        return {
            "total_rows": 0,
            "non_null_rows": 0,
            "distinct_count": 0,
            "distinct_ratio": 0.0,
        }

    try:
        df = pl.read_parquet(
            parquet_files,
            columns=[key],
            memory_map=False,
            low_memory=True,
            use_pyarrow=True,
        )
    except Exception as exc:
        logger.warning(
            "Failed key cardinality scan",
            table=str(table_path),
            key=key,
            error_type=type(exc).__name__,
            error=str(exc),
        )
        return {
            "total_rows": 0,
            "non_null_rows": 0,
            "distinct_count": 0,
            "distinct_ratio": 0.0,
        }

    total_rows = df.height
    non_null_rows = df.select(pl.col(key).is_not_null().sum()).item()
    distinct_count = df.select(pl.col(key).drop_nulls().n_unique()).item()
    distinct_ratio = (
        distinct_count / non_null_rows if non_null_rows > 0 else 0.0
    )

    return {
        "total_rows": total_rows,
        "non_null_rows": non_null_rows,
        "distinct_count": distinct_count,
        "distinct_ratio": distinct_ratio,
    }


def validate_table(
    table: str,
    bronze_path: Path,
    silver_path: Path,
    quarantine_path: Path,
    sla_thresholds: dict[str, float],
) -> TableQualityMetrics:
    """Validate quality for a single table.

    Args:
        table: Table name
        bronze_path: Path to Bronze data
        silver_path: Path to Silver data
        quarantine_path: Path to quarantine data

    Returns:
        TableQualityMetrics
    """
    logger.info(f"Validating {table}...")

    # Count rows in each layer
    bronze_rows = count_parquet_rows(bronze_path / table)
    silver_rows = count_parquet_rows(silver_path / table)
    quarantine_rows = count_parquet_rows(quarantine_path / table)

    total_processed = silver_rows + quarantine_rows

    # Calculate pass rate
    if total_processed > 0:
        pass_rate = silver_rows / total_processed
    else:
        pass_rate = 0.0
        logger.warning(f"{table}: No rows processed!")

    # Get SLA threshold
    sla_threshold = sla_thresholds.get(table, 0.95)

    # Determine status
    if pass_rate >= sla_threshold:
        status = "PASS"
    elif pass_rate >= (sla_threshold * 0.9):  # Within 10% of SLA
        status = "WARN"
    else:
        status = "FAIL"

    # Analyze quarantine reasons
    quarantine_breakdown = get_quarantine_breakdown(quarantine_path / table)

    # Calculate row loss
    row_loss = bronze_rows - total_processed
    row_loss_pct = (row_loss / bronze_rows * 100) if bronze_rows > 0 else 0.0

    # Log results
    logger.info(
        f"{table}: {status}",
        bronze_rows=bronze_rows,
        silver_rows=silver_rows,
        quarantine_rows=quarantine_rows,
        pass_rate=f"{pass_rate:.2%}",
        sla=f"{sla_threshold:.2%}",
    )

    if status in ("WARN", "FAIL"):
        logger.warning(
            f"{table}: Pass rate {pass_rate:.2%} "
            f"{'below' if status == 'FAIL' else 'near'} "
            f"SLA {sla_threshold:.2%}"
        )

    if row_loss_pct > 1.0:
        logger.warning(f"{table}: Lost {row_loss_pct:.2%} of rows ({row_loss:,} rows)")

    return TableQualityMetrics(
        table=table,
        bronze_rows=bronze_rows,
        silver_rows=silver_rows,
        quarantine_rows=quarantine_rows,
        pass_rate=pass_rate,
        sla_threshold=sla_threshold,
        status=status,
        quarantine_breakdown=quarantine_breakdown,
        row_loss=row_loss,
        row_loss_pct=row_loss_pct,
    )


def generate_markdown_report(report: SilverQualityReport, output_path: Path) -> None:
    """Generate self-documenting Markdown report.

    Args:
        report: Silver quality report
        output_path: Where to write the Markdown file
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Determine overall emoji
    if report.overall_status == "PASS":
        overall_emoji = "✅"
    elif report.overall_status == "WARN":
        overall_emoji = "⚠️"
    else:
        overall_emoji = "❌"

    lines = [
        "# Silver Layer Quality Report",
        "",
        f"**Last Updated:** {report.timestamp}",
        f"**Run ID:** `{report.run_id}`",
        f"**Overall Status:** {overall_emoji} {report.overall_status}",
        "",
        "## Summary",
        "",
        "| Table | Bronze Rows | Silver Rows | Quarantine | Pass Rate | SLA | Status |",
        "|-------|-------------|-------------|------------|-----------|-----|--------|",
    ]

    # Add table rows
    for metrics in report.table_metrics:
        status_emoji = {
            "PASS": "✅",
            "WARN": "⚠️",
            "FAIL": "❌",
        }.get(metrics.status, "❓")

        lines.append(
            f"| {metrics.table} "
            f"| {metrics.bronze_rows:,} "
            f"| {metrics.silver_rows:,} "
            f"| {metrics.quarantine_rows:,} "
            f"| {metrics.pass_rate:.2%} "
            f"| {metrics.sla_threshold:.0%} "
            f"| {status_emoji} {metrics.status} |"
        )

    # Summary stats
    total_tables = len(report.table_metrics)
    pass_pct = (report.tables_passing / total_tables * 100) if total_tables > 0 else 0

    lines.extend(
        [
            "",
            (
                f"**Tables Passing SLA:** {report.tables_passing}/{total_tables} "
                f"({pass_pct:.1f}%)"
            ),
        ]
    )

    if report.tables_warning > 0:
        lines.append(f"**Tables with Warnings:** {report.tables_warning}")

    if report.tables_failing > 0:
        lines.append(f"**Tables Failing:** {report.tables_failing}")

    # Issues section
    failing_tables = [m for m in report.table_metrics if m.status == "FAIL"]
    warning_tables = [m for m in report.table_metrics if m.status == "WARN"]

    if failing_tables or warning_tables:
        lines.extend(["", "---", "", "## Issues Detected", ""])

        for metrics in failing_tables + warning_tables:
            emoji = "❌" if metrics.status == "FAIL" else "⚠️"
            lines.extend(
                [
                    (
                        f"### {emoji} {metrics.table}: Pass Rate "
                        f"{'Below' if metrics.status == 'FAIL' else 'Near'} SLA"
                    ),
                    "",
                    (
                        f"- **Pass Rate:** {metrics.pass_rate:.2%} "
                        f"(SLA: {metrics.sla_threshold:.0%})"
                    ),
                    f"- **Quarantine Count:** {metrics.quarantine_rows:,} rows",
                    "- **Recommended Action:** Investigate top quarantine reasons",
                    "",
                ]
            )

            if metrics.quarantine_breakdown:
                lines.extend(
                    [
                        "**Top Quarantine Reasons:**",
                        "",
                        "| Reason | Count | Percentage |",
                        "|--------|-------|------------|",
                    ]
                )

                for reason in metrics.quarantine_breakdown:
                    lines.append(
                        (
                            f"| {reason['reason']} | {reason['count']:,} | "
                            f"{reason['percentage']}% |"
                        )
                    )

                lines.append("")

    # Quarantine analysis
    lines.extend(["---", "", "## Quarantine Analysis", ""])

    total_quarantined = sum(m.quarantine_rows for m in report.table_metrics)
    total_quarantine_pct = (
        (total_quarantined / report.total_processed * 100)
        if report.total_processed > 0
        else 0
    )

    lines.extend(
        [
            "### Overall Quarantine Statistics",
            "",
            (
                f"**Total Quarantined:** {total_quarantined:,} rows across "
                f"{len(report.table_metrics)} tables "
                f"({total_quarantine_pct:.1f}% of total)"
            ),
            "",
            "### Quarantine Breakdown by Table",
            "",
        ]
    )

    for metrics in sorted(
        report.table_metrics, key=lambda m: m.quarantine_rows, reverse=True
    ):
        if metrics.quarantine_rows == 0:
            continue

        quar_rate = (
            (
                metrics.quarantine_rows
                / (metrics.silver_rows + metrics.quarantine_rows)
                * 100
            )
            if (metrics.silver_rows + metrics.quarantine_rows) > 0
            else 0
        )

        emoji = "⚠️" if quar_rate > 5.0 else ""

        lines.extend(
            [
                f"#### {metrics.table}",
                f"- **Quarantine Rate:** {quar_rate:.2f}% {emoji}",
            ]
        )

        if metrics.quarantine_breakdown:
            top_reason = metrics.quarantine_breakdown[0]
            lines.append(
                f"- **Top Reason:** {top_reason['reason']} "
                f"({top_reason['percentage']}%)"
            )
        else:
            # No breakdown means only dbt-duckdb placeholder rows (all NULL)
            lines.append("- **Top Reason:** empty partition placeholder (no real failures)")

        lines.append("")

    if report.fk_mismatch_summary:
        lines.extend(["---", "", "## FK Mismatch Summary", ""])
        lines.extend(
            [
                (
                    "| Child Table | Child Key | Parent Table | Parent Key "
                    "| Missing Rows |"
                ),
                "| --- | --- | --- | --- | --- |",
            ]
        )
        for row in report.fk_mismatch_summary:
            lines.append(
                f"| {row['child_table']} | {row['child_key']} | "
                f"{row['parent_table']} | {row['parent_key']} | "
                f"{row['missing_rows']:,} |"
            )
        lines.append("")

    if report.contract_issues:
        lines.extend(["---", "", "## Contract Issues", ""])
        for issue in report.contract_issues:
            lines.append(
                f"- **{issue['check']}**: {issue['message']}"
            )
        lines.append("")

    # Metadata footer
    lines.extend(
        [
            "---",
            "",
            "## Metadata",
            "",
            "- **Generated by:** `src/validation/silver_quality.py`",
            "- **Validation Framework:** 1.0.0",
            "- **Report Format Version:** 1.0",
            "",
            "<!-- AUTO-GENERATED - DO NOT EDIT MANUALLY -->",
        ]
    )

    # Write file
    output_path.write_text("\n".join(lines))
    logger.info(f"Wrote Markdown report to: {output_path}")


def build_profile_report(
    tables: list[str],
    silver_path: Path,
    output_path: Path,
) -> None:
    """Generate a lightweight Silver profile report."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    lines = [
        "# Silver Data Profile",
        "",
        (
            f"**Generated:** "
            f"{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}"
        ),
        "",
        "## Table Profiles",
        "",
    ]

    for table in tables:
        table_path = silver_path / table
        if not table_path.exists():
            lines.extend([f"### {table}", "", "- **Status:** No data path found", ""])
            continue

        parquet_files = list(table_path.glob("**/*.parquet"))
        if not parquet_files:
            lines.extend(
                [f"### {table}", "", "- **Status:** No Parquet files found", ""]
            )
            continue

        try:
            df = pl.read_parquet(
                parquet_files,
                memory_map=False,
                low_memory=True,
                use_pyarrow=True,
            )
            schema = df.schema
            col_names = list(schema.keys())
            row_count = df.height

            null_counts = df.null_count().to_dicts()[0]
        except Exception as exc:
            logger.warning(
                "Failed to profile table",
                table=table,
                error_type=type(exc).__name__,
                error=str(exc),
            )
            lines.extend(
                [f"### {table}", "", "- **Status:** Failed to profile table", ""]
            )
            continue

        lines.extend(
            [
                f"### {table}",
                "",
                f"- **Row Count:** {row_count:,}",
                f"- **Column Count:** {len(col_names)}",
                "",
                "**Schema (column → dtype):**",
                "",
                "| Column | Dtype | Nulls |",
                "|--------|-------|-------|",
            ]
        )

        for col in col_names:
            dtype = str(schema[col])
            nulls = null_counts.get(col, 0)
            lines.append(f"| {col} | {dtype} | {nulls:,} |")

        lines.append("")

    output_path.write_text("\n".join(lines))
    logger.info(f"Wrote Silver profile report to: {output_path}")


def compute_fk_mismatch_summary(silver_path: Path) -> list[dict[str, Any]]:
    fk_pairs = [
        ("order_items", "order_id", "orders", "order_id"),
        ("return_items", "order_id", "orders", "order_id"),
        ("return_items", "return_id", "returns", "return_id"),
        ("cart_items", "cart_id", "shopping_carts", "cart_id"),
    ]

    summary: list[dict[str, Any]] = []

    for child_table, child_key, parent_table, parent_key in fk_pairs:
        child_path = silver_path / child_table
        parent_path = silver_path / parent_table
        if not child_path.exists() or not parent_path.exists():
            continue

        try:
            child_files = collect_parquet_files(child_path)
            parent_files = collect_parquet_files(parent_path)
            if not child_files or not parent_files:
                continue
            child_keys = pl.read_parquet(
                child_files,
                memory_map=False,
                low_memory=True,
                use_pyarrow=True,
            ).select(pl.col(child_key).alias("key"))
            parent_keys = (
                pl.read_parquet(
                    parent_files,
                    memory_map=False,
                    low_memory=True,
                    use_pyarrow=True,
                )
                .select(pl.col(parent_key).alias("key"))
                .filter(pl.col("key").is_not_null())
                .unique()
            )
            missing_rows = (
                child_keys.filter(pl.col("key").is_not_null())
                .join(parent_keys, on="key", how="anti")
                .select(pl.len())
                .item()
            )
        except Exception as exc:
            logger.warning(
                "Failed FK mismatch summary",
                child_table=child_table,
                parent_table=parent_table,
                error_type=type(exc).__name__,
                error=str(exc),
            )
            continue

        if missing_rows > 0:
            summary.append(
                {
                    "child_table": child_table,
                    "child_key": child_key,
                    "parent_table": parent_table,
                    "parent_key": parent_key,
                    "missing_rows": missing_rows,
                }
            )

    return summary


def collect_parquet_files(path: Path) -> list[Path]:
    """Collect valid Parquet files under a path, skipping corrupt files."""
    candidates = list(path.glob("**/*.parquet"))
    if not candidates:
        return []

    valid_files = []
    invalid_files = []
    for file_path in candidates:
        if is_parquet_file(file_path):
            valid_files.append(file_path)
        else:
            invalid_files.append(file_path)

    if invalid_files:
        sample = ", ".join(str(p) for p in invalid_files[:3])
        logger.warning(
            "Skipping invalid parquet files",
            path=str(path),
            invalid_count=len(invalid_files),
            sample=sample,
        )

    return valid_files


def is_parquet_file(path: Path) -> bool:
    """Return True if file has Parquet magic bytes in header and footer."""
    try:
        with path.open("rb") as handle:
            header = handle.read(4)
            if header != b"PAR1":
                return False
            handle.seek(-4, 2)
            footer = handle.read(4)
            return footer == b"PAR1"
    except OSError:
        return False


def list_ingest_partitions(path: Path) -> set[str]:
    """Return ingest_dt partition values (YYYY-MM-DD) for a table path."""
    partitions = set()
    for part_dir in path.glob("ingest_dt=*"):
        if not part_dir.is_dir():
            continue
        partitions.add(part_dir.name.split("=", 1)[-1])
    return partitions


def expand_partition_ranges(values: list[str]) -> list[str]:
    """Expand YYYY-MM-DD or YYYY-MM-DD..YYYY-MM-DD range strings into dates."""
    expanded: list[str] = []
    for value in values:
        item = value.strip()
        if not item:
            continue
        if ".." not in item:
            expanded.append(item)
            continue
        start_str, end_str = item.split("..", 1)
        try:
            start = date.fromisoformat(start_str.strip())
            end = date.fromisoformat(end_str.strip())
        except ValueError:
            logger.warning("Invalid partition range ignored", range=item)
            continue
        if end < start:
            logger.warning("Partition range end before start", range=item)
            continue
        current = start
        while current <= end:
            expanded.append(current.isoformat())
            current = current.fromordinal(current.toordinal() + 1)
    return expanded


def read_parquet_safe(path: Path) -> pl.DataFrame | None:
    """Read a parquet file, returning None on failure."""
    try:
        return pl.read_parquet(
            path,
            memory_map=False,
            low_memory=True,
            use_pyarrow=True,
        )
    except (ArrowInvalid, ArrowTypeError, OSError, ValueError) as exc:
        logger.error(
            f"Failed to read {path}",
            error_type=type(exc).__name__,
            error=str(exc),
        )
        return None


def main() -> int:
    """Run Silver quality validation."""
    args = parse_args()
    settings = load_settings(args.config)
    sla_thresholds = settings.pipeline.sla_thresholds or {}
    pipeline_env = os.getenv("PIPELINE_ENV", settings.pipeline.environment).lower()
    
    # Resolve paths using shared library
    paths = resolve_layer_paths(
        args.config, 
        bronze_over=args.bronze_path, 
        silver_over=args.silver_path
    )
    bronze_path = paths["bronze"]
    silver_path = paths["silver"]
    quarantine_path = paths["quarantine"]

    run_id = args.run_id or datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    logger.info(f"Starting Silver quality validation (run_id={run_id})")

    # Validate all tables
    if args.tables:
        requested_tables = [t.strip() for t in args.tables.split(",") if t.strip()]
        tables = [t for t in requested_tables if t in sla_thresholds]
    else:
        tables = list(sla_thresholds.keys())
    
    table_metrics = []
    for table in tables:
        metrics = validate_table(
            table,
            bronze_path,
            silver_path,
            quarantine_path,
            sla_thresholds,
        )
        table_metrics.append(metrics)

    # Calculate overall status
    overall_status = get_overall_status([m.status for m in table_metrics])

    # Create report
    total_processed = sum(m.silver_rows + m.quarantine_rows for m in table_metrics)
    total_quarantined = sum(m.quarantine_rows for m in table_metrics)
    total_bronze_rows = sum(m.bronze_rows for m in table_metrics)
    total_row_loss = sum(m.row_loss for m in table_metrics)
    total_quarantine_pct = (
        (total_quarantined / total_processed * 100)
        if total_processed > 0
        else 0.0
    )
    total_row_loss_pct = (
        (total_row_loss / total_bronze_rows * 100)
        if total_bronze_rows > 0
        else 0.0
    )

    contract_issues: list[dict[str, Any]] = []
    if total_quarantine_pct > settings.pipeline.max_quarantine_pct:
        contract_issues.append(
            {
                "check": "max_quarantine_pct",
                "message": (
                    f"Quarantine rate {total_quarantine_pct:.2f}% exceeds "
                    f"threshold {settings.pipeline.max_quarantine_pct:.2f}%"
                ),
            }
        )
    if total_row_loss_pct > settings.pipeline.max_row_loss_pct:
        contract_issues.append(
            {
                "check": "max_row_loss_pct",
                "message": (
                    f"Row loss {total_row_loss_pct:.2f}% exceeds "
                    f"threshold {settings.pipeline.max_row_loss_pct:.2f}%"
                ),
            }
        )

    if settings.pipeline.expected_bronze_partitions:
        expected_set = set(expand_partition_ranges(settings.pipeline.expected_bronze_partitions))
        for table in tables:
            bronze_parts = list_ingest_partitions(bronze_path / table)
            missing = sorted(expected_set - bronze_parts)
            if missing:
                sample = ", ".join(missing[:5])
                contract_issues.append(
                    {
                        "check": "missing_bronze_partitions",
                        "table": table,
                        "message": (
                            f"Missing {len(missing)} ingest_dt partitions "
                            f"for {table}: {sample}"
                        ),
                    }
                )

    if settings.pipeline.min_table_rows:
        for metrics in table_metrics:
            min_rows = settings.pipeline.min_table_rows.get(metrics.table)
            if min_rows is None:
                continue
            total_processed = metrics.silver_rows + metrics.quarantine_rows
            if total_processed < min_rows:
                contract_issues.append(
                    {
                        "check": "min_table_rows",
                        "table": metrics.table,
                        "message": (
                            f"Processed {total_processed:,} rows for "
                            f"{metrics.table}, below minimum {min_rows:,}"
                        ),
                    }
                )

    if "returns" in tables:
        returns_cardinality = compute_key_cardinality(
            silver_path / "returns", "return_id"
        )
        if (
            returns_cardinality["non_null_rows"] > 0
            and returns_cardinality["distinct_ratio"] < settings.pipeline.min_return_id_distinct_ratio
        ):
            contract_issues.append(
                {
                    "check": "returns_return_id_distinct_ratio",
                    "message": (
                        f"Distinct ratio {returns_cardinality['distinct_ratio']:.6f} "
                        f"below minimum {settings.pipeline.min_return_id_distinct_ratio:.6f}"
                    ),
                }
            )

    if "return_items" in tables:
        return_items_cardinality = compute_key_cardinality(
            silver_path / "return_items", "return_id"
        )
        if (
            return_items_cardinality["non_null_rows"] > 0
            and return_items_cardinality["distinct_ratio"] < settings.pipeline.min_return_id_distinct_ratio
        ):
            contract_issues.append(
                {
                    "check": "return_items_return_id_distinct_ratio",
                    "message": (
                        "Distinct ratio "
                        f"{return_items_cardinality['distinct_ratio']:.6f} "
                        f"below minimum {settings.pipeline.min_return_id_distinct_ratio:.6f}"
                    ),
                }
            )

    if contract_issues:
        overall_status = "FAIL" if pipeline_env == "prod" else "WARN"

    tables_passing = sum(1 for m in table_metrics if m.status == "PASS")
    tables_warning = sum(1 for m in table_metrics if m.status == "WARN")
    tables_failing = sum(1 for m in table_metrics if m.status == "FAIL")

    report = SilverQualityReport(
        run_id=run_id,
        timestamp=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        table_metrics=table_metrics,
        fk_mismatch_summary=(
            compute_fk_mismatch_summary(silver_path)
            if not args.tables
            else []
        ),
        contract_issues=contract_issues,
        overall_status=overall_status,
        tables_passing=tables_passing,
        tables_warning=tables_warning,
        tables_failing=tables_failing,
        total_quarantined=total_quarantined,
        total_processed=total_processed,
    )

    # Write JSON metrics
    metrics_dicts = [
        {
            "table": m.table,
            "row_counts": {
                "bronze_input": m.bronze_rows,
                "silver_output": m.silver_rows,
                "quarantine_output": m.quarantine_rows,
                "total_processed": m.silver_rows + m.quarantine_rows,
                "row_loss": m.row_loss,
                "row_loss_pct": m.row_loss_pct,
            },
            "pass_rate": {
                "rate": m.pass_rate,
                "sla_threshold": m.sla_threshold,
                "status": m.status,
            },
            "quarantine_breakdown": m.quarantine_breakdown,
        }
        for m in table_metrics
    ]

    write_silver_quality_metric(
        run_id=run_id,
        table_metrics=metrics_dicts,
        overall_status=overall_status,
        contract_issues=contract_issues,
    )

    # Write Markdown report
    generate_markdown_report(report, Path(args.output_report))

    profile_enabled = os.getenv("SILVER_PROFILE_ENABLED", "false").lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    if profile_enabled:
        profile_path = Path(
            os.getenv(
                "SILVER_PROFILE_REPORT", "docs/validation_reports/SILVER_PROFILE.md"
            )
        )
        build_profile_report(tables, silver_path, profile_path)

    # Print summary
    print("\n" + "=" * 70)
    print(f"Silver Quality Validation Summary (run_id={run_id})")
    print("=" * 70)
    print(f"Overall Status: {overall_status}")
    print(f"Tables Passing: {tables_passing}/{len(tables)}")
    if tables_warning > 0:
        print(f"Tables Warning: {tables_warning}")
    if tables_failing > 0:
        print(f"Tables Failing: {tables_failing}")
    print(f"\nDetailed report: {args.output_report}")
    print("=" * 70 + "\n")

    # Determine exit code
    return handle_exit(
        overall_status=overall_status,
        enforce=args.enforce_quality or args.fail_on_sla_breach,
        env=pipeline_env
    )


if __name__ == "__main__":
    raise SystemExit(main())
