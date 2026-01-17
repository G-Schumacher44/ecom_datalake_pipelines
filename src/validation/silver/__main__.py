#!/usr/bin/env python3
"""Silver Layer Quality Validation (Refactored)."""

from __future__ import annotations

import argparse
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.observability import get_logger
from src.observability.metrics import write_silver_quality_metric
from src.settings import load_settings
from src.validation.common import (
    get_overall_status,
    handle_exit,
    resolve_layer_paths,
)
from src.validation.silver.data import list_ingest_partitions
from src.validation.silver.metrics import compute_fk_mismatch_summary, validate_table
from src.validation.silver.models import SilverQualityReport
from src.validation.silver.report import build_profile_report, generate_markdown_report

logger = get_logger(__name__)

def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Validate Silver layer transformation quality.")
    parser.add_argument("--config", default="config/config.yml", help="Path to pipeline config YAML.")
    parser.add_argument("--bronze-path", default=None, help="Path to Bronze layer data (overrides env/config).")
    parser.add_argument("--silver-path", default=None, help="Path to Silver layer data (overrides env/config).")
    parser.add_argument("--quarantine-path", default=None, help="Path to quarantine data (overrides env/config).")
    parser.add_argument("--run-id", help="Run ID for this validation (auto-generated if not provided).")
    parser.add_argument("--tables", default=None, help="Comma-separated list of tables to validate (default: all).")
    parser.add_argument("--fail-on-sla-breach", action="store_true", help="Legacy: use --enforce-quality instead.")
    parser.add_argument("--enforce-quality", action="store_true", help="Exit with non-zero code on any failures (standard gate).")
    parser.add_argument("--output-report", default="docs/validation_reports/SILVER_QUALITY.md", help="Path to write Markdown report.")
    return parser.parse_args()

def expand_partition_ranges(values: list[str]) -> list[str]:
    """Expand YYYY-MM-DD or YYYY-MM-DD..YYYY-MM-DD range strings into dates."""
    from datetime import date
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

def main() -> int:
    """Run Silver quality validation."""
    args = parse_args()
    settings = load_settings(args.config)
    sla_thresholds = settings.pipeline.sla_thresholds or {}
    pipeline_env = os.getenv("PIPELINE_ENV", settings.pipeline.environment).lower()
    
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

    overall_status = get_overall_status([m.status for m in table_metrics])

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
        contract_issues.append({
            "check": "max_quarantine_pct",
            "message": f"Quarantine rate {total_quarantine_pct:.2f}% exceeds threshold {settings.pipeline.max_quarantine_pct:.2f}%",
        })
    if total_row_loss_pct > settings.pipeline.max_row_loss_pct:
        contract_issues.append({
            "check": "max_row_loss_pct",
            "message": f"Row loss {total_row_loss_pct:.2f}% exceeds threshold {settings.pipeline.max_row_loss_pct:.2f}%",
        })

    if settings.pipeline.expected_bronze_partitions:
        expected_set = set(expand_partition_ranges(settings.pipeline.expected_bronze_partitions))
        for table in tables:
            bronze_parts = list_ingest_partitions(bronze_path / table)
            missing = sorted(expected_set - bronze_parts)
            if missing:
                sample = ", ".join(missing[:5])
                contract_issues.append({
                    "check": "missing_bronze_partitions",
                    "table": table,
                    "message": f"Missing {len(missing)} ingest_dt partitions for {table}: {sample}",
                })

    if settings.pipeline.min_table_rows:
        for metrics in table_metrics:
            min_rows = settings.pipeline.min_table_rows.get(metrics.table)
            if min_rows is None:
                continue
            processed_count = metrics.silver_rows + metrics.quarantine_rows
            if processed_count < min_rows:
                contract_issues.append({
                    "check": "min_table_rows",
                    "table": metrics.table,
                    "message": f"Processed {processed_count:,} rows for {metrics.table}, below minimum {min_rows:,}",
                })

    # Note: Cardinality checks logic could also be moved to metrics.py, but for brevity keeping it here or in metrics.
    # The original monolith had compute_key_cardinality inside itself. 
    # I moved compute_key_cardinality to data.py, so we can use it here.
    from src.validation.silver.data import compute_key_cardinality
    
    if "returns" in tables:
        returns_cardinality = compute_key_cardinality(silver_path / "returns", "return_id")
        if (
            returns_cardinality["non_null_rows"] > 0
            and returns_cardinality["distinct_ratio"] < settings.pipeline.min_return_id_distinct_ratio
        ):
            contract_issues.append({
                "check": "returns_return_id_distinct_ratio",
                "message": f"Distinct ratio {returns_cardinality['distinct_ratio']:.6f} below minimum {settings.pipeline.min_return_id_distinct_ratio:.6f}",
            })

    if "return_items" in tables:
        return_items_cardinality = compute_key_cardinality(silver_path / "return_items", "return_id")
        if (
            return_items_cardinality["non_null_rows"] > 0
            and return_items_cardinality["distinct_ratio"] < settings.pipeline.min_return_id_distinct_ratio
        ):
            contract_issues.append({
                "check": "return_items_return_id_distinct_ratio",
                "message": f"Distinct ratio {return_items_cardinality['distinct_ratio']:.6f} below minimum {settings.pipeline.min_return_id_distinct_ratio:.6f}",
            })

    if contract_issues:
        overall_status = "FAIL" if pipeline_env == "prod" else "WARN"

    tables_passing = sum(1 for m in table_metrics if m.status == "PASS")
    tables_warning = sum(1 for m in table_metrics if m.status == "WARN")
    tables_failing = sum(1 for m in table_metrics if m.status == "FAIL")

    report = SilverQualityReport(
        run_id=run_id,
        timestamp=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        table_metrics=table_metrics,
        fk_mismatch_summary=(compute_fk_mismatch_summary(silver_path) if not args.tables else []),
        contract_issues=contract_issues,
        overall_status=overall_status,
        tables_passing=tables_passing,
        tables_warning=tables_warning,
        tables_failing=tables_failing,
        total_quarantined=total_quarantined,
        total_processed=total_processed,
    )

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

    generate_markdown_report(report, Path(args.output_report))

    profile_enabled = os.getenv("SILVER_PROFILE_ENABLED", "false").lower() in {"1", "true", "yes", "on"}
    if profile_enabled:
        profile_path = Path(os.getenv("SILVER_PROFILE_REPORT", "docs/validation_reports/SILVER_PROFILE.md"))
        build_profile_report(tables, silver_path, profile_path)

    logger.info("=" * 70)
    logger.info(f"Silver Quality Validation Summary (run_id={run_id})")
    logger.info("=" * 70)
    logger.info(f"Overall Status: {overall_status}")
    logger.info(f"Tables Passing: {tables_passing}/{len(tables)}")
    if tables_warning > 0:
        logger.warning(f"Tables Warning: {tables_warning}")
    if tables_failing > 0:
        logger.error(f"Tables Failing: {tables_failing}")
    logger.info(f"Detailed report: {args.output_report}")
    logger.info("=" * 70)

    return handle_exit(
        overall_status=overall_status,
        enforce=args.enforce_quality or args.fail_on_sla_breach,
        env=pipeline_env
    )

if __name__ == "__main__":
    raise SystemExit(main())
