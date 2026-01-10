#!/usr/bin/env python3
"""Profile Parquet samples from local bronze extracts."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Iterable

import polars as pl


@dataclass(frozen=True)
class PartitionProfile:
    table: str
    partition: str
    files: list[Path]
    row_count: int
    schema: dict[str, str]
    column_stats: list[dict[str, object]]


def parse_date(value: str) -> date | None:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        return None


def matches_partition(
    partition: str,
    ingest_dts: set[str],
    months: set[str],
    start_date: date | None,
    end_date: date | None,
) -> bool:
    if ingest_dts and partition in ingest_dts:
        return True
    if months and partition[:7] in months:
        return True
    if start_date and end_date:
        parsed = parse_date(partition)
        return parsed is not None and start_date <= parsed <= end_date
    return not (ingest_dts or months or (start_date and end_date))


def find_parquet_files(
    root: Path,
    max_files: int,
    tables_filter: set[str],
    ingest_dts: set[str],
    months: set[str],
    start_date: date | None,
    end_date: date | None,
) -> dict[str, dict[str, list[Path]]]:
    tables: dict[str, dict[str, list[Path]]] = {}
    for table_dir in root.iterdir():
        if not table_dir.is_dir():
            continue
        if tables_filter and table_dir.name not in tables_filter:
            continue
        partitions: dict[str, list[Path]] = {}
        for partition_dir in sorted(table_dir.glob("ingest_dt=*")):
            if not partition_dir.is_dir():
                continue
            partition_value = partition_dir.name.split("=", 1)[-1]
            if not matches_partition(
                partition_value, ingest_dts, months, start_date, end_date
            ):
                continue
            parquet_files = sorted(partition_dir.glob("*.parquet"))
            if not parquet_files:
                continue
            partitions[partition_value] = parquet_files[:max_files]
        if partitions:
            tables[table_dir.name] = partitions
    return tables


def col_stats(series: pl.Series, row_count: int) -> dict[str, object]:
    null_count = int(series.null_count())
    null_pct = round(null_count / row_count * 100, 2) if row_count else 0.0
    distinct = int(series.n_unique())
    min_val = None
    max_val = None
    percentiles = None
    top_values = None

    # Numeric/temporal columns: get min/max and percentiles
    if series.dtype.is_numeric() or series.dtype.is_temporal():
        min_val = series.min()
        max_val = series.max()
        # Get percentiles for numeric columns
        if series.dtype.is_numeric() and row_count > 0:
            try:
                p25 = series.quantile(0.25, interpolation="nearest")
                p50 = series.quantile(0.50, interpolation="nearest")
                p75 = series.quantile(0.75, interpolation="nearest")
                p95 = series.quantile(0.95, interpolation="nearest")
                percentiles = {
                    "p25": p25,
                    "p50": p50,
                    "p75": p75,
                    "p95": p95,
                }
            except Exception:
                percentiles = None

    # String columns: get top 5 values
    elif series.dtype == pl.Utf8 and row_count > 0:
        try:
            value_counts = series.value_counts(sort=True).head(5)
            top_values = [
                (row[0], int(row[1]))
                for row in value_counts.iter_rows()
            ]
        except Exception:
            top_values = None

    return {
        "column": series.name,
        "dtype": str(series.dtype),
        "null_pct": null_pct,
        "distinct": distinct,
        "min": min_val,
        "max": max_val,
        "percentiles": percentiles,
        "top_values": top_values,
    }


def profile_partition(
    files: list[Path], max_rows: int
) -> tuple[int, dict[str, str], list[dict[str, object]]]:
    frames: list[pl.DataFrame] = []
    for file_path in files:
        if max_rows > 0:
            frames.append(pl.read_parquet(file_path, n_rows=max_rows))
        else:
            frames.append(pl.read_parquet(file_path))
    if not frames:
        return 0, {}, []
    df = pl.concat(frames, how="diagonal")
    schema = {name: str(dtype) for name, dtype in df.schema.items()}
    row_count = df.height
    stats = [col_stats(df[column], row_count) for column in df.columns]
    return row_count, schema, stats


def render_markdown(profiles: Iterable[PartitionProfile]) -> str:
    lines = [
        "# Bronze Sample Profile Report",
        "",
        "Generated from local parquet samples in `samples/bronze/`.",
        "",
    ]
    profiles_list = list(profiles)
    schema_keys: dict[tuple[str, str], str] = {}
    for profile in profiles_list:
        schema_key = "|".join(f"{name}:{dtype}" for name, dtype in profile.schema.items())
        schema_keys[(profile.table, profile.partition)] = schema_key

    # Build top-level summary
    total_rows = sum(p.row_count for p in profiles_list)
    unique_tables = len(set(p.table for p in profiles_list))
    unique_partitions = len(set(p.partition for p in profiles_list))

    # Calculate per-table totals
    table_row_counts: dict[str, int] = {}
    table_partition_counts: dict[str, int] = {}
    for profile in profiles_list:
        table_row_counts[profile.table] = table_row_counts.get(profile.table, 0) + profile.row_count
        table_partition_counts[profile.table] = table_partition_counts.get(profile.table, 0) + 1

    lines.append("## Overview")
    lines.append("")
    lines.append(f"- **Tables sampled**: {unique_tables}")
    lines.append(f"- **Partitions sampled**: {unique_partitions}")
    lines.append(f"- **Total sample rows**: {total_rows:,}")
    lines.append("")
    lines.append("### Per-Table Summary")
    lines.append("")
    lines.append("| Table | Partitions | Sample Rows |")
    lines.append("| --- | --- | --- |")
    for table in sorted(table_row_counts.keys()):
        lines.append(
            f"| {table} | {table_partition_counts[table]} | {table_row_counts[table]:,} |"
        )
    lines.append("")

    # Data Quality Flags
    lines.append("### Data Quality Flags")
    lines.append("")
    quality_flags: list[tuple[str, str, str, str]] = []

    # Check for high null percentages
    for profile in profiles_list:
        for stat in profile.column_stats:
            if stat["null_pct"] > 50:
                quality_flags.append(
                    (
                        profile.table,
                        profile.partition,
                        f"high_nulls|{profile.table}|{stat['column']}",
                        f"⚠️ **{profile.table}.{stat['column']}**: {stat['null_pct']}% nulls (>50%)",
                    )
                )

    # Check for low cardinality on PRIMARY entity ID columns only
    # Exclude: metadata columns, lookup IDs (agent, region, tier, status, etc.)
    metadata_columns = {"batch_id", "ingestion_ts", "event_id", "source_file"}
    lookup_id_patterns = {"agent_id", "region_id", "tier_id", "status_id", "warehouse_id", "store_id"}

    for profile in profiles_list:
        for stat in profile.column_stats:
            col_name = stat["column"].lower()
            # Skip metadata columns
            if stat["column"] in metadata_columns:
                continue
            # Skip known lookup IDs (low cardinality is expected)
            if stat["column"] in lookup_id_patterns:
                continue

            # Flag PRIMARY entity ID columns with suspiciously low cardinality
            # These are typically table-named IDs like customer_id, order_id, product_id
            is_primary_id = (
                col_name.endswith("_id") and
                any(col_name.startswith(entity) for entity in [
                    "customer", "order", "product", "cart", "return", "item"
                ])
            )

            if is_primary_id and stat["distinct"] < 10 and stat["null_pct"] < 100:
                quality_flags.append(
                    (
                        profile.table,
                        profile.partition,
                        f"low_cardinality_id|{profile.table}|{stat['column']}",
                        f"⚠️ **{profile.table}.{stat['column']}**: Only {stat['distinct']} distinct values (expected high cardinality for primary entity ID)",
                    )
                )

    # Check for suspicious cardinality mismatches
    for profile in profiles_list:
        stat_dict = {s["column"]: s for s in profile.column_stats}
        # Check product_id vs product_name mismatches
        if "product_id" in stat_dict and "product_name" in stat_dict:
            prod_id_distinct = stat_dict["product_id"]["distinct"]
            prod_name_distinct = stat_dict["product_name"]["distinct"]
            if prod_name_distinct > prod_id_distinct * 1.5:
                quality_flags.append(
                    (
                        profile.table,
                        profile.partition,
                        f"cardinality_mismatch|{profile.table}|product_id_vs_product_name",
                        f"⚠️ **{profile.table}**: More product names than product IDs (possible duplicates/variations)",
                    )
                )

    # Check for volume anomalies (partition row count spikes)
    table_partition_rows: dict[str, list[tuple[str, int]]] = {}
    for profile in profiles_list:
        table_partition_rows.setdefault(profile.table, []).append(
            (profile.partition, profile.row_count)
        )

    for table, partition_rows in table_partition_rows.items():
        if len(partition_rows) > 3:
            row_counts = [r for _, r in partition_rows]
            avg_rows = sum(row_counts) / len(row_counts)
            for partition, rows in partition_rows:
                if rows > avg_rows * 1.5:
                    quality_flags.append(
                        (
                            table,
                            partition,
                            f"volume_spike|{table}|{partition}",
                            f"📊 **{table}** partition `{partition}`: {rows:,} rows (+{int((rows/avg_rows - 1) * 100)}% above average)",
                        )
                    )

    if quality_flags:
        # Aggregate by issue key (not exact message text)
        aggregated: dict[str, dict[str, object]] = {}
        for table, partition, issue_key, message in quality_flags:
            if issue_key not in aggregated:
                aggregated[issue_key] = {
                    "message": message,
                    "count": 0,
                    "partitions": set(),
                }
            entry = aggregated[issue_key]
            entry["count"] = int(entry["count"]) + 1
            partitions = entry["partitions"]
            if isinstance(partitions, set):
                partitions.add(f"{table}:{partition}")

        sorted_flags = sorted(
            aggregated.items(),
            key=lambda item: int(item[1]["count"]),
            reverse=True,
        )
        for issue_key, meta in sorted_flags[:10]:
            partitions = sorted(list(meta["partitions"]))[:5]
            partition_sample = ", ".join(partitions)
            lines.append(
                f"- {meta['message']} (count={meta['count']}, samples={partition_sample})"
            )
    else:
        lines.append("- ✅ No major data quality issues detected")

    lines.append("")

    lines.append("## Schema Drift")
    lines.append("")
    drift_found = False
    table_schemas: dict[str, dict[str, set[str]]] = {}
    table_schema_counts: dict[str, dict[str, int]] = {}
    for (table, partition), schema_key in schema_keys.items():
        table_schemas.setdefault(table, {}).setdefault(schema_key, set()).add(partition)
        table_schema_counts.setdefault(table, {}).setdefault(schema_key, 0)
        table_schema_counts[table][schema_key] += 1
    for table, schema_map in sorted(table_schemas.items()):
        if len(schema_map) <= 1:
            continue
        drift_found = True
        lines.append(f"### {table}")
        lines.append("")
        for schema_key, partitions in sorted(schema_map.items()):
            lines.append(f"- Schema key: `{schema_key}`")
            lines.append(f"  - Partitions: {', '.join(sorted(partitions))}")
        lines.append("")
    if not drift_found:
        lines.append("- No schema drift detected across sampled partitions.")
        lines.append("")

    lines.append("## Partition Coverage")
    lines.append("")
    lines.append("Shows which partitions were sampled per table (for temporal schema drift detection).")
    lines.append("")

    # Group partitions by table
    table_partitions: dict[str, list[str]] = {}
    for profile in profiles_list:
        table_partitions.setdefault(profile.table, []).append(profile.partition)

    for table, partitions in sorted(table_partitions.items()):
        sorted_partitions = sorted(set(partitions))
        # Group by year-month for readability
        grouped = {}
        for partition in sorted_partitions:
            year_month = partition[:7]  # YYYY-MM
            grouped.setdefault(year_month, []).append(partition)

        lines.append(f"**{table}** ({len(sorted_partitions)} partitions):")
        for year_month in sorted(grouped.keys()):
            dates = grouped[year_month]
            if len(dates) <= 5:
                lines.append(f"- `{year_month}`: {', '.join(dates)}")
            else:
                lines.append(f"- `{year_month}`: {dates[0]} ... {dates[-1]} ({len(dates)} days)")
        lines.append("")

    lines.append("## Canonical Schema Keys")
    lines.append("")
    lines.append("| Table | Canonical Schema Key | Sample Partitions |")
    lines.append("| --- | --- | --- |")
    for table, counts in sorted(table_schema_counts.items()):
        canonical_key = max(counts.items(), key=lambda item: item[1])[0]
        partitions = sorted(table_schemas.get(table, {}).get(canonical_key, set()))
        sample_partitions = ", ".join(partitions[:5])
        lines.append(f"| {table} | `{canonical_key}` | {sample_partitions} |")
    lines.append("")

    lines.append("## Column Statistics (Sample Partition)")
    lines.append("")
    lines.append("Showing detailed stats for one representative partition per table.")
    lines.append("")

    # Group profiles by table and pick first partition per table
    table_profiles: dict[str, PartitionProfile] = {}
    for profile in profiles_list:
        if profile.table not in table_profiles:
            table_profiles[profile.table] = profile

    for table, profile in sorted(table_profiles.items()):
        lines.append(f"### {table}")
        lines.append("")
        lines.append(f"**Sample**: `ingest_dt={profile.partition}` ({profile.row_count:,} rows)")
        lines.append("")
        lines.append("| Column | Type | Null % | Distinct | Stats |")
        lines.append("| --- | --- | --- | --- | --- |")

        for stat in profile.column_stats:
            # Build stats cell content
            stats_parts = []

            # Min/Max for numeric/temporal
            if stat["min"] is not None and stat["max"] is not None:
                stats_parts.append(f"Range: `{stat['min']}` to `{stat['max']}`")

            # Percentiles for numeric
            if stat["percentiles"]:
                p = stat["percentiles"]
                stats_parts.append(
                    f"p25={p['p25']}, p50={p['p50']}, p75={p['p75']}, p95={p['p95']}"
                )

            # Top values for strings
            if stat["top_values"]:
                top_str = ", ".join(
                    [f"`{val}` ({cnt})" for val, cnt in stat["top_values"][:3]]
                )
                stats_parts.append(f"Top: {top_str}")

            stats_cell = "<br>".join(stats_parts) if stats_parts else "—"

            lines.append(
                "| {column} | {dtype} | {null_pct}% | {distinct:,} | {stats} |".format(
                    column=stat["column"],
                    dtype=stat["dtype"],
                    null_pct=stat["null_pct"],
                    distinct=stat["distinct"],
                    stats=stats_cell,
                )
            )
        lines.append("")

    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Profile parquet samples.")
    parser.add_argument("--root", default="samples/bronze", help="Sample root directory")
    parser.add_argument("--max-files", type=int, default=1, help="Max parquet files per partition")
    parser.add_argument(
        "--max-rows", type=int, default=100000, help="Max rows per file sample (0=all)"
    )
    parser.add_argument(
        "--tables",
        default="",
        help="Comma-separated table names to include (optional)",
    )
    parser.add_argument(
        "--ingest-dts",
        default="",
        help="Comma-separated ingest_dt values (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--months",
        default="",
        help="Comma-separated months (YYYY-MM)",
    )
    parser.add_argument(
        "--date-range",
        default="",
        help="Date range in YYYY-MM-DD..YYYY-MM-DD format",
    )
    parser.add_argument(
        "--output",
        default="docs/planning/planning/BRONZE_PROFILE_REPORT.md",
        help="Output path for Markdown report",
    )
    args = parser.parse_args()

    root = Path(args.root)
    if not root.exists():
        raise SystemExit(f"Root not found: {root}")

    tables_filter = {name.strip() for name in args.tables.split(",") if name.strip()}
    ingest_dts = {dt.strip() for dt in args.ingest_dts.split(",") if dt.strip()}
    months = {month.strip() for month in args.months.split(",") if month.strip()}
    start_date = end_date = None
    if args.date_range:
        start_str, end_str = args.date_range.split("..")
        start_date = parse_date(start_str)
        end_date = parse_date(end_str)

    tables = find_parquet_files(
        root,
        args.max_files,
        tables_filter,
        ingest_dts,
        months,
        start_date,
        end_date,
    )

    profiles: list[PartitionProfile] = []
    for table_name, partitions in sorted(tables.items()):
        for partition, files in sorted(partitions.items()):
            row_count, schema, stats = profile_partition(files, args.max_rows)
            profiles.append(
                PartitionProfile(
                    table=table_name,
                    partition=partition,
                    files=files,
                    row_count=row_count,
                    schema=schema,
                    column_stats=stats,
                )
            )

    markdown = render_markdown(profiles)
    Path(args.output).write_text(markdown)
    print(f"Wrote {args.output}")


if __name__ == "__main__":
    main()
