"""Shared helpers for Enriched Silver runners."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from functools import wraps
import os
from typing import Any, Callable, Dict, Sequence

import polars as pl

from src.validation.base_silver_schemas import BASE_SILVER_SCHEMAS
from src.settings import load_settings, PipelineConfig


def enriched_runner(
    output_table: str,
    input_tables: Sequence[str],
):
    """Decorator to standardize Enriched Silver runner boilerplate."""

    def decorator(
        func: Callable[[Dict[str, pl.LazyFrame], PipelineConfig, str], pl.LazyFrame]
    ):
        @wraps(func)
        def wrapper(
            base_silver_path: str,
            output_path: str,
            ingest_dt: str = "2020-01-01",
        ) -> Dict[str, Any]:
            start_time = datetime.now()
            settings = load_settings()
            lookback_days = settings.pipeline.enriched_lookback_days

            # 1. Read input tables
            tables = {
                table: read_partitioned(
                    base_silver_path, table, ingest_dt, lookback_days
                )
                for table in input_tables
            }

            # 2. Execute transform
            result_lazy = func(tables, settings.pipeline, ingest_dt)

            # 3. Add lineage and materialize
            result_lazy = result_lazy.with_columns(ingest_dt=pl.lit(ingest_dt))
            result = result_lazy.collect()

            # 4. Write output
            partition_col = get_enriched_partitions()[output_table]
            write_partitioned_shards(
                result,
                output_path,
                output_table,
                partition_col,
                settings.pipeline.enriched_max_rows_per_file,
            )

            elapsed = (datetime.now() - start_time).total_seconds()
            return {
                "table": output_table,
                "output_rows": len(result),
                "processing_time_seconds": elapsed,
                "output_path": f"{output_path}/{output_table}",
            }

        return wrapper

    return decorator


def get_table_partitions() -> dict[str, str | None]:
    return load_settings().pipeline.table_partitions


def get_enriched_partitions() -> dict[str, str]:
    return load_settings().pipeline.enriched_partitions


def is_gcs_path(path: str) -> bool:
    return path.startswith("gs://")


def list_partitions(
    base_path: str,
    table: str,
    partition_key: str,
) -> list[str]:
    if is_gcs_path(base_path):
        try:
            import fsspec
        except ModuleNotFoundError as exc:
            raise RuntimeError(
                "fsspec is required for GCS partition discovery"
            ) from exc
        fs = fsspec.filesystem("gcs")
        matches = fs.glob(f"{base_path}/{table}/{partition_key}=*")
        return sorted({m.split(f"{partition_key}=")[-1].rstrip("/") for m in matches})

    table_path = os.path.join(base_path, table)
    if not os.path.isdir(table_path):
        return []
    partitions = []
    for entry in os.listdir(table_path):
        if entry.startswith(f"{partition_key}="):
            partitions.append(entry.split("=", 1)[-1])
    return sorted(partitions)


def partition_range(ingest_dt: str, lookback_days: int) -> list[str]:
    end = date.fromisoformat(ingest_dt)
    start = end - timedelta(days=max(lookback_days, 0))
    current = start
    partitions = []
    while current <= end:
        partitions.append(current.isoformat())
        current += timedelta(days=1)
    return partitions


def read_partitioned(
    base_path: str,
    table: str,
    ingest_dt: str,
    lookback_days: int,
) -> pl.LazyFrame:
    partition_key = get_table_partitions().get(table)
    if partition_key is None:
        return pl.scan_parquet(
            f"{base_path}/{table}/**/*.parquet",
            hive_partitioning=True,
            schema=BASE_SILVER_SCHEMAS.get(table),
        )

    available = set(list_partitions(base_path, table, partition_key))
    desired = [
        dt for dt in partition_range(ingest_dt, lookback_days) if dt in available
    ]
    if not desired:
        raise FileNotFoundError(
            f"No {partition_key} partitions found for {table} at {base_path}"
        )

    paths = [f"{base_path}/{table}/{partition_key}={dt}/**/*.parquet" for dt in desired]
    return pl.scan_parquet(
        paths,
        hive_partitioning=True,
        schema=BASE_SILVER_SCHEMAS.get(table),
    )


def ensure_output_dir(path: str) -> None:
    if path.startswith("gs://"):
        return
    os.makedirs(path, exist_ok=True)


def output_file(path: str) -> str:
    path = path.rstrip("/")
    return f"{path}/part-00000.parquet"


def write_sharded_parquet(
    df: pl.DataFrame,
    output_uri: str,
    max_rows_per_file: int,
) -> None:
    output_uri = output_uri.rstrip("/")
    ensure_output_dir(output_uri)

    if max_rows_per_file <= 0:
        df.write_parquet(output_file(output_uri))
        return

    if df.height == 0:
        df.head(0).write_parquet(output_file(output_uri))
        return

    if df.height <= max_rows_per_file:
        df.write_parquet(output_file(output_uri))
        return

    shard_index = 0
    for offset in range(0, df.height, max_rows_per_file):
        chunk = df.slice(offset, max_rows_per_file)
        filename = f"{output_uri}/part-{shard_index:05d}.parquet"
        chunk.write_parquet(filename)
        shard_index += 1


def normalize_partition_values(df: pl.DataFrame, partition_col: str) -> pl.DataFrame:
    dtype = df.schema.get(partition_col)
    if dtype is None:
        return df
    if dtype == pl.Date:
        return df.with_columns(
            pl.col(partition_col).dt.strftime("%Y-%m-%d").alias(partition_col)
        )
    if dtype == pl.Datetime:
        return df.with_columns(
            pl.col(partition_col)
            .cast(pl.Date)
            .dt.strftime("%Y-%m-%d")
            .alias(partition_col)
        )
    return df.with_columns(pl.col(partition_col).cast(pl.Utf8))


def write_partitioned_shards(
    df: pl.LazyFrame | pl.DataFrame,
    output_path: str,
    table: str,
    partition_col: str,
    max_rows_per_file: int,
) -> None:
    """Write partitioned parquet data supporting both LazyFrame and DataFrame inputs.

    If a LazyFrame is provided, prefer fully streaming sink_parquet operations.
    If a DataFrame is provided, fall back to the existing eager sharding logic.
    """

    # Handle LazyFrame path – fully streaming
    if isinstance(df, pl.LazyFrame):
        output_base = f"{output_path.rstrip('/')}/{table}"
        ensure_output_dir(output_base)

        # Normalize partition column values lazily
        df = df.with_columns(pl.col(partition_col).cast(pl.Utf8).alias(partition_col))

        # Use Polars native partitioned streaming write
        df.sink_parquet(
            f"{output_base}",
            compression="snappy",
            partition_by=[partition_col],
        )
        return

    # ---- Existing eager fallback for DataFrame inputs ----
    df = normalize_partition_values(df, partition_col)

    if partition_col not in df.columns:
        raise ValueError(f"Missing partition column: {partition_col}")

    partitions = (
        df.select(pl.col(partition_col).drop_nulls().unique()).to_series().to_list()
    )

    if not partitions:
        partitions = ["unknown"]
        df = df.with_columns(pl.lit("unknown").alias(partition_col))

    for value in partitions:
        chunk = df.filter(pl.col(partition_col) == value)
        output_uri = f"{output_path}/{table}/{partition_col}={value}/"
        write_sharded_parquet(chunk, output_uri, max_rows_per_file)


__all__ = [
    "get_table_partitions",
    "get_enriched_partitions",
    "read_partitioned",
    "write_partitioned_shards",
    "enriched_runner",
]
