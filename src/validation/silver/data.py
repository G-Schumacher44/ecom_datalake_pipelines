from __future__ import annotations

import logging
from pathlib import Path

import polars as pl
from polars.exceptions import ColumnNotFoundError, ComputeError, SchemaError
from pyarrow.lib import ArrowInvalid, ArrowTypeError

from src.validation.common import collect_parquet_files

logger = logging.getLogger(__name__)


def get_quarantine_breakdown(
    quarantine_path: Path,
    top_n: int = 5,
    partition_key: str | None = None,
    partitions: list[str] | None = None,
) -> list[dict]:
    """Analyze quarantine reasons and return top failures."""
    if not quarantine_path.exists():
        return []

    parquet_files = collect_parquet_files(
        quarantine_path, partition_key=partition_key, partitions=partitions
    )
    if not parquet_files:
        return []

    try:
        df = pl.read_parquet(
            parquet_files,
            memory_map=False,
            low_memory=True,
            use_pyarrow=True,
        )

        if "invalid_reason" not in df.columns:
            logger.warning(f"No invalid_reason column in {quarantine_path}")
            return []

        if "row_num" in df.columns:
            df = df.filter(pl.col("row_num").is_not_null())

        if df.height == 0:
            return []

        reason_counts = (
            df.group_by("invalid_reason")
            .agg(pl.len().alias("count"))
            .sort("count", descending=True)
            .head(top_n)
        )

        total_quarantined = df.height
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

    except (ArrowInvalid, ArrowTypeError, OSError) as e:
        logger.error(
            f"Failed to read quarantine parquet at {quarantine_path}",
            extra={
                "error_type": type(e).__name__,
                "error": str(e),
            },
        )
        return []
    except (ColumnNotFoundError, SchemaError, ComputeError) as e:
        logger.error(
            f"Failed to analyze quarantine schema for {quarantine_path}",
            extra={
                "error_type": type(e).__name__,
                "error": str(e),
            },
        )
        return []


def compute_key_cardinality(
    table_path: Path,
    key: str,
    partition_key: str | None = None,
    partitions: list[str] | None = None,
) -> dict[str, float]:
    if not table_path.exists():
        return {
            "total_rows": 0,
            "non_null_rows": 0,
            "distinct_count": 0,
            "distinct_ratio": 0.0,
        }

    parquet_files = collect_parquet_files(
        table_path, partition_key=partition_key, partitions=partitions
    )
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
    except (ArrowInvalid, ArrowTypeError, OSError, ValueError) as exc:
        logger.warning(
            "Failed key cardinality scan",
            extra={
                "table": str(table_path),
                "key": key,
                "error_type": type(exc).__name__,
                "error": str(exc),
            },
        )
        return {
            "total_rows": 0,
            "non_null_rows": 0,
            "distinct_count": 0,
            "distinct_ratio": 0.0,
        }
    except ColumnNotFoundError as exc:
        logger.warning(
            f"Key column '{key}' not found in {table_path}",
            extra={
                "error_type": type(exc).__name__,
            },
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
    distinct_ratio = distinct_count / non_null_rows if non_null_rows > 0 else 0.0

    return {
        "total_rows": total_rows,
        "non_null_rows": non_null_rows,
        "distinct_count": distinct_count,
        "distinct_ratio": distinct_ratio,
    }


def list_ingest_partitions(path: Path) -> set[str]:
    """Return ingest_dt partition values (YYYY-MM-DD) for a table path."""
    partitions = set()
    for part_dir in path.glob("ingest_dt=*"):
        if not part_dir.is_dir():
            continue
        partitions.add(part_dir.name.split("=", 1)[-1])
    return partitions
