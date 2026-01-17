from __future__ import annotations
import logging
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq
from pyarrow.lib import ArrowInvalid, ArrowTypeError

logger = logging.getLogger(__name__)

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

def count_parquet_rows(path: Path) -> int:
    """Count total rows in all Parquet files in a directory."""
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
    """Analyze quarantine reasons and return top failures."""
    if not quarantine_path.exists():
        return []

    parquet_files = collect_parquet_files(quarantine_path)
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

    except Exception as e:
        logger.error(
            f"Failed to analyze quarantine for {quarantine_path}",
            error_type=type(e).__name__,
            error=str(e),
        )
        return []

def compute_key_cardinality(table_path: Path, key: str) -> dict[str, float]:
    if not table_path.exists():
        return {"total_rows": 0, "non_null_rows": 0, "distinct_count": 0, "distinct_ratio": 0.0}

    parquet_files = collect_parquet_files(table_path)
    if not parquet_files:
        return {"total_rows": 0, "non_null_rows": 0, "distinct_count": 0, "distinct_ratio": 0.0}

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
        return {"total_rows": 0, "non_null_rows": 0, "distinct_count": 0, "distinct_ratio": 0.0}

    total_rows = df.height
    non_null_rows = df.select(pl.col(key).is_not_null().sum()).item()
    distinct_count = df.select(pl.col(key).drop_nulls().n_unique()).item()
    distinct_ratio = (distinct_count / non_null_rows if non_null_rows > 0 else 0.0)

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
