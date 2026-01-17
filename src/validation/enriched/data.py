from __future__ import annotations

from pathlib import Path

import pyarrow.parquet as pq
from pyarrow.lib import ArrowInvalid, ArrowTypeError

from src.observability import get_logger
from src.validation.common import (
    collect_parquet_files,
    count_parquet_rows,
    list_partitions,
)

logger = get_logger(__name__)


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
