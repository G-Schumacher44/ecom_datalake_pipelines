"""Shared utilities for validation scripts."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq
from pyarrow.lib import ArrowInvalid, ArrowTypeError

from src.observability import get_logger
from src.settings import load_settings

logger = get_logger(__name__)


@dataclass
class ValidationStatus:
    """Standardized validation status."""

    PASS = "PASS"
    WARN = "WARN"
    FAIL = "FAIL"


def is_gcs_path(path: str) -> bool:
    """Check if a path string is a GCS URI."""
    return path.startswith("gs://")


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


def collect_parquet_files(
    path: Path,
    partition_key: str | None = None,
    partitions: list[str] | None = None,
) -> list[Path]:
    """Collect valid Parquet files under a path, skipping corrupt files."""
    candidates: list[Path] = []
    if partition_key and partitions:
        for value in partitions:
            partition_path = path / f"{partition_key}={value}"
            if not partition_path.exists():
                continue
            candidates.extend(partition_path.glob("**/*.parquet"))
    else:
        candidates = list(path.glob("**/*.parquet"))
    if not candidates:
        return []

    valid_files = []
    invalid_files = []
    seen: set[Path] = set()
    for file_path in candidates:
        if file_path in seen:
            continue
        seen.add(file_path)
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


def count_parquet_rows(
    path: Path,
    partition_key: str | None = None,
    partitions: list[str] | None = None,
) -> int:
    """Count total rows in all Parquet files in a directory."""
    if not path.exists():
        logger.warning(f"Path does not exist: {path}")
        return 0

    parquet_files = collect_parquet_files(
        path, partition_key=partition_key, partitions=partitions
    )
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


def read_parquet_safe(
    path: Path,
    columns: list[str] | None = None,
    n_rows: int | None = None,
) -> pl.DataFrame | None:
    """Read a parquet file or directory safely, returning None on failure."""
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
            use_pyarrow=True,
        )
    except (ArrowInvalid, ArrowTypeError, OSError, ValueError) as exc:
        logger.error(
            f"Failed to read parquet at {path}",
            error_type=type(exc).__name__,
            error=str(exc),
        )
        return None


def list_partitions(path: Path, partition_key: str) -> list[str]:
    """List available partition values for a given key."""
    partitions = []
    for part_dir in path.glob(f"{partition_key}=*"):
        if part_dir.is_dir():
            partitions.append(part_dir.name.split("=", 1)[-1])
    return sorted(partitions)


def resolve_layer_paths(
    config_path: str = "config/config.yml",
    bronze_over: str | None = None,
    silver_over: str | None = None,
    enriched_over: str | None = None,
) -> dict[str, Path | str]:
    """Unified path resolution for validation scripts."""
    settings = load_settings(config_path)
    pl = settings.pipeline

    def _resolve_raw(over, env_var, bucket, prefix) -> str:
        if over:
            return over
        env_val = os.getenv(env_var)
        if env_val:
            return env_val
        return settings.resolve_path(bucket, prefix)

    def _maybe_path(value: str) -> Path | str:
        return value if is_gcs_path(value) else Path(value)

    bronze_raw = _resolve_raw(
        bronze_over, "BRONZE_BASE_PATH", pl.bronze_bucket, pl.bronze_prefix
    )
    silver_raw = _resolve_raw(
        silver_over, "SILVER_BASE_PATH", pl.silver_bucket, pl.silver_base_prefix
    )
    enriched_raw = _resolve_raw(
        enriched_over,
        "SILVER_ENRICHED_PATH",
        pl.silver_bucket,
        pl.silver_enriched_prefix,
    )

    quarantine_raw = os.getenv("SILVER_QUARANTINE_PATH")
    if not quarantine_raw:
        if is_gcs_path(silver_raw):
            quarantine_raw = f"{silver_raw.rstrip('/')}/quarantine"
        else:
            quarantine_raw = str(Path(silver_raw) / "quarantine")

    return {
        "bronze": _maybe_path(bronze_raw),
        "silver": _maybe_path(silver_raw),
        "enriched": _maybe_path(enriched_raw),
        "quarantine": _maybe_path(quarantine_raw),
    }


def get_overall_status(statuses: list[str]) -> str:
    """Determine overall status from a list of table statuses."""
    if any(s == ValidationStatus.FAIL for s in statuses):
        return ValidationStatus.FAIL
    if any(s == ValidationStatus.WARN for s in statuses):
        return ValidationStatus.WARN
    return ValidationStatus.PASS


def handle_exit(overall_status: str, enforce: bool, env: str) -> int:
    """Standardized exit logic for validation gating."""
    if overall_status == ValidationStatus.FAIL:
        if enforce or env == "prod":
            return 1
    return 0
