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
) -> dict[str, Path]:
    """Unified path resolution for validation scripts."""
    settings = load_settings(config_path)
    pl = settings.pipeline
    
    def _res(over, env_var, bucket, prefix):
        if over: return Path(over)
        env_val = os.getenv(env_var)
        if env_val: return Path(env_val)
        return Path(settings.resolve_path(bucket, prefix))

    return {
        "bronze": _res(bronze_over, "BRONZE_BASE_PATH", pl.bronze_bucket, pl.bronze_prefix),
        "silver": _res(silver_over, "SILVER_BASE_PATH", pl.silver_bucket, pl.silver_base_prefix),
        "enriched": _res(enriched_over, "SILVER_ENRICHED_PATH", pl.silver_bucket, pl.silver_enriched_prefix),
        "quarantine": Path(os.getenv("SILVER_QUARANTINE_PATH", str(_res(silver_over, "SILVER_BASE_PATH", pl.silver_bucket, pl.silver_base_prefix) / "quarantine")))
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
