"""Shared utilities for validation scripts."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Union

import polars as pl
import pyarrow.parquet as pq
from pyarrow.lib import ArrowInvalid, ArrowTypeError

from src.observability import get_logger
from src.settings import load_settings
from src.specs import load_spec_safe

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


def get_gcs_filesystem():
    try:
        import fsspec  # type: ignore
    except ModuleNotFoundError as exc:
        raise RuntimeError("gcsfs is required for gs:// validation reads") from exc
    return fsspec.filesystem("gcs")


def join_path(base: Union[Path, str], *parts: str) -> Union[Path, str]:
    base_str = str(base)
    if is_gcs_path(base_str):
        return "/".join([base_str.rstrip("/"), *parts])
    return Path(base_str).joinpath(*parts)


def path_exists(path: Union[Path, str]) -> bool:
    path_str = str(path)
    if is_gcs_path(path_str):
        fs = get_gcs_filesystem()
        return fs.exists(path_str)
    return Path(path_str).exists()


def collect_parquet_files(
    path: Union[Path, str],
    partition_key: str | None = None,
    partitions: list[str] | None = None,
) -> list[Union[Path, str]]:
    """Collect valid Parquet files under a path, skipping corrupt files."""
    path_str = str(path)
    candidates: list[Union[Path, str]] = []
    if is_gcs_path(path_str):
        fs = get_gcs_filesystem()
        if partition_key and partitions:
            for value in partitions:
                partition_path = f"{path_str.rstrip('/')}/{partition_key}={value}"
                if not fs.exists(partition_path):
                    continue
                candidates.extend(fs.glob(f"{partition_path}/**/*.parquet"))
        else:
            candidates = fs.glob(f"{path_str.rstrip('/')}/**/*.parquet")
        return sorted(candidates)

    path_obj = Path(path_str)
    if partition_key and partitions:
        for value in partitions:
            partition_path = path_obj / f"{partition_key}={value}"
            if not partition_path.exists():
                continue
            candidates.extend(partition_path.glob("**/*.parquet"))
    else:
        candidates = list(path_obj.glob("**/*.parquet"))
    if not candidates:
        return []

    valid_files = []
    invalid_files = []
    seen: set[Path] = set()
    for file_path in candidates:
        if not isinstance(file_path, Path):
            valid_files.append(file_path)
            continue
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
    path: Union[Path, str],
    partition_key: str | None = None,
    partitions: list[str] | None = None,
) -> int:
    """Count total rows in all Parquet files in a directory."""
    if not path_exists(path):
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
            if isinstance(file_path, Path):
                parquet_file = pq.ParquetFile(file_path)
            else:
                fs = get_gcs_filesystem()
                with fs.open(file_path, "rb") as handle:
                    parquet_file = pq.ParquetFile(handle)
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
    path: Union[Path, str],
    columns: list[str] | None = None,
    n_rows: int | None = None,
) -> pl.DataFrame | None:
    """Read a parquet file or directory safely, returning None on failure."""
    def _is_decimal_dtype(dtype: pl.DataType) -> bool:
        try:
            from polars.datatypes import is_decimal
        except Exception:
            is_decimal = None
        if is_decimal:
            return is_decimal(dtype)
        return str(dtype).startswith("Decimal")

    parquet_files = collect_parquet_files(path)
    if not parquet_files:
        return None
    demo_mode = os.getenv("DEMO_MODE", "").lower() in {"1", "true", "yes", "on"}
    try:
        return pl.read_parquet(
            parquet_files,
            columns=columns,
            n_rows=n_rows,
            memory_map=False,
            low_memory=True,
            use_pyarrow=not demo_mode,
        )
    except (ArrowInvalid, ArrowTypeError, OSError, ValueError) as exc:
        logger.warning(
            "Failed to read parquet; falling back to per-file read",
            path=str(path),
            error_type=type(exc).__name__,
            error=str(exc),
        )

    frames: list[pl.DataFrame] = []
    remaining = n_rows
    for parquet_file in parquet_files:
        try:
            if isinstance(parquet_file, Path):
                frame = pl.read_parquet(
                    parquet_file,
                    columns=columns,
                    n_rows=remaining,
                    memory_map=False,
                    low_memory=True,
                    use_pyarrow=False,
                )
            else:
                fs = get_gcs_filesystem()
                with fs.open(parquet_file, "rb") as handle:
                    frame = pl.read_parquet(
                        handle,
                        columns=columns,
                        n_rows=remaining,
                        memory_map=False,
                        low_memory=True,
                        use_pyarrow=False,
                    )
        except (ArrowInvalid, ArrowTypeError, OSError, ValueError) as exc:
            logger.error(
                "Failed to read parquet file",
                path=str(parquet_file),
                error_type=type(exc).__name__,
                error=str(exc),
            )
            continue

        for name, dtype in frame.schema.items():
            if dtype == pl.Categorical:
                frame = frame.with_columns(pl.col(name).cast(pl.Utf8))
            elif _is_decimal_dtype(dtype):
                frame = frame.with_columns(pl.col(name).cast(pl.Float64))

        frames.append(frame)
        if remaining is not None:
            remaining = max(remaining - frame.height, 0)
            if remaining == 0:
                break

    if not frames:
        return None

    if len(frames) == 1:
        return frames[0]

    return pl.concat(frames, how="vertical", rechunk=True)


def list_partitions(path: Union[Path, str], partition_key: str) -> list[str]:
    """List available partition values for a given key."""
    path_str = str(path)
    if is_gcs_path(path_str):
        fs = get_gcs_filesystem()
        entries = fs.glob(f"{path_str.rstrip('/')}/{partition_key}=*")
        partitions = []
        for entry in entries:
            name = entry.rstrip("/").split("/")[-1]
            if name.startswith(f"{partition_key}="):
                partitions.append(name.split("=", 1)[-1])
        return sorted(set(partitions))

    partitions = []
    for part_dir in Path(path_str).glob(f"{partition_key}=*"):
        if part_dir.is_dir():
            partitions.append(part_dir.name.split("=", 1)[-1])
    return sorted(partitions)


def resolve_layer_paths(
    config_path: str = "config/config.yml",
    bronze_over: str | None = None,
    silver_over: str | None = None,
    enriched_over: str | None = None,
    spec_path: str | None = None,
) -> dict[str, Path | str]:
    """Unified path resolution for validation scripts."""
    settings = load_settings(config_path)
    pl = settings.pipeline
    spec = load_spec_safe(spec_path)

    def _resolve_raw(over, env_var, bucket, prefix, spec_default: str | None) -> str:
        if over:
            return over
        env_val = os.getenv(env_var)
        if env_val:
            return env_val
        if spec_default:
            return spec_default
        return settings.resolve_path(bucket, prefix)

    def _maybe_path(value: str) -> Path | str:
        return value if is_gcs_path(value) else Path(value)

    bronze_raw = _resolve_raw(
        bronze_over,
        "BRONZE_BASE_PATH",
        pl.bronze_bucket,
        pl.bronze_prefix,
        spec.bronze.base_path if spec else None,
    )
    silver_raw = _resolve_raw(
        silver_over,
        "SILVER_BASE_PATH",
        pl.silver_bucket,
        pl.silver_base_prefix,
        spec.silver_base.base_path if spec else None,
    )
    enriched_raw = _resolve_raw(
        enriched_over,
        "SILVER_ENRICHED_PATH",
        pl.silver_bucket,
        pl.silver_enriched_prefix,
        spec.silver_enriched.base_path if spec else None,
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


def resolve_reports_enabled(spec_path: str | None = None) -> bool:
    """Resolve report toggle via env override or spec default."""
    env_val = os.getenv("REPORTS_ENABLED")
    if env_val is not None:
        return env_val.lower() in {"1", "true", "yes", "on"}
    spec = load_spec_safe(spec_path)
    if spec and spec.validation is not None:
        return spec.validation.reports_enabled
    return True


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
