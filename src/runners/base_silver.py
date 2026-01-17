"""Base Silver Runner (dbt wrapper).

This module replaces scripts/run_base_silver.sh, providing a Pythonic interface
for executing dbt models with proper environment setup and path handling.
"""

from __future__ import annotations

import logging
import os
import shutil
import subprocess
import sys
from pathlib import Path

from src.settings import load_settings

logger = logging.getLogger(__name__)

# Standard table list for directory creation
STANDARD_TABLES = [
    "customers",
    "product_catalog",
    "orders",
    "shopping_carts",
    "cart_items",
    "order_items",
    "returns",
    "return_items",
]


def check_virtiofs_deadlock(bronze_path: Path) -> None:
    """Detect MacOS Docker VirtioFS deadlock (Errno 35)."""
    # Only relevant for specific local paths in Docker
    if "/opt/airflow/samples" not in str(bronze_path):
        return

    if not bronze_path.exists():
        return

    try:
        # Try to read the first parquet file found
        found_files = list(bronze_path.glob("**/*.parquet"))
        if found_files:
            # Try reading 1 byte
            with open(found_files[0], "rb") as f:
                f.read(1)
    except OSError as e:
        if e.errno == 35:  # EDEADLK
            logger.error(
                "⚠️  VirtioFS read issue detected (Errno 35). "
                "Try: Docker Desktop → Settings → Change VirtioFS to gRPC FUSE"
            )
            sys.exit(1)
        raise


def ensure_local_directories(silver_path: Path, quarantine_path: Path) -> None:
    """Ensure output directories exist for local DuckDB writes."""
    if str(silver_path).startswith("gs://"):
        return

    logger.info("Ensuring local directory structure exists...")
    for table in STANDARD_TABLES:
        (silver_path / table).mkdir(parents=True, exist_ok=True)
        (quarantine_path / table).mkdir(parents=True, exist_ok=True)


def run_dbt(
    project_dir: str = "dbt_duckdb",
    profiles_dir: str = "dbt_duckdb",
    target_path: str = "/tmp/dbt_target",
    log_path: str = "/tmp/dbt_logs",
    dbt_args: list[str] | None = None,
) -> None:
    """Execute dbt run with configured environment."""
    dbt_args = dbt_args or []

    # Ensure temp paths exist
    Path(target_path).mkdir(parents=True, exist_ok=True)
    Path(log_path).mkdir(parents=True, exist_ok=True)
    Path("/tmp/dbt_duckdb").mkdir(parents=True, exist_ok=True)

    # Prepare environment
    env = os.environ.copy()
    env["DBT_TARGET_PATH"] = target_path
    env["DBT_LOG_PATH"] = log_path
    env["DBT_PARTIAL_PARSE"] = "false"

    cmd = [
        "dbt",
        "run",
        "--project-dir",
        project_dir,
        "--profiles-dir",
        profiles_dir,
        "--no-partial-parse",
    ] + dbt_args

    logger.info(f"Executing: {' '.join(cmd)}")
    
    try:
        # Stream output to stdout/stderr
        subprocess.run(cmd, env=env, check=True)
    except subprocess.CalledProcessError as e:
        logger.error(f"dbt run failed with exit code {e.returncode}")
        sys.exit(e.returncode)


def main() -> None:
    """Main entry point."""
    # Load settings to resolve defaults if env vars missing
    settings = load_settings()
    
    # Resolve paths (Env vars take precedence over config)
    bronze_path = Path(os.getenv("BRONZE_BASE_PATH", "samples/bronze"))
    silver_path = Path(os.getenv("SILVER_BASE_PATH", "data/silver/base"))
    quarantine_path = Path(
        os.getenv("SILVER_QUARANTINE_PATH", str(silver_path / "quarantine"))
    )

    logger.info(f"Bronze Source: {bronze_path}")
    logger.info(f"Silver Target: {silver_path}")

    check_virtiofs_deadlock(bronze_path)
    ensure_local_directories(silver_path, quarantine_path)

    # Pass remaining CLI arguments to dbt
    # sys.argv[0] is the script name, so we take everything after
    dbt_extra_args = sys.argv[1:]
    
    run_dbt(dbt_args=dbt_extra_args)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
