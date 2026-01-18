"""Base Silver Runner (dbt wrapper).

This module replaces scripts/run_base_silver.sh, providing a Pythonic interface
for executing dbt models with proper environment setup and path handling.
"""

from __future__ import annotations

import logging
import os
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
    silver_path.mkdir(parents=True, exist_ok=True)
    quarantine_path.mkdir(parents=True, exist_ok=True)
    for table in STANDARD_TABLES:
        (silver_path / table).mkdir(parents=True, exist_ok=True)
        (quarantine_path / table).mkdir(parents=True, exist_ok=True)


def is_gcs_path(path: str) -> bool:
    """Check whether a path is a GCS URI."""
    return path.startswith("gs://")


def use_sa_auth() -> bool:
    """Check whether service-account auth is enabled."""
    return os.getenv("USE_SA_AUTH", "").lower() in {"1", "true", "yes", "on"}


def adc_credentials_path() -> Path:
    """Resolve the ADC credentials path for gcloud/gcsfs."""
    config_dir = os.getenv("CLOUDSDK_CONFIG", "").strip()
    if config_dir:
        return Path(config_dir) / "application_default_credentials.json"
    return Path.home() / ".config" / "gcloud" / "application_default_credentials.json"


def gcloud_rsync(source: str, destination: str, delete: bool) -> None:
    """Sync paths using gcloud storage rsync."""
    cmd = ["gcloud", "storage", "rsync", "-r"]
    if delete:
        cmd.append("--delete-unmatched-destination-objects")
    cmd.extend([source, destination])
    logger.info(f"Syncing {source} -> {destination}")
    env = os.environ.copy()
    creds = env.get("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
    if use_sa_auth() and creds:
        # Ensure gcloud uses the service account JSON without interactive login.
        env["CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE"] = creds
    elif not use_sa_auth():
        adc_path = adc_credentials_path()
        if adc_path.exists():
            # Ensure gcloud uses ADC refresh token without interactive login.
            env["CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE"] = str(adc_path)
    subprocess.run(cmd, check=True, env=env)


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
    airflow_home = os.getenv("AIRFLOW_HOME", "/opt/airflow")
    bronze_path_raw = os.getenv("BRONZE_BASE_PATH", "samples/bronze")
    silver_path_raw = os.getenv("SILVER_BASE_PATH", "data/silver/base")
    quarantine_path_raw = os.getenv(
        "SILVER_QUARANTINE_PATH", f"{silver_path_raw}/quarantine"
    )

    bronze_path_effective = bronze_path_raw
    silver_path_effective = silver_path_raw
    quarantine_path_effective = quarantine_path_raw

    # Sync bronze from GCS to local
    if is_gcs_path(bronze_path_raw):
        local_bronze_root = os.getenv(
            "BRONZE_LOCAL_BASE_PATH", f"{airflow_home}/data/bronze"
        )
        Path(local_bronze_root).mkdir(parents=True, exist_ok=True)
        tables_env = os.getenv("BRONZE_SYNC_TABLES", "").strip()
        if tables_env:
            tables = [t.strip() for t in tables_env.split(",") if t.strip()]
            logger.info(
                "Syncing bronze tables from GCS to local",
                extra={"tables": tables},
            )
            for table in tables:
                gcloud_rsync(
                    f"{bronze_path_raw.rstrip('/')}/{table}",
                    str(Path(local_bronze_root) / table),
                    delete=True,
                )
        else:
            logger.info(
                f"Syncing bronze from GCS to local: {bronze_path_raw} -> {local_bronze_root}"
            )
            gcloud_rsync(bronze_path_raw, local_bronze_root, delete=True)
        bronze_path_effective = local_bronze_root

    # Prepare local silver directory for GCS targets
    if is_gcs_path(silver_path_raw):
        local_silver_root = os.getenv(
            "SILVER_LOCAL_BASE_PATH", f"{airflow_home}/data/silver/base"
        )
        Path(local_silver_root).mkdir(parents=True, exist_ok=True)
        silver_path_effective = local_silver_root
        quarantine_path_effective = os.path.join(local_silver_root, "quarantine")

    os.environ["BRONZE_BASE_PATH"] = bronze_path_effective
    os.environ["SILVER_BASE_PATH"] = silver_path_effective
    os.environ["SILVER_QUARANTINE_PATH"] = quarantine_path_effective

    bronze_path = Path(bronze_path_effective)
    silver_path = Path(silver_path_effective)
    quarantine_path = Path(quarantine_path_effective)

    logger.info(f"Bronze Source: {bronze_path_effective}")
    logger.info(f"Silver Target: {silver_path_effective}")

    check_virtiofs_deadlock(bronze_path)
    ensure_local_directories(silver_path, quarantine_path)

    # Pass remaining CLI arguments to dbt
    # sys.argv[0] is the script name, so we take everything after
    dbt_extra_args = sys.argv[1:]

    run_dbt(dbt_args=dbt_extra_args)

    # Export local silver to GCS
    if is_gcs_path(silver_path_raw):
        export_base = os.getenv("SILVER_EXPORT_BASE_PATH", "").strip()
        pipeline_env = (
            os.getenv("PIPELINE_ENV") or settings.pipeline.environment or "local"
        ).lower()
        if not export_base and pipeline_env in {"dev", "prod"}:
            export_base = silver_path_raw

        if export_base and is_gcs_path(export_base):
            logger.info(f"Exporting local silver to GCS: {export_base}")
            gcloud_rsync(str(silver_path), export_base, delete=True)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
