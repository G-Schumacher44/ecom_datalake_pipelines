"""Shared utilities for validation scripts."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.settings import load_settings

@dataclass
class ValidationStatus:
    """Standardized validation status."""
    PASS = "PASS"
    WARN = "WARN"
    FAIL = "FAIL"

def resolve_layer_paths(
    config_path: str = "config/config.yml",
    bronze_over: str | None = None,
    silver_over: str | None = None,
    enriched_over: str | None = None,
) -> dict[str, Path]:
    """Unified path resolution for validation scripts."""
    settings = load_settings(config_path).pipeline
    
    def _res(over, env_var, bucket, prefix):
        if over: return Path(over)
        env_val = os.getenv(env_var)
        if env_val: return Path(env_val)
        if bucket == "local": return Path(prefix)
        return Path(f"gs://{bucket}/{prefix}")

    return {
        "bronze": _res(bronze_over, "BRONZE_BASE_PATH", settings.bronze_bucket, settings.bronze_prefix),
        "silver": _res(silver_over, "SILVER_BASE_PATH", settings.silver_bucket, settings.silver_base_prefix),
        "enriched": _res(enriched_over, "SILVER_ENRICHED_PATH", settings.silver_bucket, settings.silver_enriched_prefix),
        "quarantine": Path(os.getenv("SILVER_QUARANTINE_PATH", str(_res(silver_over, "SILVER_BASE_PATH", settings.silver_bucket, settings.silver_base_prefix) / "quarantine")))
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
