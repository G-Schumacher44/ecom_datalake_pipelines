#!/usr/bin/env python3
"""Run dims dbt models based on the spec."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.specs import load_spec_safe  # noqa: E402

DEFAULT_MODELS = [
    "stg_ecommerce__customers",
    "stg_ecommerce__customers_quarantine",
    "stg_ecommerce__product_catalog",
    "stg_ecommerce__product_catalog_quarantine",
]


def build_models() -> list[str]:
    spec = load_spec_safe()
    if not spec:
        return DEFAULT_MODELS
    models: list[str] = []
    for table in spec.dims.tables:
        models.append(table.dbt_model)
        models.append(f"{table.dbt_model}_quarantine")
    return models


def main() -> None:
    models = build_models()
    cmd = [sys.executable, "-m", "src.runners.base_silver", "--select", *models]
    subprocess.run(cmd, check=True)


if __name__ == "__main__":
    main()
