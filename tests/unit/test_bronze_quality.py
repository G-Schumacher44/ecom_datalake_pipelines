from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.validation import bronze_quality


def _write_manifest(path: Path, rows: int = 1) -> None:
    manifest_path = path / "_MANIFEST.json"
    manifest_path.write_text(json.dumps({"total_rows": rows}))


def test_bronze_quality_passes_with_manifest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    table_path = tmp_path / "orders" / "ingest_dt=2020-01-01"
    table_path.mkdir(parents=True)
    _write_manifest(table_path, rows=10)

    monkeypatch.setenv("PIPELINE_ENV", "local")
    monkeypatch.setattr(
        bronze_quality,
        "parse_args",
        lambda: bronze_quality.argparse.Namespace(
            bronze_path=str(tmp_path),
            fail_on_issues=True,
            run_id="test",
            output_report=str(tmp_path / "report.md"),
        ),
    )

    assert bronze_quality.main() == 0


def test_bronze_quality_fails_without_manifest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    table_path = tmp_path / "orders" / "ingest_dt=2020-01-01"
    table_path.mkdir(parents=True)

    monkeypatch.setenv("PIPELINE_ENV", "local")
    monkeypatch.setattr(
        bronze_quality,
        "parse_args",
        lambda: bronze_quality.argparse.Namespace(
            bronze_path=str(tmp_path),
            fail_on_issues=True,
            run_id="test",
            output_report=str(tmp_path / "report.md"),
        ),
    )

    assert bronze_quality.main() == 1
