from unittest.mock import MagicMock, patch
import os
from pathlib import Path
import polars as pl
import pytest

from src.runners import dims_snapshot

def test_resolve_local_base_path():
    # Test absolute path
    assert dims_snapshot._resolve_local_base_path("/abs/path", "/fallback") == "/abs/path"
    # Test gs:// path (returns fallback)
    assert dims_snapshot._resolve_local_base_path("gs://bucket", "/fallback") == "/fallback"
    # Test relative path
    with patch("src.runners.dims_snapshot.AIRFLOW_HOME", "/opt/airflow"):
        res = dims_snapshot._resolve_local_base_path("data/dims", "/fallback")
        assert res == "/opt/airflow/data/dims"

def test_snapshot_dims_logic(tmp_path, monkeypatch):
    # Set up source data
    source_dir = tmp_path / "source"
    cust_dir = source_dir / "customers" / "ingestion_dt=2025-10-01"
    cust_dir.mkdir(parents=True)
    
    df = pl.DataFrame({
        "customer_id": ["C1"],
        "signup_date": ["2025-09-30"],
        "ingestion_dt": ["2025-10-01"]
    })
    df.write_parquet(cust_dir / "data.parquet")
    
    # Set up output dir
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    
    # Mock settings and specs
    monkeypatch.setenv("AIRFLOW_HOME", str(tmp_path))
    
    with patch("src.runners.dims_snapshot.load_spec_safe") as mock_spec, \
         patch("src.runners.dims_snapshot._resolve_dims_paths") as mock_paths:
        
        mock_spec_obj = MagicMock()
        mock_table = MagicMock()
        mock_table.name = "customers"
        mock_spec_obj.dims.tables = [mock_table]
        mock_spec_obj.dims.base_path = str(output_dir)
        mock_spec.return_value = mock_spec_obj
        
        mock_paths.return_value = (str(output_dir), None)
        
        dims_snapshot.snapshot_dims("2025-10-01", silver_base_path=str(source_dir))
        
    # Verify output
    snap_file = output_dir / "customers" / "snapshot_dt=2025-10-01" / "data_0.parquet"
    assert snap_file.exists()
    
    snap_df = pl.read_parquet(snap_file)
    assert snap_df.height == 1
    assert "as_of_dt" in snap_df.columns

def test_publish_dims_latest(tmp_path):
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    
    with patch("src.runners.dims_snapshot._resolve_dims_paths") as mock_paths:
        mock_paths.return_value = (str(output_dir), None)
        dims_snapshot.publish_dims_latest("2025-10-01", "run123")
        
    latest_file = output_dir / "_latest.json"
    assert latest_file.exists()
    import json
    content = json.loads(latest_file.read_text())
    assert content["run_date"] == "2025-10-01"
    assert content["run_id"] == "run123"
