#!/usr/bin/env python3
"""Summarize Parquet schemas from local bronze samples."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def find_parquet_files(root: Path, max_files: int) -> dict[str, list[Path]]:
    tables: dict[str, list[Path]] = {}
    for table_dir in root.iterdir():
        if not table_dir.is_dir():
            continue
        parquet_files = sorted(table_dir.rglob("*.parquet"))
        if not parquet_files:
            continue
        tables[table_dir.name] = parquet_files[:max_files]
    return tables


def render_markdown(summary: dict[str, dict[str, object]]) -> str:
    lines = [
        "# Bronze Sample Schema Summary",
        "",
        "Generated from local parquet samples in `samples/bronze/`.",
        "",
    ]
    for table_name, info in sorted(summary.items()):
        lines.append(f"## {table_name}")
        lines.append("")
        lines.append("Sample files:")
        for file_path in info["files"]:
            lines.append(f"- `{file_path}`")
        lines.append("")
        lines.append("Schema:")
        lines.append("")
        lines.append("| Column | Type |")
        lines.append("| --- | --- |")
        for field in info["schema"] or []:
            lines.append(f"| {field['name']} | {field['type']} |")
        lines.append("")
        lines.append("Sample row counts:")
        lines.append(f"- {info['sample_row_counts']}")
        lines.append("")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Describe parquet samples.")
    parser.add_argument("--root", default="samples/bronze", help="Sample root directory")
    parser.add_argument("--max-files", type=int, default=1, help="Max parquet files per table")
    parser.add_argument(
        "--output",
        default="docs/planning/BRONZE_SCHEMA_SAMPLE.md",
        help="Output path for Markdown summary",
    )
    args = parser.parse_args()

    root = Path(args.root)
    if not root.exists():
        raise SystemExit(f"Root not found: {root}")

    try:
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise SystemExit(
            "pyarrow is required. Install it in your env (e.g., pip install pyarrow)."
        ) from exc

    summary: dict[str, dict[str, object]] = {}
    tables = find_parquet_files(root, args.max_files)

    for table_name, files in tables.items():
        table_info = {
            "files": [str(path) for path in files],
            "schema": None,
            "sample_row_counts": [],
        }
        for parquet_path in files:
            pf = pq.ParquetFile(parquet_path)
            table_info["sample_row_counts"].append(pf.metadata.num_rows)
            if table_info["schema"] is None:
                schema = pf.schema_arrow
                table_info["schema"] = [
                    {"name": field.name, "type": str(field.type)} for field in schema
                ]
        summary[table_name] = table_info

    markdown = render_markdown(summary)
    Path(args.output).write_text(markdown)
    print(f"Wrote {args.output}")


if __name__ == "__main__":
    main()
