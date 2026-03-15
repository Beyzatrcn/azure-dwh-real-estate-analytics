"""Ingestion step for the local ELT demo pipeline.

This script copies the raw source files into an ingestion area and writes a
small manifest. The goal is to make the pipeline stages visible and reproducible
without requiring external infrastructure.
"""

from __future__ import annotations

import csv
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List


DATASETS = [
    "projects.csv",
    "budgets.csv",
    "costs.csv",
    "suppliers.csv",
    "employees.csv",
]


def _count_rows(csv_path: Path) -> int:
    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle)
        next(reader, None)
        return sum(1 for _ in reader)


def run_ingestion(project_root: Path) -> Dict[str, List[Dict[str, str]]]:
    """Copy raw CSV files into the ingestion layer and return manifest metadata."""

    raw_dir = project_root / "data" / "raw"
    ingested_dir = project_root / "data" / "ingested"
    ingested_dir.mkdir(parents=True, exist_ok=True)

    manifest_entries: List[Dict[str, str]] = []

    for dataset_name in DATASETS:
        source_path = raw_dir / dataset_name
        target_path = ingested_dir / dataset_name

        shutil.copy2(source_path, target_path)

        manifest_entries.append(
            {
                "dataset_name": dataset_name,
                "source_path": str(source_path.relative_to(project_root)),
                "ingested_path": str(target_path.relative_to(project_root)),
                "row_count": str(_count_rows(target_path)),
            }
        )

    manifest = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "datasets": manifest_entries,
    }

    manifest_path = ingested_dir / "manifest.json"
    with manifest_path.open("w", encoding="utf-8") as handle:
        json.dump(manifest, handle, indent=2)

    return manifest


def main() -> None:
    project_root = Path(__file__).resolve().parents[1]
    manifest = run_ingestion(project_root)
    print(
        f"Ingestion completed for {len(manifest['datasets'])} datasets. "
        f"Manifest written to data/ingested/manifest.json"
    )


if __name__ == "__main__":
    main()
