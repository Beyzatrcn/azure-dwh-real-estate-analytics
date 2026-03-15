"""End-to-end local pipeline runner."""

from __future__ import annotations

import json
from pathlib import Path

from ingest_data import run_ingestion
from load_dimensions import run_dimension_load
from load_facts import run_fact_load
from run_quality_checks import main as run_quality_checks_main
from transform_data import run_transform


def main() -> None:
    project_root = Path(__file__).resolve().parents[1]

    print("1/5 Ingesting raw data...")
    ingestion_manifest = run_ingestion(project_root)

    print("2/5 Transforming data...")
    transform_counts = run_transform(project_root)

    print("3/5 Loading dimensions...")
    dimension_counts = run_dimension_load(project_root)

    print("4/5 Loading facts...")
    fact_counts = run_fact_load(project_root)

    print("5/5 Running data quality checks...")
    run_quality_checks_main()

    summary = {
        "ingestion": ingestion_manifest,
        "transform": transform_counts,
        "dimensions": dimension_counts,
        "facts": fact_counts,
        "quality_report_path": "data/quality/quality_report.json",
    }

    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
