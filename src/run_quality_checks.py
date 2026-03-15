"""Standalone data-quality check runner for the local ELT demo."""

from __future__ import annotations

import json
from pathlib import Path

from data_quality_checks import run_all_checks


def main() -> None:
    project_root = Path(__file__).resolve().parents[1]
    quality_results = run_all_checks(project_root / "data" / "raw")

    quality_dir = project_root / "data" / "quality"
    quality_dir.mkdir(parents=True, exist_ok=True)

    output_path = quality_dir / "quality_report.json"
    with output_path.open("w", encoding="utf-8") as handle:
        json.dump(quality_results, handle, indent=2)

    print(f"Data-quality report written to {output_path.relative_to(project_root)}")


if __name__ == "__main__":
    main()
