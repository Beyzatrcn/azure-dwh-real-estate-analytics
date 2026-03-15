"""Transformation step for the local ELT demo pipeline.

The script standardizes selected fields, derives project phase information,
normalizes date-related helper columns, and writes processed CSV files that act
like a staging-to-core handoff for the warehouse loads.
"""

from __future__ import annotations

import csv
from datetime import date
from pathlib import Path
from typing import Dict, Iterable, List


Row = Dict[str, str]


def _read_csv(path: Path) -> List[Row]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _write_csv(path: Path, rows: Iterable[Row], fieldnames: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _normalize_text(value: str | None) -> str:
    return "" if value is None else value.strip()


def _region_code(region: str, country_code: str) -> str:
    normalized_region = _normalize_text(region).upper().replace(" ", "_")
    normalized_country = _normalize_text(country_code).upper()
    return f"{normalized_region}_{normalized_country}" if normalized_region else "UNKNOWN"


def _derive_project_phase(project_status: str, actual_end_date: str) -> str:
    if _normalize_text(actual_end_date):
        return "Closeout"

    mapping = {
        "Planning": "Initiation",
        "Execution": "Construction",
        "On Hold": "Monitoring",
        "Completed": "Closeout",
        "Cancelled": "Closeout",
    }
    return mapping.get(_normalize_text(project_status), "Unknown")


def transform_projects(rows: List[Row]) -> List[Row]:
    transformed: List[Row] = []
    for row in rows:
        current = dict(row)
        current["region_code"] = _region_code(row.get("region", ""), row.get("country", ""))
        current["country_code"] = _normalize_text(row.get("country"))
        current["project_phase"] = _derive_project_phase(
            row.get("project_status", ""),
            row.get("actual_end_date", ""),
        )
        transformed.append(current)
    return transformed


def transform_budgets(rows: List[Row]) -> List[Row]:
    transformed: List[Row] = []
    for row in rows:
        current = dict(row)
        current["budget_date"] = f"{_normalize_text(row.get('budget_period'))}-01"
        transformed.append(current)
    return transformed


def transform_costs(rows: List[Row]) -> List[Row]:
    transformed: List[Row] = []
    for row in rows:
        current = dict(row)
        current["accounting_date"] = _normalize_text(row.get("posting_date")) or _normalize_text(
            row.get("invoice_date")
        )
        transformed.append(current)
    return transformed


def transform_master_data(rows: List[Row]) -> List[Row]:
    transformed: List[Row] = []
    for row in rows:
        current = {key: _normalize_text(value) for key, value in row.items()}
        transformed.append(current)
    return transformed


def run_transform(project_root: Path) -> Dict[str, int]:
    """Transform ingested datasets and return output row counts."""

    ingested_dir = project_root / "data" / "ingested"
    processed_dir = project_root / "data" / "processed"
    processed_dir.mkdir(parents=True, exist_ok=True)

    projects = transform_projects(_read_csv(ingested_dir / "projects.csv"))
    budgets = transform_budgets(_read_csv(ingested_dir / "budgets.csv"))
    costs = transform_costs(_read_csv(ingested_dir / "costs.csv"))
    suppliers = transform_master_data(_read_csv(ingested_dir / "suppliers.csv"))
    employees = transform_master_data(_read_csv(ingested_dir / "employees.csv"))

    _write_csv(processed_dir / "projects_transformed.csv", projects, list(projects[0].keys()))
    _write_csv(processed_dir / "budgets_transformed.csv", budgets, list(budgets[0].keys()))
    _write_csv(processed_dir / "costs_transformed.csv", costs, list(costs[0].keys()))
    _write_csv(processed_dir / "suppliers_transformed.csv", suppliers, list(suppliers[0].keys()))
    _write_csv(processed_dir / "employees_transformed.csv", employees, list(employees[0].keys()))

    return {
        "projects_transformed": len(projects),
        "budgets_transformed": len(budgets),
        "costs_transformed": len(costs),
        "suppliers_transformed": len(suppliers),
        "employees_transformed": len(employees),
        "transformed_at": int(date.today().strftime("%Y%m%d")),
    }


def main() -> None:
    project_root = Path(__file__).resolve().parents[1]
    counts = run_transform(project_root)
    print(f"Transformation completed: {counts}")


if __name__ == "__main__":
    main()
