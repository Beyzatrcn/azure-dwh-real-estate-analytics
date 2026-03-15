"""Lightweight data-quality checks for local CSV-based validation.

The functions are dependency-light on purpose so the demo can run without
requiring pandas. Each check returns issue rows that can be logged, tested, or
written into a quality-reporting table later in the pipeline.
"""

from __future__ import annotations

import csv
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Tuple


Row = Dict[str, str]
Issue = Dict[str, str]


def load_csv(path: Path) -> List[Row]:
    """Load a CSV into a list of dictionaries using UTF-8 encoding."""

    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def is_missing(value: str | None) -> bool:
    """Treat None, empty strings, and whitespace-only values as missing."""

    return value is None or value.strip() == ""


def find_missing_business_keys(rows: Iterable[Row], key_name: str, entity_name: str) -> List[Issue]:
    """Return one issue per row where the declared business key is missing."""

    issues: List[Issue] = []
    for index, row in enumerate(rows, start=1):
        if is_missing(row.get(key_name)):
            issues.append(
                {
                    "check": "missing_business_key",
                    "entity": entity_name,
                    "row_number": str(index),
                    "key_name": key_name,
                }
            )
    return issues


def find_duplicates(rows: Iterable[Row], key_names: Sequence[str], entity_name: str) -> List[Issue]:
    """Return duplicate key combinations for the provided business-key columns."""

    counts: Dict[Tuple[str, ...], int] = {}
    for row in rows:
        key = tuple((row.get(name) or "").strip() for name in key_names)
        counts[key] = counts.get(key, 0) + 1

    issues: List[Issue] = []
    for key_values, count in counts.items():
        if count > 1:
            issues.append(
                {
                    "check": "duplicate_business_key",
                    "entity": entity_name,
                    "key_names": ",".join(key_names),
                    "key_values": ",".join(key_values),
                    "duplicate_count": str(count),
                }
            )
    return issues


def _parse_decimal(value: str | None) -> Decimal | None:
    """Convert numeric strings safely to Decimal."""

    if is_missing(value):
        return None
    try:
        return Decimal(str(value))
    except InvalidOperation:
        return None


def find_negative_values(rows: Iterable[Row], amount_column: str, entity_name: str, id_column: str) -> List[Issue]:
    """Return rows where the specified numeric column is below zero."""

    issues: List[Issue] = []
    for row in rows:
        amount = _parse_decimal(row.get(amount_column))
        if amount is not None and amount < 0:
            issues.append(
                {
                    "check": "negative_value",
                    "entity": entity_name,
                    "record_id": (row.get(id_column) or "").strip(),
                    "column_name": amount_column,
                    "amount": str(amount),
                }
            )
    return issues


def find_missing_foreign_keys(
    child_rows: Iterable[Row],
    child_key: str,
    parent_rows: Iterable[Row],
    parent_key: str,
    entity_name: str,
    id_column: str,
) -> List[Issue]:
    """Return child rows whose foreign-key-like values do not exist in the parent set."""

    parent_values = {
        (row.get(parent_key) or "").strip()
        for row in parent_rows
        if not is_missing(row.get(parent_key))
    }

    issues: List[Issue] = []
    for row in child_rows:
        candidate = (row.get(child_key) or "").strip()
        if not candidate:
            continue
        if candidate not in parent_values:
            issues.append(
                {
                    "check": "missing_foreign_key",
                    "entity": entity_name,
                    "record_id": (row.get(id_column) or "").strip(),
                    "foreign_key_name": child_key,
                    "foreign_key_value": candidate,
                }
            )
    return issues


def run_all_checks(data_dir: Path) -> Dict[str, List[Issue]]:
    """Run the requested data-quality checks against the demo CSV files."""

    projects = load_csv(data_dir / "projects.csv")
    budgets = load_csv(data_dir / "budgets.csv")
    costs = load_csv(data_dir / "costs.csv")
    suppliers = load_csv(data_dir / "suppliers.csv")
    employees = load_csv(data_dir / "employees.csv")

    return {
        "missing_business_keys": [
            *find_missing_business_keys(projects, "project_id", "projects"),
            *find_missing_business_keys(budgets, "budget_id", "budgets"),
            *find_missing_business_keys(costs, "cost_id", "costs"),
            *find_missing_business_keys(suppliers, "supplier_id", "suppliers"),
            *find_missing_business_keys(employees, "employee_id", "employees"),
        ],
        "duplicates": [
            *find_duplicates(projects, ("project_id", "effective_from"), "projects"),
            *find_duplicates(
                budgets,
                ("project_id", "budget_version", "cost_category_code", "budget_period", "amount_eur"),
                "budgets",
            ),
            *find_duplicates(costs, ("project_id", "supplier_id", "invoice_number"), "costs"),
            *find_duplicates(suppliers, ("supplier_id", "effective_from"), "suppliers"),
            *find_duplicates(employees, ("employee_id", "effective_from"), "employees"),
        ],
        "negative_cost_values": find_negative_values(costs, "cost_amount_eur", "costs", "cost_id"),
        "missing_foreign_keys": [
            *find_missing_foreign_keys(budgets, "project_id", projects, "project_id", "budgets", "budget_id"),
            *find_missing_foreign_keys(
                budgets,
                "approved_by_employee_id",
                employees,
                "employee_id",
                "budgets",
                "budget_id",
            ),
            *find_missing_foreign_keys(costs, "project_id", projects, "project_id", "costs", "cost_id"),
            *find_missing_foreign_keys(costs, "supplier_id", suppliers, "supplier_id", "costs", "cost_id"),
            *find_missing_foreign_keys(costs, "employee_id", employees, "employee_id", "costs", "cost_id"),
        ],
        "negative_budget_values": find_negative_values(budgets, "amount_eur", "budgets", "budget_id"),
    }
