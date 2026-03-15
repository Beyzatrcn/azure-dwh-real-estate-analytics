"""Fact loading step for the local ELT demo pipeline.

The script resolves surrogate keys from the generated dimensions and writes the
fact tables as CSV files into data/warehouse.
"""

from __future__ import annotations

import csv
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Dict, Iterable, List, Sequence


Row = Dict[str, str]


def _read_csv(path: Path) -> List[Row]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _write_csv(path: Path, rows: Iterable[Row], fieldnames: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _parse_date(value: str | None, fallback: date | None = None) -> date | None:
    if not value:
        return fallback
    return datetime.strptime(value, "%Y-%m-%d").date()


def _date_key(value: date | None) -> str:
    return "0" if value is None else value.strftime("%Y%m%d")


def _resolve_scd_key(
    dimension_rows: List[Row],
    business_key_name: str,
    business_key_value: str,
    as_of_date: date | None,
) -> str:
    if not business_key_value:
        return "0"

    for row in dimension_rows:
        if row[business_key_name] != business_key_value:
            continue
        valid_from = _parse_date(row.get("valid_from"), date(1900, 1, 1))
        valid_to = _parse_date(row.get("valid_to"), date(9999, 12, 31))
        if as_of_date is None or (valid_from and valid_to and valid_from <= as_of_date <= valid_to):
            key_name = business_key_name.replace("_id", "_key")
            return row[key_name]
    return "0"


def _resolve_current_project(project_rows: List[Row], project_id: str, as_of_date: date | None) -> Row:
    for row in project_rows:
        if row["project_id"] != project_id:
            continue
        valid_from = _parse_date(row.get("valid_from"), date(1900, 1, 1))
        valid_to = _parse_date(row.get("valid_to"), date(9999, 12, 31))
        if as_of_date is None or (valid_from and valid_to and valid_from <= as_of_date <= valid_to):
            return row
    return project_rows[0]


def _build_fact_project_cost(
    cost_rows: List[Row],
    dim_project: List[Row],
    dim_supplier: List[Row],
    dim_employee: List[Row],
) -> List[Row]:
    facts: List[Row] = []

    for index, row in enumerate(cost_rows, start=1):
        posting_date = _parse_date(row.get("posting_date"))
        invoice_date = _parse_date(row.get("invoice_date"))
        project_dim = _resolve_current_project(dim_project, row.get("project_id", ""), posting_date or invoice_date)

        facts.append(
            {
                "project_cost_key": str(index),
                "cost_id": row["cost_id"],
                "project_key": project_dim["project_key"],
                "supplier_key": _resolve_scd_key(dim_supplier, "supplier_id", row.get("supplier_id", ""), posting_date),
                "employee_key": _resolve_scd_key(dim_employee, "employee_id", row.get("employee_id", ""), posting_date),
                "region_key": project_dim["region_key"],
                "invoice_date_key": _date_key(invoice_date),
                "posting_date_key": _date_key(posting_date),
                "cost_category_code": row["cost_category_code"],
                "purchase_order_id": row["purchase_order_id"],
                "invoice_number": row["invoice_number"],
                "cost_status": row["cost_status"],
                "payment_status": row["payment_status"],
                "cost_amount_eur": row["cost_amount_eur"],
                "tax_amount_eur": row["tax_amount_eur"],
                "gross_amount_eur": str(float(row["cost_amount_eur"]) + float(row["tax_amount_eur"] or 0)),
                "currency_code": row["currency"],
                "source_system": row["source_system"],
                "source_updated_at": row["source_updated_at"],
            }
        )

    return facts


def _build_fact_project_budget(budget_rows: List[Row], dim_project: List[Row], dim_employee: List[Row]) -> List[Row]:
    facts: List[Row] = []

    for index, row in enumerate(budget_rows, start=1):
        budget_date = _parse_date(row.get("budget_date"))
        approval_date = _parse_date(row.get("approval_date"), budget_date)
        project_dim = _resolve_current_project(dim_project, row.get("project_id", ""), budget_date)

        facts.append(
            {
                "project_budget_key": str(index),
                "budget_id": row["budget_id"],
                "project_key": project_dim["project_key"],
                "employee_key": _resolve_scd_key(
                    dim_employee,
                    "employee_id",
                    row.get("approved_by_employee_id", ""),
                    approval_date,
                ),
                "region_key": project_dim["region_key"],
                "budget_date_key": _date_key(budget_date),
                "budget_version": row["budget_version"],
                "budget_status": row["budget_status"],
                "cost_category_code": row["cost_category_code"],
                "cost_category_name": row["cost_category_name"],
                "amount_eur": row["amount_eur"],
                "approval_date": row["approval_date"],
                "source_system": row["source_system"],
                "source_updated_at": row["source_updated_at"],
            }
        )

    return facts


def _month_range(start_value: date, end_value: date) -> List[date]:
    months: List[date] = []
    current = start_value.replace(day=1)
    limit = end_value.replace(day=1)
    while current <= limit:
        months.append(current)
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1, day=1)
        else:
            current = current.replace(month=current.month + 1, day=1)
    return months


def _build_fact_project_progress(
    project_rows: List[Row],
    budget_rows: List[Row],
    cost_rows: List[Row],
    dim_employee: List[Row],
) -> List[Row]:
    budget_by_project = defaultdict(float)
    cost_by_project = defaultdict(float)
    facts: List[Row] = []
    next_key = 1

    for row in budget_rows:
        budget_by_project[row["project_id"]] += float(row["amount_eur"])

    for row in cost_rows:
        cost_by_project[row["project_id"]] += float(row["cost_amount_eur"])

    current_projects = [row for row in project_rows if row["is_current"] == "1" and row["project_key"] != "0"]
    for row in current_projects:
        start_date = _parse_date(row["start_date"], date.today()) or date.today()
        planned_end_date = _parse_date(row["planned_end_date"], date.today()) or date.today()
        latest_snapshot = min(date.today(), planned_end_date)
        snapshots = _month_range(start_date, latest_snapshot)

        total_budget = budget_by_project.get(row["project_id"], 0.0)
        total_cost = cost_by_project.get(row["project_id"], 0.0)

        for position, snapshot_date in enumerate(snapshots, start=1):
            planned_progress_pct = round((position / max(len(snapshots), 1)) * 100, 2)
            actual_progress_pct = 0.0
            if total_budget > 0:
                actual_progress_pct = round(min((total_cost / total_budget) * 100, 100), 2)

            facts.append(
                {
                    "project_progress_key": str(next_key),
                    "project_key": row["project_key"],
                    "employee_key": _resolve_scd_key(
                        dim_employee,
                        "employee_id",
                        row.get("project_manager_id", ""),
                        snapshot_date,
                    ),
                    "region_key": row["region_key"],
                    "snapshot_date_key": _date_key(snapshot_date),
                    "project_status": row["project_status"],
                    "phase_name": row["project_phase"],
                    "planned_progress_pct": str(planned_progress_pct),
                    "actual_progress_pct": str(actual_progress_pct),
                    "schedule_variance_days": str(max(int(actual_progress_pct - planned_progress_pct), -30)),
                    "cost_variance_eur": str(round(total_cost - total_budget, 2)),
                    "open_issue_count": str(max(0, 8 - position)),
                    "milestone_total_count": str(len(snapshots)),
                    "milestone_completed_count": str(min(position, len(snapshots))),
                    "report_source": "SYNTHETIC_PROGRESS_ENGINE",
                    "source_updated_at": snapshot_date.isoformat(),
                }
            )
            next_key += 1

    return facts


def run_fact_load(project_root: Path) -> Dict[str, int]:
    """Build fact CSV outputs from processed data and generated dimensions."""

    processed_dir = project_root / "data" / "processed"
    warehouse_dir = project_root / "data" / "warehouse"

    budgets = _read_csv(processed_dir / "budgets_transformed.csv")
    costs = _read_csv(processed_dir / "costs_transformed.csv")
    dim_project = _read_csv(warehouse_dir / "dim_project.csv")
    dim_supplier = _read_csv(warehouse_dir / "dim_supplier.csv")
    dim_employee = _read_csv(warehouse_dir / "dim_employee.csv")

    fact_project_cost = _build_fact_project_cost(costs, dim_project, dim_supplier, dim_employee)
    fact_project_budget = _build_fact_project_budget(budgets, dim_project, dim_employee)
    fact_project_progress = _build_fact_project_progress(dim_project, budgets, costs, dim_employee)

    _write_csv(warehouse_dir / "fact_project_cost.csv", fact_project_cost, list(fact_project_cost[0].keys()))
    _write_csv(warehouse_dir / "fact_project_budget.csv", fact_project_budget, list(fact_project_budget[0].keys()))
    _write_csv(
        warehouse_dir / "fact_project_progress.csv",
        fact_project_progress,
        list(fact_project_progress[0].keys()),
    )

    return {
        "fact_project_cost": len(fact_project_cost),
        "fact_project_budget": len(fact_project_budget),
        "fact_project_progress": len(fact_project_progress),
    }


def main() -> None:
    project_root = Path(__file__).resolve().parents[1]
    counts = run_fact_load(project_root)
    print(f"Fact load completed: {counts}")


if __name__ == "__main__":
    main()
