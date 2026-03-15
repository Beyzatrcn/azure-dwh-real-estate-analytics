"""Dimension loading step for the local ELT demo pipeline.

The output is written as CSV files in data/warehouse to make the star-schema
result easy to inspect without needing a database engine.
"""

from __future__ import annotations

import csv
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Sequence

from scd_type2 import create_new_version, detect_scd2_change, expire_current_record


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


def _parse_date(value: str | None, default: date | None = None) -> date | None:
    if not value:
        return default
    return datetime.strptime(value, "%Y-%m-%d").date()


def _format_date(value: date | None) -> str:
    return "" if value is None else value.isoformat()


def _date_key(value: date) -> str:
    return value.strftime("%Y%m%d")


def _build_region_dimension(project_rows: List[Row]) -> List[Row]:
    regions = [
        {
            "region_key": "0",
            "region_code": "UNKNOWN",
            "region_name": "Unknown",
            "country_code": "UN",
            "country_name": "Unknown",
            "market_group": "Unknown",
            "source_system": "SYSTEM",
            "source_updated_at": "",
        }
    ]

    seen = set()
    next_key = 1
    for row in sorted(project_rows, key=lambda item: (item["region_code"], item["country_code"])):
        key = (row["region_code"], row["region"], row["country_code"])
        if key in seen:
            continue
        seen.add(key)
        regions.append(
            {
                "region_key": str(next_key),
                "region_code": row["region_code"],
                "region_name": row["region"],
                "country_code": row["country_code"],
                "country_name": row["country"],
                "market_group": "Germany",
                "source_system": row["source_system"],
                "source_updated_at": row["source_updated_at"],
            }
        )
        next_key += 1
    return regions


def _build_supplier_dimension(supplier_rows: List[Row]) -> List[Row]:
    dimension = [
        {
            "supplier_key": "0",
            "supplier_id": "UNKNOWN",
            "supplier_code": "UNKNOWN",
            "supplier_name": "Unknown Supplier",
            "supplier_category": "Unknown",
            "city": "Unknown",
            "country_code": "UN",
            "vat_id": "",
            "payment_terms_days": "",
            "preferred_flag": "N",
            "supplier_status": "Unknown",
            "contact_email": "",
            "source_system": "SYSTEM",
            "valid_from": "1900-01-01",
            "valid_to": "9999-12-31",
            "is_current": "1",
            "source_updated_at": "",
        }
    ]

    next_key = 1
    for row in sorted(supplier_rows, key=lambda item: (item["supplier_id"], item["effective_from"])):
        dimension.append(
            {
                "supplier_key": str(next_key),
                "supplier_id": row["supplier_id"],
                "supplier_code": row["supplier_code"],
                "supplier_name": row["supplier_name"],
                "supplier_category": row["supplier_category"],
                "city": row["city"],
                "country_code": row["country"],
                "vat_id": row["vat_id"],
                "payment_terms_days": row["payment_terms_days"],
                "preferred_flag": row["preferred_flag"],
                "supplier_status": row["supplier_status"],
                "contact_email": row["contact_email"],
                "source_system": row["source_system"],
                "valid_from": row["effective_from"],
                "valid_to": row["effective_to"],
                "is_current": "1" if row["is_current"] == "Y" else "0",
                "source_updated_at": row["source_updated_at"],
            }
        )
        next_key += 1
    return dimension


def _build_employee_dimension(employee_rows: List[Row]) -> List[Row]:
    dimension = [
        {
            "employee_key": "0",
            "employee_id": "UNKNOWN",
            "employee_number": "UNKNOWN",
            "full_name": "Unknown Employee",
            "job_title": "Unknown",
            "department": "Unknown",
            "manager_employee_id": "",
            "email": "",
            "employment_status": "Unknown",
            "employment_type": "Unknown",
            "location": "Unknown",
            "country_code": "UN",
            "hire_date": "",
            "termination_date": "",
            "source_system": "SYSTEM",
            "valid_from": "1900-01-01",
            "valid_to": "9999-12-31",
            "is_current": "1",
            "source_updated_at": "",
        }
    ]

    next_key = 1
    for row in sorted(employee_rows, key=lambda item: (item["employee_id"], item["effective_from"])):
        dimension.append(
            {
                "employee_key": str(next_key),
                "employee_id": row["employee_id"],
                "employee_number": row["employee_number"],
                "full_name": row["full_name"],
                "job_title": row["job_title"],
                "department": row["department"],
                "manager_employee_id": row["manager_employee_id"],
                "email": row["email"],
                "employment_status": row["employment_status"],
                "employment_type": row["employment_type"],
                "location": row["location"],
                "country_code": row["country"],
                "hire_date": row["hire_date"],
                "termination_date": row["termination_date"],
                "source_system": row["source_system"],
                "valid_from": row["effective_from"],
                "valid_to": row["effective_to"],
                "is_current": "1" if row["is_current"] == "Y" else "0",
                "source_updated_at": row["source_updated_at"],
            }
        )
        next_key += 1
    return dimension


def _build_project_dimension(project_rows: List[Row], region_lookup: Dict[str, str]) -> List[Row]:
    dimension = [
        {
            "project_key": "0",
            "project_id": "UNKNOWN",
            "project_code": "UNKNOWN",
            "project_name": "Unknown Project",
            "property_id": "",
            "project_type": "Unknown",
            "project_status": "Unknown",
            "project_phase": "Unknown",
            "project_manager_id": "",
            "customer_id": "",
            "contract_value_eur": "",
            "currency_code": "EUR",
            "start_date": "",
            "planned_end_date": "",
            "actual_end_date": "",
            "region_key": "0",
            "source_system": "SYSTEM",
            "valid_from": "1900-01-01",
            "valid_to": "9999-12-31",
            "is_current": "1",
            "source_updated_at": "",
        }
    ]

    next_key = 1
    grouped: Dict[str, List[Row]] = {}
    for row in project_rows:
        grouped.setdefault(row["project_id"], []).append(row)

    for project_id in sorted(grouped):
        versions = sorted(grouped[project_id], key=lambda item: item["effective_from"])
        current_versions: List[Dict[str, object]] = []

        for row in versions:
            scd_record = {
                "project_id": row["project_id"],
                "project_status": row["project_status"],
                "project_manager_id": row["project_manager_id"],
                "region": row["region_code"],
                "project_phase": row["project_phase"],
                "effective_from": _parse_date(row["effective_from"]),
            }

            if not current_versions:
                current_versions.append(create_new_version(scd_record))
            else:
                latest = current_versions[-1]
                if detect_scd2_change(latest, scd_record):
                    current_versions[-1] = expire_current_record(latest, scd_record["effective_from"])
                    current_versions.append(create_new_version(scd_record))

            current_versions[-1].update(
                {
                    "project_code": row["project_code"],
                    "project_name": row["project_name"],
                    "property_id": row["property_id"],
                    "project_type": row["project_type"],
                    "customer_id": row["customer_id"],
                    "contract_value_eur": row["contract_value_eur"],
                    "currency_code": row["currency"],
                    "start_date": row["start_date"],
                    "planned_end_date": row["planned_end_date"],
                    "actual_end_date": row["actual_end_date"],
                    "region_key": region_lookup.get(row["region_code"], "0"),
                    "source_system": row["source_system"],
                    "source_updated_at": row["source_updated_at"],
                }
            )

        for version in current_versions:
            dimension.append(
                {
                    "project_key": str(next_key),
                    "project_id": str(version["project_id"]),
                    "project_code": str(version["project_code"]),
                    "project_name": str(version["project_name"]),
                    "property_id": str(version["property_id"]),
                    "project_type": str(version["project_type"]),
                    "project_status": str(version["project_status"]),
                    "project_phase": str(version["project_phase"]),
                    "project_manager_id": str(version["project_manager_id"]),
                    "customer_id": str(version["customer_id"]),
                    "contract_value_eur": str(version["contract_value_eur"]),
                    "currency_code": str(version["currency_code"]),
                    "start_date": str(version["start_date"]),
                    "planned_end_date": str(version["planned_end_date"]),
                    "actual_end_date": str(version["actual_end_date"]),
                    "region_key": str(version["region_key"]),
                    "source_system": str(version["source_system"]),
                    "valid_from": _format_date(version["valid_from"]),
                    "valid_to": _format_date(version["valid_to"]),
                    "is_current": "1" if bool(version["is_current"]) else "0",
                    "source_updated_at": str(version["source_updated_at"]),
                }
            )
            next_key += 1

    return dimension


def _build_date_dimension(
    project_rows: List[Row],
    budget_rows: List[Row],
    cost_rows: List[Row],
) -> List[Row]:
    relevant_dates: List[date] = []

    for row in project_rows:
        for column in ("start_date", "planned_end_date", "actual_end_date", "effective_from", "effective_to"):
            parsed = _parse_date(row.get(column))
            if parsed:
                relevant_dates.append(parsed)

    for row in budget_rows:
        for column in ("budget_date", "approval_date"):
            parsed = _parse_date(row.get(column))
            if parsed:
                relevant_dates.append(parsed)

    for row in cost_rows:
        for column in ("invoice_date", "posting_date", "accounting_date"):
            parsed = _parse_date(row.get(column))
            if parsed:
                relevant_dates.append(parsed)

    if not relevant_dates:
        relevant_dates.append(date.today())

    start_date = min(relevant_dates)
    end_date = max(relevant_dates + [date.today()])

    rows = [
        {
            "date_key": "0",
            "calendar_date": "",
            "day_of_month": "",
            "day_name": "Unknown",
            "week_of_year": "",
            "month_number": "",
            "month_name": "Unknown",
            "quarter_number": "",
            "year_number": "",
            "year_month": "",
            "month_start_date": "",
            "month_end_date": "",
            "is_month_end": "0",
            "is_weekend": "0",
        }
    ]

    current = start_date
    while current <= end_date:
        month_start = current.replace(day=1)
        if current.month == 12:
            next_month = current.replace(year=current.year + 1, month=1, day=1)
        else:
            next_month = current.replace(month=current.month + 1, day=1)
        month_end = next_month - timedelta(days=1)

        rows.append(
            {
                "date_key": _date_key(current),
                "calendar_date": current.isoformat(),
                "day_of_month": str(current.day),
                "day_name": current.strftime("%A"),
                "week_of_year": current.strftime("%V"),
                "month_number": str(current.month),
                "month_name": current.strftime("%B"),
                "quarter_number": str(((current.month - 1) // 3) + 1),
                "year_number": str(current.year),
                "year_month": current.strftime("%Y-%m"),
                "month_start_date": month_start.isoformat(),
                "month_end_date": month_end.isoformat(),
                "is_month_end": "1" if current == month_end else "0",
                "is_weekend": "1" if current.weekday() >= 5 else "0",
            }
        )
        current += timedelta(days=1)

    return rows


def run_dimension_load(project_root: Path) -> Dict[str, int]:
    """Build dimension CSV outputs from the processed layer."""

    processed_dir = project_root / "data" / "processed"
    warehouse_dir = project_root / "data" / "warehouse"
    warehouse_dir.mkdir(parents=True, exist_ok=True)

    projects = _read_csv(processed_dir / "projects_transformed.csv")
    budgets = _read_csv(processed_dir / "budgets_transformed.csv")
    costs = _read_csv(processed_dir / "costs_transformed.csv")
    suppliers = _read_csv(processed_dir / "suppliers_transformed.csv")
    employees = _read_csv(processed_dir / "employees_transformed.csv")

    dim_region = _build_region_dimension(projects)
    region_lookup = {row["region_code"]: row["region_key"] for row in dim_region}
    dim_supplier = _build_supplier_dimension(suppliers)
    dim_employee = _build_employee_dimension(employees)
    dim_project = _build_project_dimension(projects, region_lookup)
    dim_date = _build_date_dimension(projects, budgets, costs)

    _write_csv(warehouse_dir / "dim_region.csv", dim_region, list(dim_region[0].keys()))
    _write_csv(warehouse_dir / "dim_supplier.csv", dim_supplier, list(dim_supplier[0].keys()))
    _write_csv(warehouse_dir / "dim_employee.csv", dim_employee, list(dim_employee[0].keys()))
    _write_csv(warehouse_dir / "dim_project.csv", dim_project, list(dim_project[0].keys()))
    _write_csv(warehouse_dir / "dim_date.csv", dim_date, list(dim_date[0].keys()))

    return {
        "dim_region": len(dim_region),
        "dim_supplier": len(dim_supplier),
        "dim_employee": len(dim_employee),
        "dim_project": len(dim_project),
        "dim_date": len(dim_date),
    }


def main() -> None:
    project_root = Path(__file__).resolve().parents[1]
    counts = run_dimension_load(project_root)
    print(f"Dimension load completed: {counts}")


if __name__ == "__main__":
    main()
