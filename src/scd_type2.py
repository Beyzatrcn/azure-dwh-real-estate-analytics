"""Helper functions for SCD Type 2 change detection.

The module is intentionally lightweight so it can be reused in local demos,
small ETL scripts, or notebook-based validation steps. It focuses on the
tracked dim_project attributes:

- project_status
- project_manager_id
- region
- project_phase
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
import hashlib
from typing import Any, Dict, Iterable, List, Optional


OPEN_ENDED_VALID_TO = date(9999, 12, 31)


@dataclass(frozen=True)
class ProjectSnapshot:
    """Represents one source-side project version used for SCD2 processing."""

    project_id: str
    project_status: Optional[str]
    project_manager_id: Optional[str]
    region: Optional[str]
    project_phase: Optional[str]
    effective_from: date
    attributes: Dict[str, Any]


def _normalize(value: Any) -> str:
    """Return a stable string representation so hash comparison is deterministic."""

    if value is None:
        return ""
    return str(value).strip()


def tracked_hash(record: Dict[str, Any]) -> str:
    """Build a SHA-256 hash from the tracked SCD2 attributes only."""

    payload = "|".join(
        [
            _normalize(record.get("project_status")),
            _normalize(record.get("project_manager_id")),
            _normalize(record.get("region")),
            _normalize(record.get("project_phase")),
        ]
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def build_snapshot(record: Dict[str, Any]) -> ProjectSnapshot:
    """Convert a dictionary into a typed snapshot for easier downstream handling."""

    return ProjectSnapshot(
        project_id=_normalize(record["project_id"]),
        project_status=record.get("project_status"),
        project_manager_id=record.get("project_manager_id"),
        region=record.get("region"),
        project_phase=record.get("project_phase"),
        effective_from=record["effective_from"],
        attributes=dict(record),
    )


def detect_scd2_change(current_record: Dict[str, Any], incoming_record: Dict[str, Any]) -> bool:
    """Return True when any tracked attribute changed and a new dimension row is needed."""

    return tracked_hash(current_record) != tracked_hash(incoming_record)


def expire_current_record(current_record: Dict[str, Any], new_effective_from: date) -> Dict[str, Any]:
    """End-date the current row one day before the new version becomes active."""

    expired = dict(current_record)
    expired["valid_to"] = new_effective_from - timedelta(days=1)
    expired["is_current"] = False
    return expired


def create_new_version(incoming_record: Dict[str, Any]) -> Dict[str, Any]:
    """Create a fresh current SCD2 row using valid_from / valid_to semantics."""

    new_record = dict(incoming_record)
    new_record["valid_from"] = incoming_record["effective_from"]
    new_record["valid_to"] = OPEN_ENDED_VALID_TO
    new_record["is_current"] = True
    new_record["scd_hash_value"] = tracked_hash(incoming_record)
    return new_record


def apply_scd2(
    current_dimension_rows: Iterable[Dict[str, Any]],
    incoming_rows: Iterable[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Apply a small in-memory SCD2 workflow.

    The function expects one current row per project_id from the dimension and one
    latest incoming row per project_id from staging. The result contains:

    - untouched current rows where nothing changed
    - expired rows where a tracked attribute changed
    - new current rows for changed or brand-new business keys
    """

    current_by_project = {row["project_id"]: dict(row) for row in current_dimension_rows}
    result: List[Dict[str, Any]] = []

    processed_project_ids = set()

    for incoming in incoming_rows:
        project_id = incoming["project_id"]
        current = current_by_project.get(project_id)

        if current is None:
            result.append(create_new_version(incoming))
            processed_project_ids.add(project_id)
            continue

        if detect_scd2_change(current, incoming):
            result.append(expire_current_record(current, incoming["effective_from"]))
            result.append(create_new_version(incoming))
        else:
            result.append(current)

        processed_project_ids.add(project_id)

    for project_id, current in current_by_project.items():
        if project_id not in processed_project_ids:
            result.append(current)

    return result
