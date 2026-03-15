# SCD Type 2 for `dim_project`

`dim_project` is modeled as a Slowly Changing Dimension Type 2 because selected project attributes can change over time and need to remain analytically traceable in their historical context.

Tracked attributes:

- `project_status`
- `project_manager_id`
- `region`
- `project_phase`

## Why SCD Type 2 is used here

Project controlling depends on historical truth, not only on the latest state. If a project changes from `Planning` to `Execution`, moves to a different region, or is reassigned to a new project manager, those changes must be visible in time-based reporting. Otherwise, historical cost and progress analysis would be evaluated against the wrong master-data version.

## How the logic works

1. The incoming project snapshot is compared against the current `dim_project` row for the same `project_id`.
2. A hash is built from the tracked attributes only.
3. If the hash changed, the current row is expired:
   - `valid_to` / `effective_to` is set to the day before the new row starts
   - `is_current` is set to `0`
4. A new row is inserted with:
   - a new surrogate key
   - the same business key `project_id`
   - `valid_from` / `effective_from` = change date
   - `valid_to` / `effective_to` = `9999-12-31`
   - `is_current` = `1`

## Result for analytics

This allows fact tables to resolve the correct project dimension version for a given business date. In reporting, analysts can answer questions such as:

- Which project manager was responsible when a cost was posted?
- In which phase was a project when the budget increased?
- How did project status changes correlate with cost overruns?
- What did the portfolio look like by region at a specific point in time?
