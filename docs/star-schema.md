# Star Schema

Dieses Diagramm zeigt das geplante analytische Kernmodell fuer Projektcontrolling im Warehouse.

```mermaid
erDiagram
    DIM_PROJECT ||--o{ FACT_PROJECT_COST : "project_key"
    DIM_PROJECT ||--o{ FACT_PROJECT_BUDGET : "project_key"
    DIM_PROJECT ||--o{ FACT_PROJECT_PROGRESS : "project_key"

    DIM_SUPPLIER ||--o{ FACT_PROJECT_COST : "supplier_key"

    DIM_EMPLOYEE ||--o{ FACT_PROJECT_COST : "employee_key"
    DIM_EMPLOYEE ||--o{ FACT_PROJECT_BUDGET : "employee_key"
    DIM_EMPLOYEE ||--o{ FACT_PROJECT_PROGRESS : "employee_key"

    DIM_REGION ||--o{ FACT_PROJECT_COST : "region_key"
    DIM_REGION ||--o{ FACT_PROJECT_BUDGET : "region_key"
    DIM_REGION ||--o{ FACT_PROJECT_PROGRESS : "region_key"
    DIM_REGION ||--o{ DIM_PROJECT : "region_key"

    DIM_DATE ||--o{ FACT_PROJECT_COST : "invoice_date_key / posting_date_key"
    DIM_DATE ||--o{ FACT_PROJECT_BUDGET : "budget_date_key"
    DIM_DATE ||--o{ FACT_PROJECT_PROGRESS : "snapshot_date_key"

    DIM_PROJECT {
        int project_key PK
        string project_id BK
        string project_status
        string project_phase
        string project_manager_id
        int region_key FK
        date valid_from
        date valid_to
        bool is_current
    }

    DIM_SUPPLIER {
        int supplier_key PK
        string supplier_id BK
        string supplier_name
        string supplier_category
        date valid_from
        date valid_to
        bool is_current
    }

    DIM_EMPLOYEE {
        int employee_key PK
        string employee_id BK
        string full_name
        string department
        string job_title
        date valid_from
        date valid_to
        bool is_current
    }

    DIM_REGION {
        int region_key PK
        string region_code BK
        string region_name
        string country_code
    }

    DIM_DATE {
        int date_key PK
        date calendar_date BK
        int month_number
        int quarter_number
        int year_number
        string year_month
    }

    FACT_PROJECT_COST {
        bigint project_cost_key PK
        string cost_id BK
        int project_key FK
        int supplier_key FK
        int employee_key FK
        int region_key FK
        int invoice_date_key FK
        int posting_date_key FK
        decimal cost_amount_eur
        decimal tax_amount_eur
        string cost_category_code
    }

    FACT_PROJECT_BUDGET {
        bigint project_budget_key PK
        string budget_id BK
        int project_key FK
        int employee_key FK
        int region_key FK
        int budget_date_key FK
        string budget_version
        decimal amount_eur
        string cost_category_code
    }

    FACT_PROJECT_PROGRESS {
        bigint project_progress_key PK
        int project_key FK
        int employee_key FK
        int region_key FK
        int snapshot_date_key FK
        string project_status
        string phase_name
        decimal planned_progress_pct
        decimal actual_progress_pct
        decimal cost_variance_eur
    }
```
