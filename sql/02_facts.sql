/*
    Fact tables and grain

    fact_project_cost
    - Grain: one source cost transaction / invoice line per project and posting date

    fact_project_budget
    - Grain: one project budget line per budget version, cost category and budget period

    fact_project_progress
    - Grain: one project progress snapshot per reporting date
*/

CREATE TABLE dw.fact_project_cost
(
    project_cost_key BIGINT IDENTITY(1,1) NOT NULL,
    cost_id VARCHAR(20) NOT NULL,
    project_key INT NOT NULL,
    supplier_key INT NOT NULL,
    employee_key INT NOT NULL,
    region_key INT NOT NULL,
    invoice_date_key INT NOT NULL,
    posting_date_key INT NOT NULL,
    cost_category_code VARCHAR(20) NOT NULL,
    purchase_order_id VARCHAR(20) NULL,
    invoice_number VARCHAR(50) NULL,
    cost_status VARCHAR(30) NULL,
    payment_status VARCHAR(30) NULL,
    cost_amount_eur DECIMAL(18,2) NOT NULL,
    tax_amount_eur DECIMAL(18,2) NULL,
    gross_amount_eur AS (cost_amount_eur + ISNULL(tax_amount_eur, 0.00)),
    currency_code CHAR(3) NULL,
    source_system VARCHAR(50) NOT NULL,
    source_updated_at DATETIME2(0) NULL,
    CONSTRAINT pk_fact_project_cost PRIMARY KEY (project_cost_key),
    CONSTRAINT uq_fact_project_cost_business UNIQUE (cost_id),
    CONSTRAINT fk_fact_project_cost_project FOREIGN KEY (project_key) REFERENCES dw.dim_project(project_key),
    CONSTRAINT fk_fact_project_cost_supplier FOREIGN KEY (supplier_key) REFERENCES dw.dim_supplier(supplier_key),
    CONSTRAINT fk_fact_project_cost_employee FOREIGN KEY (employee_key) REFERENCES dw.dim_employee(employee_key),
    CONSTRAINT fk_fact_project_cost_region FOREIGN KEY (region_key) REFERENCES dw.dim_region(region_key),
    CONSTRAINT fk_fact_project_cost_invoice_date FOREIGN KEY (invoice_date_key) REFERENCES dw.dim_date(date_key),
    CONSTRAINT fk_fact_project_cost_posting_date FOREIGN KEY (posting_date_key) REFERENCES dw.dim_date(date_key)
);
GO

CREATE TABLE dw.fact_project_budget
(
    project_budget_key BIGINT IDENTITY(1,1) NOT NULL,
    budget_id VARCHAR(20) NOT NULL,
    project_key INT NOT NULL,
    employee_key INT NOT NULL,
    region_key INT NOT NULL,
    budget_date_key INT NOT NULL,
    budget_version VARCHAR(20) NOT NULL,
    budget_status VARCHAR(30) NULL,
    cost_category_code VARCHAR(20) NOT NULL,
    cost_category_name VARCHAR(100) NULL,
    amount_eur DECIMAL(18,2) NOT NULL,
    approval_date DATE NULL,
    source_system VARCHAR(50) NOT NULL,
    source_updated_at DATETIME2(0) NULL,
    CONSTRAINT pk_fact_project_budget PRIMARY KEY (project_budget_key),
    CONSTRAINT uq_fact_project_budget_business UNIQUE (budget_id),
    CONSTRAINT fk_fact_project_budget_project FOREIGN KEY (project_key) REFERENCES dw.dim_project(project_key),
    CONSTRAINT fk_fact_project_budget_employee FOREIGN KEY (employee_key) REFERENCES dw.dim_employee(employee_key),
    CONSTRAINT fk_fact_project_budget_region FOREIGN KEY (region_key) REFERENCES dw.dim_region(region_key),
    CONSTRAINT fk_fact_project_budget_date FOREIGN KEY (budget_date_key) REFERENCES dw.dim_date(date_key)
);
GO

CREATE TABLE dw.fact_project_progress
(
    project_progress_key BIGINT IDENTITY(1,1) NOT NULL,
    project_key INT NOT NULL,
    employee_key INT NULL,
    region_key INT NOT NULL,
    snapshot_date_key INT NOT NULL,
    project_status VARCHAR(50) NULL,
    phase_name VARCHAR(50) NULL,
    planned_progress_pct DECIMAL(5,2) NULL,
    actual_progress_pct DECIMAL(5,2) NULL,
    schedule_variance_days INT NULL,
    cost_variance_eur DECIMAL(18,2) NULL,
    open_issue_count INT NULL,
    milestone_total_count INT NULL,
    milestone_completed_count INT NULL,
    report_source VARCHAR(50) NOT NULL,
    source_updated_at DATETIME2(0) NULL,
    CONSTRAINT pk_fact_project_progress PRIMARY KEY (project_progress_key),
    CONSTRAINT uq_fact_project_progress_business UNIQUE (project_key, snapshot_date_key),
    CONSTRAINT fk_fact_project_progress_project FOREIGN KEY (project_key) REFERENCES dw.dim_project(project_key),
    CONSTRAINT fk_fact_project_progress_employee FOREIGN KEY (employee_key) REFERENCES dw.dim_employee(employee_key),
    CONSTRAINT fk_fact_project_progress_region FOREIGN KEY (region_key) REFERENCES dw.dim_region(region_key),
    CONSTRAINT fk_fact_project_progress_date FOREIGN KEY (snapshot_date_key) REFERENCES dw.dim_date(date_key)
);
GO
