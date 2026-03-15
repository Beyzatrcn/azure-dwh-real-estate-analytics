/*
    Data quality checks for the construction / project controlling demo.

    The checks are written against raw staging-style tables and can be adapted
    to staging schemas or external tables in Synapse / Fabric.

    Expected source objects:
    - stg.projects_raw
    - stg.budgets_raw
    - stg.costs_raw
    - stg.suppliers_raw
    - stg.employees_raw
*/

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'stg')
BEGIN
    EXEC('CREATE SCHEMA stg');
END;
GO

/* Missing business keys */
SELECT
    'projects_missing_project_id' AS check_name,
    COUNT(*) AS issue_count
FROM stg.projects_raw
WHERE project_id IS NULL OR LTRIM(RTRIM(project_id)) = '';
GO

SELECT
    'suppliers_missing_supplier_id' AS check_name,
    COUNT(*) AS issue_count
FROM stg.suppliers_raw
WHERE supplier_id IS NULL OR LTRIM(RTRIM(supplier_id)) = '';
GO

SELECT
    'employees_missing_employee_id' AS check_name,
    COUNT(*) AS issue_count
FROM stg.employees_raw
WHERE employee_id IS NULL OR LTRIM(RTRIM(employee_id)) = '';
GO

SELECT
    'budgets_missing_budget_id' AS check_name,
    COUNT(*) AS issue_count
FROM stg.budgets_raw
WHERE budget_id IS NULL OR LTRIM(RTRIM(budget_id)) = '';
GO

SELECT
    'costs_missing_cost_id' AS check_name,
    COUNT(*) AS issue_count
FROM stg.costs_raw
WHERE cost_id IS NULL OR LTRIM(RTRIM(cost_id)) = '';
GO

/* Duplicate business keys */
SELECT
    'projects_duplicate_business_keys' AS check_name,
    project_id,
    effective_from,
    COUNT(*) AS duplicate_count
FROM stg.projects_raw
GROUP BY project_id, effective_from
HAVING COUNT(*) > 1;
GO

SELECT
    'suppliers_duplicate_business_keys' AS check_name,
    supplier_id,
    effective_from,
    COUNT(*) AS duplicate_count
FROM stg.suppliers_raw
GROUP BY supplier_id, effective_from
HAVING COUNT(*) > 1;
GO

SELECT
    'employees_duplicate_business_keys' AS check_name,
    employee_id,
    effective_from,
    COUNT(*) AS duplicate_count
FROM stg.employees_raw
GROUP BY employee_id, effective_from
HAVING COUNT(*) > 1;
GO

SELECT
    'budgets_duplicate_business_lines' AS check_name,
    project_id,
    budget_version,
    cost_category_code,
    budget_period,
    amount_eur,
    COUNT(*) AS duplicate_count
FROM stg.budgets_raw
GROUP BY
    project_id,
    budget_version,
    cost_category_code,
    budget_period,
    amount_eur
HAVING COUNT(*) > 1;
GO

SELECT
    'costs_duplicate_business_invoices' AS check_name,
    project_id,
    supplier_id,
    invoice_number,
    COUNT(*) AS duplicate_count
FROM stg.costs_raw
GROUP BY project_id, supplier_id, invoice_number
HAVING COUNT(*) > 1;
GO

/* Negative cost values */
SELECT
    'costs_negative_cost_amount' AS check_name,
    cost_id,
    project_id,
    cost_amount_eur
FROM stg.costs_raw
WHERE cost_amount_eur < 0;
GO

/* Missing foreign-key style relationships */
SELECT
    'budgets_missing_project_reference' AS check_name,
    b.budget_id,
    b.project_id
FROM stg.budgets_raw b
LEFT JOIN stg.projects_raw p
    ON b.project_id = p.project_id
WHERE p.project_id IS NULL;
GO

SELECT
    'budgets_missing_employee_reference' AS check_name,
    b.budget_id,
    b.approved_by_employee_id
FROM stg.budgets_raw b
LEFT JOIN stg.employees_raw e
    ON b.approved_by_employee_id = e.employee_id
WHERE b.approved_by_employee_id IS NOT NULL
  AND e.employee_id IS NULL;
GO

SELECT
    'costs_missing_project_reference' AS check_name,
    c.cost_id,
    c.project_id
FROM stg.costs_raw c
LEFT JOIN stg.projects_raw p
    ON c.project_id = p.project_id
WHERE p.project_id IS NULL;
GO

SELECT
    'costs_missing_supplier_reference' AS check_name,
    c.cost_id,
    c.supplier_id
FROM stg.costs_raw c
LEFT JOIN stg.suppliers_raw s
    ON c.supplier_id = s.supplier_id
WHERE s.supplier_id IS NULL;
GO

SELECT
    'costs_missing_employee_reference' AS check_name,
    c.cost_id,
    c.employee_id
FROM stg.costs_raw c
LEFT JOIN stg.employees_raw e
    ON c.employee_id = e.employee_id
WHERE e.employee_id IS NULL;
GO

/* Budget smaller than zero */
SELECT
    'budgets_negative_amount' AS check_name,
    budget_id,
    project_id,
    amount_eur
FROM stg.budgets_raw
WHERE amount_eur < 0;
GO
