/*
    SCD Type 2 load pattern for dw.dim_project

    Historized attributes
    - project_status
    - project_manager_id
    - region_key
    - project_phase

    Assumptions
    - A staging object named stg.project_project_snapshot exists
    - The staging object contains one current source row per project_id
    - region_key has already been resolved from dw.dim_region
    - effective_from is the business effective date of the incoming snapshot

    Notes
    - This script uses valid_from / valid_to semantics through effective_from / effective_to
    - Existing current records are end-dated when tracked attributes change
    - New business keys are inserted as fresh current rows
*/

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'stg')
BEGIN
    EXEC('CREATE SCHEMA stg');
END;
GO

/*
    Example staging definition for local development.
    In a production-style pipeline this object would be loaded upstream.
*/
IF OBJECT_ID('stg.project_project_snapshot', 'U') IS NULL
BEGIN
    CREATE TABLE stg.project_project_snapshot
    (
        project_id VARCHAR(20) NOT NULL,
        project_code VARCHAR(30) NOT NULL,
        project_name VARCHAR(200) NOT NULL,
        property_id VARCHAR(20) NULL,
        project_type VARCHAR(50) NULL,
        project_status VARCHAR(50) NULL,
        project_phase VARCHAR(50) NULL,
        project_manager_id VARCHAR(20) NULL,
        customer_id VARCHAR(20) NULL,
        contract_value_eur DECIMAL(18,2) NULL,
        currency_code CHAR(3) NULL,
        start_date DATE NULL,
        planned_end_date DATE NULL,
        actual_end_date DATE NULL,
        region_key INT NOT NULL,
        source_system VARCHAR(50) NOT NULL,
        effective_from DATE NOT NULL,
        source_updated_at DATETIME2(0) NULL
    );
END;
GO

WITH staged_project AS
(
    SELECT
        s.project_id,
        s.project_code,
        s.project_name,
        s.property_id,
        s.project_type,
        s.project_status,
        s.project_phase,
        s.project_manager_id,
        s.customer_id,
        s.contract_value_eur,
        s.currency_code,
        s.start_date,
        s.planned_end_date,
        s.actual_end_date,
        s.region_key,
        s.source_system,
        s.effective_from,
        s.source_updated_at,
        CONVERT(
            CHAR(64),
            HASHBYTES(
                'SHA2_256',
                CONCAT(
                    ISNULL(s.project_status, ''),
                    '|',
                    ISNULL(s.project_manager_id, ''),
                    '|',
                    CONVERT(VARCHAR(20), s.region_key),
                    '|',
                    ISNULL(s.project_phase, '')
                )
            ),
            2
        ) AS scd_hash_value
    FROM stg.project_project_snapshot s
),
current_dim AS
(
    SELECT
        d.project_key,
        d.project_id,
        d.scd_hash_value,
        d.effective_from,
        d.effective_to,
        d.is_current
    FROM dw.dim_project d
    WHERE d.is_current = 1
)
UPDATE d
SET
    d.effective_to = DATEADD(DAY, -1, s.effective_from),
    d.is_current = 0,
    d.source_updated_at = s.source_updated_at
FROM dw.dim_project d
INNER JOIN current_dim c
    ON d.project_key = c.project_key
INNER JOIN staged_project s
    ON c.project_id = s.project_id
WHERE c.scd_hash_value <> s.scd_hash_value
  AND s.effective_from > c.effective_from;
GO

WITH staged_project AS
(
    SELECT
        s.project_id,
        s.project_code,
        s.project_name,
        s.property_id,
        s.project_type,
        s.project_status,
        s.project_phase,
        s.project_manager_id,
        s.customer_id,
        s.contract_value_eur,
        s.currency_code,
        s.start_date,
        s.planned_end_date,
        s.actual_end_date,
        s.region_key,
        s.source_system,
        s.effective_from,
        s.source_updated_at,
        CONVERT(
            CHAR(64),
            HASHBYTES(
                'SHA2_256',
                CONCAT(
                    ISNULL(s.project_status, ''),
                    '|',
                    ISNULL(s.project_manager_id, ''),
                    '|',
                    CONVERT(VARCHAR(20), s.region_key),
                    '|',
                    ISNULL(s.project_phase, '')
                )
            ),
            2
        ) AS scd_hash_value
    FROM stg.project_project_snapshot s
),
current_dim AS
(
    SELECT
        d.project_id,
        d.scd_hash_value,
        d.effective_from
    FROM dw.dim_project d
    WHERE d.is_current = 1
)
INSERT INTO dw.dim_project
(
    project_id,
    project_code,
    project_name,
    property_id,
    project_type,
    project_status,
    project_phase,
    project_manager_id,
    customer_id,
    contract_value_eur,
    currency_code,
    start_date,
    planned_end_date,
    actual_end_date,
    region_key,
    source_system,
    scd_hash_value,
    effective_from,
    effective_to,
    is_current,
    source_updated_at
)
SELECT
    s.project_id,
    s.project_code,
    s.project_name,
    s.property_id,
    s.project_type,
    s.project_status,
    s.project_phase,
    s.project_manager_id,
    s.customer_id,
    s.contract_value_eur,
    s.currency_code,
    s.start_date,
    s.planned_end_date,
    s.actual_end_date,
    s.region_key,
    s.source_system,
    s.scd_hash_value,
    s.effective_from,
    CAST('9999-12-31' AS DATE),
    1,
    s.source_updated_at
FROM staged_project s
LEFT JOIN current_dim c
    ON s.project_id = c.project_id
WHERE c.project_id IS NULL
   OR c.scd_hash_value <> s.scd_hash_value;
GO

/*
    Optional validation queries for pipeline testing.
*/

-- At most one current row per business key
SELECT
    project_id,
    COUNT(*) AS current_row_count
FROM dw.dim_project
WHERE is_current = 1
GROUP BY project_id
HAVING COUNT(*) > 1;
GO

-- No invalid date windows
SELECT
    project_id,
    effective_from,
    effective_to
FROM dw.dim_project
WHERE effective_from > effective_to;
GO
