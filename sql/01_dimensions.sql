/*
    Dimension tables
    - dim_project, dim_supplier, dim_employee are modeled as SCD Type 2 dimensions
    - dim_region is a small conformed dimension
    - dim_date is a calendar dimension used by all facts
*/

CREATE TABLE dw.dim_region
(
    region_key INT IDENTITY(1,1) NOT NULL,
    region_code VARCHAR(20) NOT NULL,
    region_name VARCHAR(100) NOT NULL,
    country_code CHAR(2) NOT NULL,
    country_name VARCHAR(100) NOT NULL,
    market_group VARCHAR(50) NULL,
    source_system VARCHAR(50) NULL,
    source_updated_at DATETIME2(0) NULL,
    CONSTRAINT pk_dim_region PRIMARY KEY (region_key),
    CONSTRAINT uq_dim_region_business UNIQUE (region_code)
);
GO

CREATE TABLE dw.dim_project
(
    project_key INT IDENTITY(1,1) NOT NULL,
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
    scd_hash_value CHAR(64) NULL,
    effective_from DATE NOT NULL,
    effective_to DATE NOT NULL,
    is_current BIT NOT NULL,
    source_updated_at DATETIME2(0) NULL,
    CONSTRAINT pk_dim_project PRIMARY KEY (project_key),
    CONSTRAINT fk_dim_project_region FOREIGN KEY (region_key) REFERENCES dw.dim_region(region_key),
    CONSTRAINT ck_dim_project_dates CHECK (effective_from <= effective_to)
);
GO

CREATE UNIQUE INDEX ux_dim_project_business_version
    ON dw.dim_project(project_id, effective_from);
GO

CREATE TABLE dw.dim_supplier
(
    supplier_key INT IDENTITY(1,1) NOT NULL,
    supplier_id VARCHAR(20) NOT NULL,
    supplier_code VARCHAR(20) NULL,
    supplier_name VARCHAR(200) NOT NULL,
    supplier_category VARCHAR(50) NULL,
    city VARCHAR(100) NULL,
    country_code CHAR(2) NULL,
    vat_id VARCHAR(30) NULL,
    payment_terms_days INT NULL,
    preferred_flag CHAR(1) NULL,
    supplier_status VARCHAR(30) NULL,
    contact_email VARCHAR(200) NULL,
    source_system VARCHAR(50) NOT NULL,
    scd_hash_value CHAR(64) NULL,
    effective_from DATE NOT NULL,
    effective_to DATE NOT NULL,
    is_current BIT NOT NULL,
    source_updated_at DATETIME2(0) NULL,
    CONSTRAINT pk_dim_supplier PRIMARY KEY (supplier_key),
    CONSTRAINT ck_dim_supplier_dates CHECK (effective_from <= effective_to)
);
GO

CREATE UNIQUE INDEX ux_dim_supplier_business_version
    ON dw.dim_supplier(supplier_id, effective_from);
GO

CREATE TABLE dw.dim_employee
(
    employee_key INT IDENTITY(1,1) NOT NULL,
    employee_id VARCHAR(20) NOT NULL,
    employee_number VARCHAR(20) NULL,
    full_name VARCHAR(150) NOT NULL,
    job_title VARCHAR(100) NULL,
    department VARCHAR(100) NULL,
    manager_employee_id VARCHAR(20) NULL,
    email VARCHAR(200) NULL,
    employment_status VARCHAR(30) NULL,
    employment_type VARCHAR(30) NULL,
    location VARCHAR(100) NULL,
    country_code CHAR(2) NULL,
    hire_date DATE NULL,
    termination_date DATE NULL,
    source_system VARCHAR(50) NOT NULL,
    scd_hash_value CHAR(64) NULL,
    effective_from DATE NOT NULL,
    effective_to DATE NOT NULL,
    is_current BIT NOT NULL,
    source_updated_at DATETIME2(0) NULL,
    CONSTRAINT pk_dim_employee PRIMARY KEY (employee_key),
    CONSTRAINT ck_dim_employee_dates CHECK (effective_from <= effective_to)
);
GO

CREATE UNIQUE INDEX ux_dim_employee_business_version
    ON dw.dim_employee(employee_id, effective_from);
GO

CREATE TABLE dw.dim_date
(
    date_key INT NOT NULL,
    calendar_date DATE NOT NULL,
    day_of_month TINYINT NOT NULL,
    day_name VARCHAR(20) NOT NULL,
    week_of_year TINYINT NOT NULL,
    month_number TINYINT NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    quarter_number TINYINT NOT NULL,
    year_number SMALLINT NOT NULL,
    year_month CHAR(7) NOT NULL,
    month_start_date DATE NOT NULL,
    month_end_date DATE NOT NULL,
    is_month_end BIT NOT NULL,
    is_weekend BIT NOT NULL,
    CONSTRAINT pk_dim_date PRIMARY KEY (date_key),
    CONSTRAINT uq_dim_date_business UNIQUE (calendar_date)
);
GO
