/*
    Azure Synapse / Microsoft Fabric oriented dimensional model
    Domain: Construction and real-estate project controlling

    Conventions
    - Schema dw: dimensional warehouse objects
    - Surrogate keys: integer identity columns
    - Business keys: source-system stable identifiers
    - Unknown member pattern: reserve key 0 in ETL when source references are missing
*/

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'dw')
BEGIN
    EXEC('CREATE SCHEMA dw');
END;
GO
