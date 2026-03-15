## ☁️ Azure Cloud Integration

This project also demonstrates a **hybrid cloud data engineering architecture** using Microsoft Azure.

### Azure Data Lake Storage Gen2

Source CSV files are stored in **Azure Data Lake Storage Gen2** in a raw landing zone.

Example structure:

```
raw/
 ├ projects.csv
 ├ budgets.csv
 ├ costs.csv
 ├ employees.csv
 └ suppliers.csv
```

This layer represents the **unchanged source data**.

---

### Azure Data Factory Ingestion Pipeline

An **Azure Data Factory pipeline** was implemented to automatically ingest files from the `raw` container into an `ingested` container.

Pipeline pattern:

```
Get Metadata (list files in raw)
        ↓
ForEach
        ↓
Copy Activity
```

The pipeline dynamically processes every file found in the raw folder.

Example data flow:

```
raw/projects.csv   → ingested/projects.csv
raw/budgets.csv    → ingested/budgets.csv
raw/costs.csv      → ingested/costs.csv
raw/employees.csv  → ingested/employees.csv
raw/suppliers.csv  → ingested/suppliers.csv
```

This design implements a **metadata‑driven ingestion pipeline**, which is a common pattern in modern cloud data platforms.

---

## 🏗 Current Architecture

```
Azure Data Lake
   │
   ├ raw
   │   source CSV files
   │
   ▼
Azure Data Factory
   metadata‑driven ingestion pipeline
   │
   ▼
ingested
   ingestion layer
```

---

## 🚧 Planned Enhancements

The next development steps will extend this project into a full cloud data warehouse pipeline.

### Python Transformation Layer

Data from the **ingested layer** will be processed using Python scripts:

```
src/transform_data.py
src/load_dimensions.py
src/load_facts.py
```

Typical transformations include:

- data cleaning
- type normalization
- missing value handling
- referential integrity checks

---

### Dimensional Data Warehouse

The curated data will be modeled using a **Star Schema**.

Dimensions:

```
dim_project
dim_employee
dim_supplier
dim_region
dim_date
```

Fact tables:

```
fact_project_cost
fact_project_budget
fact_project_progress
```

---

### Slowly Changing Dimensions

Project changes will be handled using **SCD Type 2 logic**, implemented in:

```
src/scd_type2.py
sql/03_scd2_dim_project.sql
```

---

### Future Cloud Integration

In a future version, the Python pipeline will **directly read data from Azure Data Lake**, removing the need for local intermediate files.

Target architecture:

```
Azure Data Lake
raw
   ↓
Azure Data Factory
   ↓
ingested
   ↓
Python transformation
   ↓
curated
   ↓
Star schema
   ↓
Power BI
```
