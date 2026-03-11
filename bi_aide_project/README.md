# BI Aide dbt Project (Snowflake + Data Vault 2.0)

## 1. What This Project Does (and Why)

This dbt project implements a layered transformation pipeline for pharmaceutical product data.

**Goal**
- Ingest raw pharmaceutical data from IQVIA (MIDAS) and standardize it into canonical, flat tables for downstream consumption (Semarchy MDM).

**Why Data Vault 2.0**
- It provides a scalable, auditable modeling pattern with strong lineage, predictable change tracking, and clear separation of concerns.
- Hubs/Links store relationships and business keys; Satellites store descriptive attributes and historical changes.

## 2. Architecture Overview

### 2.1 Layered Blueprint

This project follows a strict layered approach:

1) **Staging** (`models/stage/`)  
   - Materialization: `table`  
   - Responsibility:
     - `UNION ALL` disparate raw extracts (HQ vs non-HQ, monthly vs quarterly)
     - Explicit column selection + `NULL AS ...` to preserve union integrity
     - Standardize/derive fields used downstream (e.g., ISO country code)

2) **Raw Vault** (`models/raw_vault/`)  
   - Materialization: `incremental`  
   - Responsibility:
     - **Hubs**: store business keys + hashed keys (`HK_*`)
     - **Links**: connect hubs (relationship entities)
     - **Satellites**: store descriptive attributes + change tracking (`HASHDIFF`)

3) **Data Mart / Canonical** (`models/data_mart/`)  
   - Materialization: `table`
   - Responsibility:
     - Produce the canonical “flat” outputs for Semarchy
     - Apply final, “soft” business rules and formatting

4) **Data Quality** (`models/dq/`)  
   - Materialization: `incremental`
   - Responsibility:
     - Warn-and-proceed pattern (reject capture tables for missing mandatory fields)

### 2.2 Data Lineage (High-Level)


This project is designed to write only into the single schema defined by the active dbt target in `profiles.yml`.

- Do **not** set `+schema:` in `dbt_project.yml`
- Do **not** set `schema=` in individual models unless you explicitly intend to override (this project avoids overrides)

### 3.2 Identifier Casing Rules (Snowflake)

Snowflake treats unquoted identifiers as uppercase. If a table was created with uppercase column names (dbt default), then downstream references must be:

- uppercase: `GEOGRAPHIC_ID`
- or double-quoted: `"GEOGRAPHIC_ID"`

Avoid unquoted lowercase references (e.g., `geographic_id`) in downstream transformations; they can produce `invalid identifier` errors at runtime.

### 3.3 Union Integrity Rules

When writing `UNION ALL` statements:
- Never use `SELECT *`
- Always list columns explicitly in the same order
- Use `NULL AS column_name` for missing columns in any branch

## 3. Key Design Decisions (The “Why” Behind the Implementation)

This section explains the core architectural choices in plain language, plus the dbt/Data Vault terms you will see in the code.

### 3.1 Why We `UNION ALL` (HQ vs Non‑HQ, Monthly vs Quarterly)

IQVIA MIDAS arrives as multiple physical tables (monthly/quarterly, HQ/non‑HQ). If we treat each table independently, every downstream model must implement multiple joins/unions and keep four schemas in sync.

Instead, we:
- **Union early in staging** to create one “standardized dataset” (`stg_iqvia_midas`) that represents the logical source system.
- **Preserve lineage** by keeping a source dataset identifier (`SOURCE_DATASET_ID`) so downstream logic can still trace where each row came from.

Why `UNION ALL` (not `UNION`)?
- `UNION ALL` does not deduplicate and is cheaper/faster; deduplication should be an explicit business rule, not an accidental side effect.

Why explicit columns (no `SELECT *`)?
- The HQ/non‑HQ tables don’t always have the same columns.
- `SELECT *` makes unions fragile: a new column appearing in one branch can break compilation or shift column order silently.

### 3.2 Why We Generate Hash Keys (HK_*) in Data Vault

In Raw Vault:
- A **Business Key (BK)** is the natural identifier of a business concept (e.g., a product “as defined by the source”).
- A **Hash Key (HK_*)** is a stable, fixed-length surrogate key derived from the BK (commonly `MD5`).

We use hash keys because:
- Business keys can be long and composite; hashing makes joins smaller and more consistent.
- Hash keys let us integrate multiple sources using consistent keying patterns.
- Hash keys are deterministic: the same BK always produces the same HK (when you apply consistent formatting rules).

**How we build a correct HK (project standard)**
- Uppercase everything to avoid case differences creating different hashes.
- Use a delimiter (`'||'`) so values don’t “run together” (to avoid accidental collisions).
- Use `COALESCE(col, '')` so nulls don’t cause inconsistent hashes.

Example pattern:

```sql
MD5(
  UPPER(
    CONCAT_WS('||',
      COALESCE(GEOGRAPHIC_ID, ''),
      COALESCE(SOURCE_DATASET_ID, ''),
      COALESCE(PRODUCT_NAME, '')
    )
  )
)
```

### 3.3 Why We Generate HASHDIFF in Satellites

Satellites store descriptive attributes (manufacturer, ATC code, strength, etc.). In Data Vault, history is tracked by detecting attribute changes.

A **HASHDIFF** is:
- an `MD5` of the relevant descriptive attributes
- used to identify when something meaningfully changed for a given hub key

Why HASHDIFF is useful:
- It makes “change detection” fast: compare a single hash rather than many columns.
- It supports SCD2-style history naturally: when HASHDIFF changes, you insert a new satellite record.

Example pattern:

```sql
MD5(
  UPPER(
    CONCAT_WS('||',
      COALESCE(SUBSTANCE, ''),
      COALESCE(ATC_CODE, ''),
      COALESCE(MANUFACTURER, ''),
      COALESCE(STRENGTH, '')
    )
  )
)
```

### 3.4 Why We Join `dim_country` in Staging

The raw tables use human-readable country names. Canonical outputs require standardized country identifiers.

We enrich in staging because:
- It makes the “country standardization” reusable for all downstream models.
- It keeps marts/vault focused on modeling, not lookup mechanics.

### 3.5 Naming Conventions Used

Model name prefixes communicate the layer:
- `stg_` = staging integration
- `hub_` = hub (business keys + HK)
- `sat_` = satellite (attributes + HASHDIFF + history)
- `link_` = link (relationships between hubs)
- `dm_` = data mart / canonical output (Semarchy-facing)
- `reject_` = data quality reject capture

Column naming conventions:
- `HK_*` = hash keys in the vault
- `HASHDIFF` = satellite change fingerprint
- `LOAD_DATETIME` = ingestion timestamp (lineage)
- `RECORD_SOURCE` = source system identifier (lineage)

## 4. Repository Structure

```
bi_aide_project/
  dbt_project.yml
  models/
    sources.yml
    schema.yml
    stage/
      stg_iqvia_midas.sql
    raw_vault/
      hub_product.sql
      sat_product_iqvia.sql
      link_product_brand.sql
    data_mart/
      dm_standardized_product_file.sql
      dm_standardized_product_relationship.sql
      dm_standardized_product_identifier.sql
      dm_standardized_product_additional_attributes.sql
      dm_standardized_atc.sql
      dm_standardized_manufacturer.sql
    dq/
      reject_iqvia_product.sql
  seeds/
    dim_country.csv
```

## 5. Setup Instructions (Local / VDI)

### 5.1 Dependencies

- Python 3.9+ (recommended)
- `dbt-core` and `dbt-snowflake` (this repo has been validated with dbt 1.11.x)
- Network access to your Snowflake account

Install:

```bash
pip install dbt-core dbt-snowflake
```

Verify:

```bash
dbt --version
```

### 5.2 Required Snowflake Access

The dbt user/role must have:
- USAGE on the target database and schema
- CREATE TABLE / CREATE VIEW privileges in the target schema
- Read access to the IQVIA shared database/schema referenced in `models/sources.yml`

### 5.3 profiles.yml (Example Skeleton)

Create `~/.dbt/profiles.yml` (do not commit secrets to git). Example structure:

```yaml
bi_aide_project:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: <account>
      user: <user>
      password: <password>
      role: <role>
      warehouse: <warehouse>
      database: DEV_DFHPMS2EU_DB
      schema: DW_DFHPMS2EU_SEMARCHY_SCHEMA
      threads: 4
      client_session_keep_alive: false
```

## 6. How to Run (Operational Procedures)

### 6.1 Standard Build

```bash
dbt seed
dbt run
dbt test
```

### 6.2 Run Selected Models

```bash
dbt run -s stg_iqvia_midas
dbt run -s +dm_standardized_product_file
dbt run -s dm_standardized_product_identifier
```

### 6.3 When You Change Staging Columns

If you add/remove columns in `stg_iqvia_midas`, rebuild it so Snowflake physical columns match:

```bash
dbt run -s stg_iqvia_midas --full-refresh
```

This avoids runtime errors like:
- `SQL compilation error: invalid identifier '<COLUMN_NAME>'`

## 7. Implementation Details (Model-by-Model)

### 7.1 Sources (`models/sources.yml`)

The IQVIA MIDAS landing sources are defined in `models/sources.yml` and include:
- `MIDAS_BOEHRINGER_DASS_BI_MTH_STG`
- `MIDAS_BOEHRINGER_DASS_BI_QTR_STG`
- `MIDAS_BOEHRINGER_DASS_BI_MTH_HQ_STG`
- `MIDAS_BOEHRINGER_DASS_BI_QTR_HQ_STG`

### 7.2 Seed: `dim_country`

`seeds/dim_country.csv` provides ISO lookups used in staging. `stg_iqvia_midas` joins on country name to derive ISO2.

### 7.3 Stage: `stg_iqvia_midas`

Key behaviors:
- Unions four MIDAS extracts into a single standardized shape
- ISO country enrichment via `dim_country`
- Derives `PRESCRIPTION_REQUIRED_INDICATOR`
- Produces canonical staging columns used throughout vault + marts

Why it looks like this:
- This is the “integration point” for multiple MIDAS extracts, so downstream models do not need to care about HQ/non‑HQ table structure.
- The output column list is treated as a contract; marts and vault models assume these columns exist and are consistently typed.

Output columns include:
- Identifiers: `BUSINESS_KEY`, `GEOGRAPHIC_ID`, `DATASET_ID`, `SOURCE_DATASET_ID`, `PRODUCT_TYPE_CODE`, `PRODUCT_NAME`
- Product attributes: `BRAND_NAME`, `SUBSTANCE`, ATC breakdown (`ATC1..ATC4`), `ATC_CODE`, `MANUFACTURER`, `CORPORATION`, etc.

### 7.4 Raw Vault: `hub_product`

Purpose:
- Store product business keys and a stable hash key `HK_PRODUCT`.

Business Key Contract:
- `GEOGRAPHIC_ID + SOURCE_DATASET_ID + PRODUCT_NAME`

Why the hub is incremental:
- Once a hub key exists, it should never change. Incremental loads avoid reprocessing all historical keys each run.

### 7.5 Raw Vault: `sat_product_iqvia`

Purpose:
- Store descriptive attributes for products and enable SCD2-style change tracking using `HASHDIFF`.

Current behavior:
- Generates `HK_PRODUCT` and `HASHDIFF`
- Loads incrementally based on load timestamp logic

Note:
- The project standard for hashing is:
  - `MD5(UPPER(CONCAT_WS('||', COALESCE(col,''), ...)))`
  - to prevent null contamination and enforce consistent casing

Why satellites exist separately from hubs:
- The hub answers “what is the business thing?”
- The satellite answers “what did we know about it at a point in time?”

### 7.6 Raw Vault: `link_product_brand`

Purpose:
- Capture relationships between PMP products and their parent Brand.

Why links exist:
- Relationships change independently of attributes.
- Links prevent duplicated relationship columns in satellites and keep the vault normalized and extensible.

### 7.7 Data Mart: Canonical Outputs

#### dm_standardized_product_file
- Canonical product flat table for Semarchy
- Joins `hub_product` to latest `sat_product_iqvia` (latest record per `HK_PRODUCT`)

Why we pick “latest satellite record”:
- Semarchy canonical extracts generally need the current view of a product.
- Vault keeps full history, but the mart publishes a simplified “current state” representation.

#### dm_standardized_product_relationship
- Canonical relationship table: Brand (parent) → PMP (child)
- Sourced from `link_product_brand`

#### dm_standardized_product_identifier
- Canonical identifier table derived from staging
- Produces a row per product and identifier type

Why identifier tables are derived from staging:
- The STTM defines identifier_id as a human-readable concat of source identifiers (not vault keys).
- Staging already contains the raw source identifiers and the dataset identifiers needed to reproduce the STTM definition.

#### dm_standardized_product_additional_attributes
- Canonical “additional attributes” output (e.g., `CHC_PRODUCT_FLAG`)
- Currently derives `CHC_PRODUCT_FLAG` from `SEMI_ETHICAL`

#### dm_standardized_atc
- Canonical ATC hierarchy/reference extracted from staging (`ATC1..ATC4`)

#### dm_standardized_manufacturer
- Canonical manufacturer/corporation reference extracted from staging

### 7.8 Data Quality: `reject_iqvia_product`

Warn-and-proceed DQ capture:
- Identifies missing mandatory fields (e.g., `GEOGRAPHIC_ID`, `SOURCE_DATASET_ID`, `PRODUCT_NAME`)
- Writes failing rows to a reject table for alerting and downstream triage

## 8. Tests, Quality Gates, and Troubleshooting

### 8.1 Tests

Model and seed tests are defined in `models/schema.yml`, including:
- not-null checks for mandatory fields
- uniqueness checks for hub/link keys where applicable
- accepted values checks for key enums (e.g., `PRODUCT_TYPE_CODE`)

Run:

```bash
dbt test
```

### 8.2 Common Failure Modes

**Invalid Identifier**
- Cause: a downstream model references a column not present in the physical upstream table.
- Fix:
  - Rebuild upstream with `--full-refresh` if the column set changed.
  - Ensure casing is consistent (UPPERCASE or quoted).

**Schema/Permission Errors**
- Cause: model tries to write to an unexpected schema (often due to `+schema:` overrides).
- Fix:
  - Remove schema overrides and rely on the schema specified in `profiles.yml`.

## 9. Performance and Optimization Techniques Used

Implemented patterns:
- Incremental materializations for Raw Vault objects (reduce compute and cost)
- `QUALIFY ROW_NUMBER()` to select the latest satellite record per hub key in marts
- Explicit column selection in unions to avoid schema drift and compilation surprises

## 10. Orchestration Guidance

This repo is compatible with any scheduler that can run shell commands (Airflow, dbt Cloud, Azure DevOps, etc.).

Recommended job order:
1) `dbt seed`
2) `dbt run -s stage`
3) `dbt run -s raw_vault`
4) `dbt run -s data_mart dq`
5) `dbt test`

## 11. Lessons Learned / Best Practices

- Treat staging output as a stable contract: if marts depend on a column, staging must expose it consistently.
- Avoid schema overrides in constrained Snowflake environments; rely on `profiles.yml`.
- In Snowflake, casing is a runtime concern; reference columns in uppercase (or quote them) to prevent `invalid identifier`.
- Maintain a clear business key contract per hub and ensure link keys align with hub key definitions.
- Keep seed reference data clean (whitespace and formatting issues can silently break lookups).
- When in doubt, make “why” explicit in the mart layer: marts are where business-facing semantics should be readable and stable.
