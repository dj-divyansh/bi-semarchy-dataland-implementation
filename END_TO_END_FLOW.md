## End-to-End Flow (Standardized Data Vault 2.0 Pattern)

### 1) Goal
This flow implements a standardized Data Vault 2.0 load pattern for an “Entity” dataset using:
- A Hub for business-entity identifiers (keyed by a HASHKEY).
- A Satellite for descriptive attributes (change detection via a HASHDIFF).
- A History/Archive table to retain prior Satellite versions.
- A reject mechanism to exclude failed data quality rows.
- Audit logging to capture run status and delta metrics.

This document is intentionally source-system neutral. Object names in your project may differ; the pattern remains the same.

### 2) Terminology (Use in This Document)
- **Business Key**: Natural key identifying the entity (e.g., external ID).
- **HASHKEY**: Hash of the Business Key (and/or standardized key components). Used as the Hub primary key and as the join key between Hub and Satellite.
- **HASHDIFF**: Hash of the descriptive attributes (normalized to stable string representations). Used to detect changes in the Satellite.
- **Active Satellite Row**: The current row for a given HASHKEY (typically indicated by `END_DATETIME IS NULL`).
- **Delta**: The subset of rows that are new or changed relative to the current active state.

Project mapping (for reference only):
- `IDENTIFIER_MD5` = HASHKEY
- `RECORD_MD5` = HASHDIFF

### 3) Core Objects (Conceptual)
- **Staging View / Table**: Prepares and standardizes incoming data, including HASHKEY and HASHDIFF.
- **Reject Set**: Contains entity keys that fail validation; these are excluded from downstream loads.
- **Hub**: Stores unique entity identifiers (HASHKEY + Business Key + metadata).
- **Satellite**: Stores descriptive attributes and HASHDIFF + metadata; maintains “active” state.
- **Satellite History**: Stores expired prior versions of Satellite rows.
- **Audit Tables**:
  - **Batch control**: run-level metadata (status, start/end, rejects count).
  - **Process log**: step/model/macro-level execution status and delta metrics.

### 4) Standard Columns and Meaning
This flow assumes the following conventional Data Vault metadata columns exist on Hub/Satellite:
- `ETL_BATCH_ID`: Identifier for the pipeline run/batch (propagates through Hub/Sat).
- `RECORD_SOURCE`: Standardized source identifier (string).
- `LOAD_DATETIME`: Load timestamp (commonly “ingestion time” into the target structure).

Satellite temporal columns:
- `START_DATETIME`: Timestamp when the current version becomes active.
- `UPDATED_DATETIME`: Timestamp used to “touch” unchanged active rows (proof of continued presence).
- `END_DATETIME`: Timestamp when a version expires. Active row is typically `END_DATETIME IS NULL`.

### 4) Execution Modes
This project supports two distinct execution paths:

#### A) dbt model runs (typical pipeline execution)
When you run `dbt run`, models materialize (views/tables/incrementals) in dependency order, and audit hooks track:
- Run start → marks batch as RUNNING.
- Model start/end → logs each model execution (optional, depending on hooks).
- Run end → marks batch as SUCCESS/FAILED and stamps end time.

#### B) Delta load macro execution (operational delta procedure)
For the delta load, use the MERGE-based macro:
- [delta_load_product_file_v2_merge](macro/delta_load_product_file%20v2%20merge.sql)
- Typical invocation:
  - `dbt run-operation delta_load_product_file_v2_merge`

This macro performs the delta load by creating temporary delta tables and applying changes to Hub/Satellite/History while writing audit metrics.

### 5) Data Preparation (Staging + Hashing)
The staging layer is responsible for producing:
- The **Business Key** (natural key)
- The **HASHKEY** derived from the Business Key (or its standardized components)
- The **HASHDIFF** derived from the descriptive attributes
- Standardized metadata columns (examples): `LOAD_DATETIME`, `ETL_BATCH_ID`, `RECORD_SOURCE`

Hashing rules (recommended standard):
- Apply deterministic normalization before hashing (trim, upper/lower convention, null handling).
- Use stable string conversions for non-string types (dates, numbers, booleans).
- Ensure attribute ordering is fixed and consistent when building HASHDIFF input.

### 6) Data Quality and Reject Handling
Rows failing validation are not loaded into the vault structures.

Standard pattern:
1. Validate required fields (non-null, non-empty, type-conformant).
2. Populate a **Reject Set** keyed by the Business Key (or a derived key).
3. Exclude reject keys from downstream processing.

This ensures:
- Hub/Satellite loads do not ingest invalid entities.
- Reject counts can be tracked per batch in audit tables.

### 7) Delta Load Macro: Detailed Step-by-Step (MERGE-Based)
Reference implementation:
- [delta_load_product_file_v2_merge](macro/delta_load_product_file%20v2%20merge.sql)

At a high level, the macro performs:
- Build a DQ-passed staging temp table.
- Mark unchanged Satellite rows (touch `UPDATED_DATETIME`).
- Build a delta temp table (exclude unchanged).
- Compute delta counts (inserted vs updated) for auditing.
- Upsert Hub for truly new HASHKEYs.
- Archive changed Satellite rows into History.
- Merge into Satellite to update changed rows and insert new rows.
- Write audit start/success with delta metrics.

#### Step 0: Build DQ-passed staging temp table
Purpose:
- Apply required-field rules once.
- Exclude reject keys once.
- Reuse this filtered set in later steps.

Output:
- Temporary table containing only valid rows and their HASHKEY/HASHDIFF.

#### Step 1: Touch unchanged active Satellite rows
Definition (UNCHANGED):
- Incoming HASHKEY matches an active Satellite row, and incoming HASHDIFF equals active HASHDIFF.

Action:
- Update `UPDATED_DATETIME` on the active Satellite row to indicate “still present in source”.

Why it matters:
- Preserves the active row while recording that it was re-observed.

#### Step 1a: Build delta temp table (changed + new)
Definition (DELTA):
- Valid incoming rows where there is no active Satellite row with the same HASHKEY + HASHDIFF.

Action:
- Create a temporary delta table used by subsequent steps.

Why it matters:
- Ensures later steps only process records that are new or changed.

#### Step 1b: Compute delta metrics for auditing
Purpose:
- Calculate delta volumes once (from the delta temp table) to support audit logging.

Common metric definitions:
- Inserted: delta rows whose HASHKEY does not exist in Hub.
- Updated: delta rows whose HASHKEY exists with a different active HASHDIFF.

#### Step 2: Upsert Hub (insert new entity keys)
Definition (NEW entity):
- HASHKEY does not exist in Hub.

Action:
- `MERGE` into Hub and insert missing HASHKEYs with Business Key and metadata.

Why it matters:
- Ensures the Hub contains the entity identifier before inserting a Satellite row for it.

#### Step 3: Archive changed Satellite rows into History
Definition (CHANGED):
- HASHKEY exists and is active in Satellite, but incoming HASHDIFF differs from active HASHDIFF.

Action:
- Insert the current active Satellite row into Satellite History and stamp `END_DATETIME = now`.
- De-duplicate the archive insert (so the same prior version is not archived twice).

Why it matters:
- Preserves the previous attribute state for auditing and temporal querying.

#### Step 4: Merge into Satellite (update changed + insert new)
Actions:
- For CHANGED (matched on active HASHKEY, HASHDIFF differs): update Satellite row to the new attribute state and refresh metadata/timestamps.
- For NEW (no active Satellite row): insert a new Satellite row.

Active-row convention:
- The active Satellite row is the one with `END_DATETIME IS NULL`.

### 8) Audit Logging and Metrics
Audit is written at two levels:

#### Run-level (batch control)
Tracks:
- `ETL_BATCH_ID`
- start/end timestamps
- overall status (RUNNING/SUCCESS/FAILED)
- rejected count (from the Reject set)

#### Process-level (process log)
Tracks:
- status for a specific execution unit (model or macro)
- start/end timestamps
- delta inserted/updated metrics
- error message (if any)

Delta macro metrics:
- **DELTA_INSERTED_ROWS**: count of delta rows whose HASHKEY does not exist in Hub (new entities).
- **DELTA_UPDATED_ROWS**: count of delta rows whose HASHKEY exists with a different HASHDIFF than the active Satellite (changed entities).
- **ROWS_PROCESSED**: inserted + updated

### 9) How to Evaluate Efficiency
Efficiency should be assessed using:
- **Audit process durations**:
  - `runtime = END_TIMESTAMP - START_TIMESTAMP` for the macro’s `MODEL_NAME`
- **Delta volume**:
  - Compare runtime vs. (DELTA_INSERTED_ROWS + DELTA_UPDATED_ROWS)
- **Warehouse behavior**:
  - Large MERGE operations can be dominated by clustering/micro-partition pruning and join selectivity.

Recommended query pattern:
- Filter `AUDIT_PROCESS_LOG` to the macro name, compute elapsed time, and optionally compute rows/sec.

Example (Snowflake-style):
```sql
select
  model_name,
  start_timestamp,
  end_timestamp,
  datediff('second', start_timestamp, end_timestamp) as runtime_seconds,
  rows_processed,
  delta_inserted_rows,
  delta_updated_rows
from <db>.<schema>.audit_process_log
where model_name = 'delta_load_product_file_v2_merge'
order by start_timestamp desc;
```

### 10) Implementation Notes (Standard)
- HASHKEY and HASHDIFF should be computed in staging for consistency and to avoid duplicate logic.
- Reject filtering should be applied before computing deltas to prevent invalid keys from causing churn.
- History archiving should happen before overwriting the active Satellite row for changed entities.
- The Hub should be populated before inserting new Satellite rows for new entities.

### 11) Operational Considerations
- Idempotency: reruns should not create duplicate Hub keys; History inserts should be de-duplicated by HASHDIFF/version logic.
- Concurrency: avoid running multiple delta operations for the same entity domain at the same time unless the target tables are protected (locks/warehouse-level controls).
- Observability: prefer audit-derived runtime and delta volumes over wall-clock logs; these align with actual target-side work performed.
