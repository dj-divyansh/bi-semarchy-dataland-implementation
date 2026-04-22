{#
    MACRO: delta_load_product_file_v2_merge
    ============================================================
    Purpose:
        MERGE-based delta/CDC load using Data Vault 2.0 conventions.
        Modularized to support multiple source systems that share the same structure.

    Delta Logic Overview (same as original):
        1. UNCHANGED: HASHKEY + HASHDIFF match active Satellite -> touch UPDATED_DATETIME
        2. CHANGED:   HASHKEY matches but HASHDIFF differs -> archive old Satellite row to History
        3. NEW:       HASHKEY not in Hub -> insert into Hub and Satellite

    Approach:
        Step 1: MERGE on SAT (UNCHANGED)
        Step 1a: Build delta temp (exclude unchanged)
        Step 2: MERGE on HUB (NEW keys)
        Step 3: INSERT to HIST (archive changed)
        Step 4: MERGE on SAT (CHANGED + NEW)

    Tables (generic):
        - Source (staging view/table): provides HASHKEY (entity) + HASHDIFF (attributes)
        - Hub: stores HASHKEY + business key
        - Satellite: stores attributes, HASHDIFF, temporal columns
        - History: archives expired Satellite versions
        - Reject: keys failing DQ, excluded from delta

    Usage examples:
        dbt run-operation delta_load_product_file_v2_merge
        dbt run-operation delta_load_product_file_v2_merge --args '{"stg_model":"STG_PRODUCT_XYZ","hub_model":"HUB","sat_model":"SAT_PRODUCT_FILE","hist_model":"SAT_PRODUCT_HIST","reject_model":"REJECT","record_source":"XYZ_SYSTEM","audit_model_name":"delta_load_product_file_v2_merge:xyz"}'
#}
{% macro delta_load_product_file_v2_merge(
    stg_model='STG_PRODUCT_VEEVA',
    hub_model='HUB',
    sat_model='SAT_PRODUCT_FILE',
    hist_model='SAT_PRODUCT_HIST',
    reject_model='REJECT',
    record_source='VEEVA_CRM',
    source_system=None,
    audit_model_name=None,
    enable_watermark_filter=true,
    watermark_column='SOURCE_LASTMODIFIEDDATE',
    watermark_lookback_minutes=5
) %}
 
    {% set etl_batch_id = env_var('ETL_BATCH_ID', env_var('DBT_JOB_RUN_ID', invocation_id)) %}
    {% set hub_table = ref(hub_model) %}
    {% set sat_table = ref(sat_model) %}
    {% set hist_table = ref(hist_model) %}
    {% set stg_table = ref(stg_model) %}
    {% set reject_table = ref(reject_model) %}

    {% set default_record_source_max_len = 255 %}
    {% set sat_record_source_max_len = default_record_source_max_len %}
    {% set hub_record_source_max_len = default_record_source_max_len %}
    {% if execute %}
        {% set sat_rs_len_sql %}
            SELECT CHARACTER_MAXIMUM_LENGTH
            FROM {{ sat_table.database }}.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{{ sat_table.schema }}'
              AND TABLE_NAME = '{{ sat_table.identifier }}'
              AND COLUMN_NAME = 'RECORD_SOURCE'
            LIMIT 1
        {% endset %}
        {% set sat_rs_len_res = run_query(sat_rs_len_sql) %}
        {% if sat_rs_len_res is not none and (sat_rs_len_res.rows | length) > 0 and sat_rs_len_res.rows[0][0] is not none %}
            {% set sat_record_source_max_len = sat_rs_len_res.rows[0][0] %}
        {% endif %}

        {% set hub_rs_len_sql %}
            SELECT CHARACTER_MAXIMUM_LENGTH
            FROM {{ hub_table.database }}.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{{ hub_table.schema }}'
              AND TABLE_NAME = '{{ hub_table.identifier }}'
              AND COLUMN_NAME = 'RECORD_SOURCE'
            LIMIT 1
        {% endset %}
        {% set hub_rs_len_res = run_query(hub_rs_len_sql) %}
        {% if hub_rs_len_res is not none and (hub_rs_len_res.rows | length) > 0 and hub_rs_len_res.rows[0][0] is not none %}
            {% set hub_record_source_max_len = hub_rs_len_res.rows[0][0] %}
        {% endif %}
    {% endif %}

    {% set tmp_dq_table = sat_table.database ~ '.' ~ sat_table.schema ~ '.TMP_STG_PRODUCT_DQ' %}
    {% set tmp_delta_table = sat_table.database ~ '.' ~ sat_table.schema ~ '.TMP_STG_PRODUCT_DELTA' %}
    {% set tmp_delta_counts_table = sat_table.database ~ '.' ~ sat_table.schema ~ '.TMP_PRODUCT_DELTA_COUNTS' %}
    {% set dq_stg = tmp_dq_table %}
    {% set filtered_stg = tmp_delta_table %}

    {% set audit_db = target.database %}
    {% set audit_schema = 'DW_DFHPMS2EU_SEMARCHY_SCHEMA' %}
    {% set audit_process_log = audit_db ~ '.' ~ audit_schema ~ '.AUDIT_PROCESS_LOG' %}
    {% set source_system = (source_system if source_system is not none else (record_source | trim | upper)) %}
    {% set audit_source_watermark = audit_db ~ '.' ~ audit_schema ~ '.AUDIT_SOURCE_WATERMARK' %}
    {% set watermark_source_object = stg_model | trim | upper %}
    {% if audit_model_name is none %}
        {% set audit_model_name = 'delta_load_product_file_v2_merge:' ~ stg_model %}
    {% endif %}

    {% set audit_start_sql %}
        MERGE INTO {{ audit_process_log }} tgt
        USING (
            SELECT
                '{{ etl_batch_id }}' AS ETL_BATCH_ID,
                '{{ audit_std_layer_name('delta') }}' AS LAYER_NAME,
                'SAT_PRODUCT' AS MODEL_NAME,
                '{{ source_system }}' AS SOURCE_SYSTEM,
                'RUNNING' AS PROCESS_STATUS,
                CURRENT_TIMESTAMP() AS START_TIMESTAMP,
                NULL::TIMESTAMP_LTZ AS END_TIMESTAMP,
                0::NUMBER AS ROWS_PROCESSED,
                ''::VARCHAR AS ERROR_MESSAGE,
                0::NUMBER AS DELTA_INSERTED_ROWS,
                0::NUMBER AS DELTA_UPDATED_ROWS
        ) src
        ON tgt.ETL_BATCH_ID = src.ETL_BATCH_ID
           AND tgt.MODEL_NAME = src.MODEL_NAME
        WHEN MATCHED THEN
            UPDATE SET
                LAYER_NAME = src.LAYER_NAME,
                SOURCE_SYSTEM = src.SOURCE_SYSTEM,
                PROCESS_STATUS = 'RUNNING',
                START_TIMESTAMP = COALESCE(tgt.START_TIMESTAMP, src.START_TIMESTAMP),
                END_TIMESTAMP = NULL,
                ROWS_PROCESSED = src.ROWS_PROCESSED,
                ERROR_MESSAGE = src.ERROR_MESSAGE,
                DELTA_INSERTED_ROWS = src.DELTA_INSERTED_ROWS,
                DELTA_UPDATED_ROWS = src.DELTA_UPDATED_ROWS
        WHEN NOT MATCHED THEN
            INSERT (ETL_BATCH_ID, LAYER_NAME, MODEL_NAME, SOURCE_SYSTEM, PROCESS_STATUS, START_TIMESTAMP, END_TIMESTAMP, ROWS_PROCESSED, ERROR_MESSAGE, DELTA_INSERTED_ROWS, DELTA_UPDATED_ROWS)
            VALUES (src.ETL_BATCH_ID, src.LAYER_NAME, src.MODEL_NAME, src.SOURCE_SYSTEM, src.PROCESS_STATUS, src.START_TIMESTAMP, src.END_TIMESTAMP, src.ROWS_PROCESSED, src.ERROR_MESSAGE, src.DELTA_INSERTED_ROWS, src.DELTA_UPDATED_ROWS);
    {% endset %}

    {% set audit_success_sql %}
        UPDATE {{ audit_process_log }}
        SET
            PROCESS_STATUS = 'SUCCESS',
            END_TIMESTAMP = CURRENT_TIMESTAMP(),
            ROWS_PROCESSED = (
                SELECT (DELTA_INSERTED_ROWS + DELTA_UPDATED_ROWS)::NUMBER
                FROM {{ tmp_delta_counts_table }}
            ),
            ERROR_MESSAGE = '',
            DELTA_INSERTED_ROWS = (SELECT DELTA_INSERTED_ROWS FROM {{ tmp_delta_counts_table }}),
            DELTA_UPDATED_ROWS = (SELECT DELTA_UPDATED_ROWS FROM {{ tmp_delta_counts_table }})
        WHERE ETL_BATCH_ID = '{{ etl_batch_id }}'
          AND MODEL_NAME = 'SAT_PRODUCT'
          AND PROCESS_STATUS = 'RUNNING';
    {% endset %}

    {% set create_dq_stg_sql %}
        CREATE OR REPLACE TEMPORARY TABLE {{ dq_stg }} AS
        SELECT stg.*
        FROM {{ stg_table }} stg
        LEFT JOIN {{ reject_table }} r
            ON upper(trim(stg.dataset_product_id)) = r.DATASET_PRODUCT_ID
        WHERE stg.GEOGRAPHIC_ID IS NOT NULL AND TRIM(stg.GEOGRAPHIC_ID) != ''
          AND stg.PRODUCT_NAME IS NOT NULL AND TRIM(stg.PRODUCT_NAME) != ''
          AND stg.DATASET_PRODUCT_ID IS NOT NULL AND TRIM(stg.DATASET_PRODUCT_ID) != ''
          {% if enable_watermark_filter %}
          AND stg.{{ watermark_column }} >= (
              SELECT COALESCE(
                  DATEADD('minute', -{{ watermark_lookback_minutes }}, MAX(WATERMARK_VALUE)),
                  '1900-01-01'::TIMESTAMP_LTZ
              )
              FROM {{ audit_source_watermark }}
              WHERE SOURCE_SYSTEM = '{{ source_system }}'
                AND SOURCE_OBJECT = '{{ watermark_source_object }}'
                AND WATERMARK_COLUMN = '{{ watermark_column }}'
          )
          {% endif %}
          AND r.DATASET_PRODUCT_ID IS NULL
    {% endset %}
 
    {% set sat_cols = adapter.get_columns_in_relation(sat_table) %}
    {% set sat_cols_upper = [] %}
    {% for c in sat_cols %}
        {% do sat_cols_upper.append(c.name | upper) %}
    {% endfor %}
    {% if 'HASHKEY' in sat_cols_upper %}
        {% set sat_hashkey_col = 'HASHKEY' %}
    {% elif 'IDENTIFIER_MD5' in sat_cols_upper %}
        {% set sat_hashkey_col = 'IDENTIFIER_MD5' %}
    {% else %}
        {% set sat_hashkey_col = 'HASHKEY' %}
    {% endif %}
    {% if 'HASHDIFF' in sat_cols_upper %}
        {% set sat_hashdiff_col = 'HASHDIFF' %}
    {% elif 'RECORD_MD5' in sat_cols_upper %}
        {% set sat_hashdiff_col = 'RECORD_MD5' %}
    {% else %}
        {% set sat_hashdiff_col = 'HASHDIFF' %}
    {% endif %}

    {% set hub_cols = adapter.get_columns_in_relation(hub_table) %}
    {% set hub_cols_upper = [] %}
    {% for c in hub_cols %}
        {% do hub_cols_upper.append(c.name | upper) %}
    {% endfor %}
    {% if 'HASHKEY' in hub_cols_upper %}
        {% set hub_hashkey_col = 'HASHKEY' %}
    {% elif 'IDENTIFIER_MD5' in hub_cols_upper %}
        {% set hub_hashkey_col = 'IDENTIFIER_MD5' %}
    {% else %}
        {% set hub_hashkey_col = 'HASHKEY' %}
    {% endif %}

    {% set hist_cols = adapter.get_columns_in_relation(hist_table) %}
    {% set hist_cols_upper = [] %}
    {% for c in hist_cols %}
        {% do hist_cols_upper.append(c.name | upper) %}
    {% endfor %}
    {% if 'HASHDIFF' in hist_cols_upper %}
        {% set hist_hashdiff_col = 'HASHDIFF' %}
    {% elif 'RECORD_MD5' in hist_cols_upper %}
        {% set hist_hashdiff_col = 'RECORD_MD5' %}
    {% else %}
        {% set hist_hashdiff_col = sat_hashdiff_col %}
    {% endif %}

    {# ------------------------------------------------------------------
       STEP 1: MERGE on SAT for UNCHANGED records.
       Updates UPDATED_DATETIME where both hashkey and hashdiff
       match (no data change). Also creates filtered delta temp table
       for subsequent steps (excluding unchanged records).
    ------------------------------------------------------------------ #}
    {% set merge_unchanged_sat_sql %}
        MERGE INTO {{ sat_table }} sat
        USING {{ dq_stg }} stg
            ON sat.{{ sat_hashkey_col }} = stg.HASHKEY
           AND sat.{{ sat_hashdiff_col }} = stg.HASHDIFF
           AND sat.END_DATETIME IS NULL
        WHEN MATCHED THEN
            UPDATE SET sat.UPDATED_DATETIME = CURRENT_TIMESTAMP()
    {% endset %}
 
    {% set create_filtered_stg_sql %}
        CREATE OR REPLACE TEMPORARY TABLE {{ filtered_stg }} AS
        SELECT stg.*
        FROM {{ dq_stg }} stg
        WHERE NOT EXISTS (
              SELECT 1
              FROM {{ sat_table }} sat
              WHERE sat.{{ sat_hashkey_col }} = stg.HASHKEY
                AND sat.{{ sat_hashdiff_col }} = stg.HASHDIFF
                AND sat.END_DATETIME IS NULL
          )
    {% endset %}

    {% set sat_columns = adapter.get_columns_in_relation(sat_table) %}
    {% set hist_insert_cols = [] %}
    {% set hist_select_cols = [] %}
    {% for col in sat_columns %}
        {% if col.name | upper != 'END_DATETIME' %}
            {% do hist_insert_cols.append(col.name) %}
            {% do hist_select_cols.append('sat.' ~ col.name) %}
        {% endif %}
    {% endfor %}
    {% do hist_insert_cols.append('END_DATETIME') %}
    {% do hist_select_cols.append('CURRENT_TIMESTAMP() AS END_DATETIME') %}
 
    {# ------------------------------------------------------------------
       STEP 2: Archive changed records to SAT_PRODUCT_HIST.
       Must run BEFORE the SAT merge for changed records so we capture
       the old version before it gets overwritten.
    ------------------------------------------------------------------ #}
    {% set archive_changed_sql %}
        INSERT INTO {{ hist_table }} ({{ hist_insert_cols | join(', ') }})
        SELECT
            {{ hist_select_cols | join(',\n            ') }}
        FROM {{ sat_table }} sat
        INNER JOIN {{ filtered_stg }} stg
            ON stg.HASHKEY = sat.{{ sat_hashkey_col }}
        WHERE sat.END_DATETIME IS NULL
          AND stg.HASHDIFF != sat.{{ sat_hashdiff_col }}
          AND NOT EXISTS (
              SELECT 1
              FROM {{ hist_table }} h
              WHERE h.{{ hist_hashdiff_col }} = sat.{{ sat_hashdiff_col }}
          )
    {% endset %}
 
    {# ------------------------------------------------------------------
       STEP 3: MERGE on SAT for CHANGED + NEW records.
       - WHEN MATCHED (changed): UPDATE all columns with new staging data,
         reset START_DATETIME (equivalent to original delete + insert).
       - WHEN NOT MATCHED (new): inserts the SAT row after HUB is upserted.
    ------------------------------------------------------------------ #}
    {% set merge_sat_sql %}
        MERGE INTO {{ sat_table }} sat
        USING {{ filtered_stg }} stg
            ON sat.{{ sat_hashkey_col }} = stg.HASHKEY
           AND sat.END_DATETIME IS NULL
        WHEN MATCHED AND stg.HASHDIFF != sat.{{ sat_hashdiff_col }} THEN
            UPDATE SET
                sat.{{ sat_hashdiff_col }} = stg.HASHDIFF,
                sat.LOAD_DATETIME = CURRENT_TIMESTAMP(),
                sat.ETL_BATCH_ID = '{{ etl_batch_id }}',
                sat.RECORD_SOURCE = LEFT(COALESCE(stg.RECORD_SOURCE, '{{ record_source }}'), {{ sat_record_source_max_len }}),
                sat.GEOGRAPHIC_ID = stg.GEOGRAPHIC_ID,
                sat.DATASET_ID = stg.DATASET_ID,
                sat.SOURCE_DATASET_ID = stg.SOURCE_DATASET_ID,
                sat.DATASET_PRODUCT_ID = stg.DATASET_PRODUCT_ID,
                sat.PRODUCT_TYPE_CODE = stg.PRODUCT_TYPE_CODE,
                sat.PRODUCT_NAME = stg.PRODUCT_NAME,
                sat.STANDARDIZED_PRODUCT_NAME = stg.STANDARDIZED_PRODUCT_NAME,
                sat.DESCRIPTION = stg.DESCRIPTION,
                sat.PRODUCT_STATUS_CODE = stg.PRODUCT_STATUS_CODE,
                sat.EFFECTIVE_DATE = stg.EFFECTIVE_DATE,
                sat.END_DATE = stg.END_DATE,
                sat.BRAND_NAME = stg.BRAND_NAME,
                sat.GENERIC_NAMES = stg.GENERIC_NAMES,
                sat.APPROVED_DATE = stg.APPROVED_DATE,
                sat.MARKETING_START_DATE = stg.MARKETING_START_DATE,
                sat.MARKETING_END_DATE = stg.MARKETING_END_DATE,
                sat.EXPIRATION_DATE = stg.EXPIRATION_DATE,
                sat.SUBSTANCE = stg.SUBSTANCE,
                sat.DOSE_FORM_CODE = stg.DOSE_FORM_CODE,
                sat.STRENGTH = stg.STRENGTH,
                sat.ROUTE_OF_ADMINISTRATION_CODE = stg.ROUTE_OF_ADMINISTRATION_CODE,
                sat.PHARMACEUTICAL_PRODUCT_QUANTITY = stg.PHARMACEUTICAL_PRODUCT_QUANTITY,
                sat.UNIT_OF_PRESENTATION_CODE = stg.UNIT_OF_PRESENTATION_CODE,
                sat.PRIMARY_PACKAGE_QUANTITY = stg.PRIMARY_PACKAGE_QUANTITY,
                sat.PRIMARY_PACKAGE_TYPE_CODE = stg.PRIMARY_PACKAGE_TYPE_CODE,
                sat.SECONDARY_PACKAGE_QUANTITY = stg.SECONDARY_PACKAGE_QUANTITY,
                sat.SECONDARY_PACKAGE_TYPE_CODE = stg.SECONDARY_PACKAGE_TYPE_CODE,
                sat.PACKAGE_QUANTITY = stg.PACKAGE_QUANTITY,
                sat.SHELF_LIFE_QUANTITY = stg.SHELF_LIFE_QUANTITY,
                sat.SHELF_LIFE_UNIT_OF_MEASURE_CODE = stg.SHELF_LIFE_UNIT_OF_MEASURE_CODE,
                sat.SAMPLE_INDICATOR = stg.SAMPLE_INDICATOR,
                sat.COMPETITOR_INDICATOR = stg.COMPETITOR_INDICATOR,
                sat.GENERIC_INDICATOR = stg.GENERIC_INDICATOR,
                sat.PRESCRIPTION_REQUIRED_INDICATOR = stg.PRESCRIPTION_REQUIRED_INDICATOR,
                sat.ATC_CODE = stg.ATC_CODE,
                sat.WHO_CODE = stg.WHO_CODE,
                sat.NFC_CODE = stg.NFC_CODE,
                sat.CHC_NEC_CODE = stg.CHC_NEC_CODE,
                sat.USC_CODE = stg.USC_CODE,
                sat.MANUFACTURER_ID = stg.MANUFACTURER_ID,
                sat.MANUFACTURER = stg.MANUFACTURER,
                sat.CORPORATION = stg.CORPORATION,
                sat.DAILY_DOSAGE_QUANTITY = stg.DAILY_DOSAGE_QUANTITY,
                sat.IS_IMPORTED = stg.IS_IMPORTED,
                sat.GENERIC_PRODUCT_DESCRIPTION = stg.GENERIC_PRODUCT_DESCRIPTION,
                sat.CHANNEL = stg.CHANNEL,
                sat.CHC_FLAG = stg.CHC_FLAG,
                sat.START_DATETIME = CURRENT_TIMESTAMP(),
                sat.END_DATETIME = NULL,
                sat.UPDATED_DATETIME = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN
            INSERT (
                {{ sat_hashkey_col }}, {{ sat_hashdiff_col }}, START_DATETIME, UPDATED_DATETIME, ETL_BATCH_ID, RECORD_SOURCE,
                GEOGRAPHIC_ID, DATASET_ID, SOURCE_DATASET_ID, DATASET_PRODUCT_ID,
                PRODUCT_TYPE_CODE, PRODUCT_NAME, STANDARDIZED_PRODUCT_NAME, DESCRIPTION,
                PRODUCT_STATUS_CODE, EFFECTIVE_DATE, END_DATE, BRAND_NAME, GENERIC_NAMES,
                APPROVED_DATE, MARKETING_START_DATE, MARKETING_END_DATE, EXPIRATION_DATE,
                SUBSTANCE, DOSE_FORM_CODE, STRENGTH, ROUTE_OF_ADMINISTRATION_CODE,
                PHARMACEUTICAL_PRODUCT_QUANTITY, UNIT_OF_PRESENTATION_CODE,
                PRIMARY_PACKAGE_QUANTITY, PRIMARY_PACKAGE_TYPE_CODE,
                SECONDARY_PACKAGE_QUANTITY, SECONDARY_PACKAGE_TYPE_CODE,
                PACKAGE_QUANTITY, SHELF_LIFE_QUANTITY, SHELF_LIFE_UNIT_OF_MEASURE_CODE,
                SAMPLE_INDICATOR, COMPETITOR_INDICATOR, GENERIC_INDICATOR,
                PRESCRIPTION_REQUIRED_INDICATOR, ATC_CODE, WHO_CODE, NFC_CODE,
                CHC_NEC_CODE, USC_CODE, MANUFACTURER_ID, MANUFACTURER, CORPORATION,
                DAILY_DOSAGE_QUANTITY, IS_IMPORTED, GENERIC_PRODUCT_DESCRIPTION,
                CHANNEL, CHC_FLAG
            )
            VALUES (
                stg.HASHKEY,
                stg.HASHDIFF,
                CURRENT_TIMESTAMP(),
                CURRENT_TIMESTAMP(),
                '{{ etl_batch_id }}',
                LEFT(COALESCE(stg.RECORD_SOURCE, '{{ record_source }}'), {{ sat_record_source_max_len }}),
                stg.GEOGRAPHIC_ID, stg.DATASET_ID, stg.SOURCE_DATASET_ID, stg.DATASET_PRODUCT_ID,
                stg.PRODUCT_TYPE_CODE, stg.PRODUCT_NAME, stg.STANDARDIZED_PRODUCT_NAME, stg.DESCRIPTION,
                stg.PRODUCT_STATUS_CODE, stg.EFFECTIVE_DATE, stg.END_DATE, stg.BRAND_NAME, stg.GENERIC_NAMES,
                stg.APPROVED_DATE, stg.MARKETING_START_DATE, stg.MARKETING_END_DATE, stg.EXPIRATION_DATE,
                stg.SUBSTANCE, stg.DOSE_FORM_CODE, stg.STRENGTH, stg.ROUTE_OF_ADMINISTRATION_CODE,
                stg.PHARMACEUTICAL_PRODUCT_QUANTITY, stg.UNIT_OF_PRESENTATION_CODE,
                stg.PRIMARY_PACKAGE_QUANTITY, stg.PRIMARY_PACKAGE_TYPE_CODE,
                stg.SECONDARY_PACKAGE_QUANTITY, stg.SECONDARY_PACKAGE_TYPE_CODE,
                stg.PACKAGE_QUANTITY, stg.SHELF_LIFE_QUANTITY, stg.SHELF_LIFE_UNIT_OF_MEASURE_CODE,
                stg.SAMPLE_INDICATOR, stg.COMPETITOR_INDICATOR, stg.GENERIC_INDICATOR,
                stg.PRESCRIPTION_REQUIRED_INDICATOR, stg.ATC_CODE, stg.WHO_CODE, stg.NFC_CODE,
                stg.CHC_NEC_CODE, stg.USC_CODE, stg.MANUFACTURER_ID, stg.MANUFACTURER, stg.CORPORATION,
                stg.DAILY_DOSAGE_QUANTITY, stg.IS_IMPORTED, stg.GENERIC_PRODUCT_DESCRIPTION,
                stg.CHANNEL, stg.CHC_FLAG
            )
    {% endset %}
 
    {# ------------------------------------------------------------------
       STEP 4: MERGE on HUB for NEW records.
       Inserts into HUB only when hashkey does not already exist.
    ------------------------------------------------------------------ #}
    {% set merge_hub_sql %}
        MERGE INTO {{ hub_table }} hub
        USING (
            SELECT DISTINCT
                stg.HASHKEY
            FROM {{ filtered_stg }} stg
        ) stg
            ON hub.{{ hub_hashkey_col }} = stg.HASHKEY
        WHEN NOT MATCHED THEN
            INSERT ({{ hub_hashkey_col }}, START_DATETIME, LOAD_DATETIME, RECORD_SOURCE, ETL_BATCH_ID, ORIGIN_STG)
            VALUES (
                stg.HASHKEY,
                CURRENT_TIMESTAMP(),
                CURRENT_TIMESTAMP(),
                LEFT('{{ record_source }}', {{ hub_record_source_max_len }}),
                '{{ etl_batch_id }}',
                'PRODUCT'
            )
    {% endset %}

    {% set update_watermark_sql %}
        MERGE INTO {{ audit_source_watermark }} tgt
        USING (
            SELECT
                '{{ source_system }}' AS SOURCE_SYSTEM,
                '{{ watermark_source_object }}' AS SOURCE_OBJECT,
                '{{ watermark_column }}' AS WATERMARK_COLUMN,
                (SELECT MAX({{ watermark_column }}) FROM {{ dq_stg }})::TIMESTAMP_LTZ AS WATERMARK_VALUE,
                CURRENT_TIMESTAMP() AS UPDATED_AT,
                '{{ etl_batch_id }}' AS ETL_BATCH_ID
        ) src
        ON tgt.SOURCE_SYSTEM = src.SOURCE_SYSTEM
           AND tgt.SOURCE_OBJECT = src.SOURCE_OBJECT
           AND tgt.WATERMARK_COLUMN = src.WATERMARK_COLUMN
        WHEN MATCHED
             AND src.WATERMARK_VALUE IS NOT NULL
             AND (tgt.WATERMARK_VALUE IS NULL OR src.WATERMARK_VALUE > tgt.WATERMARK_VALUE) THEN
            UPDATE SET
                WATERMARK_VALUE = src.WATERMARK_VALUE,
                UPDATED_AT = src.UPDATED_AT,
                ETL_BATCH_ID = src.ETL_BATCH_ID
        WHEN NOT MATCHED
             AND src.WATERMARK_VALUE IS NOT NULL THEN
            INSERT (SOURCE_SYSTEM, SOURCE_OBJECT, WATERMARK_COLUMN, WATERMARK_VALUE, UPDATED_AT, ETL_BATCH_ID)
            VALUES (src.SOURCE_SYSTEM, src.SOURCE_OBJECT, src.WATERMARK_COLUMN, src.WATERMARK_VALUE, src.UPDATED_AT, src.ETL_BATCH_ID);
    {% endset %}

    {% set create_delta_counts_sql %}
        CREATE OR REPLACE TEMPORARY TABLE {{ tmp_delta_counts_table }} AS
        SELECT
            (
                SELECT COUNT(DISTINCT stg.HASHKEY)
                FROM {{ filtered_stg }} stg
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM {{ hub_table }} hub
                    WHERE hub.{{ hub_hashkey_col }} = stg.HASHKEY
                )
            )::NUMBER AS DELTA_INSERTED_ROWS,
            (
                SELECT COUNT(DISTINCT sat.{{ sat_hashkey_col }})
                FROM {{ sat_table }} sat
                INNER JOIN {{ filtered_stg }} stg
                    ON stg.HASHKEY = sat.{{ sat_hashkey_col }}
                WHERE sat.END_DATETIME IS NULL
                  AND stg.HASHDIFF != sat.{{ sat_hashdiff_col }}
            )::NUMBER AS DELTA_UPDATED_ROWS
    {% endset %}
 
    {# ==================== EXECUTION ==================== #}
 
    {% do ensure_audit_tables() %}
    {% do run_query(audit_start_sql) %}

    {% do log("Step 0: Creating DQ-passed staging table", info=True) %}
    {% do run_query(create_dq_stg_sql) %}

    {% do log("Step 1: MERGE - updating unchanged records (hashkey + hashdiff match) - set updated_datetime", info=True) %}
    {% do run_query(merge_unchanged_sat_sql) %}
 
    {% do log("Step 1a: Creating filtered staging table - excluding unchanged records", info=True) %}
    {% do run_query(create_filtered_stg_sql) %}
    {% do run_query(create_delta_counts_sql) %}
 
    {% do log("Step 2: MERGE on HUB - inserting new hub records", info=True) %}
    {% do run_query(merge_hub_sql) %}

    {% do log("Step 3: Archiving changed records to SAT_PRODUCT_HIST", info=True) %}
    {% do run_query(archive_changed_sql) %}

    {% do log("Step 4: MERGE on SAT - updating changed records + inserting new records", info=True) %}
    {% do run_query(merge_sat_sql) %}

    {% if enable_watermark_filter %}
        {% do run_query(update_watermark_sql) %}
    {% endif %}

    {% do run_query(audit_success_sql) %}
 
    {% do log("Delta load (MERGE version) completed successfully.", info=True) %}
 
{% endmacro %}
