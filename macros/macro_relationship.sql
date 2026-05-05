{#
    MACRO: delta_load_relationship_merge
    ============================================================
    Purpose:
        MERGE-based delta/CDC load for Veeva CRM product relationship data
        into Data Vault 2.0 satellite tables.

    Delta Logic Overview:
        1. UNCHANGED records (hashkey match AND hashdiff match):
           -> Only update UPDATED_DATETIME on the active SAT record.

        2. CHANGED records (hashkey match BUT hashdiff mismatch):
           -> Archive the old SAT record to SAT_RELATIONSHIP_HIST with END_DATETIME.
           -> Replace the old SAT record with new version (UPDATE all columns).

        3. NEW records (no hashkey match in HUB):
           -> Insert into HUB_PRODUCT_FILE with LOAD_DATETIME.
           -> Insert into SAT_RELATIONSHIP with START_DATETIME.

    Approach:
        Step 1:  MERGE on SAT - handles UNCHANGED (update timestamp) + builds temp delta
        Step 2:  INSERT into HIST - archive changed records (must precede SAT merge for changed)
        Step 3:  MERGE on SAT - handles CHANGED (update all cols) + NEW (insert)
        Step 4:  MERGE on HUB - handles NEW hub records

    Tables Involved:
        - Source: stg_veeva_crm_product_relationship (dbt view)
        - Hub:    DEV_DFHPMS2EU_DB.DW_DFHPMS2EU_SEMARCHY_SCHEMA.HUB
        - Sat:    DEV_DFHPMS2EU_DB.DW_DFHPMS2EU_SEMARCHY_SCHEMA.SAT_RELATIONSHIP
        - Hist:   DEV_DFHPMS2EU_DB.DW_DFHPMS2EU_SEMARCHY_SCHEMA.SAT_RELATIONSHIP_HIST

    Usage:
        dbt run-operation delta_load_relationship_merge
#}
{% macro delta_load_relationship_merge(
    stg_model='STG_RELATIONSHIP_VEEVA',
    hub_model='HUB',
    sat_model='SAT_RELATIONSHIP',
    hist_model='SAT_RELATIONSHIP_HIST',
    reject_model='REJECT',
    record_source='VEEVA_CRM',
    source_system=None,
    audit_model_name=None,
    enable_watermark_filter=true,
    require_parent_product_id=true,
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

    {% set tmp_dq_table = sat_table.database ~ '.' ~ sat_table.schema ~ '.TMP_STG_RELATIONSHIP_DQ' %}
    {% set tmp_delta_table = sat_table.database ~ '.' ~ sat_table.schema ~ '.TMP_STG_RELATIONSHIP_DELTA' %}
    {% set tmp_delta_counts_table = sat_table.database ~ '.' ~ sat_table.schema ~ '.TMP_RELATIONSHIP_DELTA_COUNTS' %}
    {% set dq_stg = tmp_dq_table %}
    {% set filtered_stg = tmp_delta_table %}

    {% set audit_db = target.database %}
    {% set audit_schema = 'DW_DFHPMS2EU_SEMARCHY_SCHEMA' %}
    {% set audit_process_log = audit_db ~ '.' ~ audit_schema ~ '.AUDIT_PROCESS_LOG' %}
    {% set audit_source_watermark = audit_db ~ '.' ~ audit_schema ~ '.AUDIT_SOURCE_WATERMARK' %}
    {% set source_system = (source_system if source_system is not none else (record_source | trim | upper)) %}
    {% set watermark_source_object = stg_model | trim | upper %}
    {% if audit_model_name is none %}
        {% set audit_model_name = 'delta_load_relationship_merge:' ~ stg_model %}
    {% endif %}

    {% set audit_start_sql %}
        MERGE INTO {{ audit_process_log }} tgt
        USING (
            SELECT
                '{{ etl_batch_id }}' AS ETL_BATCH_ID,
                '{{ audit_std_layer_name('delta') }}' AS LAYER_NAME,
                'SAT_RELATIONSHIP' AS MODEL_NAME,
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
          AND MODEL_NAME = 'SAT_RELATIONSHIP'
          AND PROCESS_STATUS = 'RUNNING';
    {% endset %}

    {% set create_dq_stg_sql %}
        CREATE OR REPLACE TEMPORARY TABLE {{ dq_stg }} AS
        WITH rejects AS (
            SELECT DATASET_PRODUCT_ID
            FROM {{ reject_table }}
        )
        SELECT * EXCLUDE (RN)
        FROM (
            SELECT
                stg.*,
                ROW_NUMBER() OVER (
                    PARTITION BY stg.HASHKEY
                    ORDER BY stg.LOAD_DATETIME DESC, stg.HASHDIFF DESC
                ) AS RN
            FROM {{ stg_table }} stg
            LEFT JOIN rejects pr
                ON upper(trim(stg.parent_dataset_product_id)) = pr.DATASET_PRODUCT_ID
            LEFT JOIN rejects cr
                ON upper(trim(stg.child_dataset_product_id)) = cr.DATASET_PRODUCT_ID
            WHERE stg.PARENT_GEOGRAPHIC_ID IS NOT NULL AND TRIM(stg.PARENT_GEOGRAPHIC_ID) != ''
              AND stg.CHILD_GEOGRAPHIC_ID IS NOT NULL AND TRIM(stg.CHILD_GEOGRAPHIC_ID) != ''
              {% if require_parent_product_id %}
              AND stg.PARENT_DATASET_PRODUCT_ID IS NOT NULL AND TRIM(stg.PARENT_DATASET_PRODUCT_ID) != ''
              {% endif %}
              AND stg.CHILD_DATASET_PRODUCT_ID IS NOT NULL AND TRIM(stg.CHILD_DATASET_PRODUCT_ID) != ''
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
              AND pr.DATASET_PRODUCT_ID IS NULL
              AND cr.DATASET_PRODUCT_ID IS NULL
        )
        WHERE RN = 1
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
                'RELATIONSHIP'
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
                sat.PARENT_GEOGRAPHIC_ID = stg.PARENT_GEOGRAPHIC_ID,
                sat.CHILD_GEOGRAPHIC_ID = stg.CHILD_GEOGRAPHIC_ID,
                sat.PARENT_DATASET_ID = stg.PARENT_DATASET_ID,
                sat.CHILD_DATASET_ID = stg.CHILD_DATASET_ID,
                sat.PARENT_DATASET_PRODUCT_ID = stg.PARENT_DATASET_PRODUCT_ID,
                sat.CHILD_DATASET_PRODUCT_ID = stg.CHILD_DATASET_PRODUCT_ID,
                sat.RELATIONSHIP_TYPE_CODE = stg.RELATIONSHIP_TYPE_CODE,
                sat.STATUS_CODE = stg.STATUS_CODE,
                sat.EFFECTIVE_DATE = stg.EFFECTIVE_DATE,
                sat.END_DATE = stg.END_DATE,
                sat.START_DATETIME = CURRENT_TIMESTAMP(),
                sat.END_DATETIME = NULL,
                sat.UPDATED_DATETIME = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN
            INSERT (
                {{ sat_hashkey_col }},
                {{ sat_hashdiff_col }},
                START_DATETIME,
                LOAD_DATETIME,
                END_DATETIME,
                UPDATED_DATETIME,
                ETL_BATCH_ID,
                RECORD_SOURCE,
                PARENT_GEOGRAPHIC_ID,
                CHILD_GEOGRAPHIC_ID,
                PARENT_DATASET_ID,
                CHILD_DATASET_ID,
                PARENT_DATASET_PRODUCT_ID,
                CHILD_DATASET_PRODUCT_ID,
                RELATIONSHIP_TYPE_CODE,
                STATUS_CODE,
                EFFECTIVE_DATE,
                END_DATE
            )
            VALUES (
                stg.HASHKEY,
                stg.HASHDIFF,
                CURRENT_TIMESTAMP(),
                CURRENT_TIMESTAMP(),
                NULL,
                CURRENT_TIMESTAMP(),
                '{{ etl_batch_id }}',
                LEFT(COALESCE(stg.RECORD_SOURCE, '{{ record_source }}'), {{ sat_record_source_max_len }}),
                stg.PARENT_GEOGRAPHIC_ID,
                stg.CHILD_GEOGRAPHIC_ID,
                stg.PARENT_DATASET_ID,
                stg.CHILD_DATASET_ID,
                stg.PARENT_DATASET_PRODUCT_ID,
                stg.CHILD_DATASET_PRODUCT_ID,
                stg.RELATIONSHIP_TYPE_CODE,
                stg.STATUS_CODE,
                stg.EFFECTIVE_DATE,
                stg.END_DATE
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

    {% do log("Step 3: Archiving changed records to SAT_RELATIONSHIP_HIST", info=True) %}
    {% do run_query(archive_changed_sql) %}

    {% do log("Step 4: MERGE on SAT - updating changed records + inserting new records", info=True) %}
    {% do run_query(merge_sat_sql) %}

    {% if enable_watermark_filter %}
        {% do run_query(update_watermark_sql) %}
    {% endif %}

    {% do run_query(audit_success_sql) %}

    {% do log("Delta load (MERGE version) completed successfully.", info=True) %}

{% endmacro %}
