-- macros/audit_hooks.sql

{% macro generate_schema_name(custom_schema_name, node) %}
    {% if custom_schema_name is none %}
        {{ target.schema }}
    {% else %}
        {{ custom_schema_name | trim }}
    {% endif %}
{% endmacro %}

{% macro snowflake__create_schema(relation) %}
    {% set check_sql %}
        SHOW TERSE SCHEMAS LIKE '{{ relation.schema }}' IN DATABASE {{ relation.database }}
    {% endset %}

    {% if execute %}
        {% set res = run_query(check_sql) %}
        {% if res is not none and (res.rows | length) > 0 %}
            {{ return('') }}
        {% endif %}
    {% endif %}

    {% set create_sql %}
        CREATE SCHEMA IF NOT EXISTS {{ relation.database }}.{{ relation.schema }}
    {% endset %}
    {% do run_query(create_sql) %}
    {{ return('') }}
{% endmacro %}

{% macro ensure_audit_tables() %}
    {% set audit_schema = target.database ~ '.DW_DFHPMS2EU_SEMARCHY_SCHEMA' %}

    {% if execute %}
        {% set check_schema_sql %}
            SHOW TERSE SCHEMAS LIKE 'DW_DFHPMS2EU_SEMARCHY_SCHEMA' IN DATABASE {{ target.database }}
        {% endset %}
        {% set schema_res = run_query(check_schema_sql) %}
        {% if schema_res is not none and (schema_res.rows | length) == 0 %}
            {% set create_schema_sql %}
                CREATE SCHEMA IF NOT EXISTS {{ audit_schema }}
            {% endset %}
            {% do run_query(create_schema_sql) %}
        {% endif %}
    {% endif %}

    {% if execute %}
        {% set check_old_step_sql %}
            SHOW TERSE TABLES LIKE 'AUDIT_STEP_LOG' IN SCHEMA {{ audit_schema }}
        {% endset %}
        {% set check_new_step_sql %}
            SHOW TERSE TABLES LIKE 'AUDIT_PROCESS_LOG' IN SCHEMA {{ audit_schema }}
        {% endset %}
        {% set old_step_res = run_query(check_old_step_sql) %}
        {% set new_step_res = run_query(check_new_step_sql) %}
        {% if (old_step_res is not none and (old_step_res.rows | length) > 0) and (new_step_res is none or (new_step_res.rows | length) == 0) %}
            {% set rename_step_sql %}
                ALTER TABLE {{ audit_schema }}.AUDIT_STEP_LOG RENAME TO AUDIT_PROCESS_LOG
            {% endset %}
            {% do run_query(rename_step_sql) %}
        {% endif %}

        {% set check_old_run_sql %}
            SHOW TERSE TABLES LIKE 'AUDIT_RUN_LOG' IN SCHEMA {{ audit_schema }}
        {% endset %}
        {% set check_new_run_sql %}
            SHOW TERSE TABLES LIKE 'AUDIT_BATCH_CONTROL' IN SCHEMA {{ audit_schema }}
        {% endset %}
        {% set old_run_res = run_query(check_old_run_sql) %}
        {% set new_run_res = run_query(check_new_run_sql) %}
        {% if (old_run_res is not none and (old_run_res.rows | length) > 0) and (new_run_res is none or (new_run_res.rows | length) == 0) %}
            {% set rename_run_sql %}
                ALTER TABLE {{ audit_schema }}.AUDIT_RUN_LOG RENAME TO AUDIT_BATCH_CONTROL
            {% endset %}
            {% do run_query(rename_run_sql) %}
        {% endif %}
    {% endif %}

    {% set create_step_log_sql %}
        CREATE TABLE IF NOT EXISTS {{ audit_schema }}.AUDIT_PROCESS_LOG (
            ETL_BATCH_ID VARCHAR,
            LAYER_NAME VARCHAR,
            MODEL_NAME VARCHAR,
            SOURCE_SYSTEM VARCHAR,
            PROCESS_STATUS VARCHAR,
            START_TIMESTAMP TIMESTAMP_LTZ,
            END_TIMESTAMP TIMESTAMP_LTZ,
            ROWS_PROCESSED NUMBER,
            ERROR_MESSAGE VARCHAR,
            DELTA_INSERTED_ROWS NUMBER,
            DELTA_UPDATED_ROWS NUMBER
        )
    {% endset %}
    {% do run_query(create_step_log_sql) %}

    {% if execute %}
        {% set add_col_sql %}
            ALTER TABLE {{ audit_schema }}.AUDIT_PROCESS_LOG
            ADD COLUMN IF NOT EXISTS SOURCE_SYSTEM VARCHAR
        {% endset %}
        {% do run_query(add_col_sql) %}
    {% endif %}

    {% set create_run_log_sql %}
        CREATE TABLE IF NOT EXISTS {{ audit_schema }}.AUDIT_BATCH_CONTROL (
            ETL_BATCH_ID VARCHAR,
            PIPELINE_NAME VARCHAR,
            TOTAL_REJECTED_RECORDS NUMBER(18,0),
            START_TIMESTAMP TIMESTAMP_LTZ(9),
            END_TIMESTAMP TIMESTAMP_LTZ(9),
            PROCESS_STATUS VARCHAR(23),
            ERROR_MESSAGE VARCHAR
        )
    {% endset %}
    {% do run_query(create_run_log_sql) %}

    {% set create_watermark_sql %}
        CREATE TABLE IF NOT EXISTS {{ audit_schema }}.AUDIT_SOURCE_WATERMARK (
            SOURCE_SYSTEM VARCHAR,
            SOURCE_OBJECT VARCHAR,
            WATERMARK_COLUMN VARCHAR,
            WATERMARK_VALUE TIMESTAMP_LTZ,
            UPDATED_AT TIMESTAMP_LTZ,
            ETL_BATCH_ID VARCHAR
        )
    {% endset %}
    {% do run_query(create_watermark_sql) %}
    {{ return('') }}
{% endmacro %}

{% macro audit_source_system(node=None) %}
    {% set env_ss = (env_var('SOURCE_SYSTEM', '') | trim) %}
    {% if env_ss != '' %}
        {{ return(env_ss | upper) }}
    {% endif %}

    {% if node is not none %}
        {% set n = ((node.name or '') | trim | upper) %}
        {% if 'GOB360' in n %}
            {{ return('GOB360_PM') }}
        {% elif 'CLOSEUP' in n %}
            {{ return('CLOSEUP_MARKET') }}
        {% elif 'VEEVA' in n %}
            {{ return('VEEVA_CRM') }}
        {% endif %}
    {% endif %}

    {{ return('VEEVA_CRM') }}
{% endmacro %}

{% macro audit_std_layer_name(raw_layer_name) %}
    {% set ln = (raw_layer_name or 'UNKNOWN') | trim | lower %}
    {% if ln == 'stg' %}
        {{ return('STAGE') }}
    {% elif ln == 'dq' %}
        {{ return('DQ') }}
    {% elif ln == 'delta' %}
        {{ return('DELTA') }}
    {% elif ln == 'dm' %}
        {{ return('DM') }}
    {% elif ln == 'hub' %}
        {{ return('HUB') }}
    {% elif ln == 'sat' %}
        {{ return('SAT') }}
    {% elif ln == 'hist' %}
        {{ return('HIST') }}
    {% else %}
        {{ return(ln | upper) }}
    {% endif %}
{% endmacro %}

{% macro audit_std_model_name(raw_model_name) %}
    {% set n = (raw_model_name or 'UNKNOWN') | trim | upper %}
    {% if n[-11:] == '_VEEVA_CRM' %}
        {% set n = n[:-11] %}
    {% endif %}
    {% if n[-5:] == '_VEEVA' %}
        {% set n = n[:-5] %}
    {% endif %}
    {% if n[-13:] == '_PRODUCT_FILE' %}
        {% set n = n[:-13] ~ '_PRODUCT' %}
    {% endif %}
    {{ return(n) }}
{% endmacro %}

{% macro audit_relation_rowcount(node) %}
    {% if not execute %}
        {{ return(0) }}
    {% endif %}
    {% if node is none %}
        {{ return(0) }}
    {% endif %}
    {% set identifier = node.alias if node.alias is not none else node.name %}
    {% set rel = adapter.get_relation(database=node.database, schema=node.schema, identifier=identifier) %}
    {% if rel is none %}
        {{ return(0) }}
    {% endif %}
    {% set res = run_query('select count(*) as CNT from ' ~ rel) %}
    {% if res is none or (res.rows | length) == 0 %}
        {{ return(0) }}
    {% endif %}
    {{ return(res.rows[0][0]) }}
{% endmacro %}

{% macro log_run_start() %}
    {% set etl_batch_id = env_var('ETL_BATCH_ID', env_var('DBT_JOB_RUN_ID', invocation_id)) %}
    {% set pipeline_name = (env_var('SOURCE_NAME', project_name) | trim | upper) %}

    {% do ensure_audit_tables() %}

    {% set sql %}
        MERGE INTO {{ target.database }}.DW_DFHPMS2EU_SEMARCHY_SCHEMA.AUDIT_BATCH_CONTROL tgt
        USING (
            SELECT
                '{{ etl_batch_id }}' AS ETL_BATCH_ID,
                '{{ pipeline_name }}' AS PIPELINE_NAME,
                0::NUMBER AS TOTAL_REJECTED_RECORDS,
                CURRENT_TIMESTAMP() AS START_TIMESTAMP,
                NULL::TIMESTAMP_LTZ AS END_TIMESTAMP,
                'RUNNING' AS PROCESS_STATUS,
                ''::VARCHAR AS ERROR_MESSAGE
        ) src
        ON tgt.ETL_BATCH_ID = src.ETL_BATCH_ID
        WHEN NOT MATCHED THEN
            INSERT (ETL_BATCH_ID, PIPELINE_NAME, TOTAL_REJECTED_RECORDS, START_TIMESTAMP, END_TIMESTAMP, PROCESS_STATUS, ERROR_MESSAGE)
            VALUES (src.ETL_BATCH_ID, src.PIPELINE_NAME, src.TOTAL_REJECTED_RECORDS, src.START_TIMESTAMP, src.END_TIMESTAMP, src.PROCESS_STATUS, src.ERROR_MESSAGE)
        WHEN MATCHED THEN
            UPDATE SET
                PIPELINE_NAME = src.PIPELINE_NAME,
                START_TIMESTAMP = COALESCE(tgt.START_TIMESTAMP, src.START_TIMESTAMP),
                END_TIMESTAMP = NULL,
                PROCESS_STATUS = 'RUNNING',
                ERROR_MESSAGE = '';
    {% endset %}

    {% do run_query(sql) %}
    {{ return('') }}
{% endmacro %}

{% macro log_model_start() %}
    {% set etl_batch_id = env_var('ETL_BATCH_ID', env_var('DBT_JOB_RUN_ID', invocation_id)) %}
    {% set layer_name = model.fqn[1] if model.fqn|length > 1 else (model.tags[0] if model.tags else "UNKNOWN") %}
    {% set layer_name = audit_std_layer_name(layer_name) %}
    {% set model_name = audit_std_model_name(model.name) %}
    {% set source_system = audit_source_system(model) %}
    
    {% set insert_query %}
        INSERT INTO {{ target.database }}.DW_DFHPMS2EU_SEMARCHY_SCHEMA.AUDIT_PROCESS_LOG (
            ETL_BATCH_ID,
            LAYER_NAME,
            MODEL_NAME,
            SOURCE_SYSTEM,
            PROCESS_STATUS,
            START_TIMESTAMP,
            END_TIMESTAMP,
            ROWS_PROCESSED,
            ERROR_MESSAGE,
            DELTA_INSERTED_ROWS,
            DELTA_UPDATED_ROWS
        ) VALUES (
            '{{ etl_batch_id }}',
            '{{ layer_name }}',
            '{{ model_name }}',
            '{{ source_system }}',
            'RUNNING',
            CURRENT_TIMESTAMP(),
            NULL::TIMESTAMP_LTZ,
            0,
            '',
            0,
            0
        )
    {% endset %}

    {{ return(insert_query) }}
{% endmacro %}


{% macro log_model_end(status) %}
    {% set etl_batch_id = env_var('ETL_BATCH_ID', env_var('DBT_JOB_RUN_ID', invocation_id)) %}
    {% set model_name = audit_std_model_name(model.name) %}
    
    {% set update_query %}
        UPDATE {{ target.database }}.DW_DFHPMS2EU_SEMARCHY_SCHEMA.AUDIT_PROCESS_LOG
        SET
            PROCESS_STATUS = '{{ status }}',
            END_TIMESTAMP = CURRENT_TIMESTAMP(),
            ROWS_PROCESSED = COALESCE(ROWS_PROCESSED, 0),
            ERROR_MESSAGE = ''
        WHERE ETL_BATCH_ID = '{{ etl_batch_id }}'
          AND MODEL_NAME = '{{ model_name }}'
          AND PROCESS_STATUS = 'RUNNING'
    {% endset %}

    {{ return(update_query) }}
{% endmacro %}

{% macro log_run_end(results) %}
    {% set etl_batch_id = env_var('ETL_BATCH_ID', env_var('DBT_JOB_RUN_ID', invocation_id)) %}
    {% set ns = namespace(total_rejected_records=0) %}

    {% do ensure_audit_tables() %}

    {% for r in results %}
        {% if r.node is not none and r.node.resource_type == 'model' %}
            {% set source_system = audit_source_system(r.node) %}
            {% set resp = r.adapter_response or {} %}
            {% set delta_inserted = resp.get('rows_inserted') or 0 %}
            {% set delta_updated = resp.get('rows_updated') or 0 %}
            {% set rows_deleted = resp.get('rows_deleted') or 0 %}
            {% set rows_affected = resp.get('rows_affected') %}
            {% if rows_affected is none %}
                {% set rows_affected = delta_inserted + delta_updated + rows_deleted %}
            {% endif %}
            {% set error_message = (r.message or '') if r.status in ['error', 'fail'] else '' %}
            {% set safe_error_message = error_message | replace("'", "''") %}
            {% set layer_name_raw = r.node.fqn[1] if r.node.fqn|length > 1 else (r.node.tags[0] if r.node.tags else "UNKNOWN") %}
            {% set layer_name = audit_std_layer_name(layer_name_raw) %}
            {% set model_name = audit_std_model_name(r.node.name) %}
            {% set status_upper = (r.status or 'unknown') | upper %}
            {% set rows_processed = rows_affected %}
            {% if layer_name in ['STAGE', 'DM', 'DQ'] %}
                {% set rows_processed = audit_relation_rowcount(r.node) %}
            {% endif %}

            {% set step_sql %}
                MERGE INTO {{ target.database }}.DW_DFHPMS2EU_SEMARCHY_SCHEMA.AUDIT_PROCESS_LOG tgt
                USING (
                    SELECT
                        '{{ etl_batch_id }}' AS ETL_BATCH_ID,
                        '{{ layer_name }}' AS LAYER_NAME,
                        '{{ model_name }}' AS MODEL_NAME,
                        '{{ source_system }}' AS SOURCE_SYSTEM,
                        '{{ status_upper }}' AS PROCESS_STATUS,
                        CURRENT_TIMESTAMP() AS START_TIMESTAMP,
                        CURRENT_TIMESTAMP() AS END_TIMESTAMP,
                        {{ rows_processed }}::NUMBER AS ROWS_PROCESSED,
                        '{{ safe_error_message }}' AS ERROR_MESSAGE,
                        {{ delta_inserted }}::NUMBER AS DELTA_INSERTED_ROWS,
                        {{ delta_updated }}::NUMBER AS DELTA_UPDATED_ROWS
                ) src
                ON tgt.ETL_BATCH_ID = src.ETL_BATCH_ID
                   AND tgt.MODEL_NAME = src.MODEL_NAME
                WHEN MATCHED THEN
                    UPDATE SET
                        LAYER_NAME = src.LAYER_NAME,
                        SOURCE_SYSTEM = src.SOURCE_SYSTEM,
                        PROCESS_STATUS = src.PROCESS_STATUS,
                        START_TIMESTAMP = COALESCE(tgt.START_TIMESTAMP, src.START_TIMESTAMP),
                        END_TIMESTAMP = src.END_TIMESTAMP,
                        ROWS_PROCESSED = src.ROWS_PROCESSED,
                        ERROR_MESSAGE = src.ERROR_MESSAGE,
                        DELTA_INSERTED_ROWS = src.DELTA_INSERTED_ROWS,
                        DELTA_UPDATED_ROWS = src.DELTA_UPDATED_ROWS
                WHEN NOT MATCHED THEN
                    INSERT (ETL_BATCH_ID, LAYER_NAME, MODEL_NAME, SOURCE_SYSTEM, PROCESS_STATUS, START_TIMESTAMP, END_TIMESTAMP, ROWS_PROCESSED, ERROR_MESSAGE, DELTA_INSERTED_ROWS, DELTA_UPDATED_ROWS)
                    VALUES (src.ETL_BATCH_ID, src.LAYER_NAME, src.MODEL_NAME, src.SOURCE_SYSTEM, src.PROCESS_STATUS, src.START_TIMESTAMP, src.END_TIMESTAMP, src.ROWS_PROCESSED, src.ERROR_MESSAGE, src.DELTA_INSERTED_ROWS, src.DELTA_UPDATED_ROWS);
            {% endset %}

            {% do run_query(step_sql) %}

            {% if model_name == 'REJECT' %}
                {% set ns.total_rejected_records = rows_processed %}
            {% endif %}
        {% endif %}
    {% endfor %}

    {% set any_error = results | selectattr('status', 'in', ['error', 'fail']) | list | length > 0 %}
    {% if any_error %}
        {% set run_status = 'FAILED' %}
        {% set run_error = (results | selectattr('status', 'in', ['error', 'fail']) | map(attribute='message') | reject('equalto', none) | list | first) or 'Run failed' %}
    {% else %}
        {% set run_status = 'SUCCESS' %}
        {% set run_error = '' %}
    {% endif %}
    {% set safe_run_error = run_error | replace("'", "''") %}

    {% set run_sql %}
        UPDATE {{ target.database }}.DW_DFHPMS2EU_SEMARCHY_SCHEMA.AUDIT_BATCH_CONTROL
        SET
            PROCESS_STATUS = '{{ run_status }}',
            TOTAL_REJECTED_RECORDS = {{ ns.total_rejected_records }}::NUMBER,
            END_TIMESTAMP = CURRENT_TIMESTAMP(),
            ERROR_MESSAGE = '{{ safe_run_error }}'
        WHERE ETL_BATCH_ID = '{{ etl_batch_id }}';
    {% endset %}

    {% do run_query(run_sql) %}
    {{ return('') }}
{% endmacro %}
