{% macro send_load_alerts(results) %}
    {{ return(send_load_alerts_v2(results)) }}
{% endmacro %}

{% macro send_load_alerts_v2(results) %}
    {% if not execute %}
        {{ return('') }}
    {% endif %}

    {% if not var('enable_email_alerts', false) %}
        {{ return('') }}
    {% endif %}

    {% set etl_batch_id = env_var('ETL_BATCH_ID', env_var('DBT_JOB_RUN_ID', invocation_id)) %}
    {% set env_name = var('env_name', (target.name | upper)) %}
    {% set integration_name = var('email_integration_name', 'semarchy_email_intgration') %}
    {% set default_recipient = var('default_recipient', '') %}
    {% set default_cc_recipient = var('default_cc_recipient', '') %}
    {% set alert_settings = var('alert_settings', {}) %}
    {% set audit_db = var('audit_database', target.database) %}
    {% set audit_schema = var('audit_schema', 'DW_DFHPMS2EU_SEMARCHY_SCHEMA') %}
    {% set audit_process_log = audit_db ~ '.' ~ audit_schema ~ '.AUDIT_PROCESS_LOG' %}

    {% if results is none %}
        {{ return('') }}
    {% endif %}

    {% for res in results %}
        {% set result_name = none %}
        {% if res.node is not none and res.node.name is not none %}
            {% set result_name = res.node.name %}
        {% elif res.name is defined and res.name is not none %}
            {% set result_name = res.name %}
        {% elif res.unique_id is defined and res.unique_id is not none %}
            {% set result_name = (res.unique_id | string).split('.')[-1] %}
        {% endif %}

        {% if result_name is none or (result_name | string | trim) == '' %}
            {% continue %}
        {% endif %}

        {% set cfg = alert_settings.get(result_name, {}) %}

        {% if cfg is none or cfg == {} %}
            {% continue %}
        {% endif %}

        {% set status = (res.status or '') | lower %}
        {% set source_id = cfg.get('source_id', env_var('SOURCE_SYSTEM', 'N/A')) %}
        {% set dataset_id = cfg.get('dataset_id', result_name) %}
        {% set recipient = (cfg.get('recipients') if cfg.get('recipients') is not none else default_recipient) %}
        {% set cc_recipient = (cfg.get('cc_recipient') if cfg.get('cc_recipient') is not none else default_cc_recipient) %}
        {% set send_success = cfg.get('send_success', false) %}
        {% set send_failure = cfg.get('send_failure', true) %}
        {% set record_source = cfg.get('record_source', source_id) %}
        {% set stg_model = cfg.get('stg_model') %}
        {% set delta_audit_model = cfg.get('delta_audit_model') %}

        {% if recipient is none or (recipient | trim) == '' %}
            {% continue %}
        {% endif %}

        {% set rows_source_to_stg = 0 %}
        {% if stg_model is not none and (stg_model | trim) != '' %}
            {% set stg_rel = ref(stg_model) %}
            {% set stg_exists = adapter.get_relation(database=stg_rel.database, schema=stg_rel.schema, identifier=stg_rel.identifier) %}
            {% if stg_exists is not none %}
            {% set stg_sql %}
                select count(*) as CNT
                from {{ stg_rel }}
                where etl_batch_id = '{{ etl_batch_id }}'
            {% endset %}
            {% set stg_res = run_query(stg_sql) %}
            {% if stg_res is not none and (stg_res.rows | length) > 0 %}
                {% set rows_source_to_stg = (stg_res.rows[0][0] or 0) %}
            {% endif %}
            {% endif %}
        {% endif %}

        {% set rejected_rows = 0 %}
        {% set reject_rel = ref('REJECT') %}
        {% set reject_exists = adapter.get_relation(database=reject_rel.database, schema=reject_rel.schema, identifier=reject_rel.identifier) %}
        {% if reject_exists is not none %}
            {% set rej_sql %}
                select count(*) as CNT
                from {{ reject_rel }}
                where etl_batch_id = '{{ etl_batch_id }}'
                  and upper(record_source) = upper('{{ record_source }}')
            {% endset %}
            {% set rej_res = run_query(rej_sql) %}
            {% if rej_res is not none and (rej_res.rows | length) > 0 %}
                {% set rejected_rows = (rej_res.rows[0][0] or 0) %}
            {% endif %}
        {% endif %}

        {% set inserted_rows = 0 %}
        {% set updated_rows = 0 %}
        {% if delta_audit_model is not none and (delta_audit_model | trim) != '' %}
            {% set audit_exists = adapter.get_relation(database=audit_db, schema=audit_schema, identifier='AUDIT_PROCESS_LOG') %}
            {% if audit_exists is not none %}
            {% set delta_sql %}
                select
                    coalesce(delta_inserted_rows, 0) as INS,
                    coalesce(delta_updated_rows, 0) as UPD
                from {{ audit_process_log }}
                where etl_batch_id = '{{ etl_batch_id }}'
                  and model_name = '{{ delta_audit_model | trim | upper }}'
                order by end_timestamp desc
                limit 1
            {% endset %}
            {% set delta_res = run_query(delta_sql) %}
            {% if delta_res is not none and (delta_res.rows | length) > 0 %}
                {% set inserted_rows = (delta_res.rows[0][0] or 0) %}
                {% set updated_rows = (delta_res.rows[0][1] or 0) %}
            {% endif %}
            {% endif %}
        {% else %}
            {% set inserted_rows = res.adapter_response.get('rows_affected', 0) if res.adapter_response is not none else 0 %}
        {% endif %}

        {% set subject = '' %}
        {% set body = '' %}

        {% if status == 'success' and send_success %}
            {% set subject = env_name ~ ' DBT-SAT_PROD LOAD SUCCESSFUL FOR DATASET ID: ' ~ dataset_id ~ ' SOURCE DATASET ID: ' ~ source_id %}
            {% set body = 'RECORD# FROM SOURCE to STG: ' ~ rows_source_to_stg ~
                          '\nRECORD# NEW INSERTS: ' ~ inserted_rows ~
                          '\nRECORD# UPDATES: ' ~ updated_rows ~
                          '\nRECORD# REJECTED: ' ~ rejected_rows ~
                          '\nLOAD TIMESTAMP: ' ~ run_started_at.strftime('%Y-%m-%d %H:%M:%S') %}
        {% elif status in ['error', 'fail'] and send_failure %}
            {% set subject = env_name ~ ' DBT-SAT_PROD LOAD FAILED FOR DATASET ID: ' ~ dataset_id ~ ' SOURCE DATASET ID: ' ~ source_id %}
            {% set body = 'LOAD TIMESTAMP: ' ~ run_started_at.strftime('%Y-%m-%d %H:%M:%S') ~
                          '\nBATCH ID: ' ~ etl_batch_id ~
                          '\nERROR: ' ~ (res.message or '') %}
        {% endif %}

        {% if subject != '' %}
            {% set safe_subject = (subject | replace("'", "''")) %}
            {% set safe_body = (body | replace("'", "''")) %}
            {% set safe_cc = (cc_recipient | replace("'", "''")) %}
            {% set email_sql %}
                {% if cc_recipient is not none and (cc_recipient | trim) != '' %}
                    call system$send_email(
                        '{{ integration_name }}',
                        '{{ recipient }}',
                        '{{ safe_subject }}',
                        '{{ safe_body }}',
                        '{{ safe_cc }}'
                    )
                {% else %}
                    call system$send_email(
                        '{{ integration_name }}',
                        '{{ recipient }}',
                        '{{ safe_subject }}',
                        '{{ safe_body }}'
                    )
                {% endif %}
            {% endset %}
            {% do run_query(email_sql) %}
        {% endif %}
    {% endfor %}

    {{ return('') }}
{% endmacro %}
