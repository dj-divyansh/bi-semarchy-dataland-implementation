{% macro run_delta_product_closeup() %}
    {{ delta_load_product_file_v2_merge(stg_model='STG_PRODUCT_CLOSEUP', record_source='CLOSEUP_MARKET') }}
{% endmacro %}

{% macro run_delta_relationship_closeup() %}
    {{ delta_load_relationship_merge(stg_model='STG_RELATIONSHIP_CLOSEUP', record_source='CLOSEUP_MARKET') }}
{% endmacro %}

{% macro run_delta_product_gob360() %}
    {{ delta_load_product_file_v2_merge(stg_model='STG_PRODUCT_GOB360', record_source='GOB360_PM') }}
{% endmacro %}

{% macro run_delta_identifier_gob360() %}
    {{ delta_load_identifier_merge(stg_model='STG_IDENTIFIER_GOB360', record_source='GOB360_PM') }}
{% endmacro %}

{% macro run_delta_relationship_gob360() %}
    {{ delta_load_relationship_merge(stg_model='STG_RELATIONSHIP_GOB360', record_source='GOB360_PM') }}
{% endmacro %}
