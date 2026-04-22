{{ config(
    materialized='incremental',
    unique_key='HASHKEY',
    incremental_strategy='merge',
    schema='DW_DFHPMS2EU_SEMARCHY_SCHEMA',
    tags=['vault', 'current_state']
) }}

WITH source_data AS (
    SELECT 
        *,
        CURRENT_TIMESTAMP() AS UPDATED_DATETIME
    FROM {{ ref('STG_IDENTIFIER_VEEVA') }}
)

SELECT
    s.HASHKEY,
    s.HASHDIFF,
    CURRENT_TIMESTAMP() AS START_DATETIME,
    CURRENT_TIMESTAMP() AS LOAD_DATETIME,
    NULL::TIMESTAMP_LTZ AS END_DATETIME,
    s.UPDATED_DATETIME,
    s.ETL_BATCH_ID,
    s.RECORD_SOURCE,
    s.GEOGRAPHIC_ID,
    s.DATASET_ID,
    s.DATASET_PRODUCT_ID,
    s.IDENTIFIER_TYPE_CODE,
    s.IDENTIFIER_ID,
    s.STATUS_CODE,
    s.EFFECTIVE_DATE,
    s.END_DATE
FROM source_data s

{% if is_incremental() %}
  LEFT JOIN {{ this }} current_sat
    ON current_sat.HASHKEY = s.HASHKEY
  WHERE current_sat.HASHKEY IS NULL
     OR current_sat.HASHDIFF != s.HASHDIFF
{% endif %}
