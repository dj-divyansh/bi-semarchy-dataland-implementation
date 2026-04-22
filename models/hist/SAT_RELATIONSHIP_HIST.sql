{{ config(
    materialized='incremental',
    unique_key='HASHDIFF',
    schema='DW_DFHPMS2EU_SEMARCHY_SCHEMA',
    tags=['vault', 'history']
) }}

WITH incoming_data AS (
    SELECT HASHKEY, HASHDIFF
    FROM {{ ref('STG_RELATIONSHIP_VEEVA') }}
)

{% if is_incremental() %}
, expiring_records AS (
    SELECT 
        curr.* EXCLUDE (END_DATETIME),
        CURRENT_TIMESTAMP() AS END_DATETIME
    FROM {{ ref('SAT_RELATIONSHIP') }} curr
    INNER JOIN incoming_data inc 
        ON curr.HASHKEY = inc.HASHKEY
    WHERE curr.HASHDIFF != inc.HASHDIFF
)

SELECT e.*
FROM expiring_records e
LEFT JOIN {{ this }} t
    ON t.HASHDIFF = e.HASHDIFF
WHERE t.HASHDIFF IS NULL

{% else %}

SELECT 
    * EXCLUDE (END_DATETIME),
    '9999-12-31'::TIMESTAMP_LTZ AS END_DATETIME 
FROM {{ ref('SAT_RELATIONSHIP') }}
WHERE 1=0

{% endif %}
