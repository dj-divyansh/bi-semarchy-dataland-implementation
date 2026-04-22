{{ config(
    materialized='incremental',
    unique_key='HASHDIFF',
    schema='DW_DFHPMS2EU_SEMARCHY_SCHEMA',
    tags=['vault', 'history']
) }}

-- 1. Get the incoming new data
WITH incoming_data AS (
    WITH raw_stg AS (
        SELECT * FROM {{ ref('STG_PRODUCT_VEEVA') }}
    ),
    rejects AS (
        SELECT DATASET_PRODUCT_ID
        FROM {{ ref('REJECT') }}
    ),
    clean_stg AS (
        SELECT s.*
        FROM raw_stg s
        LEFT JOIN rejects r
            ON upper(trim(s.dataset_product_id)) = r.DATASET_PRODUCT_ID
        WHERE r.DATASET_PRODUCT_ID IS NULL
    )
    SELECT HASHKEY, HASHDIFF
    FROM clean_stg
)

{% if is_incremental() %}
-- 2. Find records in the Current State table that are about to be overwritten
, expiring_records AS (
    SELECT 
        curr.* EXCLUDE (END_DATETIME),
        CURRENT_TIMESTAMP() AS END_DATETIME -- Stamp it with the exact time it expired
    FROM {{ ref('SAT_PRODUCT_FILE') }} curr
    INNER JOIN incoming_data inc 
        ON curr.HASHKEY = inc.HASHKEY
    -- The core logic: If the incoming hash is different, the current row is expiring!
    WHERE curr.HASHDIFF != inc.HASHDIFF
)

SELECT e.*
FROM expiring_records e
LEFT JOIN {{ this }} t
    ON t.HASHDIFF = e.HASHDIFF
WHERE t.HASHDIFF IS NULL

{% else %}

-- 3. Initial Load (Day 1)
-- If this table is empty, we just copy everything from the Current State table
-- and set the End Date far into the future.
SELECT 
    * EXCLUDE (END_DATETIME),
    '9999-12-31'::TIMESTAMP_LTZ AS END_DATETIME 
FROM {{ ref('SAT_PRODUCT_FILE') }}
WHERE 1=0 -- On first run, it just creates the schema. It will populate on the NEXT incremental run.

{% endif %}
