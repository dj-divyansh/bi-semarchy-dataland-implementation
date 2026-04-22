{{ config(
    materialized='incremental',
    unique_key='HASHKEY',
    alias='HUB_PRODUCT_FILE',
    schema='DW_DFHPMS2EU_SEMARCHY_SCHEMA',
    tags=['vault', 'hub']
) }}

WITH staging_data AS (
    WITH product_raw_stg AS (
        SELECT * FROM {{ ref('STG_PRODUCT_VEEVA') }}
        UNION ALL
        SELECT * FROM {{ ref('STG_PRODUCT_CLOSEUP') }}
        UNION ALL
        SELECT * FROM {{ ref('STG_PRODUCT_GOB360') }}
    ),
    rejects AS (
        SELECT DATASET_PRODUCT_ID
        FROM {{ ref('REJECT') }}
    ),
    product_clean_stg AS (
        SELECT s.*
        FROM product_raw_stg s
        LEFT JOIN rejects r
            ON upper(trim(s.dataset_product_id)) = r.DATASET_PRODUCT_ID
        WHERE r.DATASET_PRODUCT_ID IS NULL
    ),
    product_hub_keys AS (
        SELECT DISTINCT
            HASHKEY,
            CURRENT_TIMESTAMP() AS START_DATETIME,
            CURRENT_TIMESTAMP() AS LOAD_DATETIME,
            RECORD_SOURCE,
            ETL_BATCH_ID,
            'PRODUCT' AS ORIGIN_STG
        FROM product_clean_stg
    ),
    identifier_hub_keys AS (
        SELECT DISTINCT
            HASHKEY,
            CURRENT_TIMESTAMP() AS START_DATETIME,
            CURRENT_TIMESTAMP() AS LOAD_DATETIME,
            RECORD_SOURCE,
            ETL_BATCH_ID,
            'IDENTIFIER' AS ORIGIN_STG
        FROM {{ ref('STG_IDENTIFIER_VEEVA') }}
        UNION ALL
        SELECT DISTINCT
            HASHKEY,
            CURRENT_TIMESTAMP() AS START_DATETIME,
            CURRENT_TIMESTAMP() AS LOAD_DATETIME,
            RECORD_SOURCE,
            ETL_BATCH_ID,
            'IDENTIFIER' AS ORIGIN_STG
        FROM {{ ref('STG_IDENTIFIER_GOB360') }}
    ),
    relationship_hub_keys AS (
        SELECT DISTINCT
            HASHKEY,
            CURRENT_TIMESTAMP() AS START_DATETIME,
            CURRENT_TIMESTAMP() AS LOAD_DATETIME,
            RECORD_SOURCE,
            ETL_BATCH_ID,
            'RELATIONSHIP' AS ORIGIN_STG
        FROM {{ ref('STG_RELATIONSHIP_VEEVA') }}
        UNION ALL
        SELECT DISTINCT
            HASHKEY,
            CURRENT_TIMESTAMP() AS START_DATETIME,
            CURRENT_TIMESTAMP() AS LOAD_DATETIME,
            RECORD_SOURCE,
            ETL_BATCH_ID,
            'RELATIONSHIP' AS ORIGIN_STG
        FROM {{ ref('STG_RELATIONSHIP_CLOSEUP') }}
        UNION ALL
        SELECT DISTINCT
            HASHKEY,
            CURRENT_TIMESTAMP() AS START_DATETIME,
            CURRENT_TIMESTAMP() AS LOAD_DATETIME,
            RECORD_SOURCE,
            ETL_BATCH_ID,
            'RELATIONSHIP' AS ORIGIN_STG
        FROM {{ ref('STG_RELATIONSHIP_GOB360') }}
    )
    SELECT * FROM product_hub_keys
    UNION ALL
    SELECT * FROM identifier_hub_keys
    UNION ALL
    SELECT * FROM relationship_hub_keys
)

SELECT
    s.HASHKEY,
    s.START_DATETIME,
    s.LOAD_DATETIME,
    s.RECORD_SOURCE,
    s.ETL_BATCH_ID,
    s.ORIGIN_STG
FROM staging_data s

{% if is_incremental() %}
    LEFT JOIN {{ this }} t
        ON t.HASHKEY = s.HASHKEY
    WHERE t.HASHKEY IS NULL
{% endif %}
