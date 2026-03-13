{{ config(materialized='incremental', unique_key='REJECT_KEY', on_schema_change='sync_all_columns', tags=['dq']) }}

WITH stg AS (
    SELECT
        GEOGRAPHIC_ID,
        SOURCE_DATASET_ID,
        SOURCE_COUNTRY_NAME,
        SOURCE_COUNTRY_CODE,
        PRODUCT_TYPE_CODE,
        BUSINESS_KEY,
        SOURCE_PRODUCT_BRAND_ID,
        SOURCE_PRODUCT_PACK_ID,
        PRODUCT_NAME
    FROM {{ ref('stg_iqvia_midas') }}
),

rejected AS (
    SELECT 
        MD5(
            CONCAT_WS(
                '||',
                COALESCE(GEOGRAPHIC_ID, ''),
                COALESCE(SOURCE_DATASET_ID, ''),
                COALESCE(SOURCE_COUNTRY_NAME, ''),
                COALESCE(SOURCE_COUNTRY_CODE, ''),
                COALESCE(PRODUCT_TYPE_CODE, ''),
                COALESCE(BUSINESS_KEY, ''),
                COALESCE(SOURCE_PRODUCT_BRAND_ID, ''),
                COALESCE(SOURCE_PRODUCT_PACK_ID, ''),
                COALESCE(PRODUCT_NAME, ''),
                CASE 
                    WHEN GEOGRAPHIC_ID IS NULL THEN 'geographic_id is missing'
                    WHEN SOURCE_DATASET_ID IS NULL THEN 'source_dataset_id is missing'
                    WHEN PRODUCT_NAME IS NULL THEN 'product_name is missing'
                END
            )
        ) AS REJECT_KEY,
        CASE
            WHEN PRODUCT_TYPE_CODE = 'Brand' THEN CONCAT('BRAND_', BUSINESS_KEY)
            ELSE BUSINESS_KEY
        END AS DATASET_PRODUCT_ID,
        SOURCE_COUNTRY_NAME,
        SOURCE_COUNTRY_CODE,
        'CRITICAL' AS ERROR_LEVEL,
        CASE 
            WHEN GEOGRAPHIC_ID IS NULL THEN 'geographic_id is missing'
            WHEN SOURCE_DATASET_ID IS NULL THEN 'source_dataset_id is missing'
            WHEN PRODUCT_NAME IS NULL THEN 'product_name is missing'
        END AS ERROR_MESSAGE,
        CURRENT_TIMESTAMP() AS LOAD_DATETIME
    FROM stg
    WHERE GEOGRAPHIC_ID IS NULL 
       OR SOURCE_DATASET_ID IS NULL 
       OR PRODUCT_NAME IS NULL
)

SELECT 
    REJECT_KEY,
    DATASET_PRODUCT_ID,
    SOURCE_COUNTRY_NAME,
    SOURCE_COUNTRY_CODE,
    ERROR_LEVEL,
    ERROR_MESSAGE,
    LOAD_DATETIME
FROM rejected
{% if is_incremental() %}
WHERE REJECT_KEY NOT IN (SELECT REJECT_KEY FROM {{ this }})
{% endif %}
