{{ config(materialized='incremental', unique_key='REJECT_KEY', tags=['dq']) }}

WITH stg AS (
    SELECT
        GEOGRAPHIC_ID,
        SOURCE_DATASET_ID,
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
                COALESCE(PRODUCT_NAME, ''),
                CASE 
                    WHEN GEOGRAPHIC_ID IS NULL THEN 'geographic_id is missing'
                    WHEN SOURCE_DATASET_ID IS NULL THEN 'source_dataset_id is missing'
                    WHEN PRODUCT_NAME IS NULL THEN 'product_name is missing'
                END
            )
        ) AS REJECT_KEY,
        PRODUCT_NAME AS DATASET_PRODUCT_ID,
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
    ERROR_LEVEL,
    ERROR_MESSAGE,
    LOAD_DATETIME
FROM rejected
{% if is_incremental() %}
WHERE REJECT_KEY NOT IN (SELECT REJECT_KEY FROM {{ this }})
{% endif %}
