{{ config(materialized='incremental', tags=['dq']) }}

WITH stg AS (
    SELECT * FROM {{ ref('stg_iqvia_midas') }}
)

SELECT 
    PRODUCT_NAME AS DATASET_PRODUCT_ID, -- Use Product Name as identifier for errors
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