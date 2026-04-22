

WITH staging_data AS (
    WITH raw_stg AS (
        SELECT * FROM DEV_DFHPMS2EU_DB.PUBLIC.STG_PRODUCT_VEEVA
    ),
    rejects AS (
        SELECT DATASET_PRODUCT_ID
        FROM DEV_DFHPMS2EU_DB.PUBLIC.REJECT
    ),
    clean_stg AS (
        SELECT s.*
        FROM raw_stg s
        LEFT JOIN rejects r
            ON upper(trim(s.dataset_product_id)) = r.DATASET_PRODUCT_ID
        WHERE r.DATASET_PRODUCT_ID IS NULL
    )
    SELECT 
        IDENTIFIER_MD5,
        DATASET_PRODUCT_ID AS BUSINESS_KEY, 
        CURRENT_TIMESTAMP() AS START_DATETIME, 
        CURRENT_TIMESTAMP() AS LOAD_DATETIME,
        RECORD_SOURCE,
        ETL_BATCH_ID
    FROM clean_stg
)

SELECT
    s.IDENTIFIER_MD5,
    s.BUSINESS_KEY,
    s.START_DATETIME,
    s.LOAD_DATETIME,
    s.RECORD_SOURCE,
    s.ETL_BATCH_ID
FROM staging_data s

