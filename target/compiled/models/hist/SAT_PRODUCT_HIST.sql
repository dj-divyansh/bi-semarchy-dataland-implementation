

-- 1. Get the incoming new data
WITH incoming_data AS (
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
    SELECT IDENTIFIER_MD5, RECORD_MD5
    FROM clean_stg
)



-- 3. Initial Load (Day 1)
-- If this table is empty, we just copy everything from the Current State table
-- and set the End Date far into the future.
SELECT 
    * EXCLUDE (END_DATETIME),
    '9999-12-31'::TIMESTAMP_LTZ AS END_DATETIME 
FROM DEV_DFHPMS2EU_DB.DW_DFHPMS2EU_SEMARCHY_SCHEMA.SAT_PRODUCT_FILE
WHERE 1=0 -- On first run, it just creates the schema. It will populate on the NEXT incremental run.

