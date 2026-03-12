{{ config(materialized='incremental', unique_key='HK_LINK', tags=['vault']) }}

WITH pmp_data AS (
    -- Only look at PMP records because they contain both PMP and Brand context
    SELECT 
        -- 1. PMP key uses the staging BUSINESS_KEY contract
        BUSINESS_KEY AS HK_PMP,

        -- 2. Brand key is the deterministic hash used by Brand rows in staging
        MD5(CONCAT(GEOGRAPHIC_ID, SOURCE_DATASET_ID, SOURCE_PRODUCT_BRAND_ID)) AS HK_BRAND,

        GEOGRAPHIC_ID,
        CURRENT_TIMESTAMP() AS LOAD_DATETIME,
        'IQVIA_MIDAS' AS RECORD_SOURCE
    FROM {{ ref('stg_iqvia_midas') }}
    WHERE PRODUCT_TYPE_CODE = 'PMP'
)

SELECT DISTINCT
    -- 3. The Link Hash Key is an MD5 of the two connected Hub Keys
    MD5(CONCAT(HK_PMP, HK_BRAND)) AS HK_LINK,
    HK_BRAND,
    HK_PMP,
    GEOGRAPHIC_ID,
    LOAD_DATETIME,
    RECORD_SOURCE
FROM pmp_data

{% if is_incremental() %}
    -- Only insert new relationships we haven't seen before
    WHERE MD5(CONCAT(HK_PMP, HK_BRAND)) NOT IN (SELECT HK_LINK FROM {{ this }})
{% endif %}
