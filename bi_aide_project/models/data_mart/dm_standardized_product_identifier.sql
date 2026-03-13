{{ config(materialized='table', tags=['mart']) }}

WITH stg AS (
    SELECT
        GEOGRAPHIC_ID,
        DATASET_ID,
        SOURCE_DATASET_ID,
        PRODUCT_TYPE_CODE,
        BUSINESS_KEY,
        SOURCE_PRODUCT_BRAND_ID,
        SOURCE_PRODUCT_PACK_ID
    FROM {{ ref('stg_iqvia_midas') }}
),

brands AS (
    SELECT DISTINCT
        GEOGRAPHIC_ID AS geographic_id,
        DATASET_ID AS dataset_id,
        
        CONCAT('BRAND_', BUSINESS_KEY) AS dataset_product_id,
        SOURCE_DATASET_ID AS identifier_type_code,
        MD5(CONCAT(GEOGRAPHIC_ID, '_', SOURCE_DATASET_ID, '_', SOURCE_PRODUCT_BRAND_ID)) AS identifier_id,
        
        'A' AS status_code,
        NULL::DATE AS effective_date,
        NULL::DATE AS end_date
    FROM stg
    WHERE PRODUCT_TYPE_CODE = 'Brand'
),

pmps AS (
    SELECT DISTINCT
        GEOGRAPHIC_ID AS geographic_id,
        DATASET_ID AS dataset_id,
        
        BUSINESS_KEY AS dataset_product_id,
        
        SOURCE_DATASET_ID AS identifier_type_code,
        MD5(CONCAT(GEOGRAPHIC_ID, '_', SOURCE_DATASET_ID, '_', SOURCE_PRODUCT_BRAND_ID, '_', SOURCE_PRODUCT_PACK_ID)) AS identifier_id,
        
        'A' AS status_code,
        NULL::DATE AS effective_date,
        NULL::DATE AS end_date
    FROM stg
    WHERE PRODUCT_TYPE_CODE = 'PMP'
)

SELECT
    geographic_id,
    dataset_id,
    dataset_product_id,
    identifier_type_code,
    identifier_id,
    status_code,
    effective_date,
    end_date
FROM brands
UNION ALL
SELECT
    geographic_id,
    dataset_id,
    dataset_product_id,
    identifier_type_code,
    identifier_id,
    status_code,
    effective_date,
    end_date
FROM pmps
