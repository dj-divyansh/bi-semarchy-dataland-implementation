{{ config(materialized='table', tags=['mart']) }}

SELECT
    GEOGRAPHIC_ID AS parent_geographic_id,
    GEOGRAPHIC_ID AS child_geographic_id,
    CONCAT('BRAND_', HK_BRAND) AS parent_dataset_id,
    HK_PMP AS child_dataset_id,
    
    -- Prefix the Brand Hash Key as requested in STTM
    CONCAT('BRAND_', HK_BRAND) AS parent_dataset_product_id,
    
    -- Child ID is just the PMP Hash Key
    HK_PMP AS child_dataset_product_id,
    
    'PMPtoBRAND' AS relationship_type_code,
    'A' AS status_code,
    NULL::DATE AS effective_date,
    NULL::DATE AS end_date

FROM {{ ref('link_product_brand') }}
