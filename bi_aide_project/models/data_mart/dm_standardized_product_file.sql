{{ config(materialized='table', tags=['mart']) }}

WITH latest_sat AS (
    SELECT
        HK_PRODUCT,
        LOAD_DATETIME,
        GEOGRAPHIC_ID,
        DATASET_ID,
        SOURCE_DATASET_ID,
        PRODUCT_TYPE_CODE,
        PRODUCT_NAME,
        BRAND_NAME,
        SUBSTANCE,
        NFC_CODE,
        DOSE_FORM_CODE,
        STRENGTH,
        COMPETITOR_INDICATOR,
        PRESCRIPTION_REQUIRED_INDICATOR,
        ATC_CODE,
        MANUFACTURER,
        CORPORATION,
        WHO_CODE
    FROM {{ ref('sat_product_iqvia') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY HK_PRODUCT ORDER BY LOAD_DATETIME DESC) = 1
),

hub AS (
    SELECT HK_PRODUCT, BUSINESS_KEY
    FROM {{ ref('hub_product') }}
)

SELECT
    s.GEOGRAPHIC_ID AS geographic_id,
    s.DATASET_ID AS dataset_id,
    s.SOURCE_DATASET_ID AS source_dataset_id,
    
    -- FIXED: Use the MD5 Hash (HK_PRODUCT) and apply the STTM prefix rule
    CASE 
        WHEN s.PRODUCT_TYPE_CODE = 'Brand' THEN CONCAT('BRAND_', h.HK_PRODUCT)
        ELSE h.HK_PRODUCT
    END AS dataset_product_id,
    
    s.PRODUCT_TYPE_CODE AS product_type_code,
    s.PRODUCT_NAME AS product_name,
    NULL AS standardized_product_name,
    s.PRODUCT_NAME AS description,
    'A' AS product_status_code,
    s.LOAD_DATETIME::DATE AS effective_date,
    NULL::DATE AS end_date,
    s.BRAND_NAME AS brand_name,
    NULL AS generic_names,
    NULL::DATE AS approved_date,
    NULL::DATE AS marketing_start_date,
    NULL::DATE AS marketing_end_date,
    NULL::DATE AS expiration_date,
    s.SUBSTANCE AS substance,
    s.DOSE_FORM_CODE AS dose_form_code,
    s.STRENGTH AS strength,
    NULL AS route_of_administration_code,
    NULL AS pharmaceutical_product_quantity,
    NULL AS unit_of_presentation_code,
    NULL AS primary_package_quantity,
    NULL AS primary_package_type_code,
    NULL AS secondary_package_quantity,
    NULL AS secondary_package_type_code,
    NULL AS package_quantity,
    NULL AS shelf_life_quantity,
    NULL AS shelf_life_unit_of_measure_code,
    NULL AS sample_indicator,
    s.COMPETITOR_INDICATOR AS competitor_indicator,
    NULL AS generic_indicator,
    s.PRESCRIPTION_REQUIRED_INDICATOR AS prescription_required_indicator,
    s.ATC_CODE AS atc_code,
    NULL AS manufacturer_id,
    s.MANUFACTURER AS manufacturer,
    s.CORPORATION AS corporation,
    s.WHO_CODE AS who_code,
    s.NFC_CODE AS nfc_code

FROM hub h
INNER JOIN latest_sat s ON h.HK_PRODUCT = s.HK_PRODUCT
