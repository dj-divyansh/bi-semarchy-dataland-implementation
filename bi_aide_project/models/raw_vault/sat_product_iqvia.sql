{{ config(materialized='incremental', unique_key=['HK_PRODUCT', 'HASHDIFF'], tags=['vault']) }}

WITH source_data AS (
    SELECT
        BUSINESS_KEY,
        GEOGRAPHIC_ID,
        DATASET_ID,
        SOURCE_DATASET_ID,
        PRODUCT_TYPE_CODE,
        PRODUCT_NAME,
        BRAND_NAME,
        SUBSTANCE,
        ATC_CODE,
        WHO_CODE,
        NFC_CODE,
        CORPORATION,
        COMPETITOR_INDICATOR,
        PRESCRIPTION_REQUIRED_INDICATOR,
        MANUFACTURER_CODE,
        MANUFACTURER,
        STRENGTH,
        DOSE_FORM_CODE
    FROM {{ ref('stg_iqvia_midas') }}
),

final AS (
    SELECT
        BUSINESS_KEY AS HK_PRODUCT,
        MD5(
            UPPER(
                CONCAT_WS(
                    '||',
                    COALESCE(SUBSTANCE, ''),
                    COALESCE(ATC_CODE, ''),
                    COALESCE(WHO_CODE, ''),
                    COALESCE(NFC_CODE, ''),
                    COALESCE(CORPORATION, ''),
                    COALESCE(COMPETITOR_INDICATOR, ''),
                    COALESCE(PRESCRIPTION_REQUIRED_INDICATOR, ''),
                    COALESCE(MANUFACTURER, ''),
                    COALESCE(STRENGTH, ''),
                    COALESCE(DOSE_FORM_CODE, ''),
                    COALESCE(PRODUCT_NAME, ''),
                    COALESCE(BRAND_NAME, '')
                )
            )
        ) AS HASHDIFF,
        CURRENT_TIMESTAMP() AS LOAD_DATETIME, 
        'IQVIA_MIDAS' AS RECORD_SOURCE,
        
        -- Explicitly passing all STG columns needed by the Data Mart
        GEOGRAPHIC_ID,
        DATASET_ID,
        SOURCE_DATASET_ID,
        PRODUCT_TYPE_CODE,
        PRODUCT_NAME,
        BRAND_NAME,
        SUBSTANCE,
        ATC_CODE,
        WHO_CODE,
        NFC_CODE,
        CORPORATION,
        COMPETITOR_INDICATOR,
        PRESCRIPTION_REQUIRED_INDICATOR,
        MANUFACTURER_CODE,
        MANUFACTURER,
        STRENGTH,
        DOSE_FORM_CODE
    FROM source_data
)

SELECT *
FROM final
{% if is_incremental() %}
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ this }} t
    WHERE t.HK_PRODUCT = final.HK_PRODUCT
      AND t.HASHDIFF = final.HASHDIFF
)
{% endif %}
