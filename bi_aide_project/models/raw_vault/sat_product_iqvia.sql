{{ config(materialized='incremental', unique_key='HASHDIFF', tags=['vault']) }}

WITH source_data AS (
    SELECT * FROM {{ ref('stg_iqvia_midas') }}
),

final AS (
    SELECT
        MD5(UPPER(CONCAT(GEOGRAPHIC_ID, '_', SOURCE_DATASET_ID, '_', PRODUCT_NAME))) AS HK_PRODUCT,
        MD5(UPPER(CONCAT_WS('||', 
            COALESCE(SUBSTANCE,''), COALESCE(ATC_CODE,''), COALESCE(WHO_CODE,''), 
            COALESCE(NFC_CODE,''), COALESCE(CORPORATION,''), 
            COALESCE(PRESCRIPTION_REQUIRED_INDICATOR,''), COALESCE(STRENGTH,''),
            COALESCE(DOSE_FORM_CODE,''), COALESCE(PRODUCT_NAME,'')
        ))) AS HASHDIFF,
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
        MANUFACTURER,
        STRENGTH,
        DOSE_FORM_CODE
    FROM source_data
)

SELECT * FROM final
{% if is_incremental() %}
  WHERE LOAD_DATETIME > (SELECT MAX(LOAD_DATETIME) FROM {{ this }})
{% endif %}