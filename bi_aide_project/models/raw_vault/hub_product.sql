{{ config(materialized='incremental', unique_key='HK_PRODUCT', tags=['vault']) }}

SELECT DISTINCT 
    BUSINESS_KEY AS HK_PRODUCT,
    BUSINESS_KEY AS BUSINESS_KEY,
    CURRENT_TIMESTAMP() AS LOAD_DATETIME,
    'IQVIA_MIDAS' AS RECORD_SOURCE
FROM {{ ref('stg_iqvia_midas') }}

{% if is_incremental() %}
    WHERE BUSINESS_KEY NOT IN (SELECT HK_PRODUCT FROM {{ this }})
{% endif %}
