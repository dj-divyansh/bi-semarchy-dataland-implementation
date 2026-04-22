
 
with stg as (
    select
        geographic_id,
        dataset_product_id,
        product_name,
        product_type_code,
        record_source
    from DEV_DFHPMS2EU_DB.PUBLIC.STG_PRODUCT_VEEVA
),
 
rejected as (
    select
        md5(
            concat_ws(
                '||',
                coalesce(upper(trim(stg.geographic_id)), ''),
                'VEEVA_CRM',
                coalesce(stg.product_type_code, ''),
                coalesce(upper(trim(stg.dataset_product_id)), ''),
                coalesce(upper(trim(stg.product_name)), ''),
                case
                    when stg.product_name is null or trim(stg.product_name) = '' then 'EMPTY_PRODUCT_NAME'
                    when stg.dataset_product_id is null or trim(stg.dataset_product_id) = '' then 'EMPTY_DATASET_PRODUCT_ID'
                    when stg.geographic_id is null or trim(stg.geographic_id) = '' then 'EMPTY_COUNTRY_CODE'
                end
            )
        ) as REJECT_KEY,
        upper(trim(stg.dataset_product_id)) as DATASET_PRODUCT_ID,
        upper(trim(stg.geographic_id)) as SOURCE_COUNTRY_CODE,
        'CRITICAL' as ERROR_LEVEL,
        case
            when stg.product_name is null or trim(stg.product_name) = '' then 'EMPTY_PRODUCT_NAME'
            when stg.dataset_product_id is null or trim(stg.dataset_product_id) = '' then 'EMPTY_DATASET_PRODUCT_ID'
            when stg.geographic_id is null or trim(stg.geographic_id) = '' then 'EMPTY_COUNTRY_CODE'
        end as ERROR_MESSAGE,
        current_timestamp() as LOAD_DATETIME,
        stg.record_source as RECORD_SOURCE
    from stg
    where (stg.product_name is null or trim(stg.product_name) = '')
       or (stg.dataset_product_id is null or trim(stg.dataset_product_id) = '')
       or (stg.geographic_id is null or trim(stg.geographic_id) = '')
)
 
select
    REJECT_KEY,
    DATASET_PRODUCT_ID,
    SOURCE_COUNTRY_CODE,
    ERROR_LEVEL,
    ERROR_MESSAGE,
    LOAD_DATETIME,
    RECORD_SOURCE
from rejected
