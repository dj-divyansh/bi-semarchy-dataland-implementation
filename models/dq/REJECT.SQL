{{ config(materialized='incremental', unique_key='REJECT_KEY', on_schema_change='sync_all_columns', tags=['dq']) }}
 
with stg as (
    select
        geographic_id,
        dataset_id,
        source_dataset_id,
        dataset_product_id,
        product_name,
        product_type_code,
        hashkey,
        hashdiff,
        source_lastmodifieddate,
        etl_batch_id,
        record_source,
        'STG_PRODUCT_VEEVA' as reject_source_model
    from {{ ref('STG_PRODUCT_VEEVA') }}

    union all

    select
        geographic_id,
        dataset_id,
        source_dataset_id,
        dataset_product_id,
        product_name,
        product_type_code,
        hashkey,
        hashdiff,
        source_lastmodifieddate,
        etl_batch_id,
        record_source,
        'STG_PRODUCT_CLOSEUP' as reject_source_model
    from {{ ref('STG_PRODUCT_CLOSEUP') }}

    union all

    select
        geographic_id,
        dataset_id,
        source_dataset_id,
        dataset_product_id,
        product_name,
        product_type_code,
        hashkey,
        hashdiff,
        source_lastmodifieddate,
        etl_batch_id,
        record_source,
        'STG_PRODUCT_GOB360' as reject_source_model
    from {{ ref('STG_PRODUCT_GOB360') }}
),

valid_countries as (
    select upper(trim(iso2_code)) as iso2_code
    from {{ source('veeva_crm_reference', 'DIM_COUNTRY') }}
),
 
rejected_base as (
    select distinct
        upper(trim(stg.dataset_product_id)) as DATASET_PRODUCT_ID,
        upper(trim(stg.geographic_id)) as SOURCE_COUNTRY_CODE,
        upper(trim(stg.dataset_id)) as DATASET_ID,
        upper(trim(stg.source_dataset_id)) as SOURCE_DATASET_ID,
        'CRITICAL' as ERROR_LEVEL,
        case
            when stg.product_name is null or trim(stg.product_name) = '' then 'EMPTY_PRODUCT_NAME'
            when stg.dataset_product_id is null or trim(stg.dataset_product_id) = '' then 'EMPTY_DATASET_PRODUCT_ID'
            when stg.geographic_id is null or trim(stg.geographic_id) = '' then 'EMPTY_COUNTRY_CODE'
            when vc.iso2_code is null then 'GEOGRAPHIC_ID_NOT_PRESENT'
        end as ERROR_CODE,
        case
            when stg.product_name is null or trim(stg.product_name) = '' then 'EMPTY_PRODUCT_NAME'
            when stg.dataset_product_id is null or trim(stg.dataset_product_id) = '' then 'EMPTY_DATASET_PRODUCT_ID'
            when stg.geographic_id is null or trim(stg.geographic_id) = '' then 'EMPTY_COUNTRY_CODE'
            when vc.iso2_code is null then 'GEOGRAPHIC_ID_NOT_PRESENT'
        end as ERROR_MESSAGE,
        current_timestamp() as LOAD_DATETIME,
        stg.source_lastmodifieddate as SOURCE_LASTMODIFIEDDATE,
        stg.hashkey as HASHKEY,
        stg.hashdiff as HASHDIFF,
        stg.etl_batch_id as ETL_BATCH_ID,
        stg.record_source as RECORD_SOURCE,
        stg.reject_source_model as REJECT_SOURCE_MODEL,
        to_variant(object_construct_keep_null(
            'geographic_id', stg.geographic_id,
            'dataset_id', stg.dataset_id,
            'source_dataset_id', stg.source_dataset_id,
            'dataset_product_id', stg.dataset_product_id,
            'product_name', stg.product_name,
            'product_type_code', stg.product_type_code,
            'hashkey', stg.hashkey,
            'hashdiff', stg.hashdiff,
            'source_lastmodifieddate', stg.source_lastmodifieddate,
            'etl_batch_id', stg.etl_batch_id,
            'record_source', stg.record_source
        )) as REJECTED_RECORD
    from stg
    left join valid_countries vc
        on upper(trim(stg.geographic_id)) = vc.iso2_code
    where (stg.product_name is null or trim(stg.product_name) = '')
       or (stg.dataset_product_id is null or trim(stg.dataset_product_id) = '')
       or (stg.geographic_id is null or trim(stg.geographic_id) = '')
       or vc.iso2_code is null
),

rejected as (
    select
        md5(
            concat_ws(
                '||',
                coalesce(b.SOURCE_COUNTRY_CODE, ''),
                coalesce(b.DATASET_ID, ''),
                coalesce(b.SOURCE_DATASET_ID, ''),
                coalesce(b.DATASET_PRODUCT_ID, ''),
                coalesce(b.ERROR_CODE, ''),
                coalesce(b.HASHKEY, ''),
                coalesce(b.HASHDIFF, ''),
                coalesce(md5(to_json(b.REJECTED_RECORD)), '')
            )
        ) as REJECT_KEY,
        b.DATASET_PRODUCT_ID,
        b.SOURCE_COUNTRY_CODE,
        b.DATASET_ID,
        b.SOURCE_DATASET_ID,
        b.ERROR_LEVEL,
        b.ERROR_CODE,
        b.ERROR_MESSAGE,
        b.LOAD_DATETIME,
        b.SOURCE_LASTMODIFIEDDATE,
        b.HASHKEY,
        b.HASHDIFF,
        b.ETL_BATCH_ID,
        b.RECORD_SOURCE,
        b.REJECT_SOURCE_MODEL,
        b.REJECTED_RECORD
    from rejected_base b
)
 
select
    r.REJECT_KEY,
    r.DATASET_PRODUCT_ID,
    r.SOURCE_COUNTRY_CODE,
    r.DATASET_ID,
    r.SOURCE_DATASET_ID,
    r.ERROR_LEVEL,
    r.ERROR_CODE,
    r.ERROR_MESSAGE,
    r.LOAD_DATETIME,
    r.SOURCE_LASTMODIFIEDDATE,
    r.HASHKEY,
    r.HASHDIFF,
    r.ETL_BATCH_ID,
    r.RECORD_SOURCE,
    r.REJECT_SOURCE_MODEL,
    r.REJECTED_RECORD
from rejected r
{% if is_incremental() %}
where not exists (
    select 1
    from {{ this }} t
    where t.REJECT_KEY = r.REJECT_KEY
)
{% endif %}
