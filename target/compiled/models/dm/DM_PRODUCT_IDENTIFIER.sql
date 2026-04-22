

with raw_stg as (
    select *
    from DEV_DFHPMS2EU_DB.PUBLIC.STG_IDENTIFIER_VEEVA
),
rejects as (
    select DATASET_PRODUCT_ID
    from DEV_DFHPMS2EU_DB.PUBLIC.REJECT
),
clean_stg as (
    select s.*
    from raw_stg s
    left join rejects r
        on upper(trim(s.dataset_product_id)) = r.DATASET_PRODUCT_ID
    where r.DATASET_PRODUCT_ID is null
),
stg as (
    select *
    from clean_stg
    where geographic_id is not null and trim(geographic_id) != ''
      and dataset_product_id is not null and trim(dataset_product_id) != ''
      and identifier_id is not null and trim(identifier_id) != ''
),

delta_records as (
    select
        stg.geographic_id,
        stg.dataset_id,
        stg.dataset_product_id,
        stg.identifier_type_code,
        stg.identifier_id,
        stg.status_code,
        stg.effective_date,
        stg.end_date
    from stg
    where not exists (
        select 1
        from DEV_DFHPMS2EU_DB.DW_DFHPMS2EU_SEMARCHY_SCHEMA.SAT_IDENTIFIER sat
        where sat.identifier_md5 = stg.identifier_md5
          and sat.record_md5 = stg.record_md5
    )
)

select * from delta_records