

with raw_stg as (
    select *
    from DEV_DFHPMS2EU_DB.PUBLIC.STG_RELATIONSHIP_VEEVA
),
rejects as (
    select DATASET_PRODUCT_ID
    from DEV_DFHPMS2EU_DB.PUBLIC.REJECT
),
clean_stg as (
    select s.*
    from raw_stg s
    left join rejects r_parent
        on upper(trim(s.parent_dataset_product_id)) = r_parent.DATASET_PRODUCT_ID
    left join rejects r_child
        on upper(trim(s.child_dataset_product_id)) = r_child.DATASET_PRODUCT_ID
    where r_parent.DATASET_PRODUCT_ID is null
      and r_child.DATASET_PRODUCT_ID is null
),
stg as (
    select *
    from clean_stg
    where parent_geographic_id is not null and trim(parent_geographic_id) != ''
      and child_geographic_id is not null and trim(child_geographic_id) != ''
      and parent_dataset_product_id is not null and trim(parent_dataset_product_id) != ''
      and child_dataset_product_id is not null and trim(child_dataset_product_id) != ''
),

delta_records as (
    select
        stg.parent_geographic_id,
        stg.child_geographic_id,
        stg.parent_dataset_id,
        stg.child_dataset_id,
        stg.parent_dataset_product_id,
        stg.child_dataset_product_id,
        stg.relationship_type_code,
        stg.status_code,
        stg.effective_date,
        stg.end_date
    from stg
    where not exists (
        select 1
        from DEV_DFHPMS2EU_DB.DW_DFHPMS2EU_SEMARCHY_SCHEMA.SAT_RELATIONSHIP sat
        where sat.identifier_md5 = stg.identifier_md5
          and sat.record_md5 = stg.record_md5
    )
)

select * from delta_records