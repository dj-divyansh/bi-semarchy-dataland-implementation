{{ config(materialized='view') }}

with hist as (
    select
        upper(trim(parent_geographic_id)) as parent_geographic_id,
        upper(trim(child_geographic_id)) as child_geographic_id,
        trim(parent_dataset_id) as parent_dataset_id,
        trim(child_dataset_id) as child_dataset_id,
        upper(trim(parent_dataset_product_id)) as parent_dataset_product_id,
        trim(cast(child_dataset_product_id as varchar)) as child_dataset_product_id,
        trim(relationship_type_code) as relationship_type_code,
        trim(status_code) as status_code,
        try_to_date(trim(effective_date)) as effective_date,
        try_to_date(trim(end_date)) as end_date,
        current_timestamp() as source_lastmodifieddate
    from {{ var('closeup_hist_database', target.database) }}.{{ var('closeup_hist_schema', 'DW_DFHPMS2EU_SEMARCHY_SCHEMA') }}.{{ var('closeup_relationship_hist_table', 'CLOSEUP_RELATIONSHIP_HIST') }}
    where parent_dataset_product_id is not null and trim(parent_dataset_product_id) != ''
      and child_dataset_product_id is not null and trim(child_dataset_product_id) != ''
),

final as (
    select
        parent_geographic_id,
        child_geographic_id,
        parent_dataset_id,
        child_dataset_id,
        parent_dataset_product_id,
        child_dataset_product_id,
        relationship_type_code,
        status_code,
        effective_date,
        end_date,
        source_lastmodifieddate,
        md5(
            concat_ws(
                '||',
                coalesce(parent_geographic_id, ''),
                coalesce(child_geographic_id, ''),
                coalesce(parent_dataset_product_id, ''),
                coalesce(child_dataset_product_id, '')
            )
        ) as hashkey,
        md5(
            concat_ws(
                '||',
                coalesce(parent_geographic_id, ''),
                coalesce(child_geographic_id, ''),
                coalesce(parent_dataset_id, ''),
                coalesce(child_dataset_id, ''),
                coalesce(parent_dataset_product_id, ''),
                coalesce(child_dataset_product_id, ''),
                coalesce(relationship_type_code, ''),
                coalesce(status_code, ''),
                coalesce(cast(effective_date as varchar), ''),
                coalesce(cast(end_date as varchar), '')
            )
        ) as hashdiff,
        current_timestamp() as load_datetime,
        '{{ env_var("ETL_BATCH_ID", env_var("DBT_JOB_RUN_ID", invocation_id)) }}' as etl_batch_id,
        'CLOSEUP_MARKET' as record_source
    from hist
)

select * from final
