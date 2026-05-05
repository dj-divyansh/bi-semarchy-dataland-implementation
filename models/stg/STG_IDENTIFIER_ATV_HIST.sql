{{ config(materialized='view') }}

with hist as (
    select
        trim(geographic_id) as geographic_id,
        trim(dataset_id) as dataset_id,
        trim(dataset_product_id) as dataset_product_id,
        trim(identifier_type_code) as identifier_type_code,
        trim(identifier_id) as identifier_id,
        trim(status_code) as status_code,
        try_to_date(trim(effective_date)) as effective_date,
        try_to_date(trim(end_date)) as end_date,
        current_timestamp() as source_lastmodifieddate
    from {{ target.database }}.DW_DFHPMS2EU_SEMARCHY_SCHEMA.ATV_IDENTIFIER_HIST
    where dataset_product_id is not null and trim(dataset_product_id) != ''
      and identifier_id is not null and trim(identifier_id) not in ('', '-')
),

final as (
    select
        geographic_id,
        dataset_id,
        dataset_product_id,
        identifier_type_code,
        identifier_id,
        status_code,
        effective_date,
        end_date,
        source_lastmodifieddate,
        md5(
            concat_ws(
                '||',
                coalesce(geographic_id, ''),
                coalesce(dataset_id, ''),
                coalesce(dataset_product_id, ''),
                coalesce(identifier_id, '')
            )
        ) as hashkey,
        md5(
            concat_ws(
                '||',
                coalesce(geographic_id, ''),
                coalesce(dataset_id, ''),
                coalesce(dataset_product_id, ''),
                coalesce(identifier_type_code, ''),
                coalesce(identifier_id, ''),
                coalesce(status_code, ''),
                coalesce(cast(effective_date as varchar), ''),
                coalesce(cast(end_date as varchar), '')
            )
        ) as hashdiff,
        current_timestamp() as load_datetime,
        '{{ env_var("ETL_BATCH_ID", env_var("DBT_JOB_RUN_ID", invocation_id)) }}' as etl_batch_id,
        'ATV' as record_source
    from hist
)

select * from final
