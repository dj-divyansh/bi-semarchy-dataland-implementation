{{ config(materialized='view') }}

with brands as (
    select
        upper(trim(m.country)) as geographic_id,
        trim(m.codigo) as brand_code,
        cast(m.last_updt_dt as timestamp_ltz) as source_lastmodifieddate
    from {{ source('closeup_market', 'MARCAS') }} m
    where m.curr_flg = 1
),

mp_products as (
    select
        upper(trim(p.country)) as geographic_id,
        trim(p.codigo) as product_code,
        trim(p.marca) as brand_code,
        cast(p.last_updt_dt as timestamp_ltz) as source_lastmodifieddate
    from {{ source('closeup_market', 'PRODUCTO') }} p
    where p.curr_flg = 1
),

relationships as (
    select
        b.geographic_id as parent_geographic_id,
        p.geographic_id as child_geographic_id,
        'CLOSEUP_MARKET' as parent_dataset_id,
        'CLOSEUP_MARKET' as child_dataset_id,
        concat('BRAND_', b.brand_code) as parent_dataset_product_id,
        p.product_code as child_dataset_product_id,
        'MPtoBRAND' as relationship_type_code,
        null as status_code,
        null as effective_date,
        null as end_date,
        greatest(
            coalesce(p.source_lastmodifieddate, '1900-01-01'::timestamp_ltz),
            coalesce(b.source_lastmodifieddate, '1900-01-01'::timestamp_ltz)
        ) as source_lastmodifieddate
    from mp_products p
    inner join brands b
        on p.brand_code = b.brand_code
       and p.geographic_id = b.geographic_id
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
    from relationships
)

select * from final
