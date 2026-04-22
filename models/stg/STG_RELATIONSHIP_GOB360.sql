{{ config(materialized='view') }}

with src as (
    select
        trim(clave) as clave,
        trim(nombre) as nombre
    from {{ source('gob360_pm', 'GOB360_MX_BOEHRINGER_CAT_CLAVES') }}
    where clave is not null and trim(clave) != ''
      and nombre is not null and trim(nombre) != ''
),

transformed as (
    select
        'MX' as parent_geographic_id,
        'MX' as child_geographic_id,
        'GOB360_PM' as parent_dataset_id,
        'GOB360_PM' as child_dataset_id,
        concat('BRAND_', upper(trim(nombre))) as parent_dataset_product_id,
        upper(trim(clave)) as child_dataset_product_id,
        'PMPtoBRAND' as relationship_type_code,
        null as status_code,
        null as effective_date,
        null as end_date,
        current_timestamp() as source_lastmodifieddate
    from src
    where 1 = 0
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
        'GOB360_PM' as record_source
    from transformed
)

select * from final
