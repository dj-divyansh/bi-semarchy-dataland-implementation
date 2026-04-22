{{ config(materialized='view') }}

with src as (
    select
        trim(clave) as clave
    from {{ source('gob360_pm', 'GOB360_MX_BOEHRINGER_CAT_CLAVES') }}
    where clave is not null and trim(clave) != ''
),

transformed as (
    select
        'MX' as geographic_id,
        'GOB360_PM' as dataset_id,
        upper(trim(clave)) as dataset_product_id,
        'GOB360_PM' as identifier_type_code,
        concat('MX_', upper(trim(clave))) as identifier_id,
        null as status_code,
        null as effective_date,
        null as end_date,
        current_timestamp() as source_lastmodifieddate
    from src
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
        'GOB360_PM' as record_source
    from transformed
)

select * from final
