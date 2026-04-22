{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['ETL_BATCH_ID', 'HASHKEY', 'HASHDIFF'],
    on_schema_change='sync_all_columns',
    schema='DW_DFHPMS2EU_SEMARCHY_SCHEMA',
    tags=['stg_snapshot']
) }}

with rejects as (
    select dataset_product_id
    from {{ ref('REJECT') }}
),

stg as (
    select s.*
    from {{ ref('STG_PRODUCT_VEEVA') }} s
    left join rejects r
        on upper(trim(s.dataset_product_id)) = r.dataset_product_id
    where r.dataset_product_id is null
),

snapshot as (
    select
        current_timestamp() as captured_at,
        stg.*
    from stg
)

select s.*
from snapshot s
