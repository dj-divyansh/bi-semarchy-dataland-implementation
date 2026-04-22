{{ config(materialized='table', schema='DW_DFHPMS2EU_SEMARCHY_SCHEMA') }}

{% set etl_batch_id = env_var('ETL_BATCH_ID', env_var('DBT_JOB_RUN_ID', invocation_id)) %}

select
    s.geographic_id,
    s.dataset_id,
    s.dataset_product_id,
    s.identifier_type_code,
    s.identifier_id,
    s.status_code,
    s.effective_date,
    s.end_date
from {{ ref('SAT_IDENTIFIER') }} s
where s.end_datetime is null
  and s.etl_batch_id = '{{ etl_batch_id }}'
