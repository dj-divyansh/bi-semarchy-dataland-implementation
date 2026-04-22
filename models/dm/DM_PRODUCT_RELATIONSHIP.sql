{{ config(materialized='table', schema='DW_DFHPMS2EU_SEMARCHY_SCHEMA') }}

{% set etl_batch_id = env_var('ETL_BATCH_ID', env_var('DBT_JOB_RUN_ID', invocation_id)) %}

select
    s.parent_geographic_id,
    s.child_geographic_id,
    s.parent_dataset_id,
    s.child_dataset_id,
    s.parent_dataset_product_id,
    s.child_dataset_product_id,
    s.relationship_type_code,
    s.status_code,
    s.effective_date,
    s.end_date
from {{ ref('SAT_RELATIONSHIP') }} s
where s.end_datetime is null
  and s.etl_batch_id = '{{ etl_batch_id }}'
