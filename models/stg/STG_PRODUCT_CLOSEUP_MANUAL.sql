{{ config(materialized='view') }}

select *
from {{ target.database }}.DW_DFHPMS2EU_SEMARCHY_SCHEMA.MANUAL_STG_PRODUCT_CLOSEUP
