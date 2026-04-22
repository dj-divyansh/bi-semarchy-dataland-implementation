
 
with raw_stg as (
    select *
    from DEV_DFHPMS2EU_DB.PUBLIC.STG_PRODUCT_VEEVA
),
rejects as (
    select DATASET_PRODUCT_ID
    from DEV_DFHPMS2EU_DB.PUBLIC.REJECT
),
clean_stg as (
    select s.*
    from raw_stg s
    left join rejects r
        on upper(trim(s.dataset_product_id)) = r.DATASET_PRODUCT_ID
    where r.DATASET_PRODUCT_ID is null
),
stg as (
    select *
    from clean_stg
    where geographic_id is not null and trim(geographic_id) != ''
      and product_name is not null and trim(product_name) != ''
      and dataset_product_id is not null and trim(dataset_product_id) != ''
),
 
delta_records as (
    select
        stg.geographic_id,
        stg.dataset_id,
        stg.source_dataset_id,
        stg.dataset_product_id,
        stg.product_type_code,
        stg.product_name,
        stg.standardized_product_name,
        stg.description,
        stg.product_status_code,
        stg.effective_date,
        stg.end_date,
        stg.brand_name,
        stg.generic_names,
        stg.approved_date,
        stg.marketing_start_date,
        stg.marketing_end_date,
        stg.expiration_date,
        stg.substance,
        stg.dose_form_code,
        stg.strength,
        stg.route_of_administration_code,
        stg.pharmaceutical_product_quantity,
        stg.unit_of_presentation_code,
        stg.primary_package_quantity,
        stg.primary_package_type_code,
        stg.secondary_package_quantity,
        stg.secondary_package_type_code,
        stg.package_quantity,
        stg.shelf_life_quantity,
        stg.shelf_life_unit_of_measure_code,
        stg.sample_indicator,
        stg.competitor_indicator,
        stg.generic_indicator,
        stg.prescription_required_indicator,
        stg.atc_code,
        stg.who_code,
        stg.nfc_code,
        stg.chc_nec_code,
        stg.usc_code,
        stg.manufacturer_id,
        stg.manufacturer,
        stg.corporation,
        stg.daily_dosage_quantity,
        stg.is_imported,
        stg.generic_product_description,
        stg.channel,
        stg.chc_flag
    from stg
    where not exists (
        select 1
        from DEV_DFHPMS2EU_DB.PUBLIC_DW_DFHPMS2EU_SEMARCHY_SCHEMA.SAT_PRODUCT_FILE sat
        where sat.identifier_md5 = stg.identifier_md5
          and sat.record_md5 = stg.record_md5
    )
)
 
select * from delta_records