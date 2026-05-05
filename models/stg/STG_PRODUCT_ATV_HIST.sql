{{ config(materialized='view') }}

with hist as (
    select
        trim(geographic_id) as geographic_id,
        trim(dataset_id) as dataset_id,
        trim(source_dataset_id) as source_dataset_id,
        trim(dataset_product_id) as dataset_product_id,
        trim(product_type_code) as product_type_code,
        trim(product_name) as product_name,
        trim(standardized_product_name) as standardized_product_name,
        trim(description) as description,
        trim(product_status_code) as product_status_code,
        try_to_date(trim(effective_date)) as effective_date,
        try_to_date(trim(end_date)) as end_date,
        trim(brand_name) as brand_name,
        trim(generic_names) as generic_names,
        try_to_date(trim(approved_date)) as approved_date,
        try_to_date(trim(marketing_start_date)) as marketing_start_date,
        try_to_date(trim(marketing_end_date)) as marketing_end_date,
        try_to_date(trim(expiration_date)) as expiration_date,
        trim(substance) as substance,
        trim(dose_form_code) as dose_form_code,
        trim(strength) as strength,
        trim(route_of_administration_code) as route_of_administration_code,
        trim(pharmaceutical_product_quantity) as pharmaceutical_product_quantity,
        trim(unit_of_presentation_code) as unit_of_presentation_code,
        trim(primary_package_quantity) as primary_package_quantity,
        trim(primary_package_type_code) as primary_package_type_code,
        trim(secondary_package_quantity) as secondary_package_quantity,
        trim(secondary_package_type_code) as secondary_package_type_code,
        trim(package_quantity) as package_quantity,
        trim(shelf_life_quantity) as shelf_life_quantity,
        trim(shelf_life_unit_of_measure_code) as shelf_life_unit_of_measure_code,
        trim(sample_indicator) as sample_indicator,
        cast(competitor_indicator as varchar) as competitor_indicator,
        trim(generic_indicator) as generic_indicator,
        trim(prescription_required_indicator) as prescription_required_indicator,
        trim(atc_code) as atc_code,
        null as who_code,
        null as nfc_code,
        null as chc_nec_code,
        null as usc_code,
        trim(manufacturer_id) as manufacturer_id,
        null as manufacturer,
        null as corporation,
        null as daily_dosage_quantity,
        null as is_imported,
        null as generic_product_description,
        null as channel,
        null as chc_flag,
        current_timestamp() as source_lastmodifieddate
    from {{ target.database }}.DW_DFHPMS2EU_SEMARCHY_SCHEMA.ATV_PRODUCT_HIST
    where dataset_product_id is not null and trim(dataset_product_id) != ''
      and product_name is not null and trim(product_name) not in ('', '-')
),

final as (
    select
        hist.*,
        md5(
            concat_ws(
                '||',
                coalesce(geographic_id, ''),
                coalesce(dataset_id, ''),
                coalesce(source_dataset_id, ''),
                coalesce(dataset_product_id, '')
            )
        ) as hashkey,
        md5(
            concat_ws(
                '||',
                coalesce(geographic_id, ''),
                coalesce(dataset_id, ''),
                coalesce(source_dataset_id, ''),
                coalesce(product_type_code, ''),
                coalesce(dataset_product_id, ''),
                coalesce(product_name, ''),
                coalesce(standardized_product_name, ''),
                coalesce(description, ''),
                coalesce(product_status_code, ''),
                coalesce(cast(effective_date as varchar), ''),
                coalesce(cast(end_date as varchar), ''),
                coalesce(brand_name, ''),
                coalesce(generic_names, ''),
                coalesce(cast(approved_date as varchar), ''),
                coalesce(cast(marketing_start_date as varchar), ''),
                coalesce(cast(marketing_end_date as varchar), ''),
                coalesce(cast(expiration_date as varchar), ''),
                coalesce(substance, ''),
                coalesce(dose_form_code, ''),
                coalesce(strength, ''),
                coalesce(route_of_administration_code, ''),
                coalesce(pharmaceutical_product_quantity, ''),
                coalesce(unit_of_presentation_code, ''),
                coalesce(primary_package_quantity, ''),
                coalesce(primary_package_type_code, ''),
                coalesce(secondary_package_quantity, ''),
                coalesce(secondary_package_type_code, ''),
                coalesce(package_quantity, ''),
                coalesce(shelf_life_quantity, ''),
                coalesce(shelf_life_unit_of_measure_code, ''),
                coalesce(sample_indicator, ''),
                coalesce(competitor_indicator, ''),
                coalesce(generic_indicator, ''),
                coalesce(prescription_required_indicator, ''),
                coalesce(atc_code, ''),
                coalesce(who_code, ''),
                coalesce(nfc_code, ''),
                coalesce(chc_nec_code, ''),
                coalesce(usc_code, ''),
                coalesce(manufacturer_id, '')
                ,
                coalesce(daily_dosage_quantity, ''),
                coalesce(corporation, ''),
                coalesce(manufacturer, ''),
                coalesce(is_imported, ''),
                coalesce(generic_product_description, ''),
                coalesce(channel, ''),
                coalesce(chc_flag, '')
            )
        ) as hashdiff,
        current_timestamp() as load_datetime,
        '{{ env_var("ETL_BATCH_ID", env_var("DBT_JOB_RUN_ID", invocation_id)) }}' as etl_batch_id,
        'ATV' as record_source
    from hist
)

select * from final
