{{ config(materialized='view') }}

with src as (
    select
        trim(clave) as clave,
        trim(nombre) as nombre,
        trim(forma_farma) as forma_farma,
        trim(concentracion) as concentracion,
        trim(presentacion) as presentacion
    from {{ source('gob360_pm', 'GOB360_MX_BOEHRINGER_CAT_CLAVES') }}
    where nombre is not null and trim(nombre) not in ('', '-')
      and forma_farma is not null and trim(forma_farma) not in ('', '-')
      and concentracion is not null and trim(concentracion) not in ('', '-')
),

pmp_products as (
    select
        'MX' as geographic_id,
        'GOB360_PM' as dataset_id,
        'GOB360_PM' as source_dataset_id,
        upper(trim(clave)) as dataset_product_id,
        'PMP' as product_type_code,
        upper(trim(concat_ws(' ', nombre, forma_farma, concentracion, nullif(presentacion, '-')))) as product_name,
        null as standardized_product_name,
        upper(trim(concat_ws(' ', nombre, forma_farma, concentracion, nullif(presentacion, '-')))) as description,
        'A' as product_status_code,
        null as effective_date,
        null as end_date,
        null as brand_name,
        null as generic_names,
        null as approved_date,
        null as marketing_start_date,
        null as marketing_end_date,
        null as expiration_date,
        upper(trim(nombre)) as substance,
        null as dose_form_code,
        null as strength,
        null as route_of_administration_code,
        null as pharmaceutical_product_quantity,
        null as unit_of_presentation_code,
        null as primary_package_quantity,
        null as primary_package_type_code,
        null as secondary_package_quantity,
        null as secondary_package_type_code,
        null as package_quantity,
        null as shelf_life_quantity,
        null as shelf_life_unit_of_measure_code,
        null as sample_indicator,
        null as competitor_indicator,
        null as generic_indicator,
        null as prescription_required_indicator,
        null as atc_code,
        null as who_code,
        null as nfc_code,
        null as chc_nec_code,
        null as usc_code,
        null as manufacturer_id,
        null as manufacturer,
        null as corporation,
        null as daily_dosage_quantity,
        null as is_imported,
        null as generic_product_description,
        null as channel,
        null as chc_flag,
        current_timestamp() as source_lastmodifieddate
    from src
    where clave is not null and trim(clave) != ''
),

transformed as (
    select * from pmp_products
),

with_md5 as (
    select
        transformed.*,
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
                coalesce(generic_names, ''),
                coalesce(cast(effective_date as varchar), ''),
                coalesce(cast(end_date as varchar), ''),
                coalesce(brand_name, ''),
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
                coalesce(manufacturer_id, ''),
                coalesce(daily_dosage_quantity, ''),
                coalesce(corporation, ''),
                coalesce(manufacturer, ''),
                coalesce(is_imported, ''),
                coalesce(generic_product_description, ''),
                coalesce(channel, ''),
                coalesce(chc_flag, '')
            )
        ) as hashdiff,
        row_number() over (
            partition by
                md5(
                    concat_ws(
                        '||',
                        coalesce(geographic_id, ''),
                        coalesce(dataset_id, ''),
                        coalesce(source_dataset_id, ''),
                        coalesce(dataset_product_id, '')
                    )
                ),
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
                        coalesce(generic_names, ''),
                        coalesce(cast(effective_date as varchar), ''),
                        coalesce(cast(end_date as varchar), ''),
                        coalesce(brand_name, ''),
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
                        coalesce(manufacturer_id, ''),
                        coalesce(daily_dosage_quantity, ''),
                        coalesce(corporation, ''),
                        coalesce(manufacturer, ''),
                        coalesce(is_imported, ''),
                        coalesce(generic_product_description, ''),
                        coalesce(channel, ''),
                        coalesce(chc_flag, '')
                    )
                )
            order by geographic_id
        ) as rn
    from transformed
),

deduped as (
    select * from with_md5 where rn = 1
)

select
    d.* exclude (rn),
    current_timestamp() as load_datetime,
    '{{ env_var("ETL_BATCH_ID", env_var("DBT_JOB_RUN_ID", invocation_id)) }}' as etl_batch_id,
    'GOB360_PM' as record_source
from deduped d
