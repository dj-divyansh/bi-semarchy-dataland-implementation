
  create or replace   view DEV_DFHPMS2EU_DB.PUBLIC.STG_PRODUCT_VEEVA
  
  
  
  
  as (
    

with apcnbrcem as (
    select
        c.country_code_bi__c,
        c.id,
        c.name,
        c.description_vod__c,
        c.external_id_vod__c,
        c.product_type_vod__c,
        c.active_bi__c,
        c.therapeutic_area_vod__c,
        c.mdm_product_type_bi__c,
        c.isdeleted,
        c.parent_product_vod__c,
        p.name as parent_name
    from DEV_DFHPMS2EU_DB.LANDING_DFHPMS2EU_SCHEMA.VEEVA_CRM_APCNBRCEM_PRODUCT_VOD__C c
    left outer join DEV_DFHPMS2EU_DB.LANDING_DFHPMS2EU_SCHEMA.VEEVA_CRM_APCNBRCEM_PRODUCT_VOD__C p
        on c.parent_product_vod__c = p.id
),

jpopp8us as (
    select
        c.country_code_bi__c,
        c.id,
        c.name,
        c.description_vod__c,
        c.external_id_vod__c,
        c.product_type_vod__c,
        c.active_bi__c,
        c.therapeutic_area_vod__c,
        c.mdm_product_type_bi__c,
        c.isdeleted,
        c.parent_product_vod__c,
        p.name as parent_name
    from DEV_DFHPMS2EU_DB.LANDING_DFHPMS2EU_SCHEMA.VEEVA_CRM_JPOPP8US_PRODUCT_VOD__C c
    left outer join DEV_DFHPMS2EU_DB.LANDING_DFHPMS2EU_SCHEMA.VEEVA_CRM_JPOPP8US_PRODUCT_VOD__C p
        on c.parent_product_vod__c = p.id
),

itawri as (
    select
        c.country_code_bi__c,
        c.id,
        c.name,
        c.description_vod__c,
        c.external_id_vod__c,
        c.product_type_vod__c,
        c.active_bi__c,
        c.therapeutic_area_vod__c,
        c.mdm_product_type_bi__c,
        c.isdeleted,
        c.parent_product_vod__c,
        p.name as parent_name
    from DEV_DFHPMS2EU_DB.LANDING_DFHPMS2EU_SCHEMA.VEEVA_CRM_ITAWRI_PRODUCT_VOD__C c
    left outer join DEV_DFHPMS2EU_DB.LANDING_DFHPMS2EU_SCHEMA.VEEVA_CRM_ITAWRI_PRODUCT_VOD__C p
        on c.parent_product_vod__c = p.id
),

latam as (
    select
        c.country_code_bi__c,
        c.id,
        c.name,
        c.description_vod__c,
        c.external_id_vod__c,
        c.product_type_vod__c,
        c.active_bi__c,
        c.therapeutic_area_vod__c,
        c.mdm_product_type_bi__c,
        c.isdeleted,
        c.parent_product_vod__c,
        p.name as parent_name
    from DEV_DFHPMS2EU_DB.LANDING_DFHPMS2EU_SCHEMA.VEEVA_CRM_LATAM_PRODUCT_VOD__C c
    left outer join DEV_DFHPMS2EU_DB.LANDING_DFHPMS2EU_SCHEMA.VEEVA_CRM_LATAM_PRODUCT_VOD__C p
        on c.parent_product_vod__c = p.id
),

source_union as (
    select * from apcnbrcem
    union
    select * from jpopp8us
    union
    select * from itawri
    union
    select * from latam
),
 
brand_filtered as (
    select distinct *, 'BRAND' as product_type_code
    from source_union
    where isdeleted = false
      and upper(product_type_vod__c) = 'DETAIL'
      and (
          upper(mdm_product_type_bi__c) in ('LOCAL BRAND', 'GLOBAL BRAND')
          or external_id_vod__c like 'B\_%' escape '\\'
      )
      and upper(mdm_product_type_bi__c) not in ('MEDICAL SEGMENT', 'INDICATION')
      and upper(name) not in (
          'DERMATOLOGY', 'ENDOCRINOLOGY', 'GASTROENTEROLOGY',
          'NEUROLOGY', 'OPTHALMOLOGY', 'RHEUMATOLOGY', 'UROLOGY'
      )
),

ind_filtered as (
    select distinct *, 'IND' as product_type_code
    from source_union
    where isdeleted = false
      and (
          upper(mdm_product_type_bi__c) = 'INDICATION'
          or (
              upper(product_type_vod__c) = 'DETAIL'
              and (mdm_product_type_bi__c = '' or mdm_product_type_bi__c is null)
              and country_code_bi__c = 'GB'
              and external_id_vod__c like 'GB\_%' escape '\\'
          )
          or (
              (upper(product_type_vod__c) = '' or product_type_vod__c is null)
              and (mdm_product_type_bi__c = '' or mdm_product_type_bi__c is null)
              and external_id_vod__c like '10_%'
          )
      )
),

pmp_filtered as (
    select distinct *, 'PMP' as product_type_code
    from source_union
    where isdeleted = false
      and (
          upper(product_type_vod__c) in ('FORM', 'ORDER', 'SAMPLE', 'SAMPLE PRODUCT GROUP')
          or (upper(product_type_vod__c) = 'BRC' and country_code_bi__c != 'CH')
          or (upper(product_type_vod__c) = 'BRC' and country_code_bi__c = 'CH' and external_id_vod__c like '%SAMPLE%')
      )
),

ta_filtered as (
    select distinct *, 'TA' as product_type_code
    from source_union
    where isdeleted = false
      and (
          upper(mdm_product_type_bi__c) = 'MEDICAL SEGMENT'
          or upper(name) in (
              'DERMATOLOGY',
              'ENDOCRINOLOGY',
              'GASTROENTEROLOGY',
              'NEUROLOGY',
              'OPTHALMOLOGY',
              'RHEUMATOLOGY',
              'UROLOGY'
          )
      )
),

typed_union as (
    select * from brand_filtered
    union all
    select * from ind_filtered
    union all
    select * from pmp_filtered
    union all
    select * from ta_filtered
),

transformed as (
    select
        upper(trim(country_code_bi__c)) as geographic_id,
        'CRM' as dataset_id,
        'VEEVA_CRM' as source_dataset_id,
        id as dataset_product_id,
        product_type_code,
        case
            when product_type_code = 'BRAND' then
                coalesce(
                    upper(trim(
                        case
                            when name like '%___(%' then substring(upper(trim(name)), 1, position('(' in upper(trim(name))) - 1)
                            when name like '%___%[%' then substring(upper(trim(name)), 1, position('[' in upper(trim(name))) - 1)
                            else name
                        end
                    )),
                    ''
                )
            else upper(trim(name))
        end as product_name,
        null as standardized_product_name,
        upper(trim(coalesce(description_vod__c, name))) as description,
        null as product_status_code,
        null as effective_date,
        null as end_date,
        case
            when product_type_code = 'PMP' then upper(trim(parent_name))
            when product_type_code = 'BRAND' then upper(trim(name))
            else null
        end as brand_name,
        null as generic_names,
        null as approved_date,
        null as marketing_start_date,
        null as marketing_end_date,
        null as expiration_date,
        null as substance,
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
        false as competitor_indicator,
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
        row_number() over (
            partition by upper(trim(country_code_bi__c)), upper(trim(id))
            order by upper(trim(id)), upper(trim(name)), upper(trim(coalesce(description_vod__c, name)))
        ) as rn
    from typed_union
    where (country_code_bi__c is not null and trim(country_code_bi__c) != '')
      and (id is not null and trim(id) != '')
      and (name is not null and trim(name) != '')
),

deduped as (
    select * from transformed where rn = 1
)

select
    geographic_id,
    dataset_id,
    source_dataset_id,
    dataset_product_id,
    product_type_code,
    product_name,
    standardized_product_name,
    description,
    product_status_code,
    effective_date,
    end_date,
    brand_name,
    generic_names,
    approved_date,
    marketing_start_date,
    marketing_end_date,
    expiration_date,
    substance,
    dose_form_code,
    strength,
    route_of_administration_code,
    pharmaceutical_product_quantity,
    unit_of_presentation_code,
    primary_package_quantity,
    primary_package_type_code,
    secondary_package_quantity,
    secondary_package_type_code,
    package_quantity,
    shelf_life_quantity,
    shelf_life_unit_of_measure_code,
    sample_indicator,
    competitor_indicator,
    generic_indicator,
    prescription_required_indicator,
    atc_code,
    who_code,
    nfc_code,
    chc_nec_code,
    usc_code,
    manufacturer_id,
    manufacturer,
    corporation,
    daily_dosage_quantity,
    is_imported,
    generic_product_description,
    channel,
    chc_flag,
    md5(
        concat_ws(
            '||',
            coalesce(geographic_id, ''),
            coalesce(dataset_id, ''),
            coalesce(source_dataset_id, ''),
            coalesce(dataset_product_id, '')
        )
    ) as identifier_md5,
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
            coalesce(to_varchar(competitor_indicator), ''),
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
    ) as record_md5,
    CURRENT_TIMESTAMP() as load_datetime,
    '66bf57be-ac57-4053-8da5-6f8d18655574' as etl_batch_id,
    'VEEVA_CRM' as record_source
from deduped
  );

