
  create or replace   view DEV_DFHPMS2EU_DB.PUBLIC.STG_RELATIONSHIP_VEEVA
  
  
  
  
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
    union all
    select * from jpopp8us
    union all
    select * from itawri
    union all
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

source as (
    select * from brand_filtered
    union all
    select * from ind_filtered
    union all
    select * from pmp_filtered
    union all
    select * from ta_filtered
),

global_brands as (
    select
        upper(trim(country_code_bi__c)) as geographic_id,
        id as dataset_product_id
    from source
    where upper(mdm_product_type_bi__c) = 'GLOBAL BRAND'
),

local_brands as (
    select
        upper(trim(country_code_bi__c)) as geographic_id,
        id as dataset_product_id,
        parent_product_vod__c as parent_product_id
    from source
    where upper(mdm_product_type_bi__c) = 'LOCAL BRAND'
),

brands as (
    select
        upper(trim(country_code_bi__c)) as geographic_id,
        id as dataset_product_id,
        parent_product_vod__c as parent_product_id
    from source
    where upper(mdm_product_type_bi__c) in ('GLOBAL BRAND', 'LOCAL BRAND')
),

pmp_products as (
    select
        upper(trim(country_code_bi__c)) as geographic_id,
        id as dataset_product_id,
        parent_product_vod__c as parent_product_id
    from source
    where product_type_code = 'PMP'
),

ta_products as (
    select
        upper(trim(country_code_bi__c)) as geographic_id,
        id as dataset_product_id
    from source
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

ind_products as (
    select
        upper(trim(country_code_bi__c)) as geographic_id,
        id as dataset_product_id,
        parent_product_vod__c as parent_product_id
    from source
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

global_rollup as (
    select
        p.geographic_id as parent_geographic_id,
        c.geographic_id as child_geographic_id,
        'VEEVA CRM' as parent_dataset_id,
        'VEEVA CRM' as child_dataset_id,
        p.dataset_product_id as parent_dataset_product_id,
        c.dataset_product_id as child_dataset_product_id,
        'GlobalRollup' as relationship_type_code,
        null as status_code,
        null as effective_date,
        null as end_date
    from local_brands c
    inner join global_brands p
        on c.parent_product_id = p.dataset_product_id
),

pmp_to_brand as (
    select
        p.geographic_id as parent_geographic_id,
        c.geographic_id as child_geographic_id,
        'VEEVA CRM' as parent_dataset_id,
        'VEEVA CRM' as child_dataset_id,
        p.dataset_product_id as parent_dataset_product_id,
        c.dataset_product_id as child_dataset_product_id,
        'PMPToBRAND' as relationship_type_code,
        null as status_code,
        null as effective_date,
        null as end_date
    from pmp_products c
    inner join brands p
        on c.parent_product_id = p.dataset_product_id
),

brand_to_ta as (
    select
        p.geographic_id as parent_geographic_id,
        c.geographic_id as child_geographic_id,
        'VEEVA CRM' as parent_dataset_id,
        'VEEVA CRM' as child_dataset_id,
        p.dataset_product_id as parent_dataset_product_id,
        c.dataset_product_id as child_dataset_product_id,
        'BRANDtoTA' as relationship_type_code,
        null as status_code,
        null as effective_date,
        null as end_date
    from brands c
    inner join ta_products p
        on c.parent_product_id = p.dataset_product_id
),

brandind_to_brand as (
    select
        p.geographic_id as parent_geographic_id,
        c.geographic_id as child_geographic_id,
        'VEEVA CRM' as parent_dataset_id,
        'VEEVA CRM' as child_dataset_id,
        p.dataset_product_id as parent_dataset_product_id,
        c.dataset_product_id as child_dataset_product_id,
        'BRANDINDtoBRAND' as relationship_type_code,
        null as status_code,
        null as effective_date,
        null as end_date
    from ind_products c
    inner join brands p
        on c.parent_product_id = p.dataset_product_id
),

all_rel as (
    select * from global_rollup
    union all
    select * from pmp_to_brand
    union all
    select * from brand_to_ta
    union all
    select * from brandind_to_brand
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
        md5(
            concat_ws(
                '||',
                coalesce(parent_geographic_id, ''),
                coalesce(child_geographic_id, ''),
                coalesce(parent_dataset_product_id, ''),
                coalesce(child_dataset_product_id, '')
            )
        ) as identifier_md5,
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
        ) as record_md5,
        current_timestamp() as load_datetime,
        '019d6d17-aa7d-71e1-97c4-9c9211538576' as etl_batch_id,
        'VEEVA_CRM' as record_source
    from all_rel
)
 
select * from final
  );

