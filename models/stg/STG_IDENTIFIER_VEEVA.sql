{{ config(materialized='view') }}

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
        p.name as parent_name,
        c.lastmodifieddate
    from {{ source('veeva_crm', 'VEEVA_CRM_APCNBRCEM_PRODUCT_VOD__C') }} c
    left outer join (
        select id, name,
            row_number() over (partition by id order by lastmodifieddate desc) as rn
        from {{ source('veeva_crm', 'VEEVA_CRM_APCNBRCEM_PRODUCT_VOD__C') }}
    ) p
        on c.parent_product_vod__c = p.id and p.rn = 1
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
        p.name as parent_name,
        c.lastmodifieddate
    from {{ source('veeva_crm', 'VEEVA_CRM_JPOPP8US_PRODUCT_VOD__C') }} c
    left outer join (
        select id, name,
            row_number() over (partition by id order by lastmodifieddate desc) as rn
        from {{ source('veeva_crm', 'VEEVA_CRM_JPOPP8US_PRODUCT_VOD__C') }}
    ) p
        on c.parent_product_vod__c = p.id and p.rn = 1
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
        p.name as parent_name,
        c.lastmodifieddate
    from {{ source('veeva_crm', 'VEEVA_CRM_ITAWRI_PRODUCT_VOD__C') }} c
    left outer join (
        select id, name,
            row_number() over (partition by id order by lastmodifieddate desc) as rn
        from {{ source('veeva_crm', 'VEEVA_CRM_ITAWRI_PRODUCT_VOD__C') }}
    ) p
        on c.parent_product_vod__c = p.id and p.rn = 1
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
        p.name as parent_name,
        c.lastmodifieddate
    from {{ source('veeva_crm', 'VEEVA_CRM_LATAM_PRODUCT_VOD__C') }} c
    left outer join (
        select id, name,
            row_number() over (partition by id order by lastmodifieddate desc) as rn
        from {{ source('veeva_crm', 'VEEVA_CRM_LATAM_PRODUCT_VOD__C') }}
    ) p
        on c.parent_product_vod__c = p.id and p.rn = 1
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
      and upper(mdm_product_type_bi__c) not in ('MEDICAL SEGMENT', 'INDICATION')
      and upper(name) not in (
          'DERMATOLOGY',
          'ENDOCRINOLOGY',
          'GASTROENTEROLOGY',
          'NEUROLOGY',
          'OPTHALMOLOGY',
          'RHEUMATOLOGY',
          'UROLOGY'
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

filtered_source as (
    select *
    from source
    where (country_code_bi__c is not null and trim(country_code_bi__c) != '')
      and (id is not null and trim(id) != '')
      and (name is not null and trim(name) != '')
      and upper(trim(name)) not like '%DUPLICATE TO%'
      and upper(trim(name)) not like '<NOT SPECIFIED>'
      and upper(trim(name)) not like '%DELETE%'
      and upper(trim(name)) not like '%NOT SPECIFIED%'
      and upper(trim(name)) not like 'BI %'
      and upper(trim(country_code_bi__c)) in (
          select upper(trim(iso2_code)) from {{ source('veeva_crm_reference', 'DIM_COUNTRY') }}
      )
),

transformed as (
    select distinct
        upper(trim(country_code_bi__c)) as geographic_id,
        'VEEVA_CRM' as dataset_id,
        trim(id) as dataset_product_id,
        'VEEVA_CRM' as identifier_type_code,
        concat(upper(trim(country_code_bi__c)), '_',
        trim(id)) as identifier_id,
        null as status_code,
        null as effective_date,
        null as end_date,
        cast(lastmodifieddate as timestamp_ltz) as source_lastmodifieddate
    from filtered_source
),

deduped as (
    select *,
        row_number() over (
            partition by geographic_id, dataset_product_id, identifier_id
            order by source_lastmodifieddate desc
        ) as rn
    from transformed
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
        'VEEVA_CRM' as record_source
    from deduped
    where rn = 1
)

select * from final