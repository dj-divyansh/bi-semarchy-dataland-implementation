{{ config(materialized='view') }}

with brand_src as (
    select
        upper(trim(m.country)) as geographic_id,
        'PRESCRIPTIONDATA' as dataset_id,
        'CLOSEUP_MARKET' as source_dataset_id,
        concat('BRAND_', trim(m.codigo)) as dataset_product_id,
        'BRAND' as product_type_code,
        trim(substr(m.nome, 1, greatest(length(m.nome) - 3, 0))) as product_name,
        trim(substr(m.nome, 1, greatest(length(m.nome) - 3, 0))) as brand_name,
        case
            when upper(trim(c.nome)) like '%BOEHRINGER ING%' then 'N'
            else 'Y'
        end as competitor_indicator,
        null as generic_indicator,
        null as prescription_required_indicator,
        null as substance,
        concat(trim(lab.cdg), trim(m.siglalab)) as manufacturer_id,
        c.nome as corporation,
        greatest(
            coalesce(cast(m.last_updt_dt as timestamp_ltz), '1900-01-01'::timestamp_ltz),
            coalesce(cast(c.last_updt_dt as timestamp_ltz), '1900-01-01'::timestamp_ltz),
            coalesce(cast(lab.last_updt_dt as timestamp_ltz), '1900-01-01'::timestamp_ltz)
        ) as source_lastmodifieddate
    from {{ source('closeup_market', 'MARCAS') }} m
    left join {{ source('closeup_market', 'CORPORACAO') }} c
        on m.siglalab = c.sigla
       and m.country = c.country
       and c.curr_flg = 1
    left join {{ source('closeup_market', 'LABORATORIO') }} lab
        on m.siglalab = lab.codigo
       and m.country = lab.country
       and lab.curr_flg = 1
    where m.curr_flg = 1
),

mp_substance as (
    select
        upper(trim(mp.country)) as geographic_id,
        trim(mp.cdgprod) as dataset_product_id,
        array_to_string(array_sort(array_agg(distinct trim(mol.nome))), '|') as substance
    from {{ source('closeup_market', 'MOLECULA_PRODUCTO') }} mp
    inner join {{ source('closeup_market', 'MOLECULA') }} mol
        on mp.cdgmol = mol.codigomole
       and mp.country = mol.country
    where mp.curr_flg = 1
      and mol.curr_flg = 1
      and mp.cdgprod is not null
      and trim(mp.cdgprod) != ''
      and mol.nome is not null
      and trim(mol.nome) != ''
    group by
        upper(trim(mp.country)),
        trim(mp.cdgprod)
),

mp_src as (
    select
        upper(trim(p.country)) as geographic_id,
        'PRESCRIPTIONDATA' as dataset_id,
        'CLOSEUP_MARKET' as source_dataset_id,
        trim(p.codigo) as dataset_product_id,
        'MP' as product_type_code,
        trim(regexp_replace(p.nome, '\s+\S+$', '')) as product_name,
        trim(substr(m.nome, 1, greatest(length(m.nome) - 3, 0))) as brand_name,
        case
            when p.flg_classific = 'G' then 'Y'
        end as generic_indicator,
        case
            when p.flg_etico = 'E' then 'Y'
            when p.flg_etico = 'P' then 'N'
        end as prescription_required_indicator,
        case
            when upper(trim(corp.nome)) like '%BOEHRINGER ING%' then 'N'
            else 'Y'
        end as competitor_indicator,
        s.substance as substance,
        concat(trim(lab.cdg), trim(p.codigolab)) as manufacturer_id,
        corp.nome as corporation,
        greatest(
            coalesce(cast(p.last_updt_dt as timestamp_ltz), '1900-01-01'::timestamp_ltz),
            coalesce(cast(m.last_updt_dt as timestamp_ltz), '1900-01-01'::timestamp_ltz),
            coalesce(cast(lab.last_updt_dt as timestamp_ltz), '1900-01-01'::timestamp_ltz),
            coalesce(cast(corp.last_updt_dt as timestamp_ltz), '1900-01-01'::timestamp_ltz)
        ) as source_lastmodifieddate
    from {{ source('closeup_market', 'PRODUCTO') }} p
    left join mp_substance s
        on s.dataset_product_id = trim(p.codigo)
       and s.geographic_id = upper(trim(p.country))
    left join {{ source('closeup_market', 'MARCAS') }} m
        on p.marca = m.codigo
       and p.country = m.country
       and m.curr_flg = 1
    left join {{ source('closeup_market', 'LABORATORIO') }} lab
        on p.codigolab = lab.codigo
       and p.country = lab.country
       and lab.curr_flg = 1
    left join {{ source('closeup_market', 'CORPORACAO') }} corp
        on lab.cdgcorporacao = corp.codigo
       and lab.country = corp.country
       and corp.curr_flg = 1
    where p.curr_flg = 1
),

all_prod as (
    select
        geographic_id,
        dataset_id,
        source_dataset_id,
        dataset_product_id,
        product_type_code,
        product_name,
        brand_name,
        competitor_indicator,
        generic_indicator,
        prescription_required_indicator,
        substance,
        manufacturer_id,
        corporation,
        source_lastmodifieddate
    from brand_src

    union all

    select
        geographic_id,
        dataset_id,
        source_dataset_id,
        dataset_product_id,
        product_type_code,
        product_name,
        brand_name,
        competitor_indicator,
        generic_indicator,
        prescription_required_indicator,
        substance,
        manufacturer_id,
        corporation,
        source_lastmodifieddate
    from mp_src
),

transformed as (
    select
        geographic_id,
        dataset_id,
        source_dataset_id,
        dataset_product_id,
        product_type_code,
        product_name,
        null as standardized_product_name,
        coalesce(nullif(trim(product_name), ''), null) as description,
        'A' as product_status_code,
        null as effective_date,
        null as end_date,
        brand_name,
        null as generic_names,
        null as approved_date,
        null as marketing_start_date,
        null as marketing_end_date,
        null as expiration_date,
        substance,
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
        competitor_indicator,
        generic_indicator,
        prescription_required_indicator,
        null as atc_code,
        null as who_code,
        null as nfc_code,
        null as chc_nec_code,
        null as usc_code,
        manufacturer_id,
        null as manufacturer,
        corporation,
        null as daily_dosage_quantity,
        null as is_imported,
        null as generic_product_description,
        null as channel,
        null as chc_flag,
        source_lastmodifieddate
    from all_prod
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
    'CLOSEUP_MARKET' as record_source
from deduped d
