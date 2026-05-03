{{
    config(
        materialized='incremental',
        unique_key=['generation_datetime', 'generation_source']
    )
}}

with generation as (
    select * from {{ ref('stg_generation') }}
),

unpivoted as (
    select generation_datetime, hour, 'natural_gas'    as generation_source, natural_gas_mwh    as generation_mwh, loaded_at from generation
    union all
    select generation_datetime, hour, 'dammed_hydro',                        dammed_hydro_mwh,               loaded_at from generation
    union all
    select generation_datetime, hour, 'lignite',                             lignite_mwh,                    loaded_at from generation
    union all
    select generation_datetime, hour, 'river',                               river_mwh,                      loaded_at from generation
    union all
    select generation_datetime, hour, 'import_coal',                         import_coal_mwh,                loaded_at from generation
    union all
    select generation_datetime, hour, 'wind',                                wind_mwh,                       loaded_at from generation
    union all
    select generation_datetime, hour, 'solar',                               solar_mwh,                      loaded_at from generation
    union all
    select generation_datetime, hour, 'fuel_oil',                            fuel_oil_mwh,                   loaded_at from generation
    union all
    select generation_datetime, hour, 'geothermal',                          geothermal_mwh,                 loaded_at from generation
    union all
    select generation_datetime, hour, 'asphaltite_coal',                     asphaltite_coal_mwh,            loaded_at from generation
    union all
    select generation_datetime, hour, 'black_coal',                          black_coal_mwh,                 loaded_at from generation
    union all
    select generation_datetime, hour, 'biomass',                             biomass_mwh,                    loaded_at from generation
    union all
    select generation_datetime, hour, 'naphta',                              naphta_mwh,                     loaded_at from generation
    union all
    select generation_datetime, hour, 'lng',                                 lng_mwh,                        loaded_at from generation
    union all
    select generation_datetime, hour, 'import_export',                       import_export_mwh,              loaded_at from generation
    union all
    select generation_datetime, hour, 'waste_heat',                          waste_heat_mwh,                 loaded_at from generation
),

final as (
    select
        cast(to_date(generation_datetime) as date)  as date_id,
        hour,
        generation_source,
        generation_mwh,
        generation_datetime,
        loaded_at
    from unpivoted

    {% if is_incremental() %}
        where generation_datetime > (select max(generation_datetime) from {{ this }})
    {% endif %}
)

select * from final