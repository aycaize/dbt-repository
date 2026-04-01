{{
    config(
        materialized='incremental',
        unique_key='generation_datetime'
    )
}}

with generation as (
    select * from {{ ref('stg_generation') }}
),

final as (
    select
        cast(to_date(generation_datetime) as date)  as date_id,
        hour                                         as hour,
        total_mwh,
        natural_gas_mwh,
        dammed_hydro_mwh,
        lignite_mwh,
        river_mwh,
        import_coal_mwh,
        wind_mwh,
        solar_mwh,
        fuel_oil_mwh,
        geothermal_mwh,
        asphaltite_coal_mwh,
        black_coal_mwh,
        biomass_mwh,
        naphta_mwh,
        lng_mwh,
        import_export_mwh,
        waste_heat_mwh,
        (wind_mwh + solar_mwh + dammed_hydro_mwh + 
         river_mwh + geothermal_mwh + biomass_mwh)  as renewable_total_mwh,
        generation_datetime,
        loaded_at
    from generation

    {% if is_incremental() %}
        where generation_datetime > (select max(generation_datetime) from {{ this }})
    {% endif %}
)

select * from final