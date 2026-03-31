with source as (
    select * from {{ source('raw', 'raw_generation') }}
),

renamed as (
    select
        date::timestamp_ntz     as generation_datetime,
        hour                    as hour,
        total                   as total_mwh,
        naturalgas              as natural_gas_mwh,
        dammedhydro             as dammed_hydro_mwh,
        lignite                 as lignite_mwh,
        river                   as river_mwh,
        importcoal              as import_coal_mwh,
        wind                    as wind_mwh,
        sun                     as solar_mwh,
        fueloil                 as fuel_oil_mwh,
        geothermal              as geothermal_mwh,
        asphaltitecoal          as asphaltite_coal_mwh,
        blackcoal               as black_coal_mwh,
        biomass                 as biomass_mwh,
        naphta                  as naphta_mwh,
        lng                     as lng_mwh,
        importexport            as import_export_mwh,
        wasteheat               as waste_heat_mwh,
        loaded_at               as loaded_at
    from source
)

select * from renamed