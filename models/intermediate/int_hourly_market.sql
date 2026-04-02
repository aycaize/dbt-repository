with prices as (
    select * from {{ ref('stg_prices') }}
),

generation as (
    select * from {{ ref('stg_generation') }}
),

consumption as (
    select * from {{ ref('stg_consumption') }}
),

final as (
    select
        p.price_datetime                                        as datetime,
        p.hour,
        p.ptf_price_tl,
        p.ptf_price_usd,
        g.total_mwh                                            as generation_mwh,
        (g.wind_mwh + g.solar_mwh + g.dammed_hydro_mwh + 
         g.river_mwh + g.geothermal_mwh + g.biomass_mwh)      as renewable_total_mwh,
        c.consumption_mwh,
        g.total_mwh - c.consumption_mwh                       as generation_consumption_diff_mwh,
        round(
            (g.wind_mwh + g.solar_mwh + g.dammed_hydro_mwh + 
             g.river_mwh + g.geothermal_mwh + g.biomass_mwh) / 
              nullif(g.total_mwh, 0) * 100, 2)                as renewable_share_pct
    from prices p
    left join generation g on p.price_datetime = g.generation_datetime
    left join consumption c on p.price_datetime = c.consumption_datetime
)

select * from final