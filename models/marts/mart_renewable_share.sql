with generation as (
    select * from {{ ref('fact_hourly_generation') }}
),

date_dim as (
    select * from {{ ref('dim_date') }}
),

daily as (
    select
        g.date_id,
        d.year,
        d.month_number,
        d.month_name,
        d.day_name,
        d.is_weekend,
        d.is_public_holiday,
        sum(g.total_mwh)                                        as total_mwh,
        sum(g.renewable_total_mwh)                              as renewable_mwh,
        sum(g.wind_mwh)                                         as wind_mwh,
        sum(g.solar_mwh)                                        as solar_mwh,
        sum(g.dammed_hydro_mwh + g.river_mwh)                  as hydro_mwh,
        sum(g.geothermal_mwh)                                   as geothermal_mwh,
        sum(g.biomass_mwh)                                      as biomass_mwh,
        round(sum(g.renewable_total_mwh) / 
              nullif(sum(g.total_mwh), 0) * 100, 2)             as renewable_share_pct
    from generation g
    left join date_dim d on g.date_id = d.date_id
    group by 1, 2, 3, 4, 5, 6, 7
)

select * from daily