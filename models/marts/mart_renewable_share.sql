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
        sum(g.generation_mwh)                                                           as total_mwh,
        sum(case when g.generation_source in ('wind', 'solar', 'dammed_hydro', 'river', 'geothermal', 'biomass') 
                 then g.generation_mwh else 0 end)                                      as renewable_mwh,
        sum(case when g.generation_source = 'wind'         then g.generation_mwh else 0 end) as wind_mwh,
        sum(case when g.generation_source = 'solar'        then g.generation_mwh else 0 end) as solar_mwh,
        sum(case when g.generation_source in ('dammed_hydro', 'river') 
                 then g.generation_mwh else 0 end)                                      as hydro_mwh,
        sum(case when g.generation_source = 'geothermal'   then g.generation_mwh else 0 end) as geothermal_mwh,
        sum(case when g.generation_source = 'biomass'      then g.generation_mwh else 0 end) as biomass_mwh,
        round(
            sum(case when g.generation_source in ('wind', 'solar', 'dammed_hydro', 'river', 'geothermal', 'biomass')
                     then g.generation_mwh else 0 end) /
            nullif(sum(g.generation_mwh), 0) * 100, 2)                                 as renewable_share_pct
    from generation g
    left join date_dim d on g.date_id = d.date_id
    group by 1, 2, 3, 4, 5, 6, 7
),

with_window_functions as (
    select
        *,
        lag(renewable_share_pct, 1) over (order by date_id)                             as prev_day_renewable_pct,
        round(renewable_share_pct - lag(renewable_share_pct, 1)
              over (order by date_id), 2)                                                as day_over_day_change_pct,
        round(avg(renewable_share_pct) over (
            order by date_id
            rows between 6 preceding and current row
        ), 2)                                                                            as moving_avg_7d_pct,
        round(avg(renewable_share_pct) over (
            order by date_id
            rows between 29 preceding and current row
        ), 2)                                                                            as moving_avg_30d_pct,
        round(avg(renewable_share_pct) over (
            partition by year, month_number
        ), 2)                                                                            as monthly_avg_renewable_pct,
        round(renewable_share_pct - avg(renewable_share_pct) over (
            partition by year, month_number
        ), 2)                                                                            as deviation_from_monthly_avg
    from daily
)

select * from with_window_functions