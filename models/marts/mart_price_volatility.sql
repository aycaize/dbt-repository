with prices as (
    select * from {{ ref('fact_hourly_prices') }}
),

date_dim as (
    select * from {{ ref('dim_date') }}
),

daily as (
    select
        p.date_id,
        d.year,
        d.month_number,
        d.month_name,
        d.day_name,
        d.is_weekend,
        d.is_public_holiday,
        round(avg(p.ptf_price_tl), 2)                           as avg_price_tl,
        round(min(p.ptf_price_tl), 2)                           as min_price_tl,
        round(max(p.ptf_price_tl), 2)                           as max_price_tl,
        round(max(p.ptf_price_tl) - min(p.ptf_price_tl), 2)    as price_range_tl,
        round(stddev(p.ptf_price_tl), 2)                        as price_stddev_tl,
        round(avg(p.ptf_price_usd), 2)                          as avg_price_usd,
        round(avg(p.ptf_price_eur), 2)                          as avg_price_eur,
        round(min(p.ptf_price_usd), 2)                          as min_price_usd,
        round(max(p.ptf_price_usd), 2)                          as max_price_usd,
        count(*)                                                 as hour_count
    from prices p
    left join date_dim d on p.date_id = d.date_id
    group by 1, 2, 3, 4, 5, 6, 7
),

with_window_functions as (
    select
        *,
        -- önceki güne göre fiyat değişimi
        lag(avg_price_tl, 1) over (order by date_id)            as prev_day_avg_price_tl,
        round(avg_price_tl - lag(avg_price_tl, 1) 
              over (order by date_id), 2)                        as day_over_day_change_tl,
        round((avg_price_tl - lag(avg_price_tl, 1) 
              over (order by date_id)) / 
              nullif(lag(avg_price_tl, 1) 
              over (order by date_id), 0) * 100, 2)             as day_over_day_change_pct,

        -- 7 günlük hareketli ortalama
        round(avg(avg_price_tl) over (
            order by date_id
            rows between 6 preceding and current row
        ), 2)                                                    as moving_avg_7d_tl,

        -- 30 günlük hareketli ortalama
        round(avg(avg_price_tl) over (
            order by date_id
            rows between 29 preceding and current row
        ), 2)                                                    as moving_avg_30d_tl,

        -- aylık ortalamaya göre sapma
        round(avg(avg_price_tl) over (
            partition by year, month_number
        ), 2)                                                    as monthly_avg_price_tl,
        
        round(avg(avg_price_usd) over (
            order by date_id
            rows between 6 preceding and current row
        ), 2)                                                    as moving_avg_7d_usd,

        -- 30 günlük hareketli ortalama
        round(avg(avg_price_usd) over (
            order by date_id
            rows between 29 preceding and current row
        ), 2)                                                    as moving_avg_30d_usd,

        round(avg_price_tl - avg(avg_price_tl) over (
            partition by year, month_number
        ), 2)                                                    as deviation_from_monthly_avg
    from daily
)

select * from with_window_functions