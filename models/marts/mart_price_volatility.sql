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
        count(*)                                                 as hour_count
    from prices p
    left join date_dim d on p.date_id = d.date_id
    group by 1, 2, 3, 4, 5, 6, 7
)

select * from daily