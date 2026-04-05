with daily as (
    select * from {{ ref('mart_price_volatility') }}
),

monthly as (
    select
        year,
        month_number,
        month_name,
        case
            when month_number in (12, 1, 2)  then 'Kış'
            when month_number in (3, 4, 5)   then 'İlkbahar'
            when month_number in (6, 7, 8)   then 'Yaz'
            when month_number in (9, 10, 11) then 'Sonbahar'
        end                                             as season,
        case
            when month_number in (1, 2, 3)   then 'Q1'
            when month_number in (4, 5, 6)   then 'Q2'
            when month_number in (7, 8, 9)   then 'Q3'
            when month_number in (10, 11, 12) then 'Q4'
        end                                             as quarter,

        -- fiyat metrikleri
        round(avg(avg_price_usd), 2)                   as avg_price_usd,
        round(avg(avg_price_tl), 2)                    as avg_price_tl,
        round(stddev(avg_price_tl), 2)                 as price_stddev_tl,
        round(max(max_price_tl), 2)                    as max_price_tl,
        round(min(min_price_tl), 2)                    as min_price_tl,
        round(max(max_price_tl) - min(min_price_tl), 2) as price_range_tl,
        count(*)                                        as day_count,

        -- değişim katsayısı (CV)
        round(stddev(avg_price_tl) / 
              nullif(avg(avg_price_tl), 0) * 100, 2)   as cv_pct,

        -- volatilite etiketi
        case
            when round(stddev(avg_price_tl) / 
                 nullif(avg(avg_price_tl), 0) * 100, 2) >= 15 then 'Yüksek Volatilite'
            when round(stddev(avg_price_tl) / 
                 nullif(avg(avg_price_tl), 0) * 100, 2) >= 8  then 'Orta Volatilite'
            else 'Düşük Volatilite'
        end                                             as volatility_label
    from daily
    group by 1, 2, 3, 4, 5
)

select * from monthly