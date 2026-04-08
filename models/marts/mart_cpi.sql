with stg as (
    select * from {{ ref('stg_cpi') }}
),

with_calculations as (
    select
        year,
        month_number,
        cpi_index,

        -- önceki aya göre yüzde değişim (aylık enflasyon)
        lag(cpi_index, 1) over (order by year, month_number)       as prev_month_cpi,
        round(
            (cpi_index - lag(cpi_index, 1) over (order by year, month_number)) /
            nullif(lag(cpi_index, 1) over (order by year, month_number), 0) * 100
        , 2)                                                        as monthly_inflation_pct,

        -- yıllık enflasyon (12 ay öncesine göre)
        lag(cpi_index, 12) over (order by year, month_number)      as prev_year_cpi,
        round(
            (cpi_index - lag(cpi_index, 12) over (order by year, month_number)) /
            nullif(lag(cpi_index, 12) over (order by year, month_number), 0) * 100
        , 2)                                                        as annual_inflation_pct,

        -- reel fiyar hesabı için deflator
        -- baz dönem olarak 2023 Ocak kullanıyoruz
        round(
            cpi_index / first_value(cpi_index) over (order by year, month_number)
        , 4)                                                        as cpi_deflator,

        loaded_at
    from stg
)

select * from with_calculations