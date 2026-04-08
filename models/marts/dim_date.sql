with date_spine as (
    select
        dateadd(day, seq4(), '2022-01-01'::date) as date_day
    --from table(generator(rowcount => 1461))  -- 4 yıl (2022-2025)
    from table(generator(rowcount => 1826))  -- 5 yıl (2022-2026)
),

final as (
    select
        date_day                                    as date_id,
        year(date_day)                              as year,
        month(date_day)                             as month_number,
        monthname(date_day)                         as month_name,
        day(date_day)                               as day_of_month,
        dayofweek(date_day)                         as day_of_week,
        dayname(date_day)                           as day_name,
        quarter(date_day)                           as quarter,
        weekofyear(date_day)                        as week_of_year,
        case
            when dayofweek(date_day) in (0, 6) then true
            else false
        end                                         as is_weekend,
        case
            when date_day in (
                '2022-01-01', '2022-04-23', '2022-05-01', '2022-05-19',
                '2022-07-15', '2022-08-30', '2022-10-29',
                '2023-01-01', '2023-04-23', '2023-05-01', '2023-05-19',
                '2023-07-15', '2023-08-30', '2023-10-29',
                '2024-01-01', '2024-04-23', '2024-05-01', '2024-05-19',
                '2024-07-15', '2024-08-30', '2024-10-29',
                '2025-01-01', '2025-04-23', '2025-05-01', '2025-05-19',
                '2025-07-15', '2025-08-30', '2025-10-29',
                '2026-01-01', '2026-04-23', '2026-05-01', '2026-05-19',
                '2026-07-15', '2026-08-30', '2026-10-29'
            ) then true
            else false
        end                                         as is_public_holiday,
        date_trunc('month', date_day)               as first_day_of_month,
        last_day(date_day)                          as last_day_of_month
    from date_spine
)

select * from final