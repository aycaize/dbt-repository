with source as (
    select * from {{ source('raw', 'raw_prices') }}
),

deduped as (
    select *,
        row_number() over (partition by date order by loaded_at desc) as rn
    from source
),

renamed as (
    select
        date::timestamp_ntz       as price_datetime,
        hour                      as hour,
        price                     as ptf_price_tl,
        priceusd                  as ptf_price_usd,
        priceeur                  as ptf_price_eur,
        loaded_at                 as loaded_at
    from deduped
    where rn = 1
)

select * from renamed