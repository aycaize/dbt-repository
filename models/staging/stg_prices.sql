with source as (
    select * from {{ source('raw', 'raw_prices') }}
),

renamed as (
    select
        date::timestamp_ntz       as price_datetime,
        hour                      as hour,
        price                     as ptf_price_tl,
        priceusd                  as ptf_price_usd,
        priceeur                  as ptf_price_eur,
        loaded_at                 as loaded_at
    from source
)

select * from renamed