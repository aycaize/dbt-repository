with prices as (
    select * from {{ ref('stg_prices') }}
),

date_dim as (
    select * from {{ ref('dim_date') }}
),

price_type_dim as (
    select * from {{ ref('dim_price_type') }}
),

final as (
    select
        -- keys
        cast(to_date(price_datetime) as date)   as date_id,
        hour                                     as hour,
        1                                        as price_type_id,  -- PTF

        -- measures
        ptf_price_tl,
        ptf_price_usd,
        ptf_price_eur,

        -- metadata
        price_datetime,
        loaded_at
    from prices
)

select * from final