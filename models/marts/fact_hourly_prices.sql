{{
    config(
        materialized='incremental',
        unique_key='price_datetime'
    )
}}

with prices as (
    select * from {{ ref('stg_prices') }}
),

final as (
    select
        cast(to_date(price_datetime) as date)   as date_id,
        hour                                     as hour,
        1                                        as price_type_id,
        ptf_price_tl,
        ptf_price_usd,
        ptf_price_eur,
        price_datetime,
        loaded_at
    from prices

    {% if is_incremental() %}
        where price_datetime > (select max(price_datetime) from {{ this }})
    {% endif %}
)

select * from final