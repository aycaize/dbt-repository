{% snapshot snap_daily_prices %}

{{
    config(
        target_schema='snapshots',
        unique_key='date_id',
        strategy='timestamp',
        updated_at='loaded_at'
    )
}}

select
    date_id,
    avg(ptf_price_tl)   as avg_price_tl,
    min(ptf_price_tl)   as min_price_tl,
    max(ptf_price_tl)   as max_price_tl,
    loaded_at
from {{ ref('fact_hourly_prices') }}
group by date_id, loaded_at

{% endsnapshot %}