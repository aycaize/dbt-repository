{{
    config(
        materialized='incremental',
        unique_key='consumption_datetime'
    )
}}

with consumption as (
    select * from {{ ref('stg_consumption') }}
),

final as (
    select
        cast(to_date(consumption_datetime) as date)  as date_id,
        hour                                          as hour,
        consumption_mwh,
        consumption_datetime,
        loaded_at
    from consumption

    {% if is_incremental() %}
        where consumption_datetime > (select max(consumption_datetime) from {{ this }})
    {% endif %}
)

select * from final