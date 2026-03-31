with consumption as (
    select * from {{ ref('stg_consumption') }}
),

final as (
    select
        -- keys
        cast(to_date(consumption_datetime) as date)  as date_id,
        hour                                          as hour,

        -- measures
        consumption_mwh,

        -- metadata
        consumption_datetime,
        loaded_at
    from consumption
)

select * from final