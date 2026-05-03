with source as (
    select * from {{ source('raw', 'raw_consumption') }}
),

deduped as (
    select *,
        row_number() over (partition by date order by loaded_at desc) as rn
    from source
),

renamed as (
    select
        date::timestamp_ntz     as consumption_datetime,
        time                    as hour,
        consumption             as consumption_mwh,
        loaded_at               as loaded_at
    from deduped
    where rn = 1
)

select * from renamed