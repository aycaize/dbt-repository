with source as (
    select * from {{ source('raw', 'raw_consumption') }}
),

renamed as (
    select
        date::timestamp_ntz     as consumption_datetime,
        time                    as hour,
        consumption             as consumption_mwh,
        loaded_at               as loaded_at
    from source
)

select * from renamed