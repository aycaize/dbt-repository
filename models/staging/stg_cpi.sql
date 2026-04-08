with source as (
    select * from {{ source('raw', 'raw_cpi') }}
),

renamed as (
    select
        year                                        as year,
        month                                       as month_number,
        cpi_index                                   as cpi_index,
        loaded_at                                   as loaded_at
    from source
)

select * from renamed