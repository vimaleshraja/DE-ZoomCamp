with source as (
    select * from {{ source('raw', 'fhv_2019_hw4') }}
),


renamed as (
    select
        -- identifiers
        cast(PUlocationID as integer) as pickup_location_id,
        cast(DOlocationID as integer) as dropoff_location_id,

        -- timestamps
        cast(pickup_datetime as timestamp) as pickup_datetime,  
        cast(dropOff_datetime as timestamp) as dropoff_datetime,

        -- other info
        dispatching_base_num as dispatching_base_num,
        SR_Flag as sr_flag,
        Affiliated_base_number as affliated_base_number

    from source
    -- Filter out records with null vendor_id (data quality requirement)
    where dispatching_base_num is not null
)

select * from renamed

-- Sample records for dev environment using deterministic date filter
{% if target.name == 'dev' %}
where pickup_datetime >= '2019-01-01' and pickup_datetime < '2019-02-01'
{% endif %}
