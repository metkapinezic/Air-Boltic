with trips as (
    select * from {{ ref('src_trip') }}
),

orders as (
    select * from {{ ref('src_order') }}
    where status = 'Finished'
),

aeroplanes as (
    select * from {{ ref('dim_aeroplanes') }}
),

fct_trips as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['o.order_id']) }} as trip_key,
        
        -- Natural keys
        o.order_id,
        t.trip_id,
        o.customer_id,
        
        -- Foreign keys
        t.airplane_id,
        {{ dbt_utils.generate_surrogate_key(['t.origin_city', 't.destination_city']) }} as route_key,
        
        -- Trip details
        t.origin_city,
        t.destination_city,
        t.origin_city || ' → ' || t.destination_city as route_name,
        t.start_timestamp,
        t.end_timestamp,
        
        -- Order details
        o.seat_no,
        o.price_eur as seat_price_eur,
        o.status,
        cast(null as timestamp) as booking_date,
        
        -- Capacity metrics
        a.max_seats as aircraft_capacity,
        
        -- Audit columns
        current_timestamp() as dbt_updated_at
    from orders o
    join trips t on o.trip_id = t.trip_id
    left join aeroplanes a on t.airplane_id = a.airplane_id
)

select * from fct_trips