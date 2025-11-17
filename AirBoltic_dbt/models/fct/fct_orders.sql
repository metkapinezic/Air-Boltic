with orders as (
    select * from {{ ref('src_order') }}
),

trips as (
    select * from {{ ref('src_trip') }}
),

fct_orders as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['o.order_id']) }} as order_key,
        
        -- Natural key
        o.order_id,
        
        -- Foreign keys
        o.customer_id,
        o.trip_id,
        
        -- Degenerate dimensions
        o.seat_no,
        o.status,
        
        -- Measures
        o.price_eur,
        
        -- Date/time dimensions (from trip)
        date(t.start_timestamp) as booking_date,
        t.start_timestamp as trip_start_timestamp,
        t.end_timestamp as trip_end_timestamp,
        
        -- Audit columns
        current_timestamp() as dbt_updated_at
    from orders o
    left join trips t on o.trip_id = t.trip_id
)

select * from fct_orders