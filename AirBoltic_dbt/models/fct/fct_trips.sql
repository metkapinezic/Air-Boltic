with trips as (
    select * from {{ ref('src_trip') }}
),

orders as (
    select 
        trip_id,
        count(*) as seats_sold,
        sum(price_eur) as total_revenue_eur,
        count(distinct customer_id) as unique_customers
    from {{ ref('src_order') }}
    where status = 'Finished'  
    group by trip_id
),

aeroplanes as (
    select * from {{ ref('dim_aeroplanes') }}
),

-- Calculate route stats directly (don't reference dim_routes)
route_stats as (
    select 
        origin_city,
        destination_city,
        avg(timestampdiff(minute, start_timestamp, end_timestamp)) as avg_travel_time_minutes
    from {{ ref('src_trip') }}
    group by origin_city, destination_city
),

fct_trips as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['t.trip_id']) }} as trip_key,
        
        -- Natural key
        t.trip_id,
        
        -- Foreign keys
        t.airplane_id,
        {{ dbt_utils.generate_surrogate_key(['t.origin_city', 't.destination_city']) }} as route_key,
        
        -- Trip details
        t.origin_city,
        t.destination_city,
        t.origin_city || ' → ' || t.destination_city as route_name,
        t.start_timestamp,
        t.end_timestamp,
        
        -- Calculated measures
        timestampdiff(minute, t.start_timestamp, t.end_timestamp) as duration_minutes,
        
        -- Route benchmark
        rs.avg_travel_time_minutes as route_avg_travel_time_minutes,
        timestampdiff(minute, t.start_timestamp, t.end_timestamp) - rs.avg_travel_time_minutes as variance_from_avg_minutes,
        
        -- Aggregated order metrics
        coalesce(o.seats_sold, 0) as seats_sold,
        coalesce(o.total_revenue_eur, 0) as total_revenue_eur,
        coalesce(o.unique_customers, 0) as unique_customers,
        
        -- Capacity metrics
        a.max_seats as aircraft_capacity,
        round(coalesce(o.seats_sold, 0) / nullif(a.max_seats, 0) * 100, 2) as load_factor_pct,
        
        -- Audit columns
        current_timestamp() as dbt_updated_at
    from trips t
    left join orders o on t.trip_id = o.trip_id
    left join aeroplanes a on t.airplane_id = a.airplane_id
    left join route_stats rs 
        on t.origin_city = rs.origin_city 
        and t.destination_city = rs.destination_city
)

select * from fct_trips