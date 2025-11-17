with trips as (
    select * from {{ ref('src_trip') }}
),

route_stats as (
    select 
        origin_city,
        destination_city,
        -- Calculate average travel time in minutes for each route
        avg(timestampdiff(minute, start_timestamp, end_timestamp)) as avg_travel_time_minutes,
        min(timestampdiff(minute, start_timestamp, end_timestamp)) as min_travel_time_minutes,
        max(timestampdiff(minute, start_timestamp, end_timestamp)) as max_travel_time_minutes,
        count(*) as total_trips
    from trips
    group by origin_city, destination_city
),

dim_routes as (
    select
        {{ dbt_utils.generate_surrogate_key(['rs.origin_city', 'rs.destination_city']) }} as route_key,
        rs.origin_city,
        rs.destination_city,
        rs.origin_city || ' → ' || rs.destination_city as route_name,
        
        -- Travel time metrics
        rs.avg_travel_time_minutes,
        rs.min_travel_time_minutes,
        rs.max_travel_time_minutes,
        round(rs.avg_travel_time_minutes / 60.0, 2) as avg_travel_time_hours,
        
        -- Route statistics
        rs.total_trips,
        
        -- Audit columns
        current_timestamp() as dbt_updated_at
    from route_stats rs
)

select * from dim_routes