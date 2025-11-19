with fct_trips as (
    select * from {{ ref('fct_trips') }}
),

agg_daily_trip_metrics as (
    select
        -- Date dimension
        cast(date_trunc('day', fct.start_timestamp) as date) as metric_date,
        
        -- Route details
        fct.route_key,
        fct.origin_city,
        fct.destination_city,
        fct.route_name,
        
        -- Aggregated metrics
        count(*) as total_seats_sold,
        sum(fct.seat_price_eur) as total_trip_revenue_eur,
        count(distinct fct.customer_id) as unique_customers,
        round(sum(fct.seat_price_eur) / nullif(count(distinct fct.customer_id), 0), 2) as avg_revenue_per_customer_eur,
        count(distinct fct.trip_id) as num_trips,
        
        -- Capacity metrics
        max(fct.aircraft_capacity) as aircraft_capacity,
        round(count(*) / nullif(sum(fct.aircraft_capacity), 0), 4) as avg_load_factor,
        
        -- Audit columns
        current_timestamp() as dbt_updated_at
    from fct_trips fct
    group by
        cast(date_trunc('day', fct.start_timestamp) as date),
        fct.route_key,
        fct.origin_city,
        fct.destination_city,
        fct.route_name
)

select * from agg_daily_trip_metrics