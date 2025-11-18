with trips as (
    select * from {{ ref('src_trip') }}
),

orders as (
    select 
        trip_id,
        count(*) as seats_sold,
        sum(price_eur) as trip_revenue_eur,
        count(distinct customer_id) as unique_customers,
        sum(price_eur) / nullif(count(distinct customer_id), 0) as revenue_per_customer_eur
    from {{ ref('src_order') }}
    where status = 'Finished'  
    group by trip_id
),

aeroplanes as (
    select * from {{ ref('dim_aeroplanes') }}
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
        
    
        
        -- Aggregated order metrics
        coalesce(o.seats_sold, 0) as seats_sold,
        coalesce(o.trip_revenue_eur, 0) as trip_revenue_eur,
        coalesce(o.unique_customers, 0) as unique_customers,
        round(coalesce(o.revenue_per_customer_eur, 0), 2) as revenue_per_customer_eur,
        
        -- Capacity metrics
        a.max_seats as aircraft_capacity,
        round(coalesce(o.seats_sold, 0) / nullif(a.max_seats, 0), 4) as load_factor,
        
        -- Audit columns
        current_timestamp() as dbt_updated_at
    from trips t
    left join orders o on t.trip_id = o.trip_id
    left join aeroplanes a on t.airplane_id = a.airplane_id
)

select * from fct_trips