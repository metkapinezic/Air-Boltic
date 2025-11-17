with aeroplanes as (
    select * from {{ ref('src_aeroplane') }}
),

aeroplane_models as (
    select * from {{ ref('src_aeroplane_model') }}
),

dim_aeroplane as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['a.airplane_id']) }} as aeroplane_key,
        
        -- Natural key
        a.airplane_id,
        
        -- Aeroplane attributes
        a.manufacturer,
        a.airplane_model,
        
        -- Model specifications (denormalized)
        coalesce(am.max_seats, 0) as max_seats,
        am.max_weight,
        am.max_distance,
        am.engine_type,
        
        -- Audit columns
        current_timestamp() as dbt_updated_at
    from aeroplanes a
    left join aeroplane_models am 
        on a.airplane_model = am.model 
        and a.manufacturer = am.manufacturer
        where a.airplane_id is not null
)

select * from dim_aeroplane