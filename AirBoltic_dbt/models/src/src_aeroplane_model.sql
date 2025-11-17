with raw_aeroplane_model as (
    select * from {{ source('airboltic', 'AEROPLANE_MODEL') }}
)
select
    manufacturer,
    model,
    max_seats,
    max_weight,
    max_distance,
    engine_type
from raw_aeroplane_model