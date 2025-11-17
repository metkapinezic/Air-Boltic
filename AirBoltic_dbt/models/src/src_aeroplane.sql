with raw_aeroplane as (
    select * from {{ source('airboltic', 'AEROPLANE') }}
)
select
    Airplane_ID,
    Airplane_Model,
    Manufacturer
from raw_aeroplane
