with raw_trip as (
    select * from {{ source('airboltic', 'TRIP') }}
)
select
    Trip_ID,
    Origin_City,
    Destination_City,
    Airplane_ID,
    Start_Timestamp,
    End_Timestamp
from raw_trip