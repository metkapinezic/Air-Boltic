with raw_customer_group as (
    select * from {{ source('airboltic', 'CUSTOMER_GROUP') }}
)
select
    ID,
    Type,
    Name,
    Registry_number
from raw_customer_group