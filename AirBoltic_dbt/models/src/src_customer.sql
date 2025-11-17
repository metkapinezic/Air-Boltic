with raw_customer as (
    select * from {{ source('airboltic', 'CUSTOMER') }}
)
select
    Customer_ID,
    Name,
    Customer_Group_ID,
    Email,
    Phone_Number
from raw_customer