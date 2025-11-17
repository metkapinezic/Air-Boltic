with raw_order as (
    select * from {{ source('airboltic', 'orders') }}
)
select
    Order_ID,
    Customer_ID,
    Trip_ID,
    Price_EUR,
    Seat_No,
    Status
from raw_order