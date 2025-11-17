with customers as (
    select * from {{ ref('src_customer') }}
),

customer_groups as (
    select * from {{ ref('src_customer_group') }}
),

dim_customers as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['c.customer_id']) }} as customer_key,
        
        -- Natural key
        c.customer_id,
        
        -- Customer attributes
        c.name as customer_name,
        c.email,
        c.phone_number,
        
        -- Customer group attributes (denormalized for performance)
        c.customer_group_id,
        cg.type as customer_group_type,
        cg.name as customer_group_name,
        cg.registry_number as customer_group_registry_number,
        
        -- Audit columns
        current_timestamp() as dbt_updated_at,
        current_timestamp() as dbt_valid_from,
        null as dbt_valid_to
    from customers c
    left join customer_groups cg on c.customer_group_id = cg.id
)

select * from dim_customers