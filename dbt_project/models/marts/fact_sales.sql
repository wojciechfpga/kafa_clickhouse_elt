{{ config(materialized='incremental') }}

with sales as (
    select
        t.id as transaction_id,
        t.user_id,
        t.product_id,
        t.quantity,
        t.value as total_value,
        t.transaction_date,
        p.price as product_price,
        p.carat,
        p.cut,
        p.color,
        p.clarity,
        u.first_name as user_first_name,
        u.state_province as user_region,
        u.experience_level,
        t.value / t.quantity as value_per_unit,
        (p.price * t.quantity) - t.value as discount_amount
    from {{ ref('transactions_clean') }} t
    left join {{ ref('products_clean') }} p on t.product_id = p.id
    left join {{ ref('dim_users') }} u on t.user_id = u.user_id
    where t.transaction_date is not null
    {% if is_incremental() %}
      and t.transaction_date > (select max(transaction_date) from {{ this }})
    {% endif %}
)

select * from sales
