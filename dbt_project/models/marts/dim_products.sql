{{ config(materialized='table', schema='marts') }}

with products as (
    select
        id as product_id,
        carat,
        cut,
        color,
        clarity,
        depth,
        table_val as table_value,
        price,
        case
            when price < 1000 then 'Low'
            when price between 1000 and 5000 then 'Medium'
            else 'High'
        end as price_category  -- przykładowa segmentacja dla analityków
    from {{ ref('products_clean') }}  -- ref do twojego modelu intermediate
)

select * from products