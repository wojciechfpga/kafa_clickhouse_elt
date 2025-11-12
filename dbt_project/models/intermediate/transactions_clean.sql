{{ config(materialized='table') }}

with no_null as (
    select *
    from {{ ref('stg_transactions') }}
    where id is not null
      and user_id is not null
      and product_id is not null
      and quantity is not null
      and value is not null
      and transaction_date is not null
),
unreal_data as (
    select *
    from no_null
    where value > 0
      and quantity > 0
)
select *
from unreal_data
