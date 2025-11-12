{{ config(materialized='table') }}

with no_null as (
    select *
    from {{ ref('stg_users') }}
    where id is not null
      and first_name is not null
      and days_of_experience is not null
      and total_purchase_amount is not null
      and state_province is not null
),
unreal_data as (
    select *
    from no_null
    where days_of_experience >= 0
      and total_purchase_amount >= 0
)
select *
from unreal_data