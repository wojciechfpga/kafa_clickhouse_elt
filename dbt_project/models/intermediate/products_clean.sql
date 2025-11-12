{{ config(materialized='table') }}

with no_null as (
    select *
    from {{ ref('stg_products') }}
    where id is not null
      and carat is not null
      and cut is not null
      and color is not null
      and clarity is not null
      and depth is not null
      and table_val is not null
      and price is not null
),
unreal_data as (
    select *
    from no_null
    where carat > 0
      and price > 0
      and depth between 30 and 80   -- przykładowy fizyczny zakres
      and table_val between 40 and 100
)
select *
from unreal_data
