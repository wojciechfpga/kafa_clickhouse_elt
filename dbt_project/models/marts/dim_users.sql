{{ config(materialized='table', schema='marts') }}

with users as (
    select
        id as user_id,
        first_name,
        days_of_experience,
        total_purchase_amount,
        state_province,
        case
            when days_of_experience < 30 then 'New'
            when days_of_experience between 30 and 365 then 'Intermediate'
            else 'Experienced'
        end as experience_level,  
        case
            when total_purchase_amount < 1000 then 'Low Spender'
            when total_purchase_amount between 1000 and 10000 then 'Medium Spender'
            else 'High Spender'
        end as spend_category  
    from {{ ref('users_clean') }} 
)

select * from users