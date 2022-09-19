{{
    config(
        materialized='incremental',
        unique_key='ID'
    )
}}
 
with source_cte as (
    select  *
         
      from {{ source('poc_fact', 'test_SF_Oppurtunity') }}
)
 
select *,
       current_timestamp() as dl_last_update_date,
       current_user() as dl_last_updated_by
  from source_cte
 
{% if is_incremental() %}
 
    where  ID not in (select ID from {{ this }})
 
{% endif %}
 