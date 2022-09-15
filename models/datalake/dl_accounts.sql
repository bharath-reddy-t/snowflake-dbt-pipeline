{{
    config(
        materialized='incremental',
        unique_key='ID'
    )
}}

with source_cte as (
    select * 
      from {{ source('poc', 'landing_sf_account_v2') }}
)

select *,
       current_timestamp() as dl_last_update_date,
       current_user() as dl_last_updated_by
  from source_cte

{% if is_incremental() %}

    where LASTMODIFIEDDATE > (select max(LASTMODIFIEDDATE) from {{ this }}) 
       or ID not in (select ID from {{ this }})

{% endif %}