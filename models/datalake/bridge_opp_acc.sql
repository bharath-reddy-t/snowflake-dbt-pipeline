{{
    config(
        materialized = 'incremental',
        unique_key = 'opportunity_id'
    )
}}

with cte as (
    select ID as opportunity_id,
           ACCOUNTID as ACCOUNT_ID,
           current_timestamp() as date_loaded 
      from {{ ref('dl_opportunity') }}
)

select * from cte

{% if is_incremental() %}

where date_loaded > (select max(date_loaded) from {{ this }}) 
   or opportunity_id not in (select opportunity_id from {{ this }})
   
{% endif %}