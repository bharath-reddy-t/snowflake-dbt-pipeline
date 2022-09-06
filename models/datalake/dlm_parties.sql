{{
    config(
        materialized='incremental',
        unique_key='PARTY_ID'
    )
}}

with source_cte as (
    select *
      from {{ source('poc', 'temp_parties') }}
)
{% if  initiation_date not in source_cte %}
    select *,current_timestamp() as initiation_date,
       current_user() as initiation_by 
       from source_cte
{% else %}
   select *,
     current_timestamp() as ingestion_date,
      current_timestamp() as updated_date,
       current_user() as updated_by 
      from source_cte
{% endif %}


{% if is_incremental() %}
    where LAST_UPDATE_DATE > (select max(LAST_UPDATE_DATE) from {{ this }}) 
{% endif %}