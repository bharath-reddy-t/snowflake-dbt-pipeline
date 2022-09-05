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
select *,
       current_user() as created 
       current_timestamp() as ingestion_timestamp
  from source_cte

{% if is_incremental() %}

    where LAST_UPDATE_DATE > (select max(LAST_UPDATE_DATE) from {{ this }}) 
       or PARTY_ID not in (select PARTY_ID from {{ this }})

{% endif %}