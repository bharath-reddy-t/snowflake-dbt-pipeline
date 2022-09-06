{{
    config(
        materialized='incremental',
        unique_key='PARTY_ID'
    )
}}

with source_cte as (
    select *
      from {{ source('poc', 'landing_hz_parties_v2') }}
)
select *,
       current_timestamp() as ingestion_timestamp,
       current_user() as dl_last_updated_by
  from source_cte

{% if is_incremental() %}

    where LAST_UPDATE_DATE > (select max(LAST_UPDATE_DATE) from {{ this }}) 
       or PARTY_ID not in (select PARTY_ID from {{ this }})

{% endif %}