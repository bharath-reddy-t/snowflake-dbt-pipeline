{{
    config(
        materialized='incremental',
        unique_key='PARTY_ID',
        post_hook = "truncate table {{ source('poc', 'temp_parties') }}"
    )
}}

with source_cte as (
    select *
      from {{ source('poc', 'temp_parties') }}
)
select *,
       current_timestamp() as ingestion_timestamp
  from source_cte

{% if is_incremental() %}
    where LAST_UPDATE_DATE > (select max(LAST_UPDATE_DATE) from {{ this }}) 
{% endif %}