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
current_timestamp() as ingestion_date,
current_timestamp() as updated_date,
       current_user() as updated_by 
from {{ref('stg_created_parties')}}

{% if is_incremental() %}
  where LAST_UPDATE_DATE > (select max(LAST_UPDATE_DATE) from {{ this }}) 
{% endif %}