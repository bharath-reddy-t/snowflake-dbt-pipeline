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

select *,current_timestamp() as initiation_date,
       current_user() as initiation_by 
       from source_cte 
{% if  ingest_date,ingest_by not in source_cte %}
    
{% endif %}