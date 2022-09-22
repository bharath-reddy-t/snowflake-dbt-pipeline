{{
    config(
        materialized = 'incremental',
        unique_key = 'ID'
    )
}}
 
with source_cte as (

    select  {{ dbt_utils.star(from=source('poc', 'raw_sf_opportunity') ,
          except=["CLOSEDATE",
                  "CREATEDDATE",
                  "LASTMODIFIEDDATE"]) }},
            try_cast(CLOSEDATE AS DATE) as CLOSEDATE,
            try_cast(CREATEDDATE as datetime) as CREATEDDATE,
            try_cast(LASTMODIFIEDDATE as datetime) as LASTMODIFIEDDATE
      from {{ source('poc', 'raw_sf_opportunity') }}
)
 
select *,
       current_timestamp() as dl_lastupdateddate,
       current_user() as dl_lastupdatedby
  from source_cte
 
{% if is_incremental() %}

 where LASTMODIFIEDDATE > (select max(LASTMODIFIEDDATE) from {{ this }}) 
    or  ID not in (select ID from {{ this }})
 
{% endif %}
 