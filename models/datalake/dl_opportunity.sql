{{
    config(
        materialized = 'incremental',
        unique_key = 'ID'
    )
}}
 
with source_cte as (
<<<<<<< HEAD
    select  cast(CLOSEDATE AS DATE) as CLOSEDATE,
            cast(CREATEDDATE as datetime) as CREATEDDATE,
            cast(LASTMODIFIEDDATE as datetime) as LASTMODIFIEDDATE,
         {{ dbt_utils.star(from=source('poc', 'raw_sf_opportunity') ,
          except=["CLOSEDATE",
                  "CREATEDDATE",
                  "LASTMODIFIEDDATE"]) }}
=======
    select cast(CLOSEDATE AS DATE) as CLOSEDATE,
           cast(CREATEDDATE as datetime) as CREATEDDATE,
           cast(LASTMODIFIEDDATE as datetime) as LASTMODIFIEDDATE,
           {{ dbt_utils.star(from=source('poc', 'raw_sf_opportunity') , except=["CLOSEDATE","CREATEDDATE","LASTMODIFIEDDATE"]) }}
>>>>>>> 5779cfa34e78392fcd4cabc07c6fd83a580f2a01
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
 