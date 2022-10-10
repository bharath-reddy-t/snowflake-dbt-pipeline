{{
    config(
        materialized = 'incremental',
        unique_key = 'ID'
       
    )
}}

with source_cte as (

    select {{ dbt_utils.star(from=source('test', 'test_accounts'), 
              except=["CREATEDDATE","LASTMODIFIEDDATE","LastViewedDate"]) 
           }},
           try_cast(CREATEDDATE as datetime) as CREATEDDATE,
           try_cast(LASTMODIFIEDDATE as datetime) as LASTMODIFIEDDATE,
           try_cast(LastViewedDate as datetime) as LastViewedDate
      from {{ source('test', 'test_accounts') }}
),

cte as(
    select *,
         --  ROW_NUMBER() OVER (PARTITION BY id order by id,LASTMODIFIEDDATE desc ) as rownumber,
           current_timestamp() as dl_last_update_date,
           current_user() as dl_last_updated_by 
      from source_cte 
 )
 
select distinct * from cte

{% if is_incremental() %}

  
     where (LASTMODIFIEDDATE > (select max(LASTMODIFIEDDATE) from {{ this }}) 
        or ID not in (select ID from {{ this }}) ) 
       

{% endif %}