{{
    config(
        materialized = 'incremental',
        unique_key = 'ID'
       
    )
}}

with source_cte as (

    select {{ dbt_utils.star(from=source('poc', 'raw_sf_account'), 
              except=["CREATEDDATE","LASTMODIFIEDDATE","LastViewedDate"] ) 
           }},

           try_cast(CREATEDDATE as datetime) as CREATEDDATE,
           try_cast(LASTMODIFIEDDATE as datetime) as LASTMODIFIEDDATE,
           try_cast(LastViewedDate as datetime) as LastViewedDate
    from {{ source('poc', 'raw_sf_account') }}
),

cte as(
    select * ,
        ROW_NUMBER() OVER (PARTITION BY id order by id,LASTMODIFIEDDATE desc ) as rownumber,
        current_timestamp() as dl_last_update_date,
        current_user() as dl_last_updated_by 
    from source_cte 
 )
{% if not is_incremental()  %}

    select *  from cte  where rownumber =1

{% endif %}

{% if is_incremental() %}

    select *  from cte
    where 
       (LASTMODIFIEDDATE > (select max(LASTMODIFIEDDATE) from {{ this }}) 
       or ID not in (select ID from {{ this }}) ) and 
       rownumber =1

{% endif %}