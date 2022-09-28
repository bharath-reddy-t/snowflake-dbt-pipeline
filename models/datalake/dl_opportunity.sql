{{
    config(
        materialized = 'incremental',
        unique_key = 'ID'
    )
}}
 
with source_cte as (

    select  {{ dbt_utils.star(from=source('poc', 'temp_opportunity') ,
         except=["CLOSEDATE",
                  "CREATEDDATE",
                  "LASTMODIFIEDDATE"]) }},
         try_cast(CLOSEDATE AS DATE) as CLOSEDATE,
         try_cast(CREATEDDATE as datetime) as CREATEDDATE,
         try_cast(LASTMODIFIEDDATE as datetime) as LASTMODIFIEDDATE
    from {{ source('poc', 'temp_opportunity') }}
),

cte as(
    select * ,
        ROW_NUMBER() OVER (PARTITION BY id order by id,LASTMODIFIEDDATE desc ) as rownumber,
        current_timestamp() as dl_lastupdateddate,
        current_user() as dl_lastupdatedby 
  from source_cte 
 )

{% if not is_incremental()  %}

    select *  from cte  where rownumber =1

{% endif %}
 
{% if is_incremental() %}
    select *  from cte
        where  (LASTMODIFIEDDATE > (select max(LASTMODIFIEDDATE) from {{ this }}) 
        or ID not in (select ID from {{ this }}) ) and 
        rownumber =1
 
{% endif %}
 