

with source_cte as (
    select *
      from {{ source('poc', 'temp_parties') }}
)
select *,current_timestamp() as initiation_date,
       current_user() as initiation_by 
       from source_cte where initiation_date not in source_cte

-- select *,
--      current_timestamp() as ingestion_date,
--       current_timestamp() as updated_date,
--        current_user() as updated_by 
--       from source_cte where initiation_date in source_cte

-- -- {% if is_incremental() %}
--     where LAST_UPDATE_DATE > (select max(LAST_UPDATE_DATE) from {{ this }}) 
-- {% endif %}