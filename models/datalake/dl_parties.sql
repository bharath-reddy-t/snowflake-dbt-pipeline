

-- with source_cte as (
--     select *
--       from {{ source('poc', 'landing_hz_parties_v2') }}
-- )

-- select *,
-- <<<<<<< HEAD
-- current_timestamp() as ingestion_date,
-- current_timestamp() as updated_date,
--        current_user() as updated_by 
-- from {{ref('stg_created_parties')}}

-- {% if is_incremental() %}
--   where LAST_UPDATE_DATE > (select max(LAST_UPDATE_DATE) from {{ this }}) 
-- =======
--        current_timestamp() as ingestion_timestamp,
--        current_user() as dl_last_updated_by
--   from source_cte

-- {% if is_incremental() %}

--     where LAST_UPDATE_DATE > (select max(LAST_UPDATE_DATE) from {{ this }}) 
--        or PARTY_ID not in (select PARTY_ID from {{ this }})

-- >>>>>>> ca2b8dc1d6455a74a3747b75e894a41bdf42705f
-- {% endif %}