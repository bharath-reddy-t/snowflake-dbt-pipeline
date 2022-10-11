select id, deal_id,
        (case 
            when ((select count(*) from {{ source('poc2','demo') }}) = 1
             and old_value is null and new_value is not null)
            then 'Cancelled'
            when old_value is null and deal_id = {{ next('deal_id') }} 
            Then {{ next('old_value') }} 
            when old_value is null and deal_id != {{ next('deal_id') }} and deal_id != {{ prev('deal_id') }}
            Then new_value 
            else old_value 
        end) as old_value,
        (case 
            when ((select count(*) from {{ source('poc2','demo') }}) = 1
             and new_value is null and old_value is not null)
            then null
            when new_value is null and deal_id = {{ prev('deal_id') }} 
            Then {{ prev('old_value') }}  
            else new_value 
        end) as new_value,
        status,
        created_date,
        created_date as start_date,
        (case
            when deal_id != {{ prev('deal_id') }} then current_timestamp()
            else {{ prev('created_date') }} 
        end) as end_date
  from {{source('poc2','demo')}}