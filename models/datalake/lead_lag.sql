select id, deal_id,
        (case 
            -- when old_value is null and new_value is not null 
            --  and lag(deal_id) over(partition by deal_id order by created_date) is null
            --  and lead(deal_id) over(partition by deal_id order by created_date) is null
            -- then 'Cancelled'
            when old_value is null and deal_id = {{ next('deal_id') }} 
            then {{ next('old_value') }} 
            when old_value is null and {{ next('deal_id') }} is null and {{ prev('deal_id') }} is null
            then new_value 
            else old_value 
        end) as old_value,
        (case 
            when new_value is null and old_value is not null
             and lag(deal_id) over(partition by deal_id order by created_date) is null
             and lead(deal_id) over(partition by deal_id order by created_date) is null
            then null
            when new_value is null and deal_id = {{ prev('deal_id') }} 
            then {{ prev('old_value') }}  
            else new_value 
        end) as new_value,
        status,
        created_date,
        created_date as start_date,
        (case
            when deal_id != {{ prev('deal_id') }} then current_timestamp()
            else {{ next('created_date') }} 
        end) as end_date
  from {{source('poc2','demo')}}
 order 
    by created_date