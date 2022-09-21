{% snapshot fact_test %} 
    {{
        config(
            target_schema = 'fact',
            unique_key = 'ID',
            strategy = 'timestamp',
            updated_at = 'do_last_update_date',
            invalidate_hard_deletes = True
        )
    }}

with accounts as(
    select id as acc_id from {{ ref('dim_accounts') }}
),
date as(
    select date_key,date from {{ ref('dim_date') }}
),
opp as(
    select * from {{ ref('temp_opp') }}
),
fact_test as(
    select
      date_key,
      id,
      acc_id,
      amount,
      do_last_update_date
       from opp o
       left join date d on d.date=o.closedate and d.date=o.createddate
       left join accounts a on a.acc_id=o.accountid 
)
select * from fact_test
{% endsnapshot %}