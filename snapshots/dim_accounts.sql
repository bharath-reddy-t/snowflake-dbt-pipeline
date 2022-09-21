{% snapshot dim_accounts %}

    {{
        config(
            target_schema = 'dim',
            unique_key = 'ID',
            strategy = 'timestamp',
            updated_at = 'dl_last_update_date',
<<<<<<< HEAD
            invalidate_hard_deletes = True,
             transient = False
=======
            invalidate_hard_deletes = True
>>>>>>> 1c25cdd83197ebe52200fd3c3cd97446cd209659
        )
    }}

    select *,
           row_number() over(order by ID) as Account_key
      from {{ ref('dl_accounts') }}

{% endsnapshot %}