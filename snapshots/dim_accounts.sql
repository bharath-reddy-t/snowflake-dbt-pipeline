{% snapshot dim_accounts %}

    {{
        config(
            target_schema = 'dim',
            unique_key = 'ID',
            strategy = 'timestamp',
            updated_at = 'dl_last_update_date',
            invalidate_hard_deletes = True,
            transient = False
        )
    }}

    select *,
           row_number() over(order by ID) as surrogate_key
      from {{ ref('dl_accounts') }}

{% endsnapshot %}