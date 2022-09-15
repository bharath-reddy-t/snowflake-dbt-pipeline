{% snapshot dim_accounts %}

    {{
        config(
            target_schema = 'reporting_poc',
            unique_key = 'ID',
            strategy = 'timestamp',
            updated_at = 'dl_last_update_date',
            invalidate_hard_deletes = True
        )
    }}

    select * from {{ ref('dl_accounts') }}

{% endsnapshot %}