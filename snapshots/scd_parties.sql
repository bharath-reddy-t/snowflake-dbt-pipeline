{% snapshot scd_parties %}

    {{
        config(
            target_schema = 'reporting_poc',
            unique_key = 'PARTY_ID',
            strategy = 'timestamp',
            updated_at = 'ingestion_timestamp',
            invalidate_hard_deletes = True
        )
    }}

    select * from {{ ref('dl_parties') }}

{% endsnapshot %}