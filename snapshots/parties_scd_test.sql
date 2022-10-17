{% snapshot parties_scd_test %}
    {{
        config(
            target_schema = 'dim',
            strategy = 'check',
            unique_key = 'PARTY_ID',
            check_cols = ["STATUS"],
            post_hook = scd_type1('PARTY_NAME', 'PARTY_TYPE', 'CUSTOMER_KEY', 'temp_parties')
        )
    }}

    select * from {{ source('poc2', 'temp_parties') }}
    
 {% endsnapshot %}