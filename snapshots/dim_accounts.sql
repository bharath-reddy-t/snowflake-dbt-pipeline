{% snapshot dim_accounts %}

    {{
        config(
            target_schema = 'dim',
            unique_key = 'account_id',
            strategy = 'timestamp',
            updated_at = 'dl_last_update_date',
            invalidate_hard_deletes = True

        )
    }}

    select ID as account_id,
           hash(account_id) as Account_key,
           {{ dbt_utils.star(from=ref('dl_accounts') , 
              except=['ID','OWNERID','CREATEDBYID','LASTMODIFIEDBYID'] ) 
           }}
      from {{ ref('dl_accounts') }}

{% endsnapshot %}