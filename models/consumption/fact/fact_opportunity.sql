
with accounts as(

        select 
            id as acc_id,
            Account_key 
            from {{ ref('dim_accounts') }} where DBT_VALID_TO IS NULL
),
date as(
        select 
            date_key,
            date 
         from {{ ref('dim_date') }}
),
opp as(
       select *
         from {{ ref('dl_opportunity') }}
),
fact_opportunity as(
    select
            id,
            a.Account_key as Account_key,
            d.date_key as closed_key,
            cd.date_key as created_key,
            amount,
            expectedrevenue,
            Probability,
            DEAL_STATE_ID__C,
            CB_ORACLE_ACCOUNT_NUMBER__C,
            MIGRATION_ID__C,
            QC_TOTAL__C,
            dl_lastupdateddate,
            dl_lastupdatedby
       from opp o
       left join date d on d.date=o.closedate 
       left join date cd on cd.date=o.createddate
       left join accounts a on a.acc_id=o.accountid 
)

select 
     *
  from fact_opportunity
