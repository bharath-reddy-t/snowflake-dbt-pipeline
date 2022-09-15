with date_cte as (
    select DATEADD(DAY, SEQ4(), '2008-10-01') AS my_date
      from TABLE(GENERATOR(ROWCOUNT=>20000))
     where my_date <= '2022-09-14'
),

final as ( 
    select date(my_date) as date,
           row_number() over(order by date) as date_key,
           day(my_date) as day_number,
           month(my_date) as month_number,
           {{ dbt_date.month_name("my_date", short=false) }}  as month_name,
           year(my_date) as year,
           {{ dbt_date.day_name("my_date", short=false) }} as day_of_week,
           quarter(my_date) as quarter
      from date_cte
)

select date_key,
       date,
       day_number,
       month_number,
       month_name,
       year,
       day_of_week,
       quarter
  from final