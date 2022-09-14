with date_cte as (
    select DATEADD(DAY, SEQ4(), '2015-04-01') AS my_date
      from TABLE(GENERATOR(ROWCOUNT=>20000))
     where my_date <= '2022-03-31'
),

final as ( 
    select date(my_date) as date,
           row_number() over(order by date) as date_key,
           day(my_date) as day_number,
           month(my_date) as month_number,
           {{ dbt_date.month_name("my_date", short=false)}}  as month_name,
           year(my_date) as year,
<<<<<<< HEAD
           dayofweek(my_date) as day_ofweek,
           case 
             when day_ofweek = 0 then 'Sunday'
             when day_ofweek = 1 then 'Monday'
             when day_ofweek = 2 then 'Tuesday' 
             when day_ofweek = 3 then 'Wednesday'
             when day_ofweek = 4 then 'Thursday'
             when day_ofweek = 5 then 'Friday'
             when day_ofweek = 6 then 'Saturday'
           end as day_of_week,
=======
           {{ dbt_date.day_name("my_date", short=false) }} as day_of_week,
>>>>>>> 494361abb82fdef1b3836151423dc28f6e5c2b49
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
