create table iceburg_agg as (select sighting_date as date,count(*) as number from iceburg group by date);
SELECT date, sum(number) OVER (ORDER BY date ROWS BETWEEN CURRENT ROW AND 3 FOLLOWING) AS r_sum FROM iceburg_agg;
