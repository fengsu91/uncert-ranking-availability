# uncert_ranking_availability
## Real world queries
~~~sql
SELECT date, sum(number) OVER (ORDER BY date
    BETWEEN CURRENT ROW AND 3 FOLLOWING) AS r_sum
FROM iceberg;
~~~
