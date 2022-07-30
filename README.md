# uncert_ranking_availability
## Real world queries
Find top 3 sizes of ice-bergs mostly observed.
~~~sql
SELECT size,count(*) AS ct FROM iceberg
GROUP BY size
ORDER BY ct DESC LIMIT 3;
~~~
For each day, find rolling sum of number of icebergs observed on that day and following 3 days.
~~~sql
SELECT date, sum(number) OVER (ORDER BY date
	BETWEEN CURRENT ROW AND 3 FOLLOWING) AS r_sum
FROM iceberg;
~~~
Find top three days with most incidents of crimes.
~~~sql
SELECT date, count(*) AS ct
FROM crimes GROUP BY date
ORDER BY ct DESC LIMIT 3;
~~~
For each crime in 2016, find the earliest year among the crime itself and nearest crime at north and south of it.
~~~sql
SELECT rid, min(year) OVER
    (ORDER BY latitude BETWEEN
		1 PRECEDING AND 1 FOLLOWING) AS min_year
FROM crimes WHERE year='2016';
~~~
Find top 5 facility with highest score on MRSA Bacteremia.
~~~sql
SELECT facility_id,facility_name,score FROM healthcare
WHERE measure_name = 'MRSA Bacteremia'
ORDER BY score LIMIT 5;
~~~
get in-line rank of facility on MRSA Bacteremia scores.
~~~sql
SELECT facility_id,facility_name,count(*) OVER
    (ORDER BY score DESC) AS rank
FROM healthcare
WHERE measure_name = 'MRSA Bacteremia';
~~~
