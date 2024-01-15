SELECT rid, count(*) OVER (PARTITION BY block) AS ct FROM crimes WHERE year='2016';
