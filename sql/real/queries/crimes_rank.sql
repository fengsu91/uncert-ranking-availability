SELECT primary_type , count(*) AS ct FROM crimes GROUP BY primary_type ORDER BY ct DESC LIMIT 3;
SELECT date_, count(*) AS ct FROM crimes GROUP BY date_ ORDER BY ct DESC LIMIT 3;
