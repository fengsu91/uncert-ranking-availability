select size, count(*) as ct from iceburg group by size order by ct desc limit 3;
