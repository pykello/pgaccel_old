SELECT count(*) FROM lineitem WHERE l_shipdate >= '1994-01-01' AND l_shipdate < '1995-01-01' AND l_discount >= 0.05 AND l_discount <= 0.07 AND l_quantity < 24;
SELECT l_shipdate, count(*), sum(l_quantity) FROM lineitem WHERE l_shipdate < '1992-01-05' GROUP BY l_shipdate;
SELECT l_shipdate, count(*), sum(l_quantity) FROM lineitem WHERE l_shipdate >= '1994-01-01' AND l_shipdate < '1995-01-01' GROUP BY l_shipdate;
SELECT l_shipdate, count(*), sum(l_quantity) FROM lineitem GROUP BY l_shipdate;
SELECT l_shipdate, count(*), sum(l_quantity) FROM lineitem WHERE l_shipdate > '2000-01-01' GROUP BY l_shipdate;
