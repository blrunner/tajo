select l_orderkey, avg(l_discount) as revenue from lineitem group by l_orderkey order by l_orderkey;