# federated-query

Rewrite a federated query into a list of queries with dependencies, each either run in a single data source or run locally.

The following query:
```SQL
select
	l_orderkey,
	sum(l_extendedprice * (1 - l_discount)) as revenue,
	o_orderdate,
	o_shippriority
from
	db1.customer,
	db1.orders,
	db2.lineitem
where
	c_mktsegment = 'AUTOMOBILE'
	and c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and o_orderdate < date '1995-03-10'
	and l_shipdate > date '1995-03-10'
group by
	l_orderkey,
	o_orderdate,
	o_shippriority
order by
  revenue desc,
  o_orderdate
```

will be rewritten into three queries: `q1`, `q2` and `q_local`:

```SQL
-- q1 (issue to db1)
SELECT "t"."o_orderkey",
       "t"."o_orderdate",
       "t"."o_shippriority"
FROM   (SELECT *
        FROM   "orders"
        WHERE  "o_orderdate" < DATE '1995-03-10') AS "t"
       inner join (SELECT *
                   FROM   "customer"
                   WHERE  "c_mktsegment" = 'AUTOMOBILE') AS "t0"
               ON "t"."o_custkey" = "t0"."c_custkey"

-- q2 (issue to db2)
SELECT "l_orderkey",
       "l_extendedprice" * ( 1 - "l_discount" ) AS "$f3"
FROM   "lineitem"
WHERE  "l_shipdate" > DATE '1995-03-10'

-- q_local (run locally, join results from db1 and db2)
SELECT "db2"."l_orderkey"     AS "L_ORDERKEY",
       Sum("db2"."$f3")       AS "REVENUE",
       "db1"."o_orderdate"    AS "O_ORDERDATE",
       "db1"."o_shippriority" AS "O_SHIPPRIORITY"
FROM   "db1"
       INNER JOIN "db2"
               ON "db1"."o_orderkey" = "db2"."l_orderkey"
GROUP  BY "db1"."o_orderdate",
          "db1"."o_shippriority",
          "db2"."l_orderkey"
ORDER  BY Sum("db2"."$f3") DESC,
          "o_orderdate" 
```
