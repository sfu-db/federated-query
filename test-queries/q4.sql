select
	o_orderpriority,
	count(*) as order_count
from
	db1.orders
where
	o_orderdate >= date '1994-07-01'
	and o_orderdate < date '1994-07-01' + interval '3' month
	and exists (
		select
			*
		from
			db2.lineitem
		where
			l_orderkey = o_orderkey
			and l_commitdate < l_receiptdate
	)
group by
	o_orderpriority
order by
	o_orderpriority
