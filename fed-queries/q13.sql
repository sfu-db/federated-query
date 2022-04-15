select
	c_count,
	count(*) as custdist
from
	(
		select
			c_custkey,
			count(o_orderkey)
		from
			db1.customer left outer join db2.orders on
				c_custkey = o_custkey
				and o_comment not like '%unusual%packages%'
		group by
			c_custkey
	) as c_orders (c_custkey, c_count)
group by
	c_count
