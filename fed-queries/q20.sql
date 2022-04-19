select
	s_name,
	s_address
from
	db1.supplier,
	db2.nation
where
	s_suppkey in (
		select
			ps_suppkey
		from
			db1.partsupp
		where
			ps_partkey in (
				select
					p_partkey
				from
					db1.part
				where
					p_name like 'thistle%'
			)
			and ps_availqty > (
				select
					0.5 * sum(l_quantity)
				from
					db1.lineitem
				where
					l_partkey = ps_partkey
					and l_suppkey = ps_suppkey
					and l_shipdate >= date '1997-01-01'
					and l_shipdate < date '1997-01-01' + interval '1' year
			)
	)
	and s_nationkey = n_nationkey
	and n_name = 'PERU'
order by
        s_name
