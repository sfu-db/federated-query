select
	ps_partkey,
	sum(ps_supplycost * ps_availqty) as myvalue
from
	db1.partsupp,
	db1.supplier,
	db2.nation
where
	ps_suppkey = s_suppkey
	and s_nationkey = n_nationkey
	and n_name = 'INDONESIA'
group by
	ps_partkey having
		sum(ps_supplycost * ps_availqty) > (
			select
				sum(ps_supplycost * ps_availqty) * 0.00001000000
			from
				db1.partsupp,
				db1.supplier,
				db2.nation
			where
				ps_suppkey = s_suppkey
				and s_nationkey = n_nationkey
				and n_name = 'INDONESIA'
		)
order by
        myvalue desc
