select
	s_acctbal,
	s_name,
	n_name,
	p_partkey,
	p_mfgr,
	s_address,
	s_phone,
	s_comment
from
	db1.part,
	db1.supplier,
	db1.partsupp,
	db2.nation,
	db2.region
where
	p_partkey = ps_partkey
	and s_suppkey = ps_suppkey
	and p_size = 3
	and p_type like '%STEEL'
	and s_nationkey = n_nationkey
	and n_regionkey = r_regionkey
	and r_name = 'AMERICA'
	and ps_supplycost = (
		select
			min(ps_supplycost)
		from
			db1.partsupp,
			db1.supplier,
			db2.nation,
			db2.region
		where
			p_partkey = ps_partkey
			and s_suppkey = ps_suppkey
			and s_nationkey = n_nationkey
			and n_regionkey = r_regionkey
			and r_name = 'AMERICA'
	)
