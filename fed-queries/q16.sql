select
	p_brand,
	p_type,
	p_size,
	count(distinct ps_suppkey) as supplier_cnt
from
	db1.partsupp,
	db1.part
where
	p_partkey = ps_partkey
	and p_brand <> 'Brand#12'
	and p_type not like 'MEDIUM BRUSHED%'
	and p_size in (26, 47, 48, 36, 38, 19, 37, 8)
	and ps_suppkey not in (
		select
			s_suppkey
		from
			db2.supplier
		where
			s_comment like '%Customer%Complaints%'
	)
group by
	p_brand,
	p_type,
	p_size
