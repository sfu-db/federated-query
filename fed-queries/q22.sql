select
	cntrycode,
	count(*) as numcust,
	sum(c_acctbal) as totacctbal
from
	(
		select
			substring(c_phone from 1 for 2) as cntrycode,
			c_acctbal
		from
			db1.customer
		where
			substring(c_phone from 1 for 2) in
				('27', '28', '19', '11', '14', '24', '15')
			and c_acctbal > (
				select
					avg(c_acctbal)
				from
					db1.customer
				where
					c_acctbal > 0.00
					and substring(c_phone from 1 for 2) in
						('27', '28', '19', '11', '14', '24', '15')
			)
			and not exists (
				select
					*
				from
					db2.orders
				where
					o_custkey = c_custkey
			)
	) as custsale
group by
	cntrycode
order by
        cntrycode
