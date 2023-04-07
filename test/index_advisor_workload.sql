select count(*)
from customer, orders
where c_custkey = o_custkey
	and o_orderdate >= date '1994-09-01'
	and o_orderdate < date '1994-09-01' + interval '1' month;

select count(*)
from
	customer,
	nation
where c_nationkey = n_nationkey
	and n_name = 'UNITED STATES';
select * from customer where c_name='Customer#000000001';
select * from customer where c_address='MG9kdTD2WBHm';
select * from customer where c_phone='25-989-741-2988';
select * from customer where c_address='MG9kdTD2WBHm' and c_phone='25-989-741-2988';
select * from customer where c_address='MG9kdTD2WBHm' and c_phone='25-989-741-2988' and c_name='Customer#000000001';
select * from customer where c_address='MG9kdTD2WBHm' and c_phone='25-989-741-2988' and c_name='Customer#000000001' and c_mktsegment = 'BUILDING';
select * from customer where c_phone='25-989-741-2988' and c_mktsegment = 'BUILDING';
select * from customer where c_address='MG9kdTD2WBHm' and c_phone='10-989-741-2988';
select count(*) from orders where o_orderstatus='O';
select count(*) from orders where o_orderpriority ='5-LOW';
select count(*) from orders where o_orderdate ='02-JAN-96 00:00:00';
select count(*) from orders where o_orderdate >'02-JAN-96 00:00:00';
select count(*) from orders where o_clerk ='Clerk#000000470';
select count(*) from orders where o_shippriority =1;
select count(*) from orders where o_shippriority =0;
select count(*) from orders where o_totalprice < 3210;
select count(*) from orders where o_totalprice < 32010;
select count(*) from orders where o_totalprice < 3210 and o_clerk ='Clerk#000000470';
select count(*) from orders where o_totalprice < 3210 and o_clerk ='Clerk#000000470' and o_orderdate >'02-JAN-96 00:00:00';
select count(*) from orders where o_totalprice < 3210 and o_orderdate >'02-JAN-96 00:00:00';
select count(*) from orders where o_totalprice < 3210 and o_orderdate >'02-JAN-96 00:00:00' and o_clerk ='Clerk#000000470';
select count(*) from orders where o_shippriority = 0 and o_orderdate >'02-JAN-96 00:00:00';
select count(*) from orders where o_shippriority = 0 and o_clerk ='Clerk#000000470';
select * from part where p_name = 'goldenrod lavender spring chocolate lace';
select count(*) from part where p_brand ='Brand#42';
select count(*) from part where p_mfgr='Manufacturer#1';
select count(*) from part where p_mfgr='Manufacturer#2';
select count(*) from part where p_mfgr='Manufacturer#2' and p_brand ='Brand#42';
select count(*) from part where p_mfgr='Manufacturer#2' and p_brand ='Brand#42';
select count(*) from part where p_type='PROMO BURNISHED COPPER';
select count(*) from part where p_size > 49;
select count(*) from part where p_container = 'MED DRUM';
select count(*) from part where p_retailprice  > 2050;
select count(*) from part where p_retailprice  > 2050 and p_container = 'MED DRUM';
select count(*) from part where p_retailprice  > 2050 and p_brand ='Brand#43';
select count(*) from part where p_retailprice  > 2050 and p_brand ='Brand#43' and p_mfgr='Manufacturer#2' and p_type='PROMO BURNISHED COPPER';
select * from lineitem where l_orderkey=5;
select count(*) from lineitem where l_partkey=2132;
select count(*) from lineitem where l_shipdate='13-MAR-96 00:00:00';
select count(*) from lineitem where l_suppkey=7706;
select count(*) from lineitem where l_linenumber = 7;
select count(*) from lineitem where l_extendedprice = 13309.60;
select count(*) from lineitem where l_quantity=17;
select count(*) from lineitem where l_extendedprice=22824.48;
select count(*) from lineitem where l_extendedprice=22824.48 and l_quantity>17;
select count(*) from lineitem where l_extendedprice=22824.48 and l_quantity>17 and l_linenumber > 6;
select count(*) from lineitem where l_suppkey=7706 and l_extendedprice=22824.48 and l_quantity>17 and l_linenumber > 6 and l_discount > 0.02;
select count(*) from lineitem where l_partkey=2132 and l_suppkey=7706 and l_extendedprice=22824.48 and l_quantity>17 and l_linenumber > 6;
select count(*) from lineitem where l_partkey=2132 and l_suppkey=7706 and l_extendedprice=22824.48 and l_shipdate='31-OCT-94 00:00:00';
select count(*) from lineitem where l_partkey=2132 and l_suppkey=7706 and l_extendedprice=22824.48 and l_commitdate='31-OCT-94 00:00:00';
select count(*) from lineitem where l_partkey=2132 and l_suppkey=7706 and l_extendedprice=22824.48 and l_commitdate='31-OCT-94 00:00:00' and l_receiptdate='20-NOV-94 00:00:00';























select count(*)
from customer, orders
where 
	c_custkey = o_custkey
	and o_orderdate >= date '1994-09-01'
	and o_orderdate < date '1994-09-01' + interval '1' month;


select
	c_custkey,
	c_name,
	sum(l_extendedprice * (1 - l_discount)) as revenue,
	c_acctbal,
	n_name,
	c_address,
	c_phone,
	c_comment
from
	customer,
	orders,
	lineitem,
	nation
where
	c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and o_orderdate >= date '1994-09-01'
	and o_orderdate < date '1994-09-01' + interval '3' month
	and l_returnflag = 'R'
	and c_nationkey = n_nationkey
group by
	c_custkey,
	c_name,
	c_acctbal,
	c_phone,
	n_name,
	c_address,
	c_comment
order by
	revenue desc
LIMIT 20;
-- using 1628502836 as a seed to the RNG


 select
	ps_partkey,
	sum(ps_supplycost * ps_availqty) as value
from
	partsupp,
	supplier,
	nation
where
	ps_suppkey = s_suppkey
	and s_nationkey = n_nationkey
	and n_name = 'UNITED STATES'
group by
	ps_partkey having
		sum(ps_supplycost * ps_availqty) > (
			select
				sum(ps_supplycost * ps_availqty) * 0.0001000000
			from
				partsupp,
				supplier,
				nation
			where
				ps_suppkey = s_suppkey
				and s_nationkey = n_nationkey
				and n_name = 'UNITED STATES'
		)
order by
	value desc
LIMIT 1;
-- using 1628502836 as a seed to the RNG


 select
	l_shipmode,
	sum(case
		when o_orderpriority = '1-URGENT'
			or o_orderpriority = '2-HIGH'
			then 1
		else 0
	end) as high_line_count,
	sum(case
		when o_orderpriority <> '1-URGENT'
			and o_orderpriority <> '2-HIGH'
			then 1
		else 0
	end) as low_line_count
from
	orders,
	lineitem
where
	o_orderkey = l_orderkey
	and l_shipmode in ('FOB', 'REG AIR')
	and l_commitdate < l_receiptdate
	and l_shipdate < l_commitdate
	and l_receiptdate >= date '1994-01-01'
	and l_receiptdate < date '1994-01-01' + interval '1' year
group by
	l_shipmode
order by
	l_shipmode
LIMIT 1;
-- using 1628502836 as a seed to the RNG


 select
	c_count,
	count(*) as custdist
from
	(
		select
			c_custkey,
			count(o_orderkey)
		from
			customer left outer join orders on
				c_custkey = o_custkey
				and o_comment not like '%special%requests%'
		group by
			c_custkey
	) as c_orders (c_custkey, c_count)
group by
	c_count
order by
	custdist desc,
	c_count desc
LIMIT 1;
-- using 1628502836 as a seed to the RNG


 select
	100.00 * sum(case
		when p_type like 'PROMO%'
			then l_extendedprice * (1 - l_discount)
		else 0
	end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
from
	lineitem,
	part
where
	l_partkey = p_partkey
	and l_shipdate >= date '1994-06-01'
	and l_shipdate < date '1994-06-01' + interval '1' month
LIMIT 1;
-- using 1628502836 as a seed to the RNG


 select
	p_brand,
	p_type,
	p_size,
	count(distinct ps_suppkey) as supplier_cnt
from
	partsupp,
	part
where
	p_partkey = ps_partkey
	and p_brand <> 'Brand#21'
	and p_type not like 'LARGE POLISHED%'
	and p_size in (19, 21, 39, 14, 22, 47, 36, 25)
	and ps_suppkey not in (
		select
			s_suppkey
		from
			supplier
		where
			s_comment like '%Customer%Complaints%'
	)
group by
	p_brand,
	p_type,
	p_size
order by
	supplier_cnt desc,
	p_brand,
	p_type,
	p_size
LIMIT 1;
-- using 1628502836 as a seed to the RNG


 select
	sum(l_extendedprice) / 7.0 as avg_yearly
from
	lineitem,
	part,
	(SELECT l_partkey AS agg_partkey, 0.2 * avg(l_quantity) AS avg_quantity FROM lineitem GROUP BY l_partkey) part_agg
where
	p_partkey = l_partkey
	and agg_partkey = l_partkey
	and p_brand = 'Brand#14'
	and p_container = 'LG CAN'
	and l_quantity < avg_quantity
LIMIT 1;
-- using 1628502836 as a seed to the RNG


 select
	c_name,
	c_custkey,
	o_orderkey,
	o_orderdate,
	o_totalprice,
	sum(l_quantity)
from
	customer,
	orders,
	lineitem
where
	o_orderkey in (
		select
			l_orderkey
		from
			lineitem
		group by
			l_orderkey having
				sum(l_quantity) > 314
	)
	and c_custkey = o_custkey
	and o_orderkey = l_orderkey
group by
	c_name,
	c_custkey,
	o_orderkey,
	o_orderdate,
	o_totalprice
order by
	o_totalprice desc,
	o_orderdate
LIMIT 100;
-- using 1628502836 as a seed to the RNG


 select
	sum(l_extendedprice* (1 - l_discount)) as revenue
from
	lineitem,
	part
where
	(
		p_partkey = l_partkey
		and p_brand = 'Brand#22'
		and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
		and l_quantity >= 7 and l_quantity <= 7 + 10
		and p_size between 1 and 5
		and l_shipmode in ('AIR', 'AIR REG')
		and l_shipinstruct = 'DELIVER IN PERSON'
	)
	or
	(
		p_partkey = l_partkey
		and p_brand = 'Brand#42'
		and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
		and l_quantity >= 14 and l_quantity <= 14 + 10
		and p_size between 1 and 10
		and l_shipmode in ('AIR', 'AIR REG')
		and l_shipinstruct = 'DELIVER IN PERSON'
	)
	or
	(
		p_partkey = l_partkey
		and p_brand = 'Brand#25'
		and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
		and l_quantity >= 26 and l_quantity <= 26 + 10
		and p_size between 1 and 15
		and l_shipmode in ('AIR', 'AIR REG')
		and l_shipinstruct = 'DELIVER IN PERSON'
	)
LIMIT 1;
-- using 1628502836 as a seed to the RNG


 select
	l_returnflag,
	l_linestatus,
	sum(l_quantity) as sum_qty,
	sum(l_extendedprice) as sum_base_price,
	sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
	sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
	avg(l_quantity) as avg_qty,
	avg(l_extendedprice) as avg_price,
	avg(l_discount) as avg_disc,
	count(*) as count_order
from
	lineitem
where
	l_shipdate <= date '1998-12-01' - interval '61' day
group by
	l_returnflag,
	l_linestatus
order by
	l_returnflag,
	l_linestatus
LIMIT 1;
-- using 1628502836 as a seed to the RNG


 select
	s_name,
	s_address
from
	supplier,
	nation
where
	s_suppkey in (
		select
			ps_suppkey
		from
			partsupp,
			(
				select
					l_partkey agg_partkey,
					l_suppkey agg_suppkey,
					0.5 * sum(l_quantity) AS agg_quantity
				from
					lineitem
				where
					l_shipdate >= date '1996-01-01'
					and l_shipdate < date '1996-01-01' + interval '1' year
				group by
					l_partkey,
					l_suppkey
			) agg_lineitem
		where
			agg_partkey = ps_partkey
			and agg_suppkey = ps_suppkey
			and ps_partkey in (
				select
					p_partkey
				from
					part
				where
					p_name like 'hot%'
			)
			and ps_availqty > agg_quantity
	)
	and s_nationkey = n_nationkey
	and n_name = 'ETHIOPIA'
order by
	s_name
LIMIT 1;
-- using 1628502836 as a seed to the RNG


 select
	s_name,
	count(*) as numwait
from
	supplier,
	lineitem l1,
	orders,
	nation
where
	s_suppkey = l1.l_suppkey
	and o_orderkey = l1.l_orderkey
	and o_orderstatus = 'F'
	and l1.l_receiptdate > l1.l_commitdate
	and exists (
		select
			*
		from
			lineitem l2
		where
			l2.l_orderkey = l1.l_orderkey
			and l2.l_suppkey <> l1.l_suppkey
	)
	and not exists (
		select
			*
		from
			lineitem l3
		where
			l3.l_orderkey = l1.l_orderkey
			and l3.l_suppkey <> l1.l_suppkey
			and l3.l_receiptdate > l3.l_commitdate
	)
	and s_nationkey = n_nationkey
	and n_name = 'ROMANIA'
group by
	s_name
order by
	numwait desc,
	s_name
LIMIT 100;
-- using 1628502836 as a seed to the RNG


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
			customer
		where
			substring(c_phone from 1 for 2) in
				('15', '20', '33', '27', '22', '13', '23')
			and c_acctbal > (
				select
					avg(c_acctbal)
				from
					customer
				where
					c_acctbal > 0.00
					and substring(c_phone from 1 for 2) in
						('15', '20', '33', '27', '22', '13', '23')
			)
			and not exists (
				select
					*
				from
					orders
				where
					o_custkey = c_custkey
			)
	) as custsale
group by
	cntrycode
order by
	cntrycode
LIMIT 1;
-- using 1628502836 as a seed to the RNG


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
	part,
	supplier,
	partsupp,
	nation,
	region
where
	p_partkey = ps_partkey
	and s_suppkey = ps_suppkey
	and p_size = 5
	and p_type like '%TIN'
	and s_nationkey = n_nationkey
	and n_regionkey = r_regionkey
	and r_name = 'AFRICA'
	and ps_supplycost = (
		select
			min(ps_supplycost)
		from
			partsupp,
			supplier,
			nation,
			region
		where
			p_partkey = ps_partkey
			and s_suppkey = ps_suppkey
			and s_nationkey = n_nationkey
			and n_regionkey = r_regionkey
			and r_name = 'AFRICA'
	)
order by
	s_acctbal desc,
	n_name,
	s_name,
	p_partkey
LIMIT 100;
-- using 1628502836 as a seed to the RNG


 select
	l_orderkey,
	sum(l_extendedprice * (1 - l_discount)) as revenue,
	o_orderdate,
	o_shippriority
from
	customer,
	orders,
	lineitem
where
	c_mktsegment = 'FURNITURE'
	and c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and o_orderdate < date '1995-03-02'
	and l_shipdate > date '1995-03-02'
group by
	l_orderkey,
	o_orderdate,
	o_shippriority
order by
	revenue desc,
	o_orderdate
LIMIT 10;
-- using 1628502836 as a seed to the RNG


 select
	o_orderpriority,
	count(*) as order_count
from
	orders
where
	o_orderdate >= date '1993-04-01'
	and o_orderdate < date '1993-04-01' + interval '3' month
	and exists (
		select
			*
		from
			lineitem
		where
			l_orderkey = o_orderkey
			and l_commitdate < l_receiptdate
	)
group by
	o_orderpriority
order by
	o_orderpriority
LIMIT 1;
-- using 1628502836 as a seed to the RNG


 select
	n_name,
	sum(l_extendedprice * (1 - l_discount)) as revenue
from
	customer,
	orders,
	lineitem,
	supplier,
	nation,
	region
where
	c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and l_suppkey = s_suppkey
	and c_nationkey = s_nationkey
	and s_nationkey = n_nationkey
	and n_regionkey = r_regionkey
	and r_name = 'MIDDLE EAST'
	and o_orderdate >= date '1997-01-01'
	and o_orderdate < date '1997-01-01' + interval '1' year
group by
	n_name
order by
	revenue desc
LIMIT 1;
-- using 1628502836 as a seed to the RNG


 select
	sum(l_extendedprice * l_discount) as revenue
from
	lineitem
where
	l_shipdate >= date '1997-01-01'
	and l_shipdate < date '1997-01-01' + interval '1' year
	and l_discount between 0.07 - 0.01 and 0.07 + 0.01
	and l_quantity < 24
LIMIT 1;
-- using 1628502836 as a seed to the RNG


 select
	supp_nation,
	cust_nation,
	l_year,
	sum(volume) as revenue
from
	(
		select
			n1.n_name as supp_nation,
			n2.n_name as cust_nation,
			extract(year from l_shipdate) as l_year,
			l_extendedprice * (1 - l_discount) as volume
		from
			supplier,
			lineitem,
			orders,
			customer,
			nation n1,
			nation n2
		where
			s_suppkey = l_suppkey
			and o_orderkey = l_orderkey
			and c_custkey = o_custkey
			and s_nationkey = n1.n_nationkey
			and c_nationkey = n2.n_nationkey
			and (
				(n1.n_name = 'PERU' and n2.n_name = 'INDONESIA')
				or (n1.n_name = 'INDONESIA' and n2.n_name = 'PERU')
			)
			and l_shipdate between date '1995-01-01' and date '1996-12-31'
	) as shipping
group by
	supp_nation,
	cust_nation,
	l_year
order by
	supp_nation,
	cust_nation,
	l_year
LIMIT 1;
-- using 1628502836 as a seed to the RNG


 select
	o_year,
	sum(case
		when nation = 'INDONESIA' then volume
		else 0
	end) / sum(volume) as mkt_share
from
	(
		select
			extract(year from o_orderdate) as o_year,
			l_extendedprice * (1 - l_discount) as volume,
			n2.n_name as nation
		from
			part,
			supplier,
			lineitem,
			orders,
			customer,
			nation n1,
			nation n2,
			region
		where
			p_partkey = l_partkey
			and s_suppkey = l_suppkey
			and l_orderkey = o_orderkey
			and o_custkey = c_custkey
			and c_nationkey = n1.n_nationkey
			and n1.n_regionkey = r_regionkey
			and r_name = 'ASIA'
			and s_nationkey = n2.n_nationkey
			and o_orderdate between date '1995-01-01' and date '1996-12-31'
			and p_type = 'ECONOMY BRUSHED COPPER'
	) as all_nations
group by
	o_year
order by
	o_year
LIMIT 1;
-- using 1628502836 as a seed to the RNG


 select
	nation,
	o_year,
	sum(amount) as sum_profit
from
	(
		select
			n_name as nation,
			extract(year from o_orderdate) as o_year,
			l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
		from
			part,
			supplier,
			lineitem,
			partsupp,
			orders,
			nation
		where
			s_suppkey = l_suppkey
			and ps_suppkey = l_suppkey
			and ps_partkey = l_partkey
			and p_partkey = l_partkey
			and o_orderkey = l_orderkey
			and s_nationkey = n_nationkey
			and p_name like '%seashell%'
	) as profit
group by
	nation,
	o_year
order by
	nation,
	o_year desc
LIMIT 1;

