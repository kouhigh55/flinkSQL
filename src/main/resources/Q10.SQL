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
    and o_orderdate >= date '[DATE]'
    and o_orderdate < date '[DATE]' + interval '3' month
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
    revenue desc;


LOAD DATA LOCAL INFILE 'D:/myself/hkust bdt/lessons/5014B/TPC-H V3.0.1/dbgen/region.tbl'     INTO TABLE region     FIELDS TERMINATED BY '|';
LOAD DATA LOCAL INFILE 'D:/myself/hkust bdt/lessons/5014B/TPC-H V3.0.1/dbgen/nation.tbl'     INTO TABLE nation     FIELDS TERMINATED BY '|';
LOAD DATA LOCAL INFILE 'D:/myself/hkust bdt/lessons/5014B/TPC-H V3.0.1/dbgen/customer.tbl' INTO TABLE customer   FIELDS TERMINATED BY '|';
LOAD DATA LOCAL INFILE 'D:/myself/hkust bdt/lessons/5014B/TPC-H V3.0.1/dbgen/supplier.tbl' INTO TABLE supplier   FIELDS TERMINATED BY '|';
LOAD DATA LOCAL INFILE 'D:/myself/hkust bdt/lessons/5014B/TPC-H V3.0.1/dbgen/part.tbl'         INTO TABLE part       FIELDS TERMINATED BY '|';
LOAD DATA LOCAL INFILE 'D:/myself/hkust bdt/lessons/5014B/TPC-H V3.0.1/dbgen/partsupp.tbl' INTO TABLE partsupp   FIELDS TERMINATED BY '|';
LOAD DATA LOCAL INFILE 'D:/myself/hkust bdt/lessons/5014B/TPC-H V3.0.1/dbgen/orders.tbl'     INTO TABLE orders     FIELDS TERMINATED BY '|';
LOAD DATA LOCAL INFILE 'D:/myself/hkust bdt/lessons/5014B/TPC-H V3.0.1/dbgen/lineitem.tbl' INTO TABLE lineitem   FIELDS TERMINATED BY '|';