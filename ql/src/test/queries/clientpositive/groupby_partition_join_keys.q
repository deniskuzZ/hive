-- A GROUP BY whose output a join consumes on a subset of its keys is partitioned on that subset, by its values, so the
-- join runs in the GROUP BY's reducer instead of shuffling its output again (TPC-DS q4, q11, q74: year_total grouped by
-- the customer and its attributes, joined on the customer). Each query runs with the rule off and on; its row count and
-- the sum of its rows' hashes must be identical.
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.vectorized.execution.enabled=true;
set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=1000000;
set hive.optimize.dynamic.partition.hashjoin=true;
set hive.convert.join.bucket.mapjoin.tez=true;
set hive.tez.auto.reducer.parallelism=false;
set hive.exec.reducers.bytes.per.reducer=200000;
set hive.exec.reducers.max=4;

-- 2,000 keys, each in both years, two attribute values for some keys, a NULL key; f_g has 10 values, f_m 1,500
create table gp_f (f_k bigint, f_a int, f_g bigint, f_y int, f_v int, f_m bigint);
insert into gp_f select pos % 2000 + 1, if(pos % 7 = 0, 1, 0), pos % 10, (pos div 2000) % 2 + 1, pos % 13, pos % 1500
from (select posexplode(split(space(5999), ' ')) as (pos, x)) t;
insert into gp_f values (null, 0, 1, 1, 5, 7), (null, 0, 1, 2, 6, 7);
create table gp_d (d_k bigint, d_i int, d_name string, d_m bigint);
insert into gp_d select pos + 1, pos + 1, concat('d', pos % 9), pos % 1500 from (select posexplode(split(space(2999), ' ')) as (pos, x)) t;
-- gp_x: the (key, attribute) pairs of gp_f
create table gp_x (x_k bigint, x_a int, x_n string);
insert into gp_x select distinct f_k, f_a, concat('x', f_k % 7) from gp_f;

-- statistics that keep each aggregate too large to broadcast but small enough per reducer: a dynamically partitioned
-- hash join over 4 reducers; f_k's 30,000 distinct values are more than 128 per partition, f_g's 10 are not
alter table gp_f update statistics set('numRows'='10000000', 'rawDataSize'='200000000');
alter table gp_f update statistics for column f_k set('numDVs'='30000', 'numNulls'='1', 'lowValue'='1', 'highValue'='30000');
alter table gp_f update statistics for column f_a set('numDVs'='2', 'numNulls'='0', 'lowValue'='0', 'highValue'='1');
alter table gp_f update statistics for column f_g set('numDVs'='10', 'numNulls'='0', 'lowValue'='0', 'highValue'='9');
alter table gp_f update statistics for column f_y set('numDVs'='2', 'numNulls'='0', 'lowValue'='1', 'highValue'='2');
alter table gp_f update statistics for column f_v set('numDVs'='13', 'numNulls'='0', 'lowValue'='0', 'highValue'='12');
alter table gp_f update statistics for column f_m set('numDVs'='30000', 'numNulls'='0', 'lowValue'='0', 'highValue'='30000');
-- gp_d: too large to broadcast, small enough per partition of the aggregate's reducer
alter table gp_d update statistics set('numRows'='100000', 'rawDataSize'='2000000');
alter table gp_d update statistics for column d_k set('numDVs'='30000', 'numNulls'='0', 'lowValue'='1', 'highValue'='30000');
alter table gp_d update statistics for column d_i set('numDVs'='30000', 'numNulls'='0', 'lowValue'='1', 'highValue'='30000');
alter table gp_d update statistics for column d_m set('numDVs'='30000', 'numNulls'='0', 'lowValue'='0', 'highValue'='30000');
alter table gp_x update statistics set('numRows'='100000', 'rawDataSize'='2000000');
alter table gp_x update statistics for column x_k set('numDVs'='30000', 'numNulls'='1', 'lowValue'='1', 'highValue'='30000');
alter table gp_x update statistics for column x_a set('numDVs'='2', 'numNulls'='0', 'lowValue'='0', 'highValue'='1');

-- q4's shape: two aggregates by (key, attribute) joined on the key; the join runs in the first aggregate's reducer
set hive.optimize.groupby.partition.join.keys=true;
explain
select count(*), sum(hash(t1.k, t1.a, t1.s, t2.a, t2.s))
from (select f_k k, f_a a, sum(f_v) s from gp_f where f_y = 1 group by f_k, f_a) t1
join (select f_k k, f_a a, sum(f_v) s from gp_f where f_y = 2 group by f_k, f_a) t2 on t1.k = t2.k;

set hive.optimize.groupby.partition.join.keys=false;
select count(*), sum(hash(t1.k, t1.a, t1.s, t2.a, t2.s))
from (select f_k k, f_a a, sum(f_v) s from gp_f where f_y = 1 group by f_k, f_a) t1
join (select f_k k, f_a a, sum(f_v) s from gp_f where f_y = 2 group by f_k, f_a) t2 on t1.k = t2.k;
set hive.optimize.groupby.partition.join.keys=true;
select count(*), sum(hash(t1.k, t1.a, t1.s, t2.a, t2.s))
from (select f_k k, f_a a, sum(f_v) s from gp_f where f_y = 1 group by f_k, f_a) t1
join (select f_k k, f_a a, sum(f_v) s from gp_f where f_y = 2 group by f_k, f_a) t2 on t1.k = t2.k;

-- the same without vectorization: the row-mode ReduceSink hashes the values as the vectorized one does
set hive.vectorized.execution.enabled=false;
select count(*), sum(hash(t1.k, t1.a, t1.s, t2.a, t2.s))
from (select f_k k, f_a a, sum(f_v) s from gp_f where f_y = 1 group by f_k, f_a) t1
join (select f_k k, f_a a, sum(f_v) s from gp_f where f_y = 2 group by f_k, f_a) t2 on t1.k = t2.k;
set hive.optimize.groupby.partition.join.keys=false;
select count(*), sum(hash(t1.k, t1.a, t1.s, t2.a, t2.s))
from (select f_k k, f_a a, sum(f_v) s from gp_f where f_y = 1 group by f_k, f_a) t1
join (select f_k k, f_a a, sum(f_v) s from gp_f where f_y = 2 group by f_k, f_a) t2 on t1.k = t2.k;
set hive.vectorized.execution.enabled=true;

-- q4's chain: three aggregates by (key, attribute) joined on the key, each join comparing two of them; every join runs
-- in the first aggregate's reducer, which is partitioned on the key once
set hive.optimize.groupby.partition.join.keys=true;
explain
select count(*), sum(hash(t1.k, t1.s, t2.s, t3.s))
from (select f_k k, f_a a, sum(f_v) s from gp_f where f_y = 1 group by f_k, f_a) t1
join (select f_k k, f_a a, sum(f_v) s from gp_f where f_y = 2 group by f_k, f_a) t2 on t1.k = t2.k
join (select f_k k, f_a a, sum(f_m) s from gp_f where f_y = 1 group by f_k, f_a) t3 on t1.k = t3.k
where t2.s > t1.s and t3.s > t2.s;

set hive.optimize.groupby.partition.join.keys=false;
select count(*), sum(hash(t1.k, t1.s, t2.s, t3.s))
from (select f_k k, f_a a, sum(f_v) s from gp_f where f_y = 1 group by f_k, f_a) t1
join (select f_k k, f_a a, sum(f_v) s from gp_f where f_y = 2 group by f_k, f_a) t2 on t1.k = t2.k
join (select f_k k, f_a a, sum(f_m) s from gp_f where f_y = 1 group by f_k, f_a) t3 on t1.k = t3.k
where t2.s > t1.s and t3.s > t2.s;
set hive.optimize.groupby.partition.join.keys=true;
select count(*), sum(hash(t1.k, t1.s, t2.s, t3.s))
from (select f_k k, f_a a, sum(f_v) s from gp_f where f_y = 1 group by f_k, f_a) t1
join (select f_k k, f_a a, sum(f_v) s from gp_f where f_y = 2 group by f_k, f_a) t2 on t1.k = t2.k
join (select f_k k, f_a a, sum(f_m) s from gp_f where f_y = 1 group by f_k, f_a) t3 on t1.k = t3.k
where t2.s > t1.s and t3.s > t2.s;

-- an aggregate partitioned on all its keys for a join is not partitioned on a subset of them for the next join: the
-- first join's other input is in the partitions of the whole key
explain
select count(*), sum(hash(t.k, t.a, t.s, x_n, d_name))
from (select f_k k, f_a a, sum(f_v) s from gp_f group by f_k, f_a) t
join gp_x on t.k = x_k and t.a = x_a
join gp_d on t.k = d_k;

set hive.optimize.groupby.partition.join.keys=false;
select count(*), sum(hash(t.k, t.a, t.s, x_n, d_name))
from (select f_k k, f_a a, sum(f_v) s from gp_f group by f_k, f_a) t
join gp_x on t.k = x_k and t.a = x_a
join gp_d on t.k = d_k;
set hive.optimize.groupby.partition.join.keys=true;
select count(*), sum(hash(t.k, t.a, t.s, x_n, d_name))
from (select f_k k, f_a a, sum(f_v) s from gp_f group by f_k, f_a) t
join gp_x on t.k = x_k and t.a = x_a
join gp_d on t.k = d_k;

-- an aggregate joined with a scan, FULL OUTER: the scan is partitioned into the aggregate's reducer
set hive.optimize.groupby.partition.join.keys=true;
explain
select count(*), sum(hash(t.k, t.a, t.s, d_name))
from (select f_k k, f_a a, sum(f_v) s from gp_f group by f_k, f_a) t full outer join gp_d on t.k = d_k;

set hive.optimize.groupby.partition.join.keys=false;
select count(*), sum(hash(t.k, t.a, t.s, d_name))
from (select f_k k, f_a a, sum(f_v) s from gp_f group by f_k, f_a) t full outer join gp_d on t.k = d_k;
set hive.optimize.groupby.partition.join.keys=true;
select count(*), sum(hash(t.k, t.a, t.s, d_name))
from (select f_k k, f_a a, sum(f_v) s from gp_f group by f_k, f_a) t full outer join gp_d on t.k = d_k;

-- two join keys, a subset of three grouping keys, in another order: the aggregate is partitioned on (m, k), the scan
-- is partitioned into its reducer on (d_m, d_k)
set hive.auto.convert.join.noconditionaltask.size=1500000;
explain
select count(*), sum(hash(t.k, t.m, t.a, t.s, d_name))
from (select f_m m, f_k k, f_a a, sum(f_v) s from gp_f group by f_k, f_m, f_a) t
join gp_d on t.m = d_m and t.k = d_k;

set hive.optimize.groupby.partition.join.keys=false;
select count(*), sum(hash(t.k, t.m, t.a, t.s, d_name))
from (select f_m m, f_k k, f_a a, sum(f_v) s from gp_f group by f_k, f_m, f_a) t
join gp_d on t.m = d_m and t.k = d_k;
set hive.optimize.groupby.partition.join.keys=true;
select count(*), sum(hash(t.k, t.m, t.a, t.s, d_name))
from (select f_m m, f_k k, f_a a, sum(f_v) s from gp_f group by f_k, f_m, f_a) t
join gp_d on t.m = d_m and t.k = d_k;

-- partitioned on two of its keys for a join, the aggregate is not partitioned on one of them for the next join
explain
select count(*), sum(hash(t.k, t.m, t.a, t.s, d_name, x_n))
from (select f_m m, f_k k, f_a a, sum(f_v) s from gp_f group by f_k, f_m, f_a) t
join gp_d on t.m = d_m and t.k = d_k
join gp_x on t.k = x_k;

set hive.optimize.groupby.partition.join.keys=false;
select count(*), sum(hash(t.k, t.m, t.a, t.s, d_name, x_n))
from (select f_m m, f_k k, f_a a, sum(f_v) s from gp_f group by f_k, f_m, f_a) t
join gp_d on t.m = d_m and t.k = d_k
join gp_x on t.k = x_k;
set hive.optimize.groupby.partition.join.keys=true;
select count(*), sum(hash(t.k, t.m, t.a, t.s, d_name, x_n))
from (select f_m m, f_k k, f_a a, sum(f_v) s from gp_f group by f_k, f_m, f_a) t
join gp_d on t.m = d_m and t.k = d_k
join gp_x on t.k = x_k;
set hive.auto.convert.join.noconditionaltask.size=1000000;

-- not partitioned on the join key: f_g has 10 distinct values for 4 partitions
explain
select count(*), sum(hash(t.g, t.k, t.s, d_name))
from (select f_g g, f_k k, sum(f_v) s from gp_f group by f_g, f_k) t join gp_d on t.g = d_k;

-- the other input's int key is cast to the bigint the aggregate's key is: both hash bigints, partitioned on the key
explain
select count(*), sum(hash(t.k, t.a, t.s, d_name))
from (select f_k k, f_a a, sum(f_v) s from gp_f group by f_k, f_a) t join gp_d on t.k = d_i;

set hive.optimize.groupby.partition.join.keys=false;
select count(*), sum(hash(t.k, t.a, t.s, d_name))
from (select f_k k, f_a a, sum(f_v) s from gp_f group by f_k, f_a) t join gp_d on t.k = d_i;
set hive.optimize.groupby.partition.join.keys=true;
select count(*), sum(hash(t.k, t.a, t.s, d_name))
from (select f_k k, f_a a, sum(f_v) s from gp_f group by f_k, f_a) t join gp_d on t.k = d_i;
