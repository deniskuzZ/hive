-- A join on the keys of the GROUP BY that produced its build input: the join runs in the reducer of the GROUP BY on
-- its stream input (hive.convert.join.bucket.mapjoin.tez), and the build aggregate runs in a reducer of its own whose
-- rows are shuffled again into the join's partitions (q78's web_sales and catalog_sales aggregates, q97's catalog_sales
-- aggregate). With hive.optimize.join.build.aggregate.fusion the build aggregate's partials are partitioned into the
-- join's vertex and merged by its hash table loader instead. Each block compares the rows of the fused plan with the
-- unfused one and with the plain dynamically partitioned hash join (hive.convert.join.bucket.mapjoin.tez=false).
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.vectorized.execution.enabled=true;
set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=500000;
set hive.optimize.groupby.unsorted.shuffle=true;
set hive.vectorized.execution.mapjoin.native.fast.hashtable.enabled=true;
set hive.optimize.dynamic.partition.hashjoin=true;
set hive.tez.auto.reducer.parallelism=false;
set hive.exec.reducers.bytes.per.reducer=200000;
set hive.exec.reducers.max=8;

-- keys 1..2000 in the stream input, 1001..3000 and 501..2500 in the build inputs; each with NULL keys and duplicates
create table psb_s (s_k bigint, s_j int, s_v int);
create table psb_w (w_k bigint, w_j int, w_v int);
create table psb_c (c_k bigint, c_j int, c_v int);
insert into psb_s select pos + 1, pos % 2, pos % 7 from (select posexplode(split(space(1999), ' ')) as (pos, x)) t;
insert into psb_s select pos + 1, pos % 2, 1 from (select posexplode(split(space(499), ' ')) as (pos, x)) t;
insert into psb_s values (null, 0, 5), (null, 1, 6);
insert into psb_w select pos + 1001, (pos + 1000) % 2, pos % 5 from (select posexplode(split(space(1999), ' ')) as (pos, x)) t;
insert into psb_w select pos + 1001, (pos + 1000) % 2, 2 from (select posexplode(split(space(299), ' ')) as (pos, x)) t;
insert into psb_w values (null, 0, 9);
insert into psb_c select pos + 501, (pos + 500) % 2, pos % 11 from (select posexplode(split(space(1999), ' ')) as (pos, x)) t;
insert into psb_c values (null, 2, 4);

-- statistics that keep every aggregate, on one key or two, too large to broadcast but small enough per reducer: a
-- dynamically partitioned hash join over 2+ reducers
alter table psb_s update statistics set('numRows'='10000000', 'rawDataSize'='120000000');
alter table psb_s update statistics for column s_k set('numDVs'='40000', 'numNulls'='1', 'lowValue'='1', 'highValue'='40000');
alter table psb_s update statistics for column s_j set('numDVs'='2', 'numNulls'='0', 'lowValue'='0', 'highValue'='1');
alter table psb_s update statistics for column s_v set('numDVs'='1000', 'numNulls'='0', 'lowValue'='0', 'highValue'='1000');
alter table psb_w update statistics set('numRows'='10000000', 'rawDataSize'='120000000');
alter table psb_w update statistics for column w_k set('numDVs'='40000', 'numNulls'='1', 'lowValue'='1', 'highValue'='40000');
alter table psb_w update statistics for column w_j set('numDVs'='2', 'numNulls'='0', 'lowValue'='0', 'highValue'='1');
alter table psb_w update statistics for column w_v set('numDVs'='1000', 'numNulls'='0', 'lowValue'='0', 'highValue'='1000');
alter table psb_c update statistics set('numRows'='10000000', 'rawDataSize'='120000000');
alter table psb_c update statistics for column c_k set('numDVs'='40000', 'numNulls'='1', 'lowValue'='1', 'highValue'='40000');
alter table psb_c update statistics for column c_j set('numDVs'='2', 'numNulls'='0', 'lowValue'='0', 'highValue'='1');
alter table psb_c update statistics for column c_v set('numDVs'='1000', 'numNulls'='0', 'lowValue'='0', 'highValue'='1000');

-- LEFT OUTER on one key: unfused, the build aggregate is a reducer of its own; fused, it runs in the join's loader
set hive.convert.join.bucket.mapjoin.tez=true;
set hive.optimize.join.build.aggregate.fusion=false;
explain
select *
from (select s_k k, sum(s_v) s from psb_s group by s_k) s
left join (select w_k k, count(*) c, sum(w_v) v from psb_w group by w_k) w on s.k = w.k;

set hive.convert.join.bucket.mapjoin.tez=true;
set hive.optimize.join.build.aggregate.fusion=true;
explain
select *
from (select s_k k, sum(s_v) s from psb_s group by s_k) s
left join (select w_k k, count(*) c, sum(w_v) v from psb_w group by w_k) w on s.k = w.k;

select count(*), count(w.k), sum(s.s), sum(w.c), sum(w.v), sum(pmod(hash(s.k, w.k), 1000003) * pmod(hash(s.s, w.c, w.v), 1000033))
from (select s_k k, sum(s_v) s from psb_s group by s_k) s
left join (select w_k k, count(*) c, sum(w_v) v from psb_w group by w_k) w on s.k = w.k;

set hive.optimize.join.build.aggregate.fusion=false;
select count(*), count(w.k), sum(s.s), sum(w.c), sum(w.v), sum(pmod(hash(s.k, w.k), 1000003) * pmod(hash(s.s, w.c, w.v), 1000033))
from (select s_k k, sum(s_v) s from psb_s group by s_k) s
left join (select w_k k, count(*) c, sum(w_v) v from psb_w group by w_k) w on s.k = w.k;

set hive.convert.join.bucket.mapjoin.tez=false;
select count(*), count(w.k), sum(s.s), sum(w.c), sum(w.v), sum(pmod(hash(s.k, w.k), 1000003) * pmod(hash(s.s, w.c, w.v), 1000033))
from (select s_k k, sum(s_v) s from psb_s group by s_k) s
left join (select w_k k, count(*) c, sum(w_v) v from psb_w group by w_k) w on s.k = w.k;

-- q78's chain on two keys: both build aggregates run in the join's loader
set hive.convert.join.bucket.mapjoin.tez=true;
set hive.optimize.join.build.aggregate.fusion=true;
explain
select *
from (select s_k k, s_j j, sum(s_v) s from psb_s group by s_k, s_j) s
left join (select w_k k, w_j j, sum(w_v) v from psb_w group by w_k, w_j) w on s.k = w.k and s.j = w.j
left join (select c_k k, c_j j, sum(c_v) v from psb_c group by c_k, c_j) c on s.k = c.k and s.j = c.j;

select count(*), count(w.k), count(c.k), sum(s.s), sum(w.v), sum(c.v),
  sum(pmod(hash(s.k, s.j, w.k, c.k), 1000003) * pmod(hash(s.s, w.v, c.v), 1000033))
from (select s_k k, s_j j, sum(s_v) s from psb_s group by s_k, s_j) s
left join (select w_k k, w_j j, sum(w_v) v from psb_w group by w_k, w_j) w on s.k = w.k and s.j = w.j
left join (select c_k k, c_j j, sum(c_v) v from psb_c group by c_k, c_j) c on s.k = c.k and s.j = c.j;

set hive.optimize.join.build.aggregate.fusion=false;
select count(*), count(w.k), count(c.k), sum(s.s), sum(w.v), sum(c.v),
  sum(pmod(hash(s.k, s.j, w.k, c.k), 1000003) * pmod(hash(s.s, w.v, c.v), 1000033))
from (select s_k k, s_j j, sum(s_v) s from psb_s group by s_k, s_j) s
left join (select w_k k, w_j j, sum(w_v) v from psb_w group by w_k, w_j) w on s.k = w.k and s.j = w.j
left join (select c_k k, c_j j, sum(c_v) v from psb_c group by c_k, c_j) c on s.k = c.k and s.j = c.j;

set hive.convert.join.bucket.mapjoin.tez=false;
select count(*), count(w.k), count(c.k), sum(s.s), sum(w.v), sum(c.v),
  sum(pmod(hash(s.k, s.j, w.k, c.k), 1000003) * pmod(hash(s.s, w.v, c.v), 1000033))
from (select s_k k, s_j j, sum(s_v) s from psb_s group by s_k, s_j) s
left join (select w_k k, w_j j, sum(w_v) v from psb_w group by w_k, w_j) w on s.k = w.k and s.j = w.j
left join (select c_k k, c_j j, sum(c_v) v from psb_c group by c_k, c_j) c on s.k = c.k and s.j = c.j;

-- q97's FULL OUTER: the build aggregate runs in the join's loader
set hive.convert.join.bucket.mapjoin.tez=true;
set hive.optimize.join.build.aggregate.fusion=true;
explain
select *
from (select s_k k, s_j j from psb_s group by s_k, s_j) s
full outer join (select c_k k, c_j j from psb_c group by c_k, c_j) c on s.k = c.k and s.j = c.j;

select count(*), count(s.k), count(c.k), count(case when s.k is not null and c.k is not null then 1 end),
  sum(pmod(hash(s.k, s.j, c.k, c.j), 1000003))
from (select s_k k, s_j j from psb_s group by s_k, s_j) s
full outer join (select c_k k, c_j j from psb_c group by c_k, c_j) c on s.k = c.k and s.j = c.j;

set hive.optimize.join.build.aggregate.fusion=false;
select count(*), count(s.k), count(c.k), count(case when s.k is not null and c.k is not null then 1 end),
  sum(pmod(hash(s.k, s.j, c.k, c.j), 1000003))
from (select s_k k, s_j j from psb_s group by s_k, s_j) s
full outer join (select c_k k, c_j j from psb_c group by c_k, c_j) c on s.k = c.k and s.j = c.j;

set hive.convert.join.bucket.mapjoin.tez=false;
select count(*), count(s.k), count(c.k), count(case when s.k is not null and c.k is not null then 1 end),
  sum(pmod(hash(s.k, s.j, c.k, c.j), 1000003))
from (select s_k k, s_j j from psb_s group by s_k, s_j) s
full outer join (select c_k k, c_j j from psb_c group by c_k, c_j) c on s.k = c.k and s.j = c.j;

-- a filter and an expression between the build aggregate and the join (HAVING, a computed column) run in the loader too
set hive.convert.join.bucket.mapjoin.tez=true;
set hive.optimize.join.build.aggregate.fusion=true;
explain
select *
from (select s_k k, sum(s_v) s from psb_s group by s_k) s
join (select w_k k, sum(w_v) * 2 v2 from psb_w group by w_k having count(*) > 1) w on s.k = w.k;

select count(*), sum(s.s), sum(w.v2), sum(pmod(hash(s.k, s.s, w.v2), 1000003))
from (select s_k k, sum(s_v) s from psb_s group by s_k) s
join (select w_k k, sum(w_v) * 2 v2 from psb_w group by w_k having count(*) > 1) w on s.k = w.k;

set hive.optimize.join.build.aggregate.fusion=false;
select count(*), sum(s.s), sum(w.v2), sum(pmod(hash(s.k, s.s, w.v2), 1000003))
from (select s_k k, sum(s_v) s from psb_s group by s_k) s
join (select w_k k, sum(w_v) * 2 v2 from psb_w group by w_k having count(*) > 1) w on s.k = w.k;

set hive.convert.join.bucket.mapjoin.tez=false;
select count(*), sum(s.s), sum(w.v2), sum(pmod(hash(s.k, s.s, w.v2), 1000003))
from (select s_k k, sum(s_v) s from psb_s group by s_k) s
join (select w_k k, sum(w_v) * 2 v2 from psb_w group by w_k having count(*) > 1) w on s.k = w.k;

-- the build aggregate grouped on more keys than the join uses: its partials are not partitioned by the join keys alone,
-- so it is not fused (the stream aggregate estimated larger, so that the join runs in its reducer)
alter table psb_s update statistics for column s_k set('numDVs'='100000', 'numNulls'='1', 'lowValue'='1', 'highValue'='100000');
set hive.convert.join.bucket.mapjoin.tez=true;
set hive.optimize.join.build.aggregate.fusion=true;
explain
select *
from (select s_k k, sum(s_v) s from psb_s group by s_k) s
join (select w_k k, w_j j, sum(w_v) v from psb_w group by w_k, w_j) w on s.k = w.k;

select count(*), sum(s.s), sum(w.v), sum(pmod(hash(s.k, w.j, s.s, w.v), 1000003))
from (select s_k k, sum(s_v) s from psb_s group by s_k) s
join (select w_k k, w_j j, sum(w_v) v from psb_w group by w_k, w_j) w on s.k = w.k;

set hive.optimize.join.build.aggregate.fusion=false;
select count(*), sum(s.s), sum(w.v), sum(pmod(hash(s.k, w.j, s.s, w.v), 1000003))
from (select s_k k, sum(s_v) s from psb_s group by s_k) s
join (select w_k k, w_j j, sum(w_v) v from psb_w group by w_k, w_j) w on s.k = w.k;

set hive.convert.join.bucket.mapjoin.tez=false;
select count(*), sum(s.s), sum(w.v), sum(pmod(hash(s.k, w.j, s.s, w.v), 1000003))
from (select s_k k, sum(s_v) s from psb_s group by s_k) s
join (select w_k k, w_j j, sum(w_v) v from psb_w group by w_k, w_j) w on s.k = w.k;

alter table psb_s update statistics for column s_k set('numDVs'='40000', 'numNulls'='1', 'lowValue'='1', 'highValue'='40000');

-- the build aggregate's keys projected in another order than the join's: it groups by the join's order, (k, j), and is
-- fused
set hive.convert.join.bucket.mapjoin.tez=true;
set hive.optimize.join.build.aggregate.fusion=true;
explain
select *
from (select s_k k, cast(s_j as bigint) j, sum(s_v) s from psb_s group by s_k, cast(s_j as bigint)) s
join (select cast(w_j as bigint) j, w_k k, sum(w_v) v from psb_w group by cast(w_j as bigint), w_k) w on s.k = w.k and s.j = w.j;

select count(*), sum(s.s), sum(w.v), sum(pmod(hash(s.k, s.j, s.s, w.v), 1000003))
from (select s_k k, cast(s_j as bigint) j, sum(s_v) s from psb_s group by s_k, cast(s_j as bigint)) s
join (select cast(w_j as bigint) j, w_k k, sum(w_v) v from psb_w group by cast(w_j as bigint), w_k) w on s.k = w.k and s.j = w.j;

set hive.convert.join.bucket.mapjoin.tez=false;
select count(*), sum(s.s), sum(w.v), sum(pmod(hash(s.k, s.j, s.s, w.v), 1000003))
from (select s_k k, cast(s_j as bigint) j, sum(s_v) s from psb_s group by s_k, cast(s_j as bigint)) s
join (select cast(w_j as bigint) j, w_k k, sum(w_v) v from psb_w group by cast(w_j as bigint), w_k) w on s.k = w.k and s.j = w.j;

-- the builds' partials in columnar blocks, which the loader decodes
set hive.vectorized.execution.columnar.shuffle.enabled=true;
set hive.convert.join.bucket.mapjoin.tez=true;
set hive.optimize.join.build.aggregate.fusion=true;
select count(*), count(w.k), count(c.k), sum(s.s), sum(w.v), sum(c.v),
  sum(pmod(hash(s.k, s.j, w.k, c.k), 1000003) * pmod(hash(s.s, w.v, c.v), 1000033))
from (select s_k k, s_j j, sum(s_v) s from psb_s group by s_k, s_j) s
left join (select w_k k, w_j j, sum(w_v) v from psb_w group by w_k, w_j) w on s.k = w.k and s.j = w.j
left join (select c_k k, c_j j, sum(c_v) v from psb_c group by c_k, c_j) c on s.k = c.k and s.j = c.j;

set hive.convert.join.bucket.mapjoin.tez=false;
select count(*), count(w.k), count(c.k), sum(s.s), sum(w.v), sum(c.v),
  sum(pmod(hash(s.k, s.j, w.k, c.k), 1000003) * pmod(hash(s.s, w.v, c.v), 1000033))
from (select s_k k, s_j j, sum(s_v) s from psb_s group by s_k, s_j) s
left join (select w_k k, w_j j, sum(w_v) v from psb_w group by w_k, w_j) w on s.k = w.k and s.j = w.j
left join (select c_k k, c_j j, sum(c_v) v from psb_c group by c_k, c_j) c on s.k = c.k and s.j = c.j;
set hive.vectorized.execution.columnar.shuffle.enabled=false;

-- the build aggregate's hash table spills sorted runs and merges them at close
set hive.convert.join.bucket.mapjoin.tez=true;
set hive.optimize.join.build.aggregate.fusion=true;
set hive.map.aggr.hash.percentmemory=0.00001;
select count(*), count(w.k), sum(s.s), sum(w.c), sum(w.v), sum(pmod(hash(s.k, w.k), 1000003) * pmod(hash(s.s, w.c, w.v), 1000033))
from (select s_k k, sum(s_v) s from psb_s group by s_k) s
left join (select w_k k, count(*) c, sum(w_v) v from psb_w group by w_k) w on s.k = w.k;
set hive.map.aggr.hash.percentmemory=0.5;
