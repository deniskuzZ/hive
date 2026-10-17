-- A join whose input is already hash partitioned on its keys by the GROUP BY that produced it runs in that GROUP BY's
-- reducer: the input is not shuffled again, and the other inputs are partitioned to match (the same hash, the same
-- partition count). Each block compares the rows with the rule off (hive.convert.join.bucket.mapjoin.tez=false).
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.vectorized.execution.enabled=true;
set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=1000000;
set hive.optimize.dynamic.partition.hashjoin=true;
set hive.tez.auto.reducer.parallelism=false;
set hive.exec.reducers.bytes.per.reducer=200000;
set hive.exec.reducers.max=4;

-- keys 1..2000 on the left, 1001..3000 on the right, each with a NULL key and duplicate rows
create table ps_a (a_k bigint, a_v int);
create table ps_b (b_k bigint, b_v int);
insert into ps_a select pos + 1, pos % 7 from (select posexplode(split(space(1999), ' ')) as (pos, x)) t;
insert into ps_a select pos + 1, 1 from (select posexplode(split(space(499), ' ')) as (pos, x)) t;
insert into ps_a values (null, 5), (null, 6);
insert into ps_b select pos + 1001, pos % 5 from (select posexplode(split(space(1999), ' ')) as (pos, x)) t;
insert into ps_b values (null, 9);

create table ps_d (d_k bigint, d_name string);
insert into ps_d select pos + 1, concat('d', pos) from (select posexplode(split(space(99), ' ')) as (pos, x)) t;

-- statistics that keep each aggregate too large to broadcast but small enough per reducer: a dynamically partitioned
-- hash join over 2+ reducers
alter table ps_a update statistics set('numRows'='10000000', 'rawDataSize'='120000000');
alter table ps_a update statistics for column a_k set('numDVs'='30000', 'numNulls'='1', 'lowValue'='1', 'highValue'='30000');
alter table ps_a update statistics for column a_v set('numDVs'='1000', 'numNulls'='0', 'lowValue'='0', 'highValue'='1000');
alter table ps_b update statistics set('numRows'='10000000', 'rawDataSize'='120000000');
alter table ps_b update statistics for column b_k set('numDVs'='30000', 'numNulls'='1', 'lowValue'='1', 'highValue'='30000');
alter table ps_b update statistics for column b_v set('numDVs'='1000', 'numNulls'='0', 'lowValue'='0', 'highValue'='1000');

-- FULL OUTER: the join runs in the left aggregate's reducer; the right aggregate is partitioned into it
set hive.convert.join.bucket.mapjoin.tez=true;
explain
select *
from (select a_k k, sum(a_v) s from ps_a group by a_k) a
full outer join (select b_k k, count(*) c from ps_b group by b_k) b on a.k = b.k;

select count(*), count(a.k), count(b.k), count(case when a.k = b.k then 1 end), sum(a.s), sum(b.c),
  sum(pmod(hash(a.k, b.k), 1000003) * pmod(hash(a.s, b.c), 1000033))
from (select a_k k, sum(a_v) s from ps_a group by a_k) a
full outer join (select b_k k, count(*) c from ps_b group by b_k) b on a.k = b.k;

set hive.convert.join.bucket.mapjoin.tez=false;
select count(*), count(a.k), count(b.k), count(case when a.k = b.k then 1 end), sum(a.s), sum(b.c),
  sum(pmod(hash(a.k, b.k), 1000003) * pmod(hash(a.s, b.c), 1000033))
from (select a_k k, sum(a_v) s from ps_a group by a_k) a
full outer join (select b_k k, count(*) c from ps_b group by b_k) b on a.k = b.k;

-- a chain: the next join on the same key runs in that reducer too, its input partitioned the same way
set hive.convert.join.bucket.mapjoin.tez=true;
explain
select *
from (select a_k k, sum(a_v) s from ps_a group by a_k) a
full outer join (select b_k k, count(*) c from ps_b group by b_k) b on a.k = b.k
left join (select b_k k, max(b_v) m from ps_b where b_v > 0 group by b_k) x on a.k = x.k;

select count(*), count(a.k), count(b.k), count(case when a.k = b.k then 1 end), sum(a.s), sum(b.c),
  sum(pmod(hash(a.k, b.k), 1000003) * pmod(hash(a.s, b.c), 1000033)), count(x.k), sum(x.m)
from (select a_k k, sum(a_v) s from ps_a group by a_k) a
full outer join (select b_k k, count(*) c from ps_b group by b_k) b on a.k = b.k
left join (select b_k k, max(b_v) m from ps_b where b_v > 0 group by b_k) x on a.k = x.k;

set hive.convert.join.bucket.mapjoin.tez=false;
select count(*), count(a.k), count(b.k), count(case when a.k = b.k then 1 end), sum(a.s), sum(b.c),
  sum(pmod(hash(a.k, b.k), 1000003) * pmod(hash(a.s, b.c), 1000033)), count(x.k), sum(x.m)
from (select a_k k, sum(a_v) s from ps_a group by a_k) a
full outer join (select b_k k, count(*) c from ps_b group by b_k) b on a.k = b.k
left join (select b_k k, max(b_v) m from ps_b where b_v > 0 group by b_k) x on a.k = x.k;

-- an int key against a bigint key: the int side is cast in its ReduceSink, so only the bigint aggregate's partitioning
-- holds; the join runs in that aggregate's reducer and the cast side is partitioned into it
set hive.convert.join.bucket.mapjoin.tez=true;
explain
select *
from (select cast(a_k as int) k, sum(a_v) s from ps_a group by cast(a_k as int)) a
full outer join (select b_k k, count(*) c from ps_b group by b_k) b on a.k = b.k;

select count(*), count(a.k), count(b.k), count(case when a.k = b.k then 1 end), sum(a.s), sum(b.c),
  sum(pmod(hash(a.k, b.k), 1000003) * pmod(hash(a.s, b.c), 1000033))
from (select cast(a_k as int) k, sum(a_v) s from ps_a group by cast(a_k as int)) a
full outer join (select b_k k, count(*) c from ps_b group by b_k) b on a.k = b.k;

set hive.convert.join.bucket.mapjoin.tez=false;
select count(*), count(a.k), count(b.k), count(case when a.k = b.k then 1 end), sum(a.s), sum(b.c),
  sum(pmod(hash(a.k, b.k), 1000003) * pmod(hash(a.s, b.c), 1000033))
from (select cast(a_k as int) k, sum(a_v) s from ps_a group by cast(a_k as int)) a
full outer join (select b_k k, count(*) c from ps_b group by b_k) b on a.k = b.k;

-- keys that are expressions on both sides: no aggregate partitions on them, so the join shuffles
set hive.convert.join.bucket.mapjoin.tez=true;
explain
select *
from (select a_k k, sum(a_v) s from ps_a group by a_k) a
full outer join (select b_k k, count(*) c from ps_b group by b_k) b on a.k + 1 = b.k + 1;

select count(*), count(a.k), count(b.k), count(case when a.k = b.k then 1 end), sum(a.s), sum(b.c),
  sum(pmod(hash(a.k, b.k), 1000003) * pmod(hash(a.s, b.c), 1000033))
from (select a_k k, sum(a_v) s from ps_a group by a_k) a
full outer join (select b_k k, count(*) c from ps_b group by b_k) b on a.k + 1 = b.k + 1;

set hive.convert.join.bucket.mapjoin.tez=false;
select count(*), count(a.k), count(b.k), count(case when a.k = b.k then 1 end), sum(a.s), sum(b.c),
  sum(pmod(hash(a.k, b.k), 1000003) * pmod(hash(a.s, b.c), 1000033))
from (select a_k k, sum(a_v) s from ps_a group by a_k) a
full outer join (select b_k k, count(*) c from ps_b group by b_k) b on a.k + 1 = b.k + 1;

-- auto-reduce: the aggregate's reducer, which now holds the join, may merge partitions at run time (here down to 2 of 3);
-- each partition of the small input must fit at that parallelism, and Tez maps the partitioned input with the same
-- ranges as the reducer's other input, so the rows stay the same
set hive.tez.auto.reducer.parallelism=true;
set hive.tez.auto.reducer.parallelism.min.threshold=0.5;
set hive.tez.min.partition.factor=0.7;
set hive.convert.join.bucket.mapjoin.tez=true;
explain
select *
from (select a_k k, sum(a_v) s from ps_a group by a_k) a
full outer join (select b_k k, count(*) c from ps_b group by b_k) b on a.k = b.k;

select count(*), count(a.k), count(b.k), count(case when a.k = b.k then 1 end), sum(a.s), sum(b.c),
  sum(pmod(hash(a.k, b.k), 1000003) * pmod(hash(a.s, b.c), 1000033))
from (select a_k k, sum(a_v) s from ps_a group by a_k) a
full outer join (select b_k k, count(*) c from ps_b group by b_k) b on a.k = b.k;

set hive.convert.join.bucket.mapjoin.tez=false;
select count(*), count(a.k), count(b.k), count(case when a.k = b.k then 1 end), sum(a.s), sum(b.c),
  sum(pmod(hash(a.k, b.k), 1000003) * pmod(hash(a.s, b.c), 1000033))
from (select a_k k, sum(a_v) s from ps_a group by a_k) a
full outer join (select b_k k, count(*) c from ps_b group by b_k) b on a.k = b.k;

-- at the default minimum (a quarter of the partitions) a partition of the small input would not fit, but it fits at the
-- estimated partitions: the join runs in the aggregate's reducer, which runs with that many and no longer auto-reduces,
-- as the dynamically partitioned hash join it replaces
set hive.tez.min.partition.factor=0.25;
set hive.convert.join.bucket.mapjoin.tez=true;
explain
select *
from (select a_k k, sum(a_v) s from ps_a group by a_k) a
full outer join (select b_k k, count(*) c from ps_b group by b_k) b on a.k = b.k;

select count(*), count(a.k), count(b.k), count(case when a.k = b.k then 1 end), sum(a.s), sum(b.c),
  sum(pmod(hash(a.k, b.k), 1000003) * pmod(hash(a.s, b.c), 1000033))
from (select a_k k, sum(a_v) s from ps_a group by a_k) a
full outer join (select b_k k, count(*) c from ps_b group by b_k) b on a.k = b.k;

set hive.convert.join.bucket.mapjoin.tez=false;
select count(*), count(a.k), count(b.k), count(case when a.k = b.k then 1 end), sum(a.s), sum(b.c),
  sum(pmod(hash(a.k, b.k), 1000003) * pmod(hash(a.s, b.c), 1000033))
from (select a_k k, sum(a_v) s from ps_a group by a_k) a
full outer join (select b_k k, count(*) c from ps_b group by b_k) b on a.k = b.k;

-- a partition of the small input that would not fit even at the estimated partitions: it shuffles
set hive.auto.convert.join.noconditionaltask.size=250000;
set hive.convert.join.bucket.mapjoin.tez=true;
explain
select *
from (select a_k k, sum(a_v) s from ps_a group by a_k) a
full outer join (select b_k k, count(*) c from ps_b group by b_k) b on a.k = b.k;
set hive.auto.convert.join.noconditionaltask.size=1000000;

-- a join the aggregate's reducer can broadcast to keeps its broadcast: partitioning the small input would remove no shuffle
set hive.tez.auto.reducer.parallelism=false;
set hive.convert.join.bucket.mapjoin.tez=true;
explain
select a.k, a.s, d.d_name
from (select a_k k, sum(a_v) s from ps_a group by a_k) a
join ps_d d on a.k = d.d_k;

-- with dynamically partitioned hash joins off, the join stays a shuffled merge join
set hive.optimize.dynamic.partition.hashjoin=false;
explain
select *
from (select a_k k, sum(a_v) s from ps_a group by a_k) a
full outer join (select b_k k, count(*) c from ps_b group by b_k) b on a.k = b.k;
