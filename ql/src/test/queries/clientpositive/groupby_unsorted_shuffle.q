-- SORT_QUERY_RESULTS
-- Reduce-side GROUP BY over an unsorted shuffle (hive.optimize.groupby.unsorted.shuffle). Each query runs with
-- the flag off, on, on without vectorization (sorted shuffle), and on with 7-row reducer batches and a tiny hash
-- table memory that spills sorted runs; the results must be identical.

create table gu_fact (k1 int, k2 int, s string, v int, d decimal(7,2), x double);
create table gu_fact2 (k1 int, k2 int);
insert into gu_fact select
  if(pos % 41 = 0, null, pos % 17), pos % 23, if(pos % 29 = 0, null, concat('s', pos % 31)),
  if(pos % 11 = 0, null, pos % 7), cast(pos % 50 * 1.25 as decimal(7,2)), pos % 13 * 0.5
from (select posexplode(split(space(3999), ' '))) t;
insert into gu_fact2 select if(pos % 37 = 0, null, pos % 19), pos % 29
from (select posexplode(split(space(2999), ' '))) t;

set hive.auto.convert.join=false;

-- two_level
set hive.optimize.reducededuplication=false;
set hive.optimize.groupby.unsorted.shuffle=true;
explain
select k2, sum(c), sum(sv), max(mx), min(mn), sum(cv)
from (select k1, k2, count(*) c, sum(v) sv, max(d) mx, min(s) mn, count(v) cv
      from gu_fact group by k1, k2) t
group by k2;
-- a reducer that does not vectorize reads a sorted shuffle (only the vectorized GROUP BY spills)
set hive.vectorized.execution.enabled=false;
explain
select k2, sum(c), sum(sv), max(mx), min(mn), sum(cv)
from (select k1, k2, count(*) c, sum(v) sv, max(d) mx, min(s) mn, count(v) cv
      from gu_fact group by k1, k2) t
group by k2;
set hive.vectorized.execution.enabled=true;
set hive.optimize.groupby.unsorted.shuffle=false;
select k2, sum(c), sum(sv), max(mx), min(mn), sum(cv)
from (select k1, k2, count(*) c, sum(v) sv, max(d) mx, min(s) mn, count(v) cv
      from gu_fact group by k1, k2) t
group by k2;
set hive.optimize.groupby.unsorted.shuffle=true;
select k2, sum(c), sum(sv), max(mx), min(mn), sum(cv)
from (select k1, k2, count(*) c, sum(v) sv, max(d) mx, min(s) mn, count(v) cv
      from gu_fact group by k1, k2) t
group by k2;
set hive.vectorized.execution.enabled=false;
select k2, sum(c), sum(sv), max(mx), min(mn), sum(cv)
from (select k1, k2, count(*) c, sum(v) sv, max(d) mx, min(s) mn, count(v) cv
      from gu_fact group by k1, k2) t
group by k2;
set hive.vectorized.execution.enabled=true;
set hive.vectorized.testing.reducer.batch.size=7;
set hive.map.aggr.hash.percentmemory=0.00001;
select k2, sum(c), sum(sv), max(mx), min(mn), sum(cv)
from (select k1, k2, count(*) c, sum(v) sv, max(d) mx, min(s) mn, count(v) cv
      from gu_fact group by k1, k2) t
group by k2;
set hive.vectorized.testing.reducer.batch.size=-1;
set hive.map.aggr.hash.percentmemory=0.5;

-- avg_stddev_two_level
set hive.optimize.reducededuplication=false;
set hive.optimize.groupby.unsorted.shuffle=true;
explain
select k2, count(*), round(sum(a), 10), round(sum(sd), 10), sum(ad)
from (select k1, k2, avg(x) a, stddev_samp(x) sd, avg(d) ad from gu_fact group by k1, k2) t
group by k2;
set hive.optimize.groupby.unsorted.shuffle=false;
select k2, count(*), round(sum(a), 10), round(sum(sd), 10), sum(ad)
from (select k1, k2, avg(x) a, stddev_samp(x) sd, avg(d) ad from gu_fact group by k1, k2) t
group by k2;
set hive.optimize.groupby.unsorted.shuffle=true;
select k2, count(*), round(sum(a), 10), round(sum(sd), 10), sum(ad)
from (select k1, k2, avg(x) a, stddev_samp(x) sd, avg(d) ad from gu_fact group by k1, k2) t
group by k2;
set hive.vectorized.execution.enabled=false;
select k2, count(*), round(sum(a), 10), round(sum(sd), 10), sum(ad)
from (select k1, k2, avg(x) a, stddev_samp(x) sd, avg(d) ad from gu_fact group by k1, k2) t
group by k2;
set hive.vectorized.execution.enabled=true;
set hive.vectorized.testing.reducer.batch.size=7;
set hive.map.aggr.hash.percentmemory=0.00001;
select k2, count(*), round(sum(a), 10), round(sum(sd), 10), sum(ad)
from (select k1, k2, avg(x) a, stddev_samp(x) sd, avg(d) ad from gu_fact group by k1, k2) t
group by k2;
set hive.vectorized.testing.reducer.batch.size=-1;
set hive.map.aggr.hash.percentmemory=0.5;

set hive.optimize.reducededuplication=true;
-- string_key
set hive.optimize.groupby.unsorted.shuffle=true;
explain
select count(*), count(s), sum(c)
from (select s, count(*) c from gu_fact group by s) t;
set hive.optimize.groupby.unsorted.shuffle=false;
select count(*), count(s), sum(c)
from (select s, count(*) c from gu_fact group by s) t;
set hive.optimize.groupby.unsorted.shuffle=true;
select count(*), count(s), sum(c)
from (select s, count(*) c from gu_fact group by s) t;
set hive.vectorized.execution.enabled=false;
select count(*), count(s), sum(c)
from (select s, count(*) c from gu_fact group by s) t;
set hive.vectorized.execution.enabled=true;
set hive.vectorized.testing.reducer.batch.size=7;
set hive.map.aggr.hash.percentmemory=0.00001;
select count(*), count(s), sum(c)
from (select s, count(*) c from gu_fact group by s) t;
set hive.vectorized.testing.reducer.batch.size=-1;
set hive.map.aggr.hash.percentmemory=0.5;

-- q97_shape
set hive.optimize.groupby.unsorted.shuffle=true;
explain
with a as (select k1, k2 from gu_fact group by k1, k2),
     b as (select k1, k2 from gu_fact2 group by k1, k2)
select sum(case when a.k1 is not null and b.k1 is null then 1 else 0 end),
       sum(case when a.k1 is null and b.k1 is not null then 1 else 0 end),
       sum(case when a.k1 is not null and b.k1 is not null then 1 else 0 end)
from a full outer join b on (a.k1 = b.k1 and a.k2 = b.k2);
set hive.optimize.groupby.unsorted.shuffle=false;
with a as (select k1, k2 from gu_fact group by k1, k2),
     b as (select k1, k2 from gu_fact2 group by k1, k2)
select sum(case when a.k1 is not null and b.k1 is null then 1 else 0 end),
       sum(case when a.k1 is null and b.k1 is not null then 1 else 0 end),
       sum(case when a.k1 is not null and b.k1 is not null then 1 else 0 end)
from a full outer join b on (a.k1 = b.k1 and a.k2 = b.k2);
set hive.optimize.groupby.unsorted.shuffle=true;
with a as (select k1, k2 from gu_fact group by k1, k2),
     b as (select k1, k2 from gu_fact2 group by k1, k2)
select sum(case when a.k1 is not null and b.k1 is null then 1 else 0 end),
       sum(case when a.k1 is null and b.k1 is not null then 1 else 0 end),
       sum(case when a.k1 is not null and b.k1 is not null then 1 else 0 end)
from a full outer join b on (a.k1 = b.k1 and a.k2 = b.k2);
set hive.vectorized.execution.enabled=false;
with a as (select k1, k2 from gu_fact group by k1, k2),
     b as (select k1, k2 from gu_fact2 group by k1, k2)
select sum(case when a.k1 is not null and b.k1 is null then 1 else 0 end),
       sum(case when a.k1 is null and b.k1 is not null then 1 else 0 end),
       sum(case when a.k1 is not null and b.k1 is not null then 1 else 0 end)
from a full outer join b on (a.k1 = b.k1 and a.k2 = b.k2);
set hive.vectorized.execution.enabled=true;
set hive.vectorized.testing.reducer.batch.size=7;
set hive.map.aggr.hash.percentmemory=0.00001;
with a as (select k1, k2 from gu_fact group by k1, k2),
     b as (select k1, k2 from gu_fact2 group by k1, k2)
select sum(case when a.k1 is not null and b.k1 is null then 1 else 0 end),
       sum(case when a.k1 is null and b.k1 is not null then 1 else 0 end),
       sum(case when a.k1 is not null and b.k1 is not null then 1 else 0 end)
from a full outer join b on (a.k1 = b.k1 and a.k2 = b.k2);
set hive.vectorized.testing.reducer.batch.size=-1;
set hive.map.aggr.hash.percentmemory=0.5;

-- union_distinct
set hive.optimize.groupby.unsorted.shuffle=true;
explain
select count(*), sum(k1), sum(k2)
from (select k1, k2 from gu_fact union select k1, k2 from gu_fact2) t;
set hive.optimize.groupby.unsorted.shuffle=false;
select count(*), sum(k1), sum(k2)
from (select k1, k2 from gu_fact union select k1, k2 from gu_fact2) t;
set hive.optimize.groupby.unsorted.shuffle=true;
select count(*), sum(k1), sum(k2)
from (select k1, k2 from gu_fact union select k1, k2 from gu_fact2) t;
set hive.vectorized.execution.enabled=false;
select count(*), sum(k1), sum(k2)
from (select k1, k2 from gu_fact union select k1, k2 from gu_fact2) t;
set hive.vectorized.execution.enabled=true;
set hive.vectorized.testing.reducer.batch.size=7;
set hive.map.aggr.hash.percentmemory=0.00001;
select count(*), sum(k1), sum(k2)
from (select k1, k2 from gu_fact union select k1, k2 from gu_fact2) t;
set hive.vectorized.testing.reducer.batch.size=-1;
set hive.map.aggr.hash.percentmemory=0.5;

-- union_distinct_group_by: the UNION's dedup is partitioned on the GROUP BY's keys, which runs COMPLETE in its reducer;
-- over the unsorted dedup output it is split into a hash aggregation and a merge of its partial results (TPC-DS q75).
-- Without vectorization it is whole again (its explain shows the COMPLETE GROUP BY over a sorted shuffle)
set hive.optimize.groupby.unsorted.shuffle=true;
explain
select count(*), sum(pmod(hash(k1, k2, sv, cv, sd, md, mv), 1000003))
from (select k1, k2, sum(v) sv, count(v) cv, sum(d) sd, max(d) md, min(v) mv
      from (select k1, k2, x, d, v from gu_fact
            union select k1, k2, k2 * 0.5, cast(k2 as decimal(7,2)), if(k2 % 3 = 0, null, k2) from gu_fact2) t
      group by k1, k2) g;
set hive.optimize.groupby.unsorted.shuffle=false;
select count(*), sum(pmod(hash(k1, k2, sv, cv, sd, md, mv), 1000003))
from (select k1, k2, sum(v) sv, count(v) cv, sum(d) sd, max(d) md, min(v) mv
      from (select k1, k2, x, d, v from gu_fact
            union select k1, k2, k2 * 0.5, cast(k2 as decimal(7,2)), if(k2 % 3 = 0, null, k2) from gu_fact2) t
      group by k1, k2) g;
set hive.optimize.groupby.unsorted.shuffle=true;
select count(*), sum(pmod(hash(k1, k2, sv, cv, sd, md, mv), 1000003))
from (select k1, k2, sum(v) sv, count(v) cv, sum(d) sd, max(d) md, min(v) mv
      from (select k1, k2, x, d, v from gu_fact
            union select k1, k2, k2 * 0.5, cast(k2 as decimal(7,2)), if(k2 % 3 = 0, null, k2) from gu_fact2) t
      group by k1, k2) g;
set hive.vectorized.execution.enabled=false;
explain
select count(*), sum(pmod(hash(k1, k2, sv, cv, sd, md, mv), 1000003))
from (select k1, k2, sum(v) sv, count(v) cv, sum(d) sd, max(d) md, min(v) mv
      from (select k1, k2, x, d, v from gu_fact
            union select k1, k2, k2 * 0.5, cast(k2 as decimal(7,2)), if(k2 % 3 = 0, null, k2) from gu_fact2) t
      group by k1, k2) g;
select count(*), sum(pmod(hash(k1, k2, sv, cv, sd, md, mv), 1000003))
from (select k1, k2, sum(v) sv, count(v) cv, sum(d) sd, max(d) md, min(v) mv
      from (select k1, k2, x, d, v from gu_fact
            union select k1, k2, k2 * 0.5, cast(k2 as decimal(7,2)), if(k2 % 3 = 0, null, k2) from gu_fact2) t
      group by k1, k2) g;
set hive.map.aggr.hash.percentmemory=0.5;
set hive.vectorized.execution.enabled=true;
set hive.vectorized.testing.reducer.batch.size=7;
set hive.map.aggr.hash.percentmemory=0.00001;
select count(*), sum(pmod(hash(k1, k2, sv, cv, sd, md, mv), 1000003))
from (select k1, k2, sum(v) sv, count(v) cv, sum(d) sd, max(d) md, min(v) mv
      from (select k1, k2, x, d, v from gu_fact
            union select k1, k2, k2 * 0.5, cast(k2 as decimal(7,2)), if(k2 % 3 = 0, null, k2) from gu_fact2) t
      group by k1, k2) g;
set hive.vectorized.testing.reducer.batch.size=-1;
set hive.map.aggr.hash.percentmemory=0.5;

-- intersect: each branch's DISTINCT and the INTERSECT's count on the same keys (behind a select that reorders them)
-- run in one reducer; the count gets one row a group and follows the unsorted merge as is (TPC-DS q38, q87)
set hive.optimize.groupby.unsorted.shuffle=true;
explain
select count(*) from (select distinct k2, k1 from gu_fact intersect select distinct k2, k1 from gu_fact2) t;
set hive.optimize.groupby.unsorted.shuffle=false;
select count(*) from (select distinct k2, k1 from gu_fact intersect select distinct k2, k1 from gu_fact2) t;
set hive.optimize.groupby.unsorted.shuffle=true;
select count(*) from (select distinct k2, k1 from gu_fact intersect select distinct k2, k1 from gu_fact2) t;
set hive.vectorized.execution.enabled=false;
select count(*) from (select distinct k2, k1 from gu_fact intersect select distinct k2, k1 from gu_fact2) t;
set hive.vectorized.execution.enabled=true;
set hive.vectorized.testing.reducer.batch.size=7;
set hive.map.aggr.hash.percentmemory=0.00001;
select count(*) from (select distinct k2, k1 from gu_fact intersect select distinct k2, k1 from gu_fact2) t;
set hive.vectorized.testing.reducer.batch.size=-1;
set hive.map.aggr.hash.percentmemory=0.5;

-- union_distinct_corr: a keyless aggregation after the dedup keeps its plan (corr's partial result is not its final
-- one, and it does not vectorize)
set hive.optimize.groupby.unsorted.shuffle=true;
explain
select round(corr(k1, k2), 10) from (select k1, k2 from gu_fact union select k1, k2 from gu_fact2) t;
set hive.optimize.groupby.unsorted.shuffle=false;
select round(corr(k1, k2), 10) from (select k1, k2 from gu_fact union select k1, k2 from gu_fact2) t;
set hive.optimize.groupby.unsorted.shuffle=true;
select round(corr(k1, k2), 10) from (select k1, k2 from gu_fact union select k1, k2 from gu_fact2) t;
set hive.vectorized.execution.enabled=false;
select round(corr(k1, k2), 10) from (select k1, k2 from gu_fact union select k1, k2 from gu_fact2) t;
set hive.vectorized.execution.enabled=true;
set hive.vectorized.testing.reducer.batch.size=7;
set hive.map.aggr.hash.percentmemory=0.00001;
select round(corr(k1, k2), 10) from (select k1, k2 from gu_fact union select k1, k2 from gu_fact2) t;
set hive.vectorized.testing.reducer.batch.size=-1;
set hive.map.aggr.hash.percentmemory=0.5;

-- topn_after
set hive.optimize.groupby.unsorted.shuffle=true;
explain
select k1, k2, c
from (select k1, k2, count(*) c from gu_fact group by k1, k2) t
order by c desc, k1, k2
limit 5;
set hive.optimize.groupby.unsorted.shuffle=false;
select k1, k2, c
from (select k1, k2, count(*) c from gu_fact group by k1, k2) t
order by c desc, k1, k2
limit 5;
set hive.optimize.groupby.unsorted.shuffle=true;
select k1, k2, c
from (select k1, k2, count(*) c from gu_fact group by k1, k2) t
order by c desc, k1, k2
limit 5;
set hive.vectorized.execution.enabled=false;
select k1, k2, c
from (select k1, k2, count(*) c from gu_fact group by k1, k2) t
order by c desc, k1, k2
limit 5;
set hive.vectorized.execution.enabled=true;
set hive.vectorized.testing.reducer.batch.size=7;
set hive.map.aggr.hash.percentmemory=0.00001;
select k1, k2, c
from (select k1, k2, count(*) c from gu_fact group by k1, k2) t
order by c desc, k1, k2
limit 5;
set hive.vectorized.testing.reducer.batch.size=-1;
set hive.map.aggr.hash.percentmemory=0.5;

-- window_over_group_by: the window reads the same shuffle sorted by its keys, so the GROUP BY is not marked
set hive.optimize.groupby.unsorted.shuffle=true;
explain
select k1, k2, c, sum(c) over (partition by k1 order by k2) running
from (select k1, k2, count(*) c from gu_fact group by k1, k2) t;
set hive.optimize.groupby.unsorted.shuffle=false;
select k1, k2, c, sum(c) over (partition by k1 order by k2) running
from (select k1, k2, count(*) c from gu_fact group by k1, k2) t;
set hive.optimize.groupby.unsorted.shuffle=true;
select k1, k2, c, sum(c) over (partition by k1 order by k2) running
from (select k1, k2, count(*) c from gu_fact group by k1, k2) t;
set hive.vectorized.execution.enabled=false;
select k1, k2, c, sum(c) over (partition by k1 order by k2) running
from (select k1, k2, count(*) c from gu_fact group by k1, k2) t;
set hive.vectorized.execution.enabled=true;
set hive.vectorized.testing.reducer.batch.size=7;
set hive.map.aggr.hash.percentmemory=0.00001;
select k1, k2, c, sum(c) over (partition by k1 order by k2) running
from (select k1, k2, count(*) c from gu_fact group by k1, k2) t;
set hive.vectorized.testing.reducer.batch.size=-1;
set hive.map.aggr.hash.percentmemory=0.5;

-- auto_reduce: Tez merges the partitions of an unsorted edge into fewer tasks, from one vertex or a union
set hive.optimize.groupby.unsorted.shuffle=true;
set hive.tez.auto.reducer.parallelism=true;
set hive.exec.reducers.bytes.per.reducer=2000;
set hive.optimize.reducededuplication=false;
select k2, sum(c), sum(sv), max(mx), min(mn), sum(cv)
from (select k1, k2, count(*) c, sum(v) sv, max(d) mx, min(s) mn, count(v) cv
      from gu_fact group by k1, k2) t
group by k2;
select count(*), sum(k1), sum(k2)
from (select k1, k2 from gu_fact union select k1, k2 from gu_fact2) t;
set hive.optimize.reducededuplication=true;
set hive.exec.reducers.bytes.per.reducer=256000000;
set hive.tez.auto.reducer.parallelism=false;

-- a map join in the GROUP BY's reducer: without vectorization only the GROUP BY's shuffle is sorted again; the
-- broadcast input of the map join keeps its edge
create table gu_dim (k int, name string);
insert into gu_dim select pos, concat('n', pos % 5) from (select posexplode(split(space(16), ' '))) t;
set hive.auto.convert.join=true;
set hive.optimize.groupby.unsorted.shuffle=true;
set hive.vectorized.execution.enabled=false;
explain
select d.name, sum(t.c) from (select k1, count(*) c from gu_fact group by k1) t join gu_dim d on t.k1 = d.k
group by d.name;
select d.name, sum(t.c) from (select k1, count(*) c from gu_fact group by k1) t join gu_dim d on t.k1 = d.k
group by d.name;
set hive.optimize.groupby.unsorted.shuffle=false;
select d.name, sum(t.c) from (select k1, count(*) c from gu_fact group by k1) t join gu_dim d on t.k1 = d.k
group by d.name;
set hive.vectorized.execution.enabled=true;
set hive.auto.convert.join=false;
