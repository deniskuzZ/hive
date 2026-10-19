-- SORT_QUERY_RESULTS
-- MASK_STATS
-- Columnar shuffle (hive.vectorized.execution.columnar.shuffle.enabled): a vectorized ReduceSink of an unsorted edge
-- into the vectorized input of a reducer ships columnar blocks. Each query runs with the flag off and on; the results
-- must be identical. The big table of a dynamically partitioned hash join travels columnar while its small table
-- keeps the row path, so both must land in the same partitions. A GROUP BY reads an unsorted shuffle when it feeds a
-- ReduceSink, hence the ORDER BYs.

create table cs_fact (k1 int, k2 bigint, s string, v int, d decimal(7,2), dw decimal(25,4), x double, ts timestamp,
  b boolean, dt date);
create table cs_dim (k1 int, k2 bigint, w string);
insert into cs_fact select
  if(pos % 41 = 0, null, pos % 17), if(pos % 43 = 0, null, pos % 23 - 11), if(pos % 29 = 0, null, concat('s', pos % 31)),
  if(pos % 11 = 0, null, pos % 7 - 3), if(pos % 13 = 0, null, cast(pos % 50 * 1.25 - 20 as decimal(7,2))),
  cast(pos * 123456789.0001 - 2e11 as decimal(25,4)), if(pos % 19 = 0, null, pos % 13 * 0.5),
  if(pos % 31 = 0, null, cast(from_unixtime(1767225600 + pos * 3607) as timestamp)),
  if(pos % 37 = 0, null, pos % 3 = 0), date_add('2025-06-01', pos % 90)
from (select posexplode(split(space(3999), ' '))) t;
insert into cs_dim select if(pos % 47 = 0, null, pos % 17), pos % 23 - 11, concat('w', pos)
from (select posexplode(split(space(390), ' '))) t;

set hive.auto.convert.join=true;
set hive.optimize.dynamic.partition.hashjoin=true;
set hive.auto.convert.join.noconditionaltask.size=2000;
set hive.exec.reducers.bytes.per.reducer=20000;
set hive.tez.auto.reducer.parallelism=false;
set hive.stats.fetch.column.stats=false;
set hive.optimize.groupby.unsorted.shuffle=true;
set hive.optimize.reducededuplication=false;
set hive.vectorized.execution.enabled=true;

-- a dynamically partitioned hash join, then a GROUP BY over an unsorted shuffle
set hive.vectorized.execution.columnar.shuffle.enabled=true;
explain vectorization only operator
select f.k1, f.k2, count(*), sum(f.v), sum(f.d), sum(f.dw), max(f.s), min(f.ts), max(f.x), count(f.b), max(f.dt),
  max(m.w)
from cs_fact f join cs_dim m on f.k1 = m.k1 and f.k2 = m.k2
group by f.k1, f.k2
order by f.k1, f.k2;
set hive.vectorized.execution.columnar.shuffle.enabled=false;
select f.k1, f.k2, count(*), sum(f.v), sum(f.d), sum(f.dw), max(f.s), min(f.ts), max(f.x), count(f.b), max(f.dt),
  max(m.w)
from cs_fact f join cs_dim m on f.k1 = m.k1 and f.k2 = m.k2
group by f.k1, f.k2
order by f.k1, f.k2;
set hive.vectorized.execution.columnar.shuffle.enabled=true;
select f.k1, f.k2, count(*), sum(f.v), sum(f.d), sum(f.dw), max(f.s), min(f.ts), max(f.x), count(f.b), max(f.dt),
  max(m.w)
from cs_fact f join cs_dim m on f.k1 = m.k1 and f.k2 = m.k2
group by f.k1, f.k2
order by f.k1, f.k2;

-- the join's rows themselves, of every value type
select f.*, m.w
from cs_fact f join cs_dim m on f.k1 = m.k1 and f.k2 = m.k2
where f.k1 < 3;
set hive.vectorized.execution.columnar.shuffle.enabled=false;
select f.*, m.w
from cs_fact f join cs_dim m on f.k1 = m.k1 and f.k2 = m.k2
where f.k1 < 3;

-- keys of every fixed width (bigint, boolean, date, tinyint, smallint) with nulls
set hive.vectorized.execution.columnar.shuffle.enabled=true;
explain vectorization only operator
select k2, b, dt, cast(k1 as tinyint), cast(v as smallint), count(*), sum(d), sum(dw), max(s), min(x), max(ts)
from cs_fact group by k2, b, dt, cast(k1 as tinyint), cast(v as smallint)
order by k2, b, dt;
select k2, b, dt, cast(k1 as tinyint), cast(v as smallint), count(*), sum(d), sum(dw), max(s), min(x), max(ts)
from cs_fact group by k2, b, dt, cast(k1 as tinyint), cast(v as smallint)
order by k2, b, dt;
set hive.vectorized.execution.columnar.shuffle.enabled=false;
select k2, b, dt, cast(k1 as tinyint), cast(v as smallint), count(*), sum(d), sum(dw), max(s), min(x), max(ts)
from cs_fact group by k2, b, dt, cast(k1 as tinyint), cast(v as smallint)
order by k2, b, dt;

-- not columnar: a string key, and a reducer that does not vectorize
set hive.vectorized.execution.columnar.shuffle.enabled=true;
explain vectorization only operator
select s, count(*), sum(d) from cs_fact group by s order by s;
set hive.vectorized.execution.reduce.enabled=false;
explain vectorization only operator
select k2, count(*), sum(d) from cs_fact group by k2 order by k2;
select k2, count(*), sum(d) from cs_fact group by k2 order by k2;
set hive.vectorized.execution.reduce.enabled=true;
select k2, count(*), sum(d) from cs_fact group by k2 order by k2;

-- a ReduceSink of keys only (TPC-DS q97's GROUP BY of customer and item without aggregates)
set hive.vectorized.execution.columnar.shuffle.enabled=true;
explain vectorization only operator
select k1, k2 from cs_fact group by k1, k2 order by k1, k2;
select k1, k2 from cs_fact group by k1, k2 order by k1, k2;
set hive.vectorized.execution.columnar.shuffle.enabled=false;
select k1, k2 from cs_fact group by k1, k2 order by k1, k2;
