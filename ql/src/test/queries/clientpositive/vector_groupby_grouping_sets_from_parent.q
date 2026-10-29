-- SORT_QUERY_RESULTS
-- A vectorized hash aggregation of grouping sets aggregates the input rows only in the sets no other set contains, and
-- every other set from the partial results of its parent, the set with the fewest keys that contains it
-- (hive.vectorized.groupby.grouping.sets.from.parent). Each query runs with it off and on; the results must be
-- identical.
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.vectorized.execution.enabled=true;
set hive.vectorized.execution.reduce.enabled=true;
set hive.fetch.task.conversion=none;

create table gsp (a string, b int, c string, d decimal(10,2), e double) stored as orc;
insert into gsp select
  concat('a', pos % 7), if(pos % 11 = 0, null, pos % 5), if(pos % 13 = 0, null, concat('c', pos % 17)),
  cast(pos % 101 * 1.25 as decimal(10,2)), if(pos % 9 = 0, null, pos % 23 * 0.5)
from (select posexplode(split(space(2999), ' '))) t;

-- ROLLUP, each level from the one above it, with every kind of aggregation
set hive.vectorized.groupby.grouping.sets.from.parent=false;
select a, b, c, count(*), count(c), sum(b), sum(d), min(c), max(d), min(e), avg(d), avg(e), stddev_samp(e),
  grouping__id
from gsp group by rollup(a, b, c);
set hive.vectorized.groupby.grouping.sets.from.parent=true;
select a, b, c, count(*), count(c), sum(b), sum(d), min(c), max(d), min(e), avg(d), avg(e), stddev_samp(e),
  grouping__id
from gsp group by rollup(a, b, c);

-- CUBE: a set has several children
set hive.vectorized.groupby.grouping.sets.from.parent=false;
select a, b, c, count(*), sum(d), max(c) from gsp group by cube(a, b, c);
set hive.vectorized.groupby.grouping.sets.from.parent=true;
select a, b, c, count(*), sum(d), max(c) from gsp group by cube(a, b, c);

-- no set has every key: two sets get the input rows
set hive.vectorized.groupby.grouping.sets.from.parent=false;
select a, b, c, count(*), sum(d) from gsp group by a, b, c grouping sets ((a, b), (a, c), (a), ());
set hive.vectorized.groupby.grouping.sets.from.parent=true;
select a, b, c, count(*), sum(d) from gsp group by a, b, c grouping sets ((a, b), (a, c), (a), ());

-- the tables flush on every batch: groups reach the children of their set whenever they leave its table
set hive.map.aggr.hash.percentmemory=0.0000001;
set hive.vectorized.groupby.grouping.sets.from.parent=false;
select a, b, c, count(*), sum(d), avg(e) from gsp group by cube(a, b, c);
set hive.vectorized.groupby.grouping.sets.from.parent=true;
select a, b, c, count(*), sum(d), avg(e) from gsp group by cube(a, b, c);
set hive.map.aggr.hash.percentmemory=0.5;

-- the hash aggregation turns to streaming after its first check
set hive.map.aggr.hash.min.reduction.stats=false;
set hive.map.aggr.hash.min.reduction=0.0001;
set hive.groupby.mapaggr.checkinterval=100;
set hive.vectorized.groupby.maxentries=10;
set hive.vectorized.groupby.grouping.sets.from.parent=false;
select a, b, c, count(*), sum(d) from gsp group by rollup(a, b, c);
set hive.vectorized.groupby.grouping.sets.from.parent=true;
select a, b, c, count(*), sum(d) from gsp group by rollup(a, b, c);
set hive.map.aggr.hash.min.reduction.stats=true;
set hive.map.aggr.hash.min.reduction=0.99;
set hive.groupby.mapaggr.checkinterval=100000;
set hive.vectorized.groupby.maxentries=1000000;

-- a reducer aggregates the grouping sets of input partitioned by one key (hive.optimize.grouping.set.threshold)
set hive.optimize.grouping.set.threshold=1;
explain vectorization detail
select a, b, c, count(*), sum(d) from gsp group by rollup(a, b, c);
set hive.vectorized.groupby.grouping.sets.from.parent=false;
select a, b, c, count(*), sum(d) from gsp group by rollup(a, b, c);
set hive.vectorized.groupby.grouping.sets.from.parent=true;
select a, b, c, count(*), sum(d) from gsp group by rollup(a, b, c);
