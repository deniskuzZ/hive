-- A capped semijoin whose bloom filter is built by map tasks that run in row mode and merged in a vectorized reducer:
-- both must size the filter the same way, or the merge drops the partial filters and the semijoin removes rows that
-- match. The text scans run in row mode without vector or row deserialization; row mode waits for the runtime values.
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.vectorized.execution.enabled=true;
set hive.vectorized.execution.reduce.enabled=true;
set hive.auto.convert.join=false;
set hive.tez.dynamic.semijoin.reduction=true;
set hive.tez.bigtable.minsize.semijoin.reduction=1;
set hive.tez.min.bloom.filter.entries=1;
set hive.tez.dynamic.semijoin.reduction.threshold=0.0;

create table bc_big (k int, v int) stored as textfile;
create table bc_small (k int, f int) stored as textfile;
insert into bc_big select pos, pos % 7 from (select posexplode(split(space(4999), ' '))) t;
insert into bc_small select pos * 3, pos % 2 from (select posexplode(split(space(999), ' '))) t;
analyze table bc_big compute statistics for columns;
analyze table bc_small compute statistics for columns;

set hive.vectorized.use.vector.serde.deserialize=false;
set hive.vectorized.use.row.serde.deserialize=false;

-- 500 keys against a filter capped at 50 entries
set hive.tez.max.bloom.filter.entries=50;

-- without the cap the semijoin is removed: the rows to expect
set hive.tez.bloom.filter.cap=false;
select count(*), sum(b.v) from bc_big b join bc_small s on b.k = s.k where s.f = 1;

-- capped: the filter is built in the row-mode map task and merged in the vectorized reducer
set hive.tez.bloom.filter.cap=true;
explain vectorization only
select count(*), sum(b.v) from bc_big b join bc_small s on b.k = s.k where s.f = 1;
explain
select count(*), sum(b.v) from bc_big b join bc_small s on b.k = s.k where s.f = 1;
select count(*), sum(b.v) from bc_big b join bc_small s on b.k = s.k where s.f = 1;
