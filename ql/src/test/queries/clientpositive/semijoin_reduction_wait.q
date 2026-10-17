--! qt:dataset:srcpart
-- A semijoin filter passes rows until its runtime values arrive, except one whose scan's rows produce a pruning event
-- or another semijoin's values: it waits for them (DynamicValue(..., wait)).
set hive.explain.user=false;
set hive.tez.dynamic.partition.pruning=true;
set hive.tez.dynamic.semijoin.reduction=true;
set hive.auto.convert.join=false;
set hive.tez.bigtable.minsize.semijoin.reduction=1;
set hive.tez.min.bloom.filter.entries=1;
set hive.stats.fetch.column.stats=true;
set hive.tez.bloom.filter.factor=1.0f;

create table sjw_big (key string, value string) stored as orc;
create table sjw_small (key1 string, value1 string) stored as orc;
insert overwrite table sjw_big select key, value from srcpart where ds = '2008-04-08';
insert overwrite table sjw_small select key, value from srcpart where ds = '2008-04-09' limit 20;
analyze table sjw_big compute statistics for columns;
analyze table sjw_small compute statistics for columns;

-- the rows of sjw_big feed only the join: its filter does not wait
explain select count(*) from sjw_big join sjw_small on (sjw_big.key = sjw_small.key1);
select count(*) from sjw_big join sjw_small on (sjw_big.key = sjw_small.key1);

-- a scan whose rows produce a pruning event waits for its semijoin values: rows it passed before they arrived would go
-- into the event, which cannot be taken back (TPC-DS q58: every partition of the fact tables)
set hive.auto.convert.join=true;
set hive.tez.dynamic.semijoin.reduction.threshold=0;
create table sjw_fact (v int) partitioned by (d_sk int) stored as orc;
create table sjw_date (d_sk int, d_date string, d_week int) stored as orc;
insert overwrite table sjw_fact partition (d_sk) select pos % 7, pos % 40 from (select posexplode(split(space(799), ' '))) t;
insert overwrite table sjw_date select pos, concat('2000-01-', pos), pos div 7 from (select posexplode(split(space(39), ' '))) t;
analyze table sjw_fact compute statistics for columns;
analyze table sjw_date compute statistics for columns;
explain
select count(*), sum(f.v) from sjw_fact f
join sjw_date d on f.d_sk = d.d_sk
join (select d_date from sjw_date where d_week = 3) w on d.d_date = w.d_date;
select count(*), sum(f.v) from sjw_fact f
join sjw_date d on f.d_sk = d.d_sk
join (select d_date from sjw_date where d_week = 3) w on d.d_date = w.d_date;
set hive.tez.dynamic.semijoin.reduction=false;
select count(*), sum(f.v) from sjw_fact f
join sjw_date d on f.d_sk = d.d_sk
join (select d_date from sjw_date where d_week = 3) w on d.d_date = w.d_date;
