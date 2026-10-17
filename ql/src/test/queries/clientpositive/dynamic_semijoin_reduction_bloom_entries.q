-- Bloom filter sizing of semijoin reducers: the expected entries of a multi column semijoin are the product of the
-- NDVs of its keys, capped by the rows of the source.
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.auto.convert.join=false;
set hive.tez.dynamic.semijoin.reduction=true;
set hive.tez.dynamic.semijoin.reduction.multicolumn=true;
set hive.tez.bigtable.minsize.semijoin.reduction=1;
set hive.tez.min.bloom.filter.entries=1;

create table sales (item int, ticket int);
create table returns (item int, ticket int, reason int);

alter table sales update statistics set('numRows'='100000000', 'rawDataSize'='800000000');
alter table sales update statistics for column item set('numDVs'='1000', 'numNulls'='0', 'lowValue'='1', 'highValue'='1000');
alter table sales update statistics for column ticket set('numDVs'='10000000', 'numNulls'='0', 'lowValue'='1', 'highValue'='10000000');
alter table returns update statistics set('numRows'='1000000', 'rawDataSize'='12000000');
alter table returns update statistics for column item set('numDVs'='1000', 'numNulls'='0', 'lowValue'='1', 'highValue'='1000');
alter table returns update statistics for column ticket set('numDVs'='1000000', 'numNulls'='0', 'lowValue'='1', 'highValue'='10000000');
alter table returns update statistics for column reason set('numDVs'='10', 'numNulls'='0', 'lowValue'='1', 'highValue'='10');

-- The bloom filter over hash(item, ticket) expects one entry per returned row
explain
select count(*) from sales s join returns r on s.item = r.item and s.ticket = r.ticket where r.reason = 1;

-- A composite filter expecting more entries than the maximum is not built: each single column semijoin is checked on
-- its own, and none survives here
set hive.tez.max.bloom.filter.entries=10000;

explain
select count(*) from sales s join returns r on s.item = r.item and s.ticket = r.ticket where r.reason = 1;
