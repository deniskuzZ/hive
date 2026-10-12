-- store_sales joined with store_returns on (ticket number, item), as in TPC-DS. A multi-column semijoin whose composite
-- bloom filter exceeds hive.tez.max.bloom.filter.entries is removed with all its columns, so the single column
-- semijoins are merged only when the composite fits; otherwise each is checked on its own and the selective one stays.
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.auto.convert.join=false;
set hive.tez.dynamic.semijoin.reduction=true;
set hive.tez.dynamic.semijoin.reduction.multicolumn=true;
set hive.tez.bigtable.minsize.semijoin.reduction=1;
set hive.tez.min.bloom.filter.entries=1;

create table store_sales (ss_ticket_number bigint, ss_item_sk bigint, ss_quantity int);
create table store_returns (sr_ticket_number bigint, sr_item_sk bigint, sr_return_quantity int);
alter table store_sales update statistics set('numRows'='100000000', 'rawDataSize'='2000000000');
alter table store_sales update statistics for column ss_ticket_number set('numDVs'='5000000', 'numNulls'='0', 'lowValue'='1', 'highValue'='5000000');
alter table store_sales update statistics for column ss_item_sk set('numDVs'='100000', 'numNulls'='0', 'lowValue'='1', 'highValue'='100000');
alter table store_sales update statistics for column ss_quantity set('numDVs'='100', 'numNulls'='0', 'lowValue'='1', 'highValue'='100');
alter table store_returns update statistics set('numRows'='1000000', 'rawDataSize'='20000000');
alter table store_returns update statistics for column sr_ticket_number set('numDVs'='500000', 'numNulls'='0', 'lowValue'='1', 'highValue'='5000000');
alter table store_returns update statistics for column sr_item_sk set('numDVs'='1000', 'numNulls'='0', 'lowValue'='1', 'highValue'='100000');
alter table store_returns update statistics for column sr_return_quantity set('numDVs'='100', 'numNulls'='0', 'lowValue'='1', 'highValue'='100');

-- The composite filter (500,000 x 1,000 keys, capped by 1,000,000 rows) exceeds 100,000 entries: the item filter
-- (1,000 keys) is kept on its own, the ticket filter (500,000 keys) is removed
set hive.tez.max.bloom.filter.entries=100000;
explain
select ss_quantity, sr_return_quantity
from store_sales join store_returns on ss_ticket_number = sr_ticket_number and ss_item_sk = sr_item_sk;

-- Hinted semijoins without entries over a composite above the max entries: both stay, as hinted ones are not removed
explain
select /*+ semi(store_returns, sr_ticket_number, store_sales), semi(store_returns, sr_item_sk, store_sales) */
  ss_quantity, sr_return_quantity
from store_sales join store_returns on ss_ticket_number = sr_ticket_number and ss_item_sk = sr_item_sk;

-- The composite filter fits: the semijoins are merged into one
set hive.tez.max.bloom.filter.entries=100000000;
explain
select ss_quantity, sr_return_quantity
from store_sales join store_returns on ss_ticket_number = sr_ticket_number and ss_item_sk = sr_item_sk;

-- The merged filter's source is below a bucket map join, which sets the number of buckets on its descendants: the
-- operators of the merged filter carry operator traits like the rest of the plan
set hive.cbo.enable=false;
set hive.strict.checks.bucketing=false;
set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=10000000;
set hive.convert.join.bucket.mapjoin.tez=true;
create table item_bucketed (item_sk bigint, brand int) clustered by (item_sk) into 4 buckets stored as orc;
create table sales_bucketed (item_sk bigint, ticket_number bigint, quantity int) clustered by (item_sk) into 4 buckets stored as orc;
alter table item_bucketed update statistics set('numRows'='1000', 'rawDataSize'='12000');
alter table item_bucketed update statistics for column item_sk set('numDVs'='1000', 'numNulls'='0', 'lowValue'='1', 'highValue'='1000000');
alter table item_bucketed update statistics for column brand set('numDVs'='100', 'numNulls'='0', 'lowValue'='1', 'highValue'='100');
alter table sales_bucketed update statistics set('numRows'='100000000', 'rawDataSize'='2000000000');
alter table sales_bucketed update statistics for column item_sk set('numDVs'='1000000', 'numNulls'='0', 'lowValue'='1', 'highValue'='1000000');
alter table sales_bucketed update statistics for column ticket_number set('numDVs'='1000', 'numNulls'='0', 'lowValue'='1', 'highValue'='1000');
alter table sales_bucketed update statistics for column quantity set('numDVs'='100', 'numNulls'='0', 'lowValue'='1', 'highValue'='100');
explain
select s.brand, store_returns.sr_return_quantity
from (select sales_bucketed.item_sk, sales_bucketed.ticket_number, item_bucketed.brand
      from sales_bucketed join item_bucketed on sales_bucketed.item_sk = item_bucketed.item_sk) s
join store_returns on store_returns.sr_item_sk = s.item_sk and store_returns.sr_ticket_number = s.ticket_number;
