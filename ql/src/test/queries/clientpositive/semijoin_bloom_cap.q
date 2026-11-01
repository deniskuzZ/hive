-- A semijoin whose bloom filter's estimated entries exceed hive.tez.max.bloom.filter.entries: with
-- hive.tez.bloom.filter.cap it keeps a filter of that size, its benefit counting the false positives the smaller filter
-- lets through, instead of being removed.
-- 1. TPC-DS q80's store branch with the 10 TB statistics and the cluster's hive.stats.join.ndv.readjustment: the ticket
-- semijoin from the filtered store_sales into store_returns is estimated at 100.6M entries, over the 100M maximum.
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
create table date_dim (d_date_sk bigint, d_date date);
create table store (s_store_sk bigint, s_store_id string);
create table item (i_item_sk bigint, i_current_price decimal(7,2));
create table promotion (p_promo_sk bigint, p_channel_tv string);
create table store_sales (ss_sold_date_sk bigint, ss_item_sk bigint, ss_store_sk bigint, ss_promo_sk bigint,
  ss_ticket_number bigint, ss_ext_sales_price decimal(7,2), ss_net_profit decimal(7,2));
create table store_returns (sr_item_sk bigint, sr_ticket_number bigint, sr_return_amt decimal(7,2),
  sr_net_loss decimal(7,2));

alter table date_dim update statistics set('numRows'='73049','rawDataSize'='3449552');
alter table date_dim update statistics for column d_date_sk set('numDVs'='67850','numNulls'='0','lowValue'='2415022','highValue'='2488070');
alter table date_dim update statistics for column d_date set('numDVs'='76511','numNulls'='0','lowValue'='-25566','highValue'='47482');
alter table store update statistics set('numRows'='1500','rawDataSize'='381681');
alter table store update statistics for column s_store_sk set('numDVs'='1542','numNulls'='0','lowValue'='1','highValue'='1500');
alter table store update statistics for column s_store_id set('numDVs'='772','numNulls'='0','avgColLen'='16.0','maxColLen'='16');
alter table item update statistics set('numRows'='402000','rawDataSize'='82813239');
alter table item update statistics for column i_item_sk set('numDVs'='400976','numNulls'='0','lowValue'='1','highValue'='402000');
alter table item update statistics for column i_current_price set('numDVs'='9347','numNulls'='1015','lowValue'='0.09','highValue'='99.99');
alter table promotion update statistics set('numRows'='2000','rawDataSize'='193254');
alter table promotion update statistics for column p_promo_sk set('numDVs'='2057','numNulls'='0','lowValue'='1','highValue'='2000');
alter table promotion update statistics for column p_channel_tv set('numDVs'='1','numNulls'='23','avgColLen'='0.9885','maxColLen'='1');
alter table store_sales update statistics set('numRows'='28800426268','rawDataSize'='2229181912010');
alter table store_sales update statistics for column ss_sold_date_sk set('numDVs'='1824','numNulls'='0','lowValue'='2450816','highValue'='2452642');
alter table store_sales update statistics for column ss_item_sk set('numDVs'='400976','numNulls'='0','lowValue'='1','highValue'='402000');
alter table store_sales update statistics for column ss_store_sk set('numDVs'='759','numNulls'='1296279123','lowValue'='1','highValue'='1498');
alter table store_sales update statistics for column ss_promo_sk set('numDVs'='2057','numNulls'='1295880527','lowValue'='1','highValue'='2000');
alter table store_sales update statistics for column ss_ticket_number set('numDVs'='2340688183','numNulls'='0','lowValue'='1','highValue'='2400000000');
alter table store_sales update statistics for column ss_ext_sales_price set('numDVs'='611727','numNulls'='1296749100','lowValue'='0','highValue'='19972');
alter table store_sales update statistics for column ss_net_profit set('numDVs'='1030528','numNulls'='1296598743','lowValue'='-10000','highValue'='9986');
alter table store_returns update statistics set('numRows'='2879513368','rawDataSize'='234915083047');
alter table store_returns update statistics for column sr_item_sk set('numDVs'='400976','numNulls'='0','lowValue'='1','highValue'='402000');
alter table store_returns update statistics for column sr_ticket_number set('numDVs'='1638485510','numNulls'='0','lowValue'='1','highValue'='2400000000');
alter table store_returns update statistics for column sr_return_amt set('numDVs'='509200','numNulls'='100742000','lowValue'='0','highValue'='19643');
alter table store_returns update statistics for column sr_net_loss set('numDVs'='529418','numNulls'='100686536','lowValue'='0.5','highValue'='9999.09');

set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask.size=1431655765;
set hive.optimize.dynamic.partition.hashjoin=true;
set hive.optimize.shared.work.semijoin=false;
set hive.stats.join.ndv.readjustment=true;
set hive.optimize.join.push.through.outer.join=true;
set hive.optimize.unwrap.cast.in.comparison=true;
set hive.stats.filter.between.range=true;

-- removed: no semijoin into store_returns
set hive.tez.bloom.filter.cap=false;
explain
select s_store_id, sum(ss_ext_sales_price), sum(coalesce(sr_return_amt, 0)), sum(ss_net_profit - coalesce(sr_net_loss, 0))
from store_sales left outer join store_returns on (ss_item_sk = sr_item_sk and ss_ticket_number = sr_ticket_number),
  date_dim, store, item, promotion
where ss_sold_date_sk = d_date_sk and d_date between cast('2002-08-06' as date) and (cast('2002-08-06' as date) + 30 days)
  and ss_store_sk = s_store_sk and ss_item_sk = i_item_sk and i_current_price > 50 and ss_promo_sk = p_promo_sk
  and p_channel_tv = 'N'
group by s_store_id;

-- capped: store_returns gets the ticket semijoin
set hive.tez.bloom.filter.cap=true;
explain
select s_store_id, sum(ss_ext_sales_price), sum(coalesce(sr_return_amt, 0)), sum(ss_net_profit - coalesce(sr_net_loss, 0))
from store_sales left outer join store_returns on (ss_item_sk = sr_item_sk and ss_ticket_number = sr_ticket_number),
  date_dim, store, item, promotion
where ss_sold_date_sk = d_date_sk and d_date between cast('2002-08-06' as date) and (cast('2002-08-06' as date) + 30 days)
  and ss_store_sk = s_store_sk and ss_item_sk = i_item_sk and i_current_price > 50 and ss_promo_sk = p_promo_sk
  and p_channel_tv = 'N'
group by s_store_id;

-- the outer join on the bare store_sales scan: 2.34B estimated entries in a filter for 100M pass nearly every row,
-- so the capped filter has no benefit and is still removed
set hive.optimize.join.push.through.outer.join=false;
explain
select s_store_id, sum(ss_ext_sales_price), sum(coalesce(sr_return_amt, 0)), sum(ss_net_profit - coalesce(sr_net_loss, 0))
from store_sales left outer join store_returns on (ss_item_sk = sr_item_sk and ss_ticket_number = sr_ticket_number),
  date_dim, store, item, promotion
where ss_sold_date_sk = d_date_sk and d_date between cast('2002-08-06' as date) and (cast('2002-08-06' as date) + 30 days)
  and ss_store_sk = s_store_sk and ss_item_sk = i_item_sk and i_current_price > 50 and ss_promo_sk = p_promo_sk
  and p_channel_tv = 'N'
group by s_store_id;

-- 2. 500 keys against a domain of 5,000: a selective semijoin. Capped at 250 entries its filter passes ~21 % of the
-- other rows and still pays; capped at 50, ~99 %, and it does not. Forced with a filter capped far below its keys, it
-- returns the rows the plan without it returns.
set hive.optimize.join.push.through.outer.join=false;
set hive.optimize.unwrap.cast.in.comparison=false;
set hive.stats.filter.between.range=false;
set hive.auto.convert.join=false;
set hive.tez.bigtable.minsize.semijoin.reduction=1;
set hive.tez.min.bloom.filter.entries=1;
create table bc_big (k int, v int);
create table bc_small (k int, f int);
insert into bc_big select pos, pos % 7 from (select posexplode(split(space(4999), ' '))) t;
insert into bc_small select pos * 3, pos % 2 from (select posexplode(split(space(999), ' '))) t;
analyze table bc_big compute statistics for columns;
analyze table bc_small compute statistics for columns;

set hive.tez.max.bloom.filter.entries=250;
-- removed: 500 estimated entries over the maximum
set hive.tez.bloom.filter.cap=false;
explain
select count(*), sum(b.v) from bc_big b join bc_small s on b.k = s.k where s.f = 1;
-- capped at 250: kept
set hive.tez.bloom.filter.cap=true;
explain
select count(*), sum(b.v) from bc_big b join bc_small s on b.k = s.k where s.f = 1;

-- capped at 50: removed
set hive.tez.max.bloom.filter.entries=50;
explain
select count(*), sum(b.v) from bc_big b join bc_small s on b.k = s.k where s.f = 1;

-- forced at 50 entries, and without it: the same rows
set hive.tez.dynamic.semijoin.reduction.threshold=0.0;
set hive.tez.bloom.filter.cap=false;
select count(*), sum(b.v) from bc_big b join bc_small s on b.k = s.k where s.f = 1;
set hive.tez.bloom.filter.cap=true;
explain
select count(*), sum(b.v) from bc_big b join bc_small s on b.k = s.k where s.f = 1;
select count(*), sum(b.v) from bc_big b join bc_small s on b.k = s.k where s.f = 1;
