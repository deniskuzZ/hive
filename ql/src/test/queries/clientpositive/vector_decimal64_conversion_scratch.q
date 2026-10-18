-- SORT_QUERY_RESULTS
-- TPC-DS q78 on Parquet with the 10 TB statistics: a vectorized select adds a decimal64 sum (web) and a decimal one
-- (catalog, from a map join's small table). The decimal64 operand gets a conversion after the expression was built; a
-- reused scratch column made it share its output with the other operand, and the sum read the catalog value twice.
-- Vectorized and row mode must return the same rows.
set hive.mapred.mode=nonstrict;
create table date_dim (d_date_sk bigint, d_year int) stored as parquet;
create table web_sales (ws_sold_date_sk bigint, ws_item_sk bigint, ws_bill_customer_sk bigint, ws_order_number bigint,
  ws_quantity int, ws_wholesale_cost decimal(7,2), ws_sales_price decimal(7,2)) stored as parquet;
create table web_returns (wr_order_number bigint, wr_item_sk bigint) stored as parquet;
create table catalog_sales (cs_sold_date_sk bigint, cs_item_sk bigint, cs_bill_customer_sk bigint, cs_order_number bigint,
  cs_quantity int, cs_wholesale_cost decimal(7,2), cs_sales_price decimal(7,2)) stored as parquet;
create table catalog_returns (cr_order_number bigint, cr_item_sk bigint) stored as parquet;
create table store_sales (ss_sold_date_sk bigint, ss_item_sk bigint, ss_customer_sk bigint, ss_ticket_number bigint,
  ss_quantity int, ss_wholesale_cost decimal(7,2), ss_sales_price decimal(7,2)) stored as parquet;
create table store_returns (sr_ticket_number bigint, sr_item_sk bigint) stored as parquet;
insert into date_dim select pos + 1, 2000 + pos % 3 from (select posexplode(split(space(29), ' '))) t;
insert into web_sales select pos % 30 + 1, pos % 7 + 1, pos % 5 + 1, pos % 40, pos % 9 + 1,
  if(pos % 17 = 0, null, cast(pos % 50 + 1.25 as decimal(7,2))), cast(pos % 30 + 0.5 as decimal(7,2))
  from (select posexplode(split(space(599), ' '))) t;
insert into web_returns select pos * 3, pos % 7 + 1 from (select posexplode(split(space(13), ' '))) t;
insert into catalog_sales select pos % 30 + 1, pos % 7 + 1, pos % 5 + 1, pos % 50, pos % 8 + 1,
  if(pos % 19 = 0, null, cast(pos % 40 + 2.5 as decimal(7,2))), cast(pos % 20 + 0.25 as decimal(7,2))
  from (select posexplode(split(space(699), ' '))) t;
insert into catalog_returns select pos * 4, pos % 7 + 1 from (select posexplode(split(space(11), ' '))) t;
insert into store_sales select pos % 30 + 1, pos % 7 + 1, pos % 5 + 1, pos % 60, pos % 10 + 1,
  if(pos % 23 = 0, null, cast(pos % 60 + 1.5 as decimal(7,2))), cast(pos % 25 + 0.75 as decimal(7,2))
  from (select posexplode(split(space(799), ' '))) t;
insert into store_returns select pos * 5, pos % 7 + 1 from (select posexplode(split(space(9), ' '))) t;
analyze table date_dim compute statistics for columns;
analyze table web_sales compute statistics for columns;
analyze table web_returns compute statistics for columns;
analyze table catalog_sales compute statistics for columns;
analyze table catalog_returns compute statistics for columns;
analyze table store_sales compute statistics for columns;
analyze table store_returns compute statistics for columns;


-- dynamically partitioned hash joins, as on the cluster
set hive.auto.convert.join=true;
set hive.optimize.dynamic.partition.hashjoin=true;
set hive.auto.convert.join.noconditionaltask.size=2000000000;
set hive.exec.reducers.max=64;
set hive.convert.join.bucket.mapjoin.tez=false;
alter table date_dim update statistics set('numRows'='73049','rawDataSize'='876588');
alter table date_dim update statistics for column d_date_sk set('numDVs'='73049','numNulls'='0','lowValue'='2415022','highValue'='2488070');
alter table date_dim update statistics for column d_year set('numDVs'='200','numNulls'='0','lowValue'='1900','highValue'='2100');
alter table web_sales update statistics set('numRows'='7199963324','rawDataSize'='403197946144');
alter table web_sales update statistics for column ws_sold_date_sk set('numDVs'='1823','numNulls'='1799','lowValue'='2450816','highValue'='2452642');
alter table web_sales update statistics for column ws_item_sk set('numDVs'='402000','numNulls'='0','lowValue'='1','highValue'='402000');
alter table web_sales update statistics for column ws_bill_customer_sk set('numDVs'='65000000','numNulls'='1800000','lowValue'='1','highValue'='65000000');
alter table web_sales update statistics for column ws_order_number set('numDVs'='600000000','numNulls'='0','lowValue'='1','highValue'='600000000');
alter table web_sales update statistics for column ws_quantity set('numDVs'='100','numNulls'='1800000','lowValue'='1','highValue'='100');
alter table web_sales update statistics for column ws_wholesale_cost set('numDVs'='9900','numNulls'='1800000','lowValue'='1.00','highValue'='100.00');
alter table web_sales update statistics for column ws_sales_price set('numDVs'='29000','numNulls'='1800000','lowValue'='0.00','highValue'='300.00');
alter table web_returns update statistics set('numRows'='720000000','rawDataSize'='11520000000');
alter table web_returns update statistics for column wr_order_number set('numDVs'='420000000','numNulls'='0','lowValue'='1','highValue'='600000000');
alter table web_returns update statistics for column wr_item_sk set('numDVs'='402000','numNulls'='0','lowValue'='1','highValue'='402000');
alter table catalog_sales update statistics set('numRows'='14399964710','rawDataSize'='806398023760');
alter table catalog_sales update statistics for column cs_sold_date_sk set('numDVs'='1823','numNulls'='3600000','lowValue'='2450815','highValue'='2452654');
alter table catalog_sales update statistics for column cs_item_sk set('numDVs'='402000','numNulls'='0','lowValue'='1','highValue'='402000');
alter table catalog_sales update statistics for column cs_bill_customer_sk set('numDVs'='65000000','numNulls'='3600000','lowValue'='1','highValue'='65000000');
alter table catalog_sales update statistics for column cs_order_number set('numDVs'='1600000000','numNulls'='0','lowValue'='1','highValue'='1600000000');
alter table catalog_sales update statistics for column cs_quantity set('numDVs'='100','numNulls'='3600000','lowValue'='1','highValue'='100');
alter table catalog_sales update statistics for column cs_wholesale_cost set('numDVs'='9900','numNulls'='3600000','lowValue'='1.00','highValue'='100.00');
alter table catalog_sales update statistics for column cs_sales_price set('numDVs'='29000','numNulls'='3600000','lowValue'='0.00','highValue'='300.00');
alter table catalog_returns update statistics set('numRows'='1440000000','rawDataSize'='23040000000');
alter table catalog_returns update statistics for column cr_order_number set('numDVs'='900000000','numNulls'='0','lowValue'='1','highValue'='1600000000');
alter table catalog_returns update statistics for column cr_item_sk set('numDVs'='402000','numNulls'='0','lowValue'='1','highValue'='402000');
alter table store_sales update statistics set('numRows'='28799983563','rawDataSize'='1612799079528');
alter table store_sales update statistics for column ss_sold_date_sk set('numDVs'='1823','numNulls'='1300000000','lowValue'='2450816','highValue'='2452642');
alter table store_sales update statistics for column ss_item_sk set('numDVs'='402000','numNulls'='0','lowValue'='1','highValue'='402000');
alter table store_sales update statistics for column ss_customer_sk set('numDVs'='65000000','numNulls'='1300000000','lowValue'='1','highValue'='65000000');
alter table store_sales update statistics for column ss_ticket_number set('numDVs'='2400000000','numNulls'='0','lowValue'='1','highValue'='2400000000');
alter table store_sales update statistics for column ss_quantity set('numDVs'='100','numNulls'='1300000000','lowValue'='1','highValue'='100');
alter table store_sales update statistics for column ss_wholesale_cost set('numDVs'='9900','numNulls'='1300000000','lowValue'='1.00','highValue'='100.00');
alter table store_sales update statistics for column ss_sales_price set('numDVs'='19900','numNulls'='1300000000','lowValue'='0.00','highValue'='200.00');
alter table store_returns update statistics set('numRows'='2879960000','rawDataSize'='46079360000');
alter table store_returns update statistics for column sr_ticket_number set('numDVs'='1600000000','numNulls'='0','lowValue'='1','highValue'='2400000000');
alter table store_returns update statistics for column sr_item_sk set('numDVs'='402000','numNulls'='0','lowValue'='1','highValue'='402000');


set hive.vectorized.execution.enabled=true;
with ws as
  (select d_year AS ws_sold_year, ws_item_sk,
    ws_bill_customer_sk ws_customer_sk,
    sum(ws_quantity) ws_qty,
    sum(ws_wholesale_cost) ws_wc,
    sum(ws_sales_price) ws_sp
   from web_sales
   left join web_returns on wr_order_number=ws_order_number and ws_item_sk=wr_item_sk
   join date_dim on ws_sold_date_sk = d_date_sk
   where wr_order_number is null
   group by d_year, ws_item_sk, ws_bill_customer_sk
   ),
cs as
  (select d_year AS cs_sold_year, cs_item_sk,
    cs_bill_customer_sk cs_customer_sk,
    sum(cs_quantity) cs_qty,
    sum(cs_wholesale_cost) cs_wc,
    sum(cs_sales_price) cs_sp
   from catalog_sales
   left join catalog_returns on cr_order_number=cs_order_number and cs_item_sk=cr_item_sk
   join date_dim on cs_sold_date_sk = d_date_sk
   where cr_order_number is null
   group by d_year, cs_item_sk, cs_bill_customer_sk
   ),
ss as
  (select d_year AS ss_sold_year, ss_item_sk,
    ss_customer_sk,
    sum(ss_quantity) ss_qty,
    sum(ss_wholesale_cost) ss_wc,
    sum(ss_sales_price) ss_sp
   from store_sales
   left join store_returns on sr_ticket_number=ss_ticket_number and ss_item_sk=sr_item_sk
   join date_dim on ss_sold_date_sk = d_date_sk
   where sr_ticket_number is null
   group by d_year, ss_item_sk, ss_customer_sk
   )
 select 
ss_customer_sk,
round(ss_qty/(coalesce(ws_qty,0)+coalesce(cs_qty,0)),2) ratio,
ss_qty store_qty, ss_wc store_wholesale_cost, ss_sp store_sales_price,
coalesce(ws_qty,0)+coalesce(cs_qty,0) other_chan_qty,
coalesce(ws_wc,0)+coalesce(cs_wc,0) other_chan_wholesale_cost,
coalesce(ws_sp,0)+coalesce(cs_sp,0) other_chan_sales_price
from ss
left join ws on (ws_sold_year=ss_sold_year and ws_item_sk=ss_item_sk and ws_customer_sk=ss_customer_sk)
left join cs on (cs_sold_year=ss_sold_year and cs_item_sk=ss_item_sk and cs_customer_sk=ss_customer_sk)
where (coalesce(ws_qty,0)>0 or coalesce(cs_qty, 0)>0) and ss_sold_year=2001
order by 
  ss_customer_sk,
  ss_qty desc, ss_wc desc, ss_sp desc,
  other_chan_qty,
  other_chan_wholesale_cost,
  other_chan_sales_price,
  ratio
limit 100;

set hive.vectorized.execution.enabled=false;
with ws as
  (select d_year AS ws_sold_year, ws_item_sk,
    ws_bill_customer_sk ws_customer_sk,
    sum(ws_quantity) ws_qty,
    sum(ws_wholesale_cost) ws_wc,
    sum(ws_sales_price) ws_sp
   from web_sales
   left join web_returns on wr_order_number=ws_order_number and ws_item_sk=wr_item_sk
   join date_dim on ws_sold_date_sk = d_date_sk
   where wr_order_number is null
   group by d_year, ws_item_sk, ws_bill_customer_sk
   ),
cs as
  (select d_year AS cs_sold_year, cs_item_sk,
    cs_bill_customer_sk cs_customer_sk,
    sum(cs_quantity) cs_qty,
    sum(cs_wholesale_cost) cs_wc,
    sum(cs_sales_price) cs_sp
   from catalog_sales
   left join catalog_returns on cr_order_number=cs_order_number and cs_item_sk=cr_item_sk
   join date_dim on cs_sold_date_sk = d_date_sk
   where cr_order_number is null
   group by d_year, cs_item_sk, cs_bill_customer_sk
   ),
ss as
  (select d_year AS ss_sold_year, ss_item_sk,
    ss_customer_sk,
    sum(ss_quantity) ss_qty,
    sum(ss_wholesale_cost) ss_wc,
    sum(ss_sales_price) ss_sp
   from store_sales
   left join store_returns on sr_ticket_number=ss_ticket_number and ss_item_sk=sr_item_sk
   join date_dim on ss_sold_date_sk = d_date_sk
   where sr_ticket_number is null
   group by d_year, ss_item_sk, ss_customer_sk
   )
 select 
ss_customer_sk,
round(ss_qty/(coalesce(ws_qty,0)+coalesce(cs_qty,0)),2) ratio,
ss_qty store_qty, ss_wc store_wholesale_cost, ss_sp store_sales_price,
coalesce(ws_qty,0)+coalesce(cs_qty,0) other_chan_qty,
coalesce(ws_wc,0)+coalesce(cs_wc,0) other_chan_wholesale_cost,
coalesce(ws_sp,0)+coalesce(cs_sp,0) other_chan_sales_price
from ss
left join ws on (ws_sold_year=ss_sold_year and ws_item_sk=ss_item_sk and ws_customer_sk=ss_customer_sk)
left join cs on (cs_sold_year=ss_sold_year and cs_item_sk=ss_item_sk and cs_customer_sk=ss_customer_sk)
where (coalesce(ws_qty,0)>0 or coalesce(cs_qty, 0)>0) and ss_sold_year=2001
order by 
  ss_customer_sk,
  ss_qty desc, ss_wc desc, ss_sp desc,
  other_chan_qty,
  other_chan_wholesale_cost,
  other_chan_sales_price,
  ratio
limit 100;
set hive.vectorized.execution.enabled=true;
