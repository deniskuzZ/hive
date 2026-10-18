-- A join on several keys whose combined key is unique on one side joins a foreign key to that primary key: TPC-DS
-- store_sales x store_returns on (customer, item, ticket), every return matching its sale. With the 10 TB statistics
-- (no constraints) the distinct value formula estimated 152M rows where 2.6B match, and q17 broadcast a join of the
-- two into the catalog_sales scan that held 60.9M rows (hash table over the memory limit, re-execution).
-- The estimate is the store_returns rows times the fraction of store_sales that passes its filters: q29's join,
-- where both sides are filtered by date first, stays small and keeps its broadcast.
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
create table date_dim (d_date_sk bigint, d_quarter_name string, d_year int, d_moy int);
create table store (s_store_sk bigint, s_state string);
create table item (i_item_sk bigint, i_item_id string, i_item_desc string);
create table store_sales (ss_sold_date_sk bigint, ss_item_sk bigint, ss_customer_sk bigint, ss_ticket_number bigint,
  ss_store_sk bigint, ss_quantity int);
create table store_returns (sr_returned_date_sk bigint, sr_item_sk bigint, sr_customer_sk bigint,
  sr_ticket_number bigint, sr_return_quantity int);
create table catalog_sales (cs_sold_date_sk bigint, cs_bill_customer_sk bigint, cs_item_sk bigint, cs_quantity int);

-- the 10 TB statistics (tpcds_partitioned_parquet_10000)
alter table date_dim update statistics set('numRows'='73049','rawDataSize'='3449552');
alter table date_dim update statistics for column d_date_sk set('numDVs'='67850','numNulls'='0','lowValue'='2415022','highValue'='2488070');
alter table date_dim update statistics for column d_quarter_name set('numDVs'='800','numNulls'='0','avgColLen'='6.0','maxColLen'='6');
alter table date_dim update statistics for column d_year set('numDVs'='201','numNulls'='0','lowValue'='1900','highValue'='2100');
alter table date_dim update statistics for column d_moy set('numDVs'='12','numNulls'='0','lowValue'='1','highValue'='12');
alter table store update statistics set('numRows'='1500','rawDataSize'='140000');
alter table store update statistics for column s_store_sk set('numDVs'='1500','numNulls'='0','lowValue'='1','highValue'='1500');
alter table store update statistics for column s_state set('numDVs'='50','numNulls'='10','avgColLen'='2.0','maxColLen'='2');
alter table item update statistics set('numRows'='402000','rawDataSize'='117199080');
alter table item update statistics for column i_item_sk set('numDVs'='402000','numNulls'='0','lowValue'='1','highValue'='402000');
alter table item update statistics for column i_item_id set('numDVs'='201000','numNulls'='0','avgColLen'='16.0','maxColLen'='16');
alter table item update statistics for column i_item_desc set('numDVs'='300000','numNulls'='1000','avgColLen'='100.0','maxColLen'='200');
alter table store_sales update statistics set('numRows'='28800426268','rawDataSize'='2229181912010');
alter table store_sales update statistics for column ss_sold_date_sk set('numDVs'='1824','numNulls'='0','lowValue'='2450816','highValue'='2452642');
alter table store_sales update statistics for column ss_item_sk set('numDVs'='400976','numNulls'='0','lowValue'='1','highValue'='402000');
alter table store_sales update statistics for column ss_customer_sk set('numDVs'='65000001','numNulls'='1296217501','lowValue'='1','highValue'='65000000');
alter table store_sales update statistics for column ss_ticket_number set('numDVs'='2340688183','numNulls'='0','lowValue'='1','highValue'='2400000000');
alter table store_sales update statistics for column ss_store_sk set('numDVs'='759','numNulls'='1296279123','lowValue'='1','highValue'='1498');
alter table store_sales update statistics for column ss_quantity set('numDVs'='100','numNulls'='1296000000','lowValue'='1','highValue'='100');
alter table store_returns update statistics set('numRows'='2879513368','rawDataSize'='234915083047');
alter table store_returns update statistics for column sr_returned_date_sk set('numDVs'='2004','numNulls'='0','lowValue'='2450820','highValue'='2452822');
alter table store_returns update statistics for column sr_item_sk set('numDVs'='400976','numNulls'='0','lowValue'='1','highValue'='402000');
alter table store_returns update statistics for column sr_customer_sk set('numDVs'='65000001','numNulls'='100729208','lowValue'='1','highValue'='65000000');
alter table store_returns update statistics for column sr_ticket_number set('numDVs'='1638485510','numNulls'='0','lowValue'='1','highValue'='2400000000');
alter table store_returns update statistics for column sr_return_quantity set('numDVs'='100','numNulls'='100000000','lowValue'='1','highValue'='100');
alter table catalog_sales update statistics set('numRows'='14399964710','rawDataSize'='1501542577703');
alter table catalog_sales update statistics for column cs_sold_date_sk set('numDVs'='1837','numNulls'='0','lowValue'='2450815','highValue'='2452654');
alter table catalog_sales update statistics for column cs_bill_customer_sk set('numDVs'='65000001','numNulls'='72014457','lowValue'='1','highValue'='65000000');
alter table catalog_sales update statistics for column cs_item_sk set('numDVs'='400976','numNulls'='0','lowValue'='1','highValue'='402000');
alter table catalog_sales update statistics for column cs_quantity set('numDVs'='100','numNulls'='72000000','lowValue'='1','highValue'='100');

-- map joins as on the cluster
set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask.size=1431655765;
set hive.optimize.dynamic.partition.hashjoin=true;
set hive.convert.join.bucket.mapjoin.tez=false;

-- every return matches its sale: about 2.6B rows (the returns with a customer)
explain
select count(*) from store_sales, store_returns
where ss_customer_sk = sr_customer_sk and ss_item_sk = sr_item_sk and ss_ticket_number = sr_ticket_number;

-- q29's join: both sides filtered by date, the returns keep the fraction of sales of one month
explain
select count(*) from store_sales, store_returns, date_dim d1, date_dim d2
where ss_customer_sk = sr_customer_sk and ss_item_sk = sr_item_sk and ss_ticket_number = sr_ticket_number
  and ss_sold_date_sk = d1.d_date_sk and d1.d_moy = 4 and d1.d_year = 1998
  and sr_returned_date_sk = d2.d_date_sk and d2.d_moy between 4 and 7 and d2.d_year = 1998;

-- q17: the join of sales and returns is not broadcast into the catalog_sales scan
explain
select i_item_id, i_item_desc, s_state, count(ss_quantity), avg(ss_quantity), count(sr_return_quantity),
       avg(sr_return_quantity), count(cs_quantity), avg(cs_quantity)
from store_sales, store_returns, catalog_sales, date_dim d1, date_dim d2, date_dim d3, store, item
where d1.d_quarter_name = '2000Q1' and d1.d_date_sk = ss_sold_date_sk and i_item_sk = ss_item_sk
  and s_store_sk = ss_store_sk and ss_customer_sk = sr_customer_sk and ss_item_sk = sr_item_sk
  and ss_ticket_number = sr_ticket_number and sr_returned_date_sk = d2.d_date_sk
  and d2.d_quarter_name in ('2000Q1','2000Q2','2000Q3') and sr_customer_sk = cs_bill_customer_sk
  and sr_item_sk = cs_item_sk and cs_sold_date_sk = d3.d_date_sk
  and d3.d_quarter_name in ('2000Q1','2000Q2','2000Q3')
group by i_item_id, i_item_desc, s_state
order by i_item_id, i_item_desc, s_state
limit 100;

-- q24: the key of the customer x store side ranges over customer x store (65M x 1,500 combinations), not over the
-- customers: store_sales keeps the fraction of its (customer, store) pairs the side holds, and the join stays small
create table customer (c_customer_sk bigint, c_current_addr_sk bigint, c_birth_country string);
create table customer_address (ca_address_sk bigint, ca_zip string, ca_country string);
create table store_zip (s_store_sk bigint, s_zip string, s_market_id int);
alter table customer update statistics set('numRows'='65000000','rawDataSize'='6500000000');
alter table customer_address update statistics set('numRows'='32500000','rawDataSize'='3250000000');
alter table store_zip update statistics set('numRows'='1500','rawDataSize'='60000');
alter table customer update statistics for column c_customer_sk set('numDVs'='65000000','numNulls'='0','lowValue'='1','highValue'='65000000');
alter table customer update statistics for column c_current_addr_sk set('numDVs'='30000000','numNulls'='0','lowValue'='1','highValue'='32500000');
alter table customer update statistics for column c_birth_country set('numDVs'='211','numNulls'='0','avgColLen'='8.4','maxColLen'='20');
alter table customer_address update statistics for column ca_address_sk set('numDVs'='32500000','numNulls'='0','lowValue'='1','highValue'='32500000');
alter table customer_address update statistics for column ca_zip set('numDVs'='9900','numNulls'='0','avgColLen'='5.0','maxColLen'='10');
alter table customer_address update statistics for column ca_country set('numDVs'='1','numNulls'='0','avgColLen'='13.0','maxColLen'='13');
alter table store_zip update statistics for column s_store_sk set('numDVs'='1500','numNulls'='0','lowValue'='1','highValue'='1500');
alter table store_zip update statistics for column s_zip set('numDVs'='1000','numNulls'='0','avgColLen'='5.0','maxColLen'='10');
alter table store_zip update statistics for column s_market_id set('numDVs'='10','numNulls'='0','lowValue'='1','highValue'='10');
explain
select count(*) from store_sales, customer, customer_address, store_zip
where ss_customer_sk = c_customer_sk and ss_store_sk = s_store_sk and c_current_addr_sk = ca_address_sk
  and s_zip = ca_zip and s_market_id = 7 and c_birth_country <> upper(ca_country);

-- q72: a join that repeats a side's rows leaves that side's key no longer unique. d1 x d2 on d_week_seq repeats each
-- d1 date for the 7 days of its week, so catalog_sales x (d1 x d2) multiplies the sales of d_year 2000 by 7. Taken as
-- a primary key, d1.d_date_sk gave their fraction alone (286M of 1.43B with the 1 TB statistics) where 2.0B match, and
-- q72 broadcast the join into the inventory scan (hash table over the memory limit, re-execution).
create table date_dim_week (d_date_sk bigint, d_week_seq int, d_year int);
create table catalog_sales_1tb (cs_sold_date_sk bigint, cs_item_sk bigint);
alter table date_dim_week update statistics set('numRows'='73049','rawDataSize'='876588');
alter table date_dim_week update statistics for column d_date_sk set('numDVs'='67850','numNulls'='0','lowValue'='2415022','highValue'='2488070');
alter table date_dim_week update statistics for column d_week_seq set('numDVs'='11297','numNulls'='0','lowValue'='1','highValue'='10436');
alter table date_dim_week update statistics for column d_year set('numDVs'='199','numNulls'='0','lowValue'='1900','highValue'='2100');
alter table catalog_sales_1tb update statistics set('numRows'='1432777090','rawDataSize'='11462216720');
alter table catalog_sales_1tb update statistics for column cs_sold_date_sk set('numDVs'='1837','numNulls'='0','lowValue'='2450815','highValue'='2452654');
alter table catalog_sales_1tb update statistics for column cs_item_sk set('numDVs'='304009','numNulls'='0','lowValue'='1','highValue'='300000');
explain
select count(*) from catalog_sales_1tb
join date_dim_week d1 on cs_sold_date_sk = d1.d_date_sk
join date_dim_week d2 on d1.d_week_seq = d2.d_week_seq
where d1.d_year = 2000;
