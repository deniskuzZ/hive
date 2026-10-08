-- SORT_QUERY_RESULTS
-- Partial aggregate pushed through an inner join (hive.optimize.partial.aggr.join.transpose).
-- Each query runs with the rule off, on, and on without vectorization; the three results must be identical.

create table pa_date (d_date_sk int, d_year int, d_moy int);
create table pa_customer (c_customer_sk int, c_customer_id string, c_first_name string, c_last_name string,
  c_birth_year int);
create table pa_item (i_item_sk int, i_category string, i_brand string);
create table pa_store (s_store_sk int, s_gmt_offset decimal(5,2));
create table pa_store_sales (ss_sold_date_sk int, ss_customer_sk int, ss_item_sk int, ss_store_sk int,
  ss_quantity int, ss_list_price decimal(7,2), ss_ext_sales_price decimal(7,2),
  ss_ext_list_price decimal(7,2), ss_ext_discount_amt decimal(7,2));
create table pa_web_sales (ws_sold_date_sk int, ws_bill_customer_sk int, ws_ext_list_price decimal(7,2),
  ws_ext_discount_amt decimal(7,2));

insert into pa_date select pos + 1, if(pos < 4, 2000, 2001), pos % 4 + 1
from (select posexplode(split(space(7), ' '))) t;
insert into pa_customer select pos + 1, concat('C', pos + 1), concat('F', pos % 5), concat('L', pos % 4),
  1950 + pos % 6
from (select posexplode(split(space(21), ' '))) t;
-- a duplicated customer key: the join fans out partial rows
insert into pa_customer values (5, 'C5b', 'F9', 'L9', 1960);
insert into pa_item select pos + 1, if(pos % 2 = 0, 'Electronics', 'Books'), concat('B', pos % 3)
from (select posexplode(split(space(9), ' '))) t;
-- store 4 has no sales
insert into pa_store values (1, -7), (2, -7), (3, -5), (4, -7);
insert into pa_store_sales select
  pos % 6 + 1,
  if(pos % 37 = 0, null, pos % 20 + 1),
  pos % 10 + 1,
  pos % 3 + 1,
  if(pos % 11 = 0, null, pos % 7),
  cast(pos % 9 * 2.5 as decimal(7,2)),
  if(pos % 13 = 0, null, cast(pos % 50 * 1.25 as decimal(7,2))),
  cast(pos % 40 * 3.75 as decimal(7,2)),
  cast(pos % 5 * 0.5 as decimal(7,2))
from (select posexplode(split(space(399), ' '))) t;
insert into pa_web_sales select
  pos % 6 + 1,
  if(pos % 29 = 0, null, pos % 15 + 1),
  cast(pos % 30 * 4.25 as decimal(7,2)),
  cast(pos % 4 * 0.75 as decimal(7,2))
from (select posexplode(split(space(299), ' '))) t;

analyze table pa_date compute statistics for columns;
analyze table pa_customer compute statistics for columns;
analyze table pa_item compute statistics for columns;
analyze table pa_store compute statistics for columns;
analyze table pa_store_sales compute statistics for columns;
analyze table pa_web_sales compute statistics for columns;


-- q4_like
set hive.optimize.partial.aggr.join.transpose=true;
explain cbo
with year_total as (
 select c_customer_id customer_id, c_first_name customer_first_name, c_last_name customer_last_name,
        d_year dyear, sum(ss_ext_list_price - ss_ext_discount_amt) year_total, 's' sale_type
 from pa_customer, pa_store_sales, pa_date
 where c_customer_sk = ss_customer_sk and ss_sold_date_sk = d_date_sk
 group by c_customer_id, c_first_name, c_last_name, d_year
 union all
 select c_customer_id customer_id, c_first_name customer_first_name, c_last_name customer_last_name,
        d_year dyear, sum(ws_ext_list_price - ws_ext_discount_amt) year_total, 'w' sale_type
 from pa_customer, pa_web_sales, pa_date
 where c_customer_sk = ws_bill_customer_sk and ws_sold_date_sk = d_date_sk
 group by c_customer_id, c_first_name, c_last_name, d_year)
select t_s_secyear.customer_id, t_s_secyear.customer_first_name, t_s_secyear.customer_last_name,
       t_s_firstyear.year_total, t_s_secyear.year_total, t_w_firstyear.year_total, t_w_secyear.year_total
from year_total t_s_firstyear, year_total t_s_secyear, year_total t_w_firstyear, year_total t_w_secyear
where t_s_secyear.customer_id = t_s_firstyear.customer_id
  and t_s_firstyear.customer_id = t_w_secyear.customer_id
  and t_s_firstyear.customer_id = t_w_firstyear.customer_id
  and t_s_firstyear.sale_type = 's' and t_w_firstyear.sale_type = 'w'
  and t_s_secyear.sale_type = 's' and t_w_secyear.sale_type = 'w'
  and t_s_firstyear.dyear = 2000 and t_s_secyear.dyear = 2001
  and t_w_firstyear.dyear = 2000 and t_w_secyear.dyear = 2001
  and t_s_firstyear.year_total > 0 and t_w_firstyear.year_total > 0
  and case when t_w_firstyear.year_total > 0 then t_w_secyear.year_total / t_w_firstyear.year_total else null end
      > case when t_s_firstyear.year_total > 0 then t_s_secyear.year_total / t_s_firstyear.year_total else null end;
set hive.optimize.partial.aggr.join.transpose=false;
with year_total as (
 select c_customer_id customer_id, c_first_name customer_first_name, c_last_name customer_last_name,
        d_year dyear, sum(ss_ext_list_price - ss_ext_discount_amt) year_total, 's' sale_type
 from pa_customer, pa_store_sales, pa_date
 where c_customer_sk = ss_customer_sk and ss_sold_date_sk = d_date_sk
 group by c_customer_id, c_first_name, c_last_name, d_year
 union all
 select c_customer_id customer_id, c_first_name customer_first_name, c_last_name customer_last_name,
        d_year dyear, sum(ws_ext_list_price - ws_ext_discount_amt) year_total, 'w' sale_type
 from pa_customer, pa_web_sales, pa_date
 where c_customer_sk = ws_bill_customer_sk and ws_sold_date_sk = d_date_sk
 group by c_customer_id, c_first_name, c_last_name, d_year)
select t_s_secyear.customer_id, t_s_secyear.customer_first_name, t_s_secyear.customer_last_name,
       t_s_firstyear.year_total, t_s_secyear.year_total, t_w_firstyear.year_total, t_w_secyear.year_total
from year_total t_s_firstyear, year_total t_s_secyear, year_total t_w_firstyear, year_total t_w_secyear
where t_s_secyear.customer_id = t_s_firstyear.customer_id
  and t_s_firstyear.customer_id = t_w_secyear.customer_id
  and t_s_firstyear.customer_id = t_w_firstyear.customer_id
  and t_s_firstyear.sale_type = 's' and t_w_firstyear.sale_type = 'w'
  and t_s_secyear.sale_type = 's' and t_w_secyear.sale_type = 'w'
  and t_s_firstyear.dyear = 2000 and t_s_secyear.dyear = 2001
  and t_w_firstyear.dyear = 2000 and t_w_secyear.dyear = 2001
  and t_s_firstyear.year_total > 0 and t_w_firstyear.year_total > 0
  and case when t_w_firstyear.year_total > 0 then t_w_secyear.year_total / t_w_firstyear.year_total else null end
      > case when t_s_firstyear.year_total > 0 then t_s_secyear.year_total / t_s_firstyear.year_total else null end;
set hive.optimize.partial.aggr.join.transpose=true;
with year_total as (
 select c_customer_id customer_id, c_first_name customer_first_name, c_last_name customer_last_name,
        d_year dyear, sum(ss_ext_list_price - ss_ext_discount_amt) year_total, 's' sale_type
 from pa_customer, pa_store_sales, pa_date
 where c_customer_sk = ss_customer_sk and ss_sold_date_sk = d_date_sk
 group by c_customer_id, c_first_name, c_last_name, d_year
 union all
 select c_customer_id customer_id, c_first_name customer_first_name, c_last_name customer_last_name,
        d_year dyear, sum(ws_ext_list_price - ws_ext_discount_amt) year_total, 'w' sale_type
 from pa_customer, pa_web_sales, pa_date
 where c_customer_sk = ws_bill_customer_sk and ws_sold_date_sk = d_date_sk
 group by c_customer_id, c_first_name, c_last_name, d_year)
select t_s_secyear.customer_id, t_s_secyear.customer_first_name, t_s_secyear.customer_last_name,
       t_s_firstyear.year_total, t_s_secyear.year_total, t_w_firstyear.year_total, t_w_secyear.year_total
from year_total t_s_firstyear, year_total t_s_secyear, year_total t_w_firstyear, year_total t_w_secyear
where t_s_secyear.customer_id = t_s_firstyear.customer_id
  and t_s_firstyear.customer_id = t_w_secyear.customer_id
  and t_s_firstyear.customer_id = t_w_firstyear.customer_id
  and t_s_firstyear.sale_type = 's' and t_w_firstyear.sale_type = 'w'
  and t_s_secyear.sale_type = 's' and t_w_secyear.sale_type = 'w'
  and t_s_firstyear.dyear = 2000 and t_s_secyear.dyear = 2001
  and t_w_firstyear.dyear = 2000 and t_w_secyear.dyear = 2001
  and t_s_firstyear.year_total > 0 and t_w_firstyear.year_total > 0
  and case when t_w_firstyear.year_total > 0 then t_w_secyear.year_total / t_w_firstyear.year_total else null end
      > case when t_s_firstyear.year_total > 0 then t_s_secyear.year_total / t_s_firstyear.year_total else null end;
set hive.vectorized.execution.enabled=false;
with year_total as (
 select c_customer_id customer_id, c_first_name customer_first_name, c_last_name customer_last_name,
        d_year dyear, sum(ss_ext_list_price - ss_ext_discount_amt) year_total, 's' sale_type
 from pa_customer, pa_store_sales, pa_date
 where c_customer_sk = ss_customer_sk and ss_sold_date_sk = d_date_sk
 group by c_customer_id, c_first_name, c_last_name, d_year
 union all
 select c_customer_id customer_id, c_first_name customer_first_name, c_last_name customer_last_name,
        d_year dyear, sum(ws_ext_list_price - ws_ext_discount_amt) year_total, 'w' sale_type
 from pa_customer, pa_web_sales, pa_date
 where c_customer_sk = ws_bill_customer_sk and ws_sold_date_sk = d_date_sk
 group by c_customer_id, c_first_name, c_last_name, d_year)
select t_s_secyear.customer_id, t_s_secyear.customer_first_name, t_s_secyear.customer_last_name,
       t_s_firstyear.year_total, t_s_secyear.year_total, t_w_firstyear.year_total, t_w_secyear.year_total
from year_total t_s_firstyear, year_total t_s_secyear, year_total t_w_firstyear, year_total t_w_secyear
where t_s_secyear.customer_id = t_s_firstyear.customer_id
  and t_s_firstyear.customer_id = t_w_secyear.customer_id
  and t_s_firstyear.customer_id = t_w_firstyear.customer_id
  and t_s_firstyear.sale_type = 's' and t_w_firstyear.sale_type = 'w'
  and t_s_secyear.sale_type = 's' and t_w_secyear.sale_type = 'w'
  and t_s_firstyear.dyear = 2000 and t_s_secyear.dyear = 2001
  and t_w_firstyear.dyear = 2000 and t_w_secyear.dyear = 2001
  and t_s_firstyear.year_total > 0 and t_w_firstyear.year_total > 0
  and case when t_w_firstyear.year_total > 0 then t_w_secyear.year_total / t_w_firstyear.year_total else null end
      > case when t_s_firstyear.year_total > 0 then t_s_secyear.year_total / t_s_firstyear.year_total else null end;
set hive.vectorized.execution.enabled=true;

-- q61_like
set hive.optimize.partial.aggr.join.transpose=true;
explain cbo
select promotions, total, cast(promotions as decimal(15,4)) / cast(total as decimal(15,4)) * 100
from (select sum(ss_ext_sales_price) promotions
      from pa_store_sales, pa_store, pa_date, pa_customer, pa_item
      where ss_sold_date_sk = d_date_sk and ss_store_sk = s_store_sk and ss_customer_sk = c_customer_sk
        and ss_item_sk = i_item_sk and i_category = 'Electronics' and s_gmt_offset = -7 and d_year = 2000
        and ss_quantity > 2) promotional_sales,
     (select sum(ss_ext_sales_price) total
      from pa_store_sales, pa_store, pa_date, pa_customer, pa_item
      where ss_sold_date_sk = d_date_sk and ss_store_sk = s_store_sk and ss_customer_sk = c_customer_sk
        and ss_item_sk = i_item_sk and i_category = 'Electronics' and s_gmt_offset = -7 and d_year = 2000) all_sales;
set hive.optimize.partial.aggr.join.transpose=false;
select promotions, total, cast(promotions as decimal(15,4)) / cast(total as decimal(15,4)) * 100
from (select sum(ss_ext_sales_price) promotions
      from pa_store_sales, pa_store, pa_date, pa_customer, pa_item
      where ss_sold_date_sk = d_date_sk and ss_store_sk = s_store_sk and ss_customer_sk = c_customer_sk
        and ss_item_sk = i_item_sk and i_category = 'Electronics' and s_gmt_offset = -7 and d_year = 2000
        and ss_quantity > 2) promotional_sales,
     (select sum(ss_ext_sales_price) total
      from pa_store_sales, pa_store, pa_date, pa_customer, pa_item
      where ss_sold_date_sk = d_date_sk and ss_store_sk = s_store_sk and ss_customer_sk = c_customer_sk
        and ss_item_sk = i_item_sk and i_category = 'Electronics' and s_gmt_offset = -7 and d_year = 2000) all_sales;
set hive.optimize.partial.aggr.join.transpose=true;
select promotions, total, cast(promotions as decimal(15,4)) / cast(total as decimal(15,4)) * 100
from (select sum(ss_ext_sales_price) promotions
      from pa_store_sales, pa_store, pa_date, pa_customer, pa_item
      where ss_sold_date_sk = d_date_sk and ss_store_sk = s_store_sk and ss_customer_sk = c_customer_sk
        and ss_item_sk = i_item_sk and i_category = 'Electronics' and s_gmt_offset = -7 and d_year = 2000
        and ss_quantity > 2) promotional_sales,
     (select sum(ss_ext_sales_price) total
      from pa_store_sales, pa_store, pa_date, pa_customer, pa_item
      where ss_sold_date_sk = d_date_sk and ss_store_sk = s_store_sk and ss_customer_sk = c_customer_sk
        and ss_item_sk = i_item_sk and i_category = 'Electronics' and s_gmt_offset = -7 and d_year = 2000) all_sales;
set hive.vectorized.execution.enabled=false;
select promotions, total, cast(promotions as decimal(15,4)) / cast(total as decimal(15,4)) * 100
from (select sum(ss_ext_sales_price) promotions
      from pa_store_sales, pa_store, pa_date, pa_customer, pa_item
      where ss_sold_date_sk = d_date_sk and ss_store_sk = s_store_sk and ss_customer_sk = c_customer_sk
        and ss_item_sk = i_item_sk and i_category = 'Electronics' and s_gmt_offset = -7 and d_year = 2000
        and ss_quantity > 2) promotional_sales,
     (select sum(ss_ext_sales_price) total
      from pa_store_sales, pa_store, pa_date, pa_customer, pa_item
      where ss_sold_date_sk = d_date_sk and ss_store_sk = s_store_sk and ss_customer_sk = c_customer_sk
        and ss_item_sk = i_item_sk and i_category = 'Electronics' and s_gmt_offset = -7 and d_year = 2000) all_sales;
set hive.vectorized.execution.enabled=true;

-- all_functions
set hive.optimize.partial.aggr.join.transpose=true;
explain cbo
select c_last_name, count(*), count(ss_quantity), sum(ss_quantity), sum(ss_ext_sales_price),
       min(ss_list_price), max(ss_list_price), min(ss_ext_sales_price), max(ss_ext_sales_price),
       avg(ss_ext_sales_price), avg(ss_quantity)
from pa_store_sales join pa_customer on ss_customer_sk = c_customer_sk
group by c_last_name;
set hive.optimize.partial.aggr.join.transpose=false;
select c_last_name, count(*), count(ss_quantity), sum(ss_quantity), sum(ss_ext_sales_price),
       min(ss_list_price), max(ss_list_price), min(ss_ext_sales_price), max(ss_ext_sales_price),
       avg(ss_ext_sales_price), avg(ss_quantity)
from pa_store_sales join pa_customer on ss_customer_sk = c_customer_sk
group by c_last_name;
set hive.optimize.partial.aggr.join.transpose=true;
select c_last_name, count(*), count(ss_quantity), sum(ss_quantity), sum(ss_ext_sales_price),
       min(ss_list_price), max(ss_list_price), min(ss_ext_sales_price), max(ss_ext_sales_price),
       avg(ss_ext_sales_price), avg(ss_quantity)
from pa_store_sales join pa_customer on ss_customer_sk = c_customer_sk
group by c_last_name;
set hive.vectorized.execution.enabled=false;
select c_last_name, count(*), count(ss_quantity), sum(ss_quantity), sum(ss_ext_sales_price),
       min(ss_list_price), max(ss_list_price), min(ss_ext_sales_price), max(ss_ext_sales_price),
       avg(ss_ext_sales_price), avg(ss_quantity)
from pa_store_sales join pa_customer on ss_customer_sk = c_customer_sk
group by c_last_name;
set hive.vectorized.execution.enabled=true;

-- count_star_dim_key
set hive.optimize.partial.aggr.join.transpose=true;
explain cbo
select d_year, count(*) from pa_store_sales join pa_date on ss_sold_date_sk = d_date_sk group by d_year;
set hive.optimize.partial.aggr.join.transpose=false;
select d_year, count(*) from pa_store_sales join pa_date on ss_sold_date_sk = d_date_sk group by d_year;
set hive.optimize.partial.aggr.join.transpose=true;
select d_year, count(*) from pa_store_sales join pa_date on ss_sold_date_sk = d_date_sk group by d_year;
set hive.vectorized.execution.enabled=false;
select d_year, count(*) from pa_store_sales join pa_date on ss_sold_date_sk = d_date_sk group by d_year;
set hive.vectorized.execution.enabled=true;

-- args_on_right
set hive.optimize.partial.aggr.join.transpose=true;
explain cbo
select d_moy, sum(ss_ext_sales_price), max(ss_quantity)
from pa_date join pa_store_sales on d_date_sk = ss_sold_date_sk group by d_moy;
set hive.optimize.partial.aggr.join.transpose=false;
select d_moy, sum(ss_ext_sales_price), max(ss_quantity)
from pa_date join pa_store_sales on d_date_sk = ss_sold_date_sk group by d_moy;
set hive.optimize.partial.aggr.join.transpose=true;
select d_moy, sum(ss_ext_sales_price), max(ss_quantity)
from pa_date join pa_store_sales on d_date_sk = ss_sold_date_sk group by d_moy;
set hive.vectorized.execution.enabled=false;
select d_moy, sum(ss_ext_sales_price), max(ss_quantity)
from pa_date join pa_store_sales on d_date_sk = ss_sold_date_sk group by d_moy;
set hive.vectorized.execution.enabled=true;

-- fact_and_dim_keys
set hive.optimize.partial.aggr.join.transpose=true;
explain cbo
select ss_store_sk, d_year, sum(ss_ext_list_price), count(ss_ext_sales_price)
from pa_store_sales join pa_date on ss_sold_date_sk = d_date_sk group by ss_store_sk, d_year;
set hive.optimize.partial.aggr.join.transpose=false;
select ss_store_sk, d_year, sum(ss_ext_list_price), count(ss_ext_sales_price)
from pa_store_sales join pa_date on ss_sold_date_sk = d_date_sk group by ss_store_sk, d_year;
set hive.optimize.partial.aggr.join.transpose=true;
select ss_store_sk, d_year, sum(ss_ext_list_price), count(ss_ext_sales_price)
from pa_store_sales join pa_date on ss_sold_date_sk = d_date_sk group by ss_store_sk, d_year;
set hive.vectorized.execution.enabled=false;
select ss_store_sk, d_year, sum(ss_ext_list_price), count(ss_ext_sales_price)
from pa_store_sales join pa_date on ss_sold_date_sk = d_date_sk group by ss_store_sk, d_year;
set hive.vectorized.execution.enabled=true;

-- distinct_q38_shape
set hive.optimize.partial.aggr.join.transpose=true;
explain cbo
select count(*) from (
  select distinct c_last_name, c_first_name, d_date_sk
  from pa_store_sales join pa_date on ss_sold_date_sk = d_date_sk join pa_customer on ss_customer_sk = c_customer_sk
  where d_year = 2000) t;
set hive.optimize.partial.aggr.join.transpose=false;
select count(*) from (
  select distinct c_last_name, c_first_name, d_date_sk
  from pa_store_sales join pa_date on ss_sold_date_sk = d_date_sk join pa_customer on ss_customer_sk = c_customer_sk
  where d_year = 2000) t;
set hive.optimize.partial.aggr.join.transpose=true;
select count(*) from (
  select distinct c_last_name, c_first_name, d_date_sk
  from pa_store_sales join pa_date on ss_sold_date_sk = d_date_sk join pa_customer on ss_customer_sk = c_customer_sk
  where d_year = 2000) t;
set hive.vectorized.execution.enabled=false;
select count(*) from (
  select distinct c_last_name, c_first_name, d_date_sk
  from pa_store_sales join pa_date on ss_sold_date_sk = d_date_sk join pa_customer on ss_customer_sk = c_customer_sk
  where d_year = 2000) t;
set hive.vectorized.execution.enabled=true;

-- non_equi
set hive.optimize.partial.aggr.join.transpose=true;
explain cbo
select c_first_name, c_birth_year, sum(ss_ext_sales_price), count(*)
from pa_store_sales join pa_customer on ss_customer_sk = c_customer_sk and ss_quantity < c_birth_year - 1952
group by c_first_name, c_birth_year;
set hive.optimize.partial.aggr.join.transpose=false;
select c_first_name, c_birth_year, sum(ss_ext_sales_price), count(*)
from pa_store_sales join pa_customer on ss_customer_sk = c_customer_sk and ss_quantity < c_birth_year - 1952
group by c_first_name, c_birth_year;
set hive.optimize.partial.aggr.join.transpose=true;
select c_first_name, c_birth_year, sum(ss_ext_sales_price), count(*)
from pa_store_sales join pa_customer on ss_customer_sk = c_customer_sk and ss_quantity < c_birth_year - 1952
group by c_first_name, c_birth_year;
set hive.vectorized.execution.enabled=false;
select c_first_name, c_birth_year, sum(ss_ext_sales_price), count(*)
from pa_store_sales join pa_customer on ss_customer_sk = c_customer_sk and ss_quantity < c_birth_year - 1952
group by c_first_name, c_birth_year;
set hive.vectorized.execution.enabled=true;

-- two_level_kept
set hive.optimize.partial.aggr.join.transpose=true;
explain cbo
select i_category, d_year, sum(ss_ext_sales_price), count(*), max(ss_quantity), count(ss_quantity)
from pa_store_sales join pa_item on ss_item_sk = i_item_sk join pa_date on ss_sold_date_sk = d_date_sk
group by i_category, d_year;
set hive.optimize.partial.aggr.join.transpose=false;
select i_category, d_year, sum(ss_ext_sales_price), count(*), max(ss_quantity), count(ss_quantity)
from pa_store_sales join pa_item on ss_item_sk = i_item_sk join pa_date on ss_sold_date_sk = d_date_sk
group by i_category, d_year;
set hive.optimize.partial.aggr.join.transpose=true;
select i_category, d_year, sum(ss_ext_sales_price), count(*), max(ss_quantity), count(ss_quantity)
from pa_store_sales join pa_item on ss_item_sk = i_item_sk join pa_date on ss_sold_date_sk = d_date_sk
group by i_category, d_year;
set hive.vectorized.execution.enabled=false;
select i_category, d_year, sum(ss_ext_sales_price), count(*), max(ss_quantity), count(ss_quantity)
from pa_store_sales join pa_item on ss_item_sk = i_item_sk join pa_date on ss_sold_date_sk = d_date_sk
group by i_category, d_year;
set hive.vectorized.execution.enabled=true;

-- two_level_dropped
set hive.optimize.partial.aggr.join.transpose=true;
explain cbo
select i_category, ss_item_sk, ss_sold_date_sk, d_year, sum(ss_ext_sales_price), min(ss_list_price), count(*)
from pa_store_sales join pa_item on ss_item_sk = i_item_sk join pa_date on ss_sold_date_sk = d_date_sk
group by i_category, ss_item_sk, ss_sold_date_sk, d_year;
set hive.optimize.partial.aggr.join.transpose=false;
select i_category, ss_item_sk, ss_sold_date_sk, d_year, sum(ss_ext_sales_price), min(ss_list_price), count(*)
from pa_store_sales join pa_item on ss_item_sk = i_item_sk join pa_date on ss_sold_date_sk = d_date_sk
group by i_category, ss_item_sk, ss_sold_date_sk, d_year;
set hive.optimize.partial.aggr.join.transpose=true;
select i_category, ss_item_sk, ss_sold_date_sk, d_year, sum(ss_ext_sales_price), min(ss_list_price), count(*)
from pa_store_sales join pa_item on ss_item_sk = i_item_sk join pa_date on ss_sold_date_sk = d_date_sk
group by i_category, ss_item_sk, ss_sold_date_sk, d_year;
set hive.vectorized.execution.enabled=false;
select i_category, ss_item_sk, ss_sold_date_sk, d_year, sum(ss_ext_sales_price), min(ss_list_price), count(*)
from pa_store_sales join pa_item on ss_item_sk = i_item_sk join pa_date on ss_sold_date_sk = d_date_sk
group by i_category, ss_item_sk, ss_sold_date_sk, d_year;
set hive.vectorized.execution.enabled=true;

-- decimal_sum_expr
set hive.optimize.partial.aggr.join.transpose=true;
explain cbo
select c_customer_id, sum(ss_ext_list_price - ss_ext_discount_amt) s, sum(ss_ext_sales_price * 2) s2
from pa_store_sales join pa_customer on ss_customer_sk = c_customer_sk group by c_customer_id;
set hive.optimize.partial.aggr.join.transpose=false;
select c_customer_id, sum(ss_ext_list_price - ss_ext_discount_amt) s, sum(ss_ext_sales_price * 2) s2
from pa_store_sales join pa_customer on ss_customer_sk = c_customer_sk group by c_customer_id;
set hive.optimize.partial.aggr.join.transpose=true;
select c_customer_id, sum(ss_ext_list_price - ss_ext_discount_amt) s, sum(ss_ext_sales_price * 2) s2
from pa_store_sales join pa_customer on ss_customer_sk = c_customer_sk group by c_customer_id;
set hive.vectorized.execution.enabled=false;
select c_customer_id, sum(ss_ext_list_price - ss_ext_discount_amt) s, sum(ss_ext_sales_price * 2) s2
from pa_store_sales join pa_customer on ss_customer_sk = c_customer_sk group by c_customer_id;
set hive.vectorized.execution.enabled=true;

-- left_join_right_args
set hive.optimize.partial.aggr.join.transpose=true;
explain cbo
select ss_store_sk, d_year, count(c_customer_sk), sum(c_birth_year), count(*)
from pa_store_sales join pa_date on ss_sold_date_sk = d_date_sk
left join pa_customer on ss_customer_sk = c_customer_sk and c_birth_year > 1953
group by ss_store_sk, d_year;
set hive.optimize.partial.aggr.join.transpose=false;
select ss_store_sk, d_year, count(c_customer_sk), sum(c_birth_year), count(*)
from pa_store_sales join pa_date on ss_sold_date_sk = d_date_sk
left join pa_customer on ss_customer_sk = c_customer_sk and c_birth_year > 1953
group by ss_store_sk, d_year;
set hive.optimize.partial.aggr.join.transpose=true;
select ss_store_sk, d_year, count(c_customer_sk), sum(c_birth_year), count(*)
from pa_store_sales join pa_date on ss_sold_date_sk = d_date_sk
left join pa_customer on ss_customer_sk = c_customer_sk and c_birth_year > 1953
group by ss_store_sk, d_year;
set hive.vectorized.execution.enabled=false;
select ss_store_sk, d_year, count(c_customer_sk), sum(c_birth_year), count(*)
from pa_store_sales join pa_date on ss_sold_date_sk = d_date_sk
left join pa_customer on ss_customer_sk = c_customer_sk and c_birth_year > 1953
group by ss_store_sk, d_year;
set hive.vectorized.execution.enabled=true;

-- right_join_left_args
set hive.optimize.partial.aggr.join.transpose=true;
explain cbo
select c_last_name, count(ss_quantity), sum(ss_ext_sales_price), count(*)
from pa_store_sales right join pa_customer on ss_customer_sk = c_customer_sk group by c_last_name;
set hive.optimize.partial.aggr.join.transpose=false;
select c_last_name, count(ss_quantity), sum(ss_ext_sales_price), count(*)
from pa_store_sales right join pa_customer on ss_customer_sk = c_customer_sk group by c_last_name;
set hive.optimize.partial.aggr.join.transpose=true;
select c_last_name, count(ss_quantity), sum(ss_ext_sales_price), count(*)
from pa_store_sales right join pa_customer on ss_customer_sk = c_customer_sk group by c_last_name;
set hive.vectorized.execution.enabled=false;
select c_last_name, count(ss_quantity), sum(ss_ext_sales_price), count(*)
from pa_store_sales right join pa_customer on ss_customer_sk = c_customer_sk group by c_last_name;
set hive.vectorized.execution.enabled=true;

-- right_join_fact_args
set hive.optimize.partial.aggr.join.transpose=true;
explain cbo
select s_store_sk, count(ss_quantity), sum(ss_ext_sales_price), count(*), max(ss_list_price)
from pa_store_sales right join pa_store on ss_store_sk = s_store_sk group by s_store_sk;
set hive.optimize.partial.aggr.join.transpose=false;
select s_store_sk, count(ss_quantity), sum(ss_ext_sales_price), count(*), max(ss_list_price)
from pa_store_sales right join pa_store on ss_store_sk = s_store_sk group by s_store_sk;
set hive.optimize.partial.aggr.join.transpose=true;
select s_store_sk, count(ss_quantity), sum(ss_ext_sales_price), count(*), max(ss_list_price)
from pa_store_sales right join pa_store on ss_store_sk = s_store_sk group by s_store_sk;
set hive.vectorized.execution.enabled=false;
select s_store_sk, count(ss_quantity), sum(ss_ext_sales_price), count(*), max(ss_list_price)
from pa_store_sales right join pa_store on ss_store_sk = s_store_sk group by s_store_sk;
set hive.vectorized.execution.enabled=true;

-- args_on_both_sides
set hive.optimize.partial.aggr.join.transpose=true;
explain cbo
select d_year, sum(ss_quantity + d_moy) from pa_store_sales join pa_date on ss_sold_date_sk = d_date_sk group by d_year;
set hive.optimize.partial.aggr.join.transpose=false;
select d_year, sum(ss_quantity + d_moy) from pa_store_sales join pa_date on ss_sold_date_sk = d_date_sk group by d_year;
set hive.optimize.partial.aggr.join.transpose=true;
select d_year, sum(ss_quantity + d_moy) from pa_store_sales join pa_date on ss_sold_date_sk = d_date_sk group by d_year;
set hive.vectorized.execution.enabled=false;
select d_year, sum(ss_quantity + d_moy) from pa_store_sales join pa_date on ss_sold_date_sk = d_date_sk group by d_year;
set hive.vectorized.execution.enabled=true;

-- the pushed aggregate is a map-side GROUP BY right below the join's shuffle
set hive.optimize.partial.aggr.join.transpose=true;
explain
select c_last_name, sum(ss_ext_sales_price), count(*)
from pa_store_sales join pa_customer on ss_customer_sk = c_customer_sk group by c_last_name;

-- a merged decimal SUM keeps its type
create table pa_ctas_on as
select c_customer_id, sum(ss_ext_sales_price) s, count(ss_quantity) c
from pa_store_sales join pa_customer on ss_customer_sk = c_customer_sk group by c_customer_id;
describe pa_ctas_on;
set hive.optimize.partial.aggr.join.transpose=false;
create table pa_ctas_off as
select c_customer_id, sum(ss_ext_sales_price) s, count(ss_quantity) c
from pa_store_sales join pa_customer on ss_customer_sk = c_customer_sk group by c_customer_id;
describe pa_ctas_off;

