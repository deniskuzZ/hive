-- SORT_QUERY_RESULTS
-- A partial aggregate pushed through an inner join when its arguments are CASEs whose condition is on one join input
-- and whose value is on the other (TPC-DS q2, q43): the value is aggregated below the join and the CASE applied to the
-- partial result above it. Each query runs with the rule off, on, and on without vectorization; the results must be
-- identical.

create table pc_date (d_date_sk int, d_year int, d_week_seq int, d_day_name string);
create table pc_store (s_store_sk int, s_store_id string, s_store_name string);
create table pc_sales (ss_sold_date_sk int, ss_store_sk int, ss_sales_price decimal(7,2), ss_quantity int);

-- four weeks over two years, a day name per date
insert into pc_date select pos + 1, if(pos < 14, 2000, 2001), pos div 7 + 1,
  elt(pos % 7 + 1, 'Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday')
from (select posexplode(split(space(27), ' '))) t;
insert into pc_store select pos + 1, concat('S', pos % 3), concat('store', pos % 2)
from (select posexplode(split(space(5), ' '))) t;
insert into pc_sales select pos % 28 + 1, pos % 6 + 1,
  if(pos % 17 = 0, null, cast(pos % 50 * 1.25 as decimal(7,2))), if(pos % 11 = 0, null, pos % 9)
from (select posexplode(split(space(5999), ' '))) t;

analyze table pc_date compute statistics for columns;
analyze table pc_store compute statistics for columns;
analyze table pc_sales compute statistics for columns;

-- q43: keys on the sales side, CASE conditions on the date side
set hive.optimize.partial.aggr.join.transpose=true;
explain cbo
select ss_store_sk,
  sum(case when d_day_name = 'Sunday' then ss_sales_price else null end) sun,
  sum(case when d_day_name = 'Monday' then ss_sales_price else null end) mon,
  count(case when d_day_name = 'Tuesday' then ss_quantity end) tue_cnt,
  min(case when d_day_name = 'Wednesday' then ss_sales_price end) wed_min,
  max(case when d_day_name = 'Thursday' then ss_sales_price end) thu_max,
  sum(case when d_day_name = 'Friday' then ss_sales_price * ss_quantity else 0 end) fri_amt
from pc_sales join pc_date on ss_sold_date_sk = d_date_sk
where d_year = 2000
group by ss_store_sk;

set hive.optimize.partial.aggr.join.transpose=false;
select ss_store_sk,
  sum(case when d_day_name = 'Sunday' then ss_sales_price else null end) sun,
  sum(case when d_day_name = 'Monday' then ss_sales_price else null end) mon,
  count(case when d_day_name = 'Tuesday' then ss_quantity end) tue_cnt,
  min(case when d_day_name = 'Wednesday' then ss_sales_price end) wed_min,
  max(case when d_day_name = 'Thursday' then ss_sales_price end) thu_max,
  sum(case when d_day_name = 'Friday' then ss_sales_price * ss_quantity else 0 end) fri_amt
from pc_sales join pc_date on ss_sold_date_sk = d_date_sk
where d_year = 2000
group by ss_store_sk;

set hive.optimize.partial.aggr.join.transpose=true;
select ss_store_sk,
  sum(case when d_day_name = 'Sunday' then ss_sales_price else null end) sun,
  sum(case when d_day_name = 'Monday' then ss_sales_price else null end) mon,
  count(case when d_day_name = 'Tuesday' then ss_quantity end) tue_cnt,
  min(case when d_day_name = 'Wednesday' then ss_sales_price end) wed_min,
  max(case when d_day_name = 'Thursday' then ss_sales_price end) thu_max,
  sum(case when d_day_name = 'Friday' then ss_sales_price * ss_quantity else 0 end) fri_amt
from pc_sales join pc_date on ss_sold_date_sk = d_date_sk
where d_year = 2000
group by ss_store_sk;

set hive.vectorized.execution.enabled=false;
select ss_store_sk,
  sum(case when d_day_name = 'Sunday' then ss_sales_price else null end) sun,
  sum(case when d_day_name = 'Monday' then ss_sales_price else null end) mon,
  count(case when d_day_name = 'Tuesday' then ss_quantity end) tue_cnt,
  min(case when d_day_name = 'Wednesday' then ss_sales_price end) wed_min,
  max(case when d_day_name = 'Thursday' then ss_sales_price end) thu_max,
  sum(case when d_day_name = 'Friday' then ss_sales_price * ss_quantity else 0 end) fri_amt
from pc_sales join pc_date on ss_sold_date_sk = d_date_sk
where d_year = 2000
group by ss_store_sk;
set hive.vectorized.execution.enabled=true;

-- q2: the key on the date side, so the date key is the only pushed key
set hive.optimize.partial.aggr.join.transpose=true;
explain cbo
select d_week_seq, sum(case when d_day_name = 'Sunday' then ss_sales_price end) sun,
  sum(case when d_day_name = 'Saturday' then ss_sales_price end) sat
from pc_sales join pc_date on ss_sold_date_sk = d_date_sk
group by d_week_seq;

set hive.optimize.partial.aggr.join.transpose=false;
select d_week_seq, sum(case when d_day_name = 'Sunday' then ss_sales_price end) sun,
  sum(case when d_day_name = 'Saturday' then ss_sales_price end) sat
from pc_sales join pc_date on ss_sold_date_sk = d_date_sk
group by d_week_seq;

set hive.optimize.partial.aggr.join.transpose=true;
select d_week_seq, sum(case when d_day_name = 'Sunday' then ss_sales_price end) sun,
  sum(case when d_day_name = 'Saturday' then ss_sales_price end) sat
from pc_sales join pc_date on ss_sold_date_sk = d_date_sk
group by d_week_seq;

-- a division the plan computes below the join, for every row already, is pushed; a COUNT whose ELSE counts is not
set hive.optimize.partial.aggr.join.transpose=true;
explain cbo
select ss_store_sk, sum(case when d_day_name = 'Sunday' then ss_sales_price / ss_quantity end) sun
from pc_sales join pc_date on ss_sold_date_sk = d_date_sk
group by ss_store_sk;

explain cbo
select ss_store_sk, count(case when d_day_name = 'Sunday' then ss_quantity else 0 end) sun
from pc_sales join pc_date on ss_sold_date_sk = d_date_sk
group by ss_store_sk;

set hive.optimize.partial.aggr.join.transpose=false;
select ss_store_sk, count(case when d_day_name = 'Sunday' then ss_quantity else 0 end) sun
from pc_sales join pc_date on ss_sold_date_sk = d_date_sk
group by ss_store_sk;

set hive.optimize.partial.aggr.join.transpose=true;
select ss_store_sk, count(case when d_day_name = 'Sunday' then ss_quantity else 0 end) sun
from pc_sales join pc_date on ss_sold_date_sk = d_date_sk
group by ss_store_sk;
