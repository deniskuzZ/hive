-- SORT_QUERY_RESULTS
-- A partial aggregate pushed through an inner join when its arguments are CASEs whose condition is on one join input
-- and whose value is on the other (TPC-DS q2, q43): the value is aggregated below the join and the CASE applied to the
-- partial result above it (hive.optimize.partial.aggr.join.transpose). Each query runs with the optimization off, on,
-- and on without vectorization (the q43 shape also on map joins and with the pushed GROUP BYs in pass-through); the
-- results must be identical.

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
explain
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

-- the GROUP BY above the join merges IF(condition, partial result, NULL) vectorized
explain vectorization detail
select ss_store_sk,
  sum(case when d_day_name = 'Sunday' then ss_sales_price else null end) sun,
  count(case when d_day_name = 'Tuesday' then ss_quantity end) tue_cnt
from pc_sales join pc_date on ss_sold_date_sk = d_date_sk
where d_year = 2000
group by ss_store_sk;

-- q43 with the store join: the GROUP BY is pushed below the store join, then below the date join, where a Select
-- applies the CASE conditions to the partial results
set hive.optimize.partial.aggr.join.transpose=true;
explain
select s_store_name, s_store_id,
  sum(case when d_day_name = 'Sunday' then ss_sales_price else null end) sun,
  sum(case when d_day_name = 'Monday' then ss_sales_price else null end) mon,
  count(case when d_day_name = 'Tuesday' then ss_quantity end) tue_cnt,
  sum(case when d_day_name = 'Friday' then ss_sales_price * ss_quantity else 0 end) fri_amt
from pc_sales join pc_date on ss_sold_date_sk = d_date_sk join pc_store on ss_store_sk = s_store_sk
where d_year = 2000
group by s_store_name, s_store_id;

set hive.optimize.partial.aggr.join.transpose=false;
select s_store_name, s_store_id,
  sum(case when d_day_name = 'Sunday' then ss_sales_price else null end) sun,
  sum(case when d_day_name = 'Monday' then ss_sales_price else null end) mon,
  count(case when d_day_name = 'Tuesday' then ss_quantity end) tue_cnt,
  sum(case when d_day_name = 'Friday' then ss_sales_price * ss_quantity else 0 end) fri_amt
from pc_sales join pc_date on ss_sold_date_sk = d_date_sk join pc_store on ss_store_sk = s_store_sk
where d_year = 2000
group by s_store_name, s_store_id;

set hive.optimize.partial.aggr.join.transpose=true;
select s_store_name, s_store_id,
  sum(case when d_day_name = 'Sunday' then ss_sales_price else null end) sun,
  sum(case when d_day_name = 'Monday' then ss_sales_price else null end) mon,
  count(case when d_day_name = 'Tuesday' then ss_quantity end) tue_cnt,
  sum(case when d_day_name = 'Friday' then ss_sales_price * ss_quantity else 0 end) fri_amt
from pc_sales join pc_date on ss_sold_date_sk = d_date_sk join pc_store on ss_store_sk = s_store_sk
where d_year = 2000
group by s_store_name, s_store_id;

set hive.vectorized.execution.enabled=false;
select s_store_name, s_store_id,
  sum(case when d_day_name = 'Sunday' then ss_sales_price else null end) sun,
  sum(case when d_day_name = 'Monday' then ss_sales_price else null end) mon,
  count(case when d_day_name = 'Tuesday' then ss_quantity end) tue_cnt,
  sum(case when d_day_name = 'Friday' then ss_sales_price * ss_quantity else 0 end) fri_amt
from pc_sales join pc_date on ss_sold_date_sk = d_date_sk join pc_store on ss_store_sk = s_store_sk
where d_year = 2000
group by s_store_name, s_store_id;
set hive.vectorized.execution.enabled=true;

-- an ELSE other than NULL or 0 adds to the result for the rows failing the condition: not pushed below the date join
set hive.optimize.partial.aggr.join.transpose=true;
explain
select ss_store_sk, sum(case when d_day_name = 'Sunday' then ss_sales_price else 1 end) sun
from pc_sales join pc_date on ss_sold_date_sk = d_date_sk
group by ss_store_sk;

-- q2: the key on the date side, so the date key is the only pushed key
set hive.optimize.partial.aggr.join.transpose=true;
explain
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
explain
select ss_store_sk, sum(case when d_day_name = 'Sunday' then ss_sales_price / ss_quantity end) sun
from pc_sales join pc_date on ss_sold_date_sk = d_date_sk
group by ss_store_sk;

explain
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

-- map joins, as on a cluster: the CASE above the join and the merging group by run vectorized in the map vertex,
-- also with every hash group by switched to pass-through
set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask.size=100000000;
set hive.optimize.partial.aggr.join.transpose=true;
explain vectorization detail
select ss_store_sk,
  sum(case when d_day_name = 'Sunday' then ss_sales_price else null end) sun,
  count(case when d_day_name = 'Tuesday' then ss_quantity end) tue_cnt,
  min(case when d_day_name = 'Wednesday' then ss_sales_price end) wed_min,
  max(case when d_day_name = 'Thursday' then ss_sales_price end) thu_max,
  sum(case when d_day_name = 'Friday' then ss_sales_price * ss_quantity else 0 end) fri_amt
from pc_sales join pc_date on ss_sold_date_sk = d_date_sk
where d_year = 2000
group by ss_store_sk;

explain
select s_store_name, s_store_id,
  sum(case when d_day_name = 'Sunday' then ss_sales_price else null end) sun,
  count(case when d_day_name = 'Saturday' then ss_quantity end) sat_cnt
from pc_date, pc_sales, pc_store
where d_date_sk = ss_sold_date_sk and s_store_sk = ss_store_sk and d_year = 2000
group by s_store_name, s_store_id;

set hive.optimize.partial.aggr.join.transpose=false;
select ss_store_sk,
  sum(case when d_day_name = 'Sunday' then ss_sales_price else null end) sun,
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
  count(case when d_day_name = 'Tuesday' then ss_quantity end) tue_cnt,
  min(case when d_day_name = 'Wednesday' then ss_sales_price end) wed_min,
  max(case when d_day_name = 'Thursday' then ss_sales_price end) thu_max,
  sum(case when d_day_name = 'Friday' then ss_sales_price * ss_quantity else 0 end) fri_amt
from pc_sales join pc_date on ss_sold_date_sk = d_date_sk
where d_year = 2000
group by ss_store_sk;
set hive.vectorized.execution.enabled=true;

set hive.optimize.partial.aggr.join.transpose.min.reduction=0.0;
set hive.groupby.mapaggr.checkinterval=0;
set hive.vectorized.groupby.maxentries=0;
select ss_store_sk,
  sum(case when d_day_name = 'Sunday' then ss_sales_price else null end) sun,
  count(case when d_day_name = 'Tuesday' then ss_quantity end) tue_cnt,
  min(case when d_day_name = 'Wednesday' then ss_sales_price end) wed_min,
  max(case when d_day_name = 'Thursday' then ss_sales_price end) thu_max,
  sum(case when d_day_name = 'Friday' then ss_sales_price * ss_quantity else 0 end) fri_amt
from pc_sales join pc_date on ss_sold_date_sk = d_date_sk
where d_year = 2000
group by ss_store_sk;
set hive.optimize.partial.aggr.join.transpose.min.reduction=0.8;
set hive.groupby.mapaggr.checkinterval=100000;
set hive.vectorized.groupby.maxentries=1000000;

set hive.optimize.partial.aggr.join.transpose=false;
select s_store_name, s_store_id,
  sum(case when d_day_name = 'Sunday' then ss_sales_price else null end) sun,
  count(case when d_day_name = 'Saturday' then ss_quantity end) sat_cnt
from pc_date, pc_sales, pc_store
where d_date_sk = ss_sold_date_sk and s_store_sk = ss_store_sk and d_year = 2000
group by s_store_name, s_store_id;

set hive.optimize.partial.aggr.join.transpose=true;
select s_store_name, s_store_id,
  sum(case when d_day_name = 'Sunday' then ss_sales_price else null end) sun,
  count(case when d_day_name = 'Saturday' then ss_quantity end) sat_cnt
from pc_date, pc_sales, pc_store
where d_date_sk = ss_sold_date_sk and s_store_sk = ss_store_sk and d_year = 2000
group by s_store_name, s_store_id;
set hive.auto.convert.join=false;

-- Edge cases. The date side has duplicate keys whose rows disagree on the condition (a partial joined to several
-- rows, gated per row), NULL conditions, and a third input on the same key holding the condition; store 7's values
-- are all NULL and store 8 sells only on days no CASE asks for.
create table pc_date_dup (d_date_sk int, d_day_name string);
insert into pc_date_dup select d_date_sk, d_day_name from pc_date;
insert into pc_date_dup select d_date_sk, if(d_day_name = 'Sunday', 'Monday', 'Sunday') from pc_date where d_date_sk % 3 = 0;
insert into pc_date_dup select d_date_sk, null from pc_date where d_date_sk % 4 = 0;
create table pc_flag (f_date_sk int, f_holiday boolean);
insert into pc_flag select d_date_sk, d_date_sk % 5 = 0 from pc_date;
insert into pc_sales select pos % 28 + 1, 7, null, null from (select posexplode(split(space(99), ' '))) t;
insert into pc_sales select 5, 8, cast(pos as decimal(7,2)), pos from (select posexplode(split(space(49), ' '))) t;
analyze table pc_date_dup compute statistics for columns;
analyze table pc_flag compute statistics for columns;
analyze table pc_sales compute statistics for columns;

-- the duplicate keys expand the join, which the push is otherwise not allowed to do
set hive.optimize.partial.aggr.join.transpose.max.join.expansion=10;
set hive.optimize.partial.aggr.join.transpose=true;
explain
select ss_store_sk,
  sum(case when d_day_name = 'Sunday' then ss_sales_price end) sun,
  sum(case when d_day_name = 'Monday' then ss_sales_price else 0 end) mon,
  count(case when d_day_name = 'Sunday' then ss_quantity end) sun_cnt,
  min(case when d_day_name = 'Monday' then ss_sales_price end) mon_min
from pc_sales join pc_date_dup on ss_sold_date_sk = d_date_sk
group by ss_store_sk;

set hive.optimize.partial.aggr.join.transpose=false;
select ss_store_sk,
  sum(case when d_day_name = 'Sunday' then ss_sales_price end) sun,
  sum(case when d_day_name = 'Monday' then ss_sales_price else 0 end) mon,
  count(case when d_day_name = 'Sunday' then ss_quantity end) sun_cnt,
  min(case when d_day_name = 'Monday' then ss_sales_price end) mon_min
from pc_sales join pc_date_dup on ss_sold_date_sk = d_date_sk
group by ss_store_sk;

set hive.optimize.partial.aggr.join.transpose=true;
select ss_store_sk,
  sum(case when d_day_name = 'Sunday' then ss_sales_price end) sun,
  sum(case when d_day_name = 'Monday' then ss_sales_price else 0 end) mon,
  count(case when d_day_name = 'Sunday' then ss_quantity end) sun_cnt,
  min(case when d_day_name = 'Monday' then ss_sales_price end) mon_min
from pc_sales join pc_date_dup on ss_sold_date_sk = d_date_sk
group by ss_store_sk;

set hive.vectorized.execution.enabled=false;
select ss_store_sk,
  sum(case when d_day_name = 'Sunday' then ss_sales_price end) sun,
  sum(case when d_day_name = 'Monday' then ss_sales_price else 0 end) mon,
  count(case when d_day_name = 'Sunday' then ss_quantity end) sun_cnt,
  min(case when d_day_name = 'Monday' then ss_sales_price end) mon_min
from pc_sales join pc_date_dup on ss_sold_date_sk = d_date_sk
group by ss_store_sk;
set hive.vectorized.execution.enabled=true;
set hive.optimize.partial.aggr.join.transpose.max.join.expansion=1.1;

-- the condition on a third input joined on the same key
explain
select ss_store_sk, sum(case when f_holiday then ss_sales_price end) holiday,
  count(case when f_holiday then ss_quantity end) holiday_cnt
from pc_sales join pc_date on ss_sold_date_sk = d_date_sk join pc_flag on ss_sold_date_sk = f_date_sk
where d_year = 2001
group by ss_store_sk;

set hive.optimize.partial.aggr.join.transpose=false;
select ss_store_sk, sum(case when f_holiday then ss_sales_price end) holiday,
  count(case when f_holiday then ss_quantity end) holiday_cnt
from pc_sales join pc_date on ss_sold_date_sk = d_date_sk join pc_flag on ss_sold_date_sk = f_date_sk
where d_year = 2001
group by ss_store_sk;

set hive.optimize.partial.aggr.join.transpose=true;
select ss_store_sk, sum(case when f_holiday then ss_sales_price end) holiday,
  count(case when f_holiday then ss_quantity end) holiday_cnt
from pc_sales join pc_date on ss_sold_date_sk = d_date_sk join pc_flag on ss_sold_date_sk = f_date_sk
where d_year = 2001
group by ss_store_sk;

-- not split (no partial group by below the join): two WHENs, a condition over both inputs, MIN with ELSE 0
explain
select ss_store_sk,
  sum(case when d_day_name = 'Sunday' then ss_sales_price when d_day_name = 'Monday' then ss_quantity end) s
from pc_sales join pc_date on ss_sold_date_sk = d_date_sk
group by ss_store_sk;

explain
select ss_store_sk, sum(case when d_week_seq > ss_quantity then ss_sales_price end) s
from pc_sales join pc_date on ss_sold_date_sk = d_date_sk
group by ss_store_sk;

explain
select ss_store_sk, min(case when d_day_name = 'Sunday' then ss_sales_price else 0 end) s
from pc_sales join pc_date on ss_sold_date_sk = d_date_sk
group by ss_store_sk;

-- a CASE whose type differs from its value's (without CBO, nothing casts the int value to the decimal of ELSE 0.0):
-- the value is cast to the CASE's type below the join
set hive.cbo.enable=false;
explain
select ss_store_sk, sum(case when d_day_name = 'Sunday' then ss_quantity else 0.0 end) s
from pc_sales join pc_date on ss_sold_date_sk = d_date_sk
group by ss_store_sk;

set hive.optimize.partial.aggr.join.transpose=false;
select ss_store_sk, sum(case when d_day_name = 'Sunday' then ss_quantity else 0.0 end) s
from pc_sales join pc_date on ss_sold_date_sk = d_date_sk
group by ss_store_sk;

set hive.optimize.partial.aggr.join.transpose=true;
select ss_store_sk, sum(case when d_day_name = 'Sunday' then ss_quantity else 0.0 end) s
from pc_sales join pc_date on ss_sold_date_sk = d_date_sk
group by ss_store_sk;
set hive.cbo.enable=true;
