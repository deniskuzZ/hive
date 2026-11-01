-- SORT_QUERY_RESULTS
-- An inner join over a LEFT outer join, its condition on the outer join's preserved side only, is moved below it:
-- (s LEFT JOIN r) JOIN d becomes (s JOIN d) LEFT JOIN r (TPC-DS q80: store_sales LEFT JOIN store_returns, then the
-- date, item and promotion joins). Each query runs with the rule off and on; the results must be identical.
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;

create table pj_s (s_k int, s_d int, s_i int, s_v int);
create table pj_r (r_k int, r_i int, r_v int);
create table pj_d (d_k int, d_flag string);
create table pj_i (i_k int, i_price int);
create table pj_x (x_k int, x_v int);

-- 40 sales, a return for every third (some with no matching item), NULL keys on both sides
insert into pj_s select pos, if(pos % 13 = 0, null, pos % 10), pos % 7, pos from (select posexplode(split(space(39), ' '))) t;
insert into pj_r select pos * 3, if(pos % 4 = 0, 99, (pos * 3) % 7), pos * 100 from (select posexplode(split(space(13), ' '))) t
  union all select null, 1, -1;
insert into pj_d select pos, if(pos < 4, 'Y', 'N') from (select posexplode(split(space(9), ' '))) t;
insert into pj_i select pos, pos * 10 from (select posexplode(split(space(6), ' '))) t;
-- three rows per key: a join on it multiplies the rows
insert into pj_x select pos % 4, pos from (select posexplode(split(space(11), ' '))) t;

analyze table pj_s compute statistics for columns;
analyze table pj_r compute statistics for columns;
analyze table pj_d compute statistics for columns;
analyze table pj_i compute statistics for columns;
analyze table pj_x compute statistics for columns;

-- q80's shape: the date and item joins move below the outer join
set hive.optimize.join.push.through.outer.join=true;
explain cbo
select s_k, s_v, r_v, i_price
from pj_s left outer join pj_r on (s_k = r_k and s_i = r_i), pj_d, pj_i
where s_d = d_k and d_flag = 'Y' and s_i = i_k and i_price > 20;

set hive.optimize.join.push.through.outer.join=false;
select s_k, s_v, r_v, i_price
from pj_s left outer join pj_r on (s_k = r_k and s_i = r_i), pj_d, pj_i
where s_d = d_k and d_flag = 'Y' and s_i = i_k and i_price > 20;

set hive.optimize.join.push.through.outer.join=true;
select s_k, s_v, r_v, i_price
from pj_s left outer join pj_r on (s_k = r_k and s_i = r_i), pj_d, pj_i
where s_d = d_k and d_flag = 'Y' and s_i = i_k and i_price > 20;

-- a residual condition on the outer join moves with it
explain cbo
select s_k, r_v from pj_s left outer join pj_r on (s_k = r_k and r_v > s_v), pj_d
where s_d = d_k and d_flag = 'Y';

set hive.optimize.join.push.through.outer.join=false;
select s_k, r_v from pj_s left outer join pj_r on (s_k = r_k and r_v > s_v), pj_d
where s_d = d_k and d_flag = 'Y';

set hive.optimize.join.push.through.outer.join=true;
select s_k, r_v from pj_s left outer join pj_r on (s_k = r_k and r_v > s_v), pj_d
where s_d = d_k and d_flag = 'Y';

-- two outer joins: the date join moves below both
explain cbo
select s_k, r1.r_v, r2.r_v from pj_s left outer join pj_r r1 on (s_k = r1.r_k)
  left outer join pj_r r2 on (s_i = r2.r_i and r2.r_v < 300), pj_d
where s_d = d_k and d_flag = 'Y';

set hive.optimize.join.push.through.outer.join=false;
select s_k, r1.r_v, r2.r_v from pj_s left outer join pj_r r1 on (s_k = r1.r_k)
  left outer join pj_r r2 on (s_i = r2.r_i and r2.r_v < 300), pj_d
where s_d = d_k and d_flag = 'Y';

set hive.optimize.join.push.through.outer.join=true;
select s_k, r1.r_v, r2.r_v from pj_s left outer join pj_r r1 on (s_k = r1.r_k)
  left outer join pj_r r2 on (s_i = r2.r_i and r2.r_v < 300), pj_d
where s_d = d_k and d_flag = 'Y';

-- not moved: the inner join's condition references the null-generating side (without rejecting its NULLs, else
-- the outer join becomes an inner one); its equality on the preserved side keeps it under the row gate, so only the
-- reference check stops it
explain cbo
select s_k, r_v, i_price from pj_s left outer join pj_r on (s_k = r_k), pj_i
where s_i = i_k and coalesce(r_v, 0) < i_price;

set hive.optimize.join.push.through.outer.join=false;
select s_k, r_v, i_price from pj_s left outer join pj_r on (s_k = r_k), pj_i
where s_i = i_k and coalesce(r_v, 0) < i_price;

set hive.optimize.join.push.through.outer.join=true;
select s_k, r_v, i_price from pj_s left outer join pj_r on (s_k = r_k), pj_i
where s_i = i_k and coalesce(r_v, 0) < i_price;

-- not moved: the inner join multiplies the preserved side's rows
explain cbo
select s_k, r_v, x_v from pj_s left outer join pj_r on (s_k = r_k), pj_x
where s_i = x_k;

set hive.optimize.join.push.through.outer.join=false;
select s_k, r_v, x_v from pj_s left outer join pj_r on (s_k = r_k), pj_x
where s_i = x_k;

set hive.optimize.join.push.through.outer.join=true;
select s_k, r_v, x_v from pj_s left outer join pj_r on (s_k = r_k), pj_x
where s_i = x_k;
