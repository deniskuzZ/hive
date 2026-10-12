--! qt:dataset:src
-- SORT_QUERY_RESULTS
-- GROUP BYs that read a sorted shuffle with hive.optimize.groupby.unsorted.shuffle: a ROLLUP, whose merge carries
-- the grouping set id, and a constant key; the results must be identical with the flag off and on.

create table gux (k1 int, k2 int, s string, v int);
insert into gux select pos % 17, pos % 23, concat('s', pos % 31), pos % 7
from (select posexplode(split(space(999), ' '))) t;

set hive.auto.convert.join=false;
set hive.remove.orderby.in.subquery=false;

set hive.optimize.groupby.unsorted.shuffle=true;
explain
select k1, k2, sum(v) from gux group by k1, k2 with rollup order by k1, k2;
explain
select c, k1, max(s) from (select 'constant' c, k1, s from gux) t group by c, k1 order by c, k1;
explain
select constant_col, key, max(value) from (select 'constant' as constant_col, key, value from src distribute by constant_col, key sort by constant_col, key, value) a group by constant_col, key order by constant_col, key limit 10;

set hive.optimize.groupby.unsorted.shuffle=false;
-- the grouping set id pruned from the output
select k1, k2, sum(v) from gux group by k1, k2 with rollup order by k1, k2;
select k1, grouping(k1), sum(v), count(*) from gux group by k1 with rollup order by k1;
select k1, k2, max(s) from gux group by cube(k1, k2) order by k1, k2;
select c, k1, max(s) from (select 'constant' c, k1, s from gux) t group by c, k1 order by c, k1;
select constant_col, key, max(value) from (select 'constant' as constant_col, key, value from src distribute by constant_col, key sort by constant_col, key, value) a group by constant_col, key order by constant_col, key limit 10;

set hive.optimize.groupby.unsorted.shuffle=true;
select k1, k2, sum(v) from gux group by k1, k2 with rollup order by k1, k2;
select k1, grouping(k1), sum(v), count(*) from gux group by k1 with rollup order by k1;
select k1, k2, max(s) from gux group by cube(k1, k2) order by k1, k2;
select c, k1, max(s) from (select 'constant' c, k1, s from gux) t group by c, k1 order by c, k1;
select constant_col, key, max(value) from (select 'constant' as constant_col, key, value from src distribute by constant_col, key sort by constant_col, key, value) a group by constant_col, key order by constant_col, key limit 10;
