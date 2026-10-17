-- SORT_QUERY_RESULTS
-- a join on a column of a broadcast join's small side (d.x, _col0 like the GROUP BY key) must not be taken as
-- partitioned by the GROUP BY
set hive.mapred.mode=nonstrict;
set hive.cbo.enable=false;
set hive.explain.user=false;
set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=1000000;
set hive.optimize.dynamic.partition.hashjoin=true;
set hive.tez.auto.reducer.parallelism=false;
set hive.exec.reducers.bytes.per.reducer=40000000;
set hive.exec.reducers.max=4;

create table f_sk (k bigint, v int, pad string);
create table d_sk (x bigint, k bigint);
create table b_sk (x bigint, name string);
insert into f_sk select pos % 16 + 1, pos, 'ppppp' from (select posexplode(split(space(199), ' '))) t;
insert into d_sk select pos + 101, pos + 1 from (select posexplode(split(space(15), ' '))) t;
insert into b_sk select pos + 101, concat('n', pos) from (select posexplode(split(space(15), ' '))) t;

alter table f_sk update statistics set('numRows'='10000000', 'rawDataSize'='5000000000');
alter table f_sk update statistics for column k set('numDVs'='3000000', 'numNulls'='0', 'lowValue'='1', 'highValue'='3000000');
alter table f_sk update statistics for column v set('numDVs'='1000', 'numNulls'='0', 'lowValue'='0', 'highValue'='1000');
alter table f_sk update statistics for column pad set('numDVs'='1000000', 'numNulls'='0', 'avgColLen'='500', 'maxColLen'='500');
alter table d_sk update statistics set('numRows'='20000', 'rawDataSize'='320000');
alter table d_sk update statistics for column x set('numDVs'='20000', 'numNulls'='0', 'lowValue'='1', 'highValue'='20000');
alter table d_sk update statistics for column k set('numDVs'='20000', 'numNulls'='0', 'lowValue'='1', 'highValue'='3000000');
alter table b_sk update statistics set('numRows'='60000', 'rawDataSize'='1560000');
alter table b_sk update statistics for column x set('numDVs'='60000', 'numNulls'='0', 'lowValue'='1', 'highValue'='60000');
alter table b_sk update statistics for column name set('numDVs'='60000', 'numNulls'='0', 'avgColLen'='10', 'maxColLen'='10');

set hive.convert.join.bucket.mapjoin.tez=true;
explain
select g.k, g.s, length(g.p), d.x, b.name
from (select k, sum(v) s, max(pad) p from f_sk group by k) g join (select x, k from d_sk) d on g.k = d.k
join b_sk b on d.x = b.x;
select g.k, g.s, length(g.p), d.x, b.name
from (select k, sum(v) s, max(pad) p from f_sk group by k) g join (select x, k from d_sk) d on g.k = d.k
join b_sk b on d.x = b.x;
set hive.convert.join.bucket.mapjoin.tez=false;
select g.k, g.s, length(g.p), d.x, b.name
from (select k, sum(v) s, max(pad) p from f_sk group by k) g join (select x, k from d_sk) d on g.k = d.k
join b_sk b on d.x = b.x;
