-- A dynamically partitioned hash join leaves its output hash partitioned on its keys in its reducer. A following join
-- on the same keys joins each partition with the matching partition of its small input, a bucket map join over a
-- CUSTOM_SIMPLE_EDGE, instead of broadcasting the small input to every node; a join on other keys broadcasts. The
-- small input is hashed as the partitioned join's inputs are, so the results equal those of broadcast joins. A join
-- on a key of the same type that is not the partitioning key broadcasts too.
set hive.mapred.mode=nonstrict;
-- keep the written join order: the joins on c_customer_sk follow the customer x sales join
set hive.cbo.enable=false;
set hive.explain.user=false;
set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=1000000;
set hive.optimize.dynamic.partition.hashjoin=true;
set hive.convert.join.bucket.mapjoin.tez=true;
set hive.tez.auto.reducer.parallelism=false;
set hive.exec.reducers.bytes.per.reducer=40000000;
set hive.exec.reducers.max=4;

create table customer_cp (c_customer_sk bigint, c_name string);
create table sales_cp (s_customer_sk bigint, s_amount int, s_item_sk bigint);
create table web_cp (w_customer_sk bigint);
create table catalog_cp (x_customer_sk bigint);
create table names_cp (n_name string, n_rank int);
create table item_cp (i_item_sk bigint);

insert into customer_cp values (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e'), (6, 'f'), (7, 'g'), (8, 'h'),
  (9, 'i'), (10, 'j'), (11, 'k'), (12, 'l'), (13, 'm'), (14, 'n'), (15, 'o'), (16, 'p'), (null, 'q');
insert into sales_cp values (1, 10, 116), (1, 11, 115), (2, 20, 114), (3, 30, 113), (4, 40, 112), (5, 50, 111),
  (6, 60, 110), (7, 70, 109), (8, 80, 108), (9, 90, 107), (10, 100, 106), (11, 110, 105), (12, 120, 104),
  (13, 130, 103), (14, 140, 102), (15, 150, 101), (16, 160, 116), (16, 161, 199), (17, 170, 101), (null, 0, null);
insert into web_cp values (1), (1), (3), (5), (7), (9), (11), (13), (15), (17), (19), (null);
insert into catalog_cp values (2), (4), (4), (6), (8), (10), (12), (14), (16), (18), (null);
insert into names_cp values ('a', 1), ('c', 3), ('e', 5), ('z', 26);
insert into item_cp values (101), (102), (103), (104), (105), (106), (107), (108), (109), (110), (111), (112), (113),
  (114), (115), (116), (117), (null);

-- statistics that make customer too large to broadcast but small enough per reducer: a dynamically partitioned hash
-- join with sales on 4 reducers; the other inputs are small enough to broadcast
alter table customer_cp update statistics set('numRows'='30000', 'rawDataSize'='600000');
alter table customer_cp update statistics for column c_customer_sk set('numDVs'='30000', 'numNulls'='1', 'lowValue'='1', 'highValue'='30000');
alter table customer_cp update statistics for column c_name set('numDVs'='30000', 'numNulls'='0', 'avgColLen'='10', 'maxColLen'='10');
alter table sales_cp update statistics set('numRows'='10000000', 'rawDataSize'='120000000');
alter table sales_cp update statistics for column s_customer_sk set('numDVs'='30000', 'numNulls'='1', 'lowValue'='1', 'highValue'='30000');
alter table sales_cp update statistics for column s_amount set('numDVs'='1000', 'numNulls'='0', 'lowValue'='0', 'highValue'='1000');
alter table sales_cp update statistics for column s_item_sk set('numDVs'='1000', 'numNulls'='1', 'lowValue'='1', 'highValue'='1000');
alter table web_cp update statistics set('numRows'='10000', 'rawDataSize'='80000');
alter table web_cp update statistics for column w_customer_sk set('numDVs'='5000', 'numNulls'='1', 'lowValue'='1', 'highValue'='1000000');
alter table catalog_cp update statistics set('numRows'='10000', 'rawDataSize'='80000');
alter table catalog_cp update statistics for column x_customer_sk set('numDVs'='5000', 'numNulls'='1', 'lowValue'='1', 'highValue'='1000000');
alter table names_cp update statistics set('numRows'='100', 'rawDataSize'='1500');
alter table names_cp update statistics for column n_name set('numDVs'='100', 'numNulls'='0', 'avgColLen'='10', 'maxColLen'='10');
alter table names_cp update statistics for column n_rank set('numDVs'='100', 'numNulls'='0', 'lowValue'='1', 'highValue'='100');
alter table item_cp update statistics set('numRows'='1000', 'rawDataSize'='8000');
alter table item_cp update statistics for column i_item_sk set('numDVs'='1000', 'numNulls'='1', 'lowValue'='1', 'highValue'='1000');

-- both left joins on c_customer_sk join the partitions of the customer x sales join; the ones on s_item_sk (same type
-- as the partitioning key, another column) and on c_name broadcast
explain
select c.c_customer_sk, c.c_name, s.s_amount, w.w_customer_sk, x.x_customer_sk, i.i_item_sk, n.n_rank
from customer_cp c
join sales_cp s on c.c_customer_sk = s.s_customer_sk
left join (select distinct w_customer_sk from web_cp) w on c.c_customer_sk = w.w_customer_sk
left join (select distinct x_customer_sk from catalog_cp) x on c.c_customer_sk = x.x_customer_sk
left join (select distinct i_item_sk from item_cp) i on s.s_item_sk = i.i_item_sk
left join names_cp n on c.c_name = n.n_name;

select c.c_customer_sk, c.c_name, s.s_amount, w.w_customer_sk, x.x_customer_sk, i.i_item_sk, n.n_rank
from customer_cp c
join sales_cp s on c.c_customer_sk = s.s_customer_sk
left join (select distinct w_customer_sk from web_cp) w on c.c_customer_sk = w.w_customer_sk
left join (select distinct x_customer_sk from catalog_cp) x on c.c_customer_sk = x.x_customer_sk
left join (select distinct i_item_sk from item_cp) i on s.s_item_sk = i.i_item_sk
left join names_cp n on c.c_name = n.n_name
order by c.c_customer_sk, s.s_amount;

-- a null-safe key broadcasts: the left join leaves sales 17's null customer key in the partition of 17, while the
-- small input's null is in the partition of null (with a partitioned join, sales 17 would miss it)
explain
select s.s_customer_sk, s.s_amount, c.c_customer_sk, c.c_name, w.w_hit
from sales_cp s
left join customer_cp c on s.s_customer_sk = c.c_customer_sk
left join (select distinct w_customer_sk, 1 as w_hit from web_cp) w on c.c_customer_sk <=> w.w_customer_sk;

select s.s_customer_sk, s.s_amount, c.c_customer_sk, c.c_name, w.w_hit
from sales_cp s
left join customer_cp c on s.s_customer_sk = c.c_customer_sk
left join (select distinct w_customer_sk, 1 as w_hit from web_cp) w on c.c_customer_sk <=> w.w_customer_sk
order by s.s_customer_sk, s.s_amount;

-- the same with broadcast joins: the same rows
set hive.convert.join.bucket.mapjoin.tez=false;
select c.c_customer_sk, c.c_name, s.s_amount, w.w_customer_sk, x.x_customer_sk, i.i_item_sk, n.n_rank
from customer_cp c
join sales_cp s on c.c_customer_sk = s.s_customer_sk
left join (select distinct w_customer_sk from web_cp) w on c.c_customer_sk = w.w_customer_sk
left join (select distinct x_customer_sk from catalog_cp) x on c.c_customer_sk = x.x_customer_sk
left join (select distinct i_item_sk from item_cp) i on s.s_item_sk = i.i_item_sk
left join names_cp n on c.c_name = n.n_name
order by c.c_customer_sk, s.s_amount;

select s.s_customer_sk, s.s_amount, c.c_customer_sk, c.c_name, w.w_hit
from sales_cp s
left join customer_cp c on s.s_customer_sk = c.c_customer_sk
left join (select distinct w_customer_sk, 1 as w_hit from web_cp) w on c.c_customer_sk <=> w.w_customer_sk
order by s.s_customer_sk, s.s_amount;

-- two keys: the aggregate's keys are (y, x) and the partitioned join hashes (x, y). A following join on (x, y) joins by
-- partition; one on (y, x) broadcasts, since its small input would be hashed over (y, x) and miss the partitions
create table g2_cp (x bigint, y bigint, v int);
create table b2_cp (x bigint, y bigint, w int);
create table c2_cp (x bigint, y bigint, n string);
insert into g2_cp select pos % 53, pos % 47 + 100, pos % 7 from (select posexplode(split(space(2999), ' ')) as (pos, z)) t;
insert into b2_cp select pos % 53, pos % 47 + 100, pos % 5 from (select posexplode(split(space(2490), ' ')) as (pos, z)) t;
insert into c2_cp select pos % 53, pos % 47 + 100, concat('n', pos) from (select posexplode(split(space(2490), ' ')) as (pos, z)) t;
alter table g2_cp update statistics set('numRows'='3000000', 'rawDataSize'='60000000');
alter table g2_cp update statistics for column x set('numDVs'='2000', 'numNulls'='0', 'lowValue'='0', 'highValue'='2000');
alter table g2_cp update statistics for column y set('numDVs'='1500', 'numNulls'='0', 'lowValue'='100', 'highValue'='1600');
alter table b2_cp update statistics set('numRows'='30000', 'rawDataSize'='600000');
alter table b2_cp update statistics for column x set('numDVs'='200', 'numNulls'='0', 'lowValue'='0', 'highValue'='200');
alter table b2_cp update statistics for column y set('numDVs'='150', 'numNulls'='0', 'lowValue'='100', 'highValue'='250');

set hive.convert.join.bucket.mapjoin.tez=true;
explain
select g.x, g.y, g.s, b.w, c.n
from (select y, x, sum(v) s from g2_cp group by y, x) g
join b2_cp b on g.x = b.x and g.y = b.y
left join c2_cp c on g.x = c.x and g.y = c.y;

select count(*), count(c.n), sum(pmod(hash(g.x, g.y, g.s, b.w, c.n), 1000003))
from (select y, x, sum(v) s from g2_cp group by y, x) g
join b2_cp b on g.x = b.x and g.y = b.y
left join c2_cp c on g.x = c.x and g.y = c.y;

explain
select g.x, g.y, g.s, b.w, c.n
from (select y, x, sum(v) s from g2_cp group by y, x) g
join b2_cp b on g.x = b.x and g.y = b.y
left join c2_cp c on g.y = c.y and g.x = c.x;

select count(*), count(c.n), sum(pmod(hash(g.x, g.y, g.s, b.w, c.n), 1000003))
from (select y, x, sum(v) s from g2_cp group by y, x) g
join b2_cp b on g.x = b.x and g.y = b.y
left join c2_cp c on g.y = c.y and g.x = c.x;

-- the same with broadcast joins: the same rows
set hive.convert.join.bucket.mapjoin.tez=false;
select count(*), count(c.n), sum(pmod(hash(g.x, g.y, g.s, b.w, c.n), 1000003))
from (select y, x, sum(v) s from g2_cp group by y, x) g
join b2_cp b on g.x = b.x and g.y = b.y
left join c2_cp c on g.y = c.y and g.x = c.x;
