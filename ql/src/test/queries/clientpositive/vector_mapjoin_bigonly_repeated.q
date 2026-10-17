-- An inner join whose small table contributes no columns forwards a batch whose join key repeats once per duplicate
-- key. A second such join below it must still see the whole batch on every forward: each big row matches 2 x 2 rows.
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.auto.convert.join=true;
set hive.vectorized.execution.enabled=true;

create table bigonly_big (v int) partitioned by (k int) stored as orc;
create table bigonly_dup1 (k int) stored as orc;
create table bigonly_dup2 (k int) stored as orc;

insert into bigonly_big partition (k = 37) values (1), (2), (3);
insert into bigonly_dup1 values (37), (37), (58);
insert into bigonly_dup2 values (37), (37), (67);
-- the partitioned table is the big table: its join key, the partition column, repeats in every batch
alter table bigonly_big partition (k = 37) update statistics set('numRows'='10000000', 'rawDataSize'='40000000');

explain vectorization detail
select b.v from bigonly_big b
join bigonly_dup1 d1 on b.k = d1.k
join bigonly_dup2 d2 on b.k = (d2.k + 0);

select b.v from bigonly_big b
join bigonly_dup1 d1 on b.k = d1.k
join bigonly_dup2 d2 on b.k = (d2.k + 0)
order by b.v;

-- the same without vectorization: the reference result
set hive.vectorized.execution.enabled=false;
select b.v from bigonly_big b
join bigonly_dup1 d1 on b.k = d1.k
join bigonly_dup2 d2 on b.k = (d2.k + 0)
order by b.v;
