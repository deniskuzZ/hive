--! qt:dataset:src
-- Two map joins in one vertex reading the same small table, with the big table at different positions,
-- share one cached hash table. CBO is off to keep the written join order.
set hive.cbo.enable=false;
set hive.auto.convert.join=true;
set hive.optimize.shared.work.mapjoin.cache.reuse=true;

create table mj_fact (k int, v int);
create table mj_dim (k int, flag int);
insert into mj_dim values (1, 1), (2, 1), (3, 0), (4, 1), (5, 0);
insert into mj_fact select pmod(cast(key as int), 7), cast(key as int) from src;
analyze table mj_fact compute statistics for columns;
analyze table mj_dim compute statistics for columns;

explain
select a.s1, b.s2 from
  (select sum(f.v) s1, count(*) c1 from mj_dim d join mj_fact f on d.k = f.k where d.flag = 1) a,
  (select sum(f.v) s2, count(*) c2 from mj_fact f join mj_dim d on f.k = d.k where d.flag = 1) b;

select a.s1, a.c1, b.s2, b.c2 from
  (select sum(f.v) s1, count(*) c1 from mj_dim d join mj_fact f on d.k = f.k where d.flag = 1) a,
  (select sum(f.v) s2, count(*) c2 from mj_fact f join mj_dim d on f.k = d.k where d.flag = 1) b;

set hive.vectorized.execution.enabled=false;
select a.s1, a.c1, b.s2, b.c2 from
  (select sum(f.v) s1, count(*) c1 from mj_dim d join mj_fact f on d.k = f.k where d.flag = 1) a,
  (select sum(f.v) s2, count(*) c2 from mj_fact f join mj_dim d on f.k = d.k where d.flag = 1) b;

set hive.vectorized.execution.enabled=true;
set hive.auto.convert.join=false;
select a.s1, a.c1, b.s2, b.c2 from
  (select sum(f.v) s1, count(*) c1 from mj_dim d join mj_fact f on d.k = f.k where d.flag = 1) a,
  (select sum(f.v) s2, count(*) c2 from mj_fact f join mj_dim d on f.k = d.k where d.flag = 1) b;
