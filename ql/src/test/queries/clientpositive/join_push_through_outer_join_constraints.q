-- With referential constraints, HiveJoinSwapConstraintsRule moves a PK-FK join that keeps every row of its FK side
-- above the joins it does not filter; HiveJoinPushThroughOuterJoinRule moves an inner join below a LEFT outer join. The
-- two run in one rule collection: (A LEFT JOIN B) JOIN C, C the PK side of such a join with A, is not moved below the
-- outer join (it filters nothing there), so the collection reaches a fixpoint (before: moved and moved back forever).
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.optimize.join.push.through.outer.join=true;

create table pjc_f (f_k int, f_d int not null, f_v int);
create table pjc_b (b_k int, b_v int);
create table pjc_d (d_k int not null, d_x int);
alter table pjc_d add constraint pjc_d_pk primary key (d_k) disable novalidate rely;
alter table pjc_f add constraint pjc_f_fk foreign key (f_d) references pjc_d(d_k) disable novalidate rely;

insert into pjc_d select pos, pos % 3 from (select posexplode(split(space(9), ' '))) t;
insert into pjc_f select pos, pos % 10, pos from (select posexplode(split(space(39), ' '))) t;
insert into pjc_b select pos * 2, pos from (select posexplode(split(space(19), ' '))) t;
analyze table pjc_f compute statistics for columns;
analyze table pjc_b compute statistics for columns;
analyze table pjc_d compute statistics for columns;

-- C a GROUP BY of the PK with an aggregate
explain cbo
select f_k, b_v, d.mx
from pjc_f left outer join pjc_b on f_k = b_k
join (select d_k, max(d_x) mx from pjc_d group by d_k) d on f_d = d.d_k;

select f_k, b_v, d.mx
from pjc_f left outer join pjc_b on f_k = b_k
join (select d_k, max(d_x) mx from pjc_d group by d_k) d on f_d = d.d_k
order by f_k;

-- C the PK under a lossless cast
explain cbo
select f_k, b_v, d.d_x
from pjc_f left outer join pjc_b on f_k = b_k
join (select cast(d_k as bigint) d_k, d_x from pjc_d) d on f_d = d.d_k;

-- C the PK table
explain cbo
select f_k, b_v, d_x
from pjc_f left outer join pjc_b on f_k = b_k
join pjc_d on f_d = d_k;
