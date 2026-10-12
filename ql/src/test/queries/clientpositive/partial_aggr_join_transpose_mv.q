-- A materialized view over an aggregate of a join is still used for rewriting with
-- hive.optimize.partial.aggr.join.transpose: the view plan is loaded without pushing partial aggregates.
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.strict.checks.cartesian.product=false;
set hive.stats.fetch.column.stats=true;
set hive.materializedview.rewriting=true;
set hive.materializedview.rewriting.sql=false;

create table pam_fact (k int, x bigint) stored as orc tblproperties ('transactional'='true');
create table pam_dim (k int, v string) stored as orc tblproperties ('transactional'='true');
insert into pam_fact select pos % 200, pos from (select posexplode(split(space(3999), ' '))) t;
insert into pam_dim select pos, concat('v', pos % 7) from (select posexplode(split(space(199), ' '))) t;
analyze table pam_fact compute statistics for columns;
analyze table pam_dim compute statistics for columns;

set hive.optimize.partial.aggr.join.transpose=true;
create materialized view pam_mv as
select d.v, sum(f.x) s from pam_fact f join pam_dim d on f.k = d.k group by d.v;

set hive.optimize.partial.aggr.join.transpose=false;
explain cbo
select d.v, sum(f.x) s from pam_fact f join pam_dim d on f.k = d.k group by d.v;

set hive.optimize.partial.aggr.join.transpose=true;
explain cbo
select d.v, sum(f.x) s from pam_fact f join pam_dim d on f.k = d.k group by d.v;

drop materialized view pam_mv;
