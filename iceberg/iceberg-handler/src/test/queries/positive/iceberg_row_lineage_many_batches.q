-- Row lineage ids of a file read in several vectorized batches: every row keeps its own id
set hive.fetch.task.conversion=none;

create table ice_rl (id int) stored by iceberg tblproperties ('format-version'='3');
insert into ice_rl select pos from (select posexplode(split(space(2999), ' '))) t;

set hive.vectorized.execution.enabled=true;
select count(*), count(distinct ROW__LINEAGE__ID), min(ROW__LINEAGE__ID), max(ROW__LINEAGE__ID) from ice_rl;
select count(*) from ice_rl where ROW__LINEAGE__ID = id;

set hive.vectorized.execution.enabled=false;
select count(*), count(distinct ROW__LINEAGE__ID), min(ROW__LINEAGE__ID), max(ROW__LINEAGE__ID) from ice_rl;
select count(*) from ice_rl where ROW__LINEAGE__ID = id;

-- a rewritten file stores the ids it carries, read in several batches too
create table ice_rl_cow (id int) stored by iceberg
tblproperties ('format-version'='3', 'write.update.mode'='copy-on-write');
insert into ice_rl_cow select pos from (select posexplode(split(space(2999), ' '))) t;
update ice_rl_cow set id = id + 10000 where id % 1000 = 0;

set hive.vectorized.execution.enabled=true;
select count(*), count(distinct ROW__LINEAGE__ID), min(ROW__LINEAGE__ID), max(ROW__LINEAGE__ID) from ice_rl_cow;
select count(*) from ice_rl_cow where ROW__LINEAGE__ID = id % 10000;

set hive.vectorized.execution.enabled=false;
select count(*), count(distinct ROW__LINEAGE__ID), min(ROW__LINEAGE__ID), max(ROW__LINEAGE__ID) from ice_rl_cow;
select count(*) from ice_rl_cow where ROW__LINEAGE__ID = id % 10000;
