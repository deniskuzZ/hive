-- The deleted records of a V3 merge-on-read DML travel in columnar blocks: a batch of one data file's rows goes to its
-- reducer as one block, with FILE__PATH, PARTITION__SPEC__ID and PARTITION__KEY once. The DMLs leave the same rows
-- and one DV per data file with the columnar shuffle on and off.
set hive.explain.user=false;
set hive.vectorized.execution.enabled=true;
set hive.vectorized.execution.reduce.enabled=true;

create external table cs_on (id int, name string, part int) partitioned by spec (part)
stored by iceberg tblproperties ('format-version'='3');
create external table cs_off (id int, name string, part int) partitioned by spec (part)
stored by iceberg tblproperties ('format-version'='3');
insert into cs_on select pos, concat('n', pos), pos % 3 from (select posexplode(split(space(2999), ' '))) t;
insert into cs_off select * from cs_on;

create external table cs_src (id int) stored by iceberg tblproperties ('format-version'='3');
insert into cs_src select pos * 5 from (select posexplode(split(space(599), ' '))) t;

set hive.vectorized.execution.columnar.shuffle.enabled=true;
explain vectorization only operator delete from cs_on where id % 7 = 0;
delete from cs_on where id % 7 = 0;
update cs_on set name = 'u' where id % 11 = 0;
merge into cs_on t using cs_src s on t.id = s.id
when matched and t.id % 2 = 0 then update set name = 'm'
when matched then delete
when not matched then insert values (s.id, 'i', s.id % 3);

set hive.vectorized.execution.columnar.shuffle.enabled=false;
delete from cs_off where id % 7 = 0;
update cs_off set name = 'u' where id % 11 = 0;
merge into cs_off t using cs_src s on t.id = s.id
when matched and t.id % 2 = 0 then update set name = 'm'
when matched then delete
when not matched then insert values (s.id, 'i', s.id % 3);

select count(*), count(distinct id), sum(id) from cs_on;
select count(*) from cs_on a full outer join cs_off b on a.id = b.id and a.name = b.name and a.part = b.part
where a.id is null or b.id is null;
select count(*), count(distinct referenced_data_file) from default.cs_on.delete_files;
select count(*), count(distinct referenced_data_file) from default.cs_off.delete_files;
