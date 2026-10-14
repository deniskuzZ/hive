-- A join keeps each column's fraction of nulls: after a selective join a nullable column still counts toward the
-- join's data size, the size a map join's hash table is planned with.
set hive.stats.fetch.column.stats=true;

create table nn_fact (k bigint, v1 bigint, v2 bigint, v3 bigint);
create table nn_dim (k bigint, f int);

alter table nn_fact update statistics set('numRows'='10000000','rawDataSize'='320000000');
alter table nn_fact update statistics for column k set('numDVs'='1000','numNulls'='0','lowValue'='1','highValue'='1000');
alter table nn_fact update statistics for column v1 set('numDVs'='100000','numNulls'='500000','lowValue'='1','highValue'='100000');
alter table nn_fact update statistics for column v2 set('numDVs'='100000','numNulls'='500000','lowValue'='1','highValue'='100000');
alter table nn_fact update statistics for column v3 set('numDVs'='100000','numNulls'='500000','lowValue'='1','highValue'='100000');
alter table nn_dim update statistics set('numRows'='1000','rawDataSize'='12000');
alter table nn_dim update statistics for column k set('numDVs'='1000','numNulls'='0','lowValue'='1','highValue'='1000');
alter table nn_dim update statistics for column f set('numDVs'='1000','numNulls'='0','lowValue'='1','highValue'='1000');

-- 10000 of 10M rows survive the join, 5% of v1, v2, v3 null: about 8 + 3 * 0.95 * 8 bytes a row
explain select f.k, f.v1, f.v2, f.v3 from nn_fact f join nn_dim d on f.k = d.k where d.f = 7;

-- An outer join counts the outer rows whose key finds no match from the key statistics of both sides: 100 of the 500
-- left keys are not among the 400 right keys, so 200 of the 1000 left rows (2 a key), not all of them
create table nn_left (k bigint, v bigint);
create table nn_right (k bigint, w bigint);
alter table nn_left update statistics set('numRows'='1000','rawDataSize'='16000');
alter table nn_left update statistics for column k set('numDVs'='500','numNulls'='0','lowValue'='1','highValue'='500');
alter table nn_left update statistics for column v set('numDVs'='1000','numNulls'='0','lowValue'='1','highValue'='1000');
alter table nn_right update statistics set('numRows'='1000','rawDataSize'='16000');
alter table nn_right update statistics for column k set('numDVs'='400','numNulls'='0','lowValue'='1','highValue'='400');
alter table nn_right update statistics for column w set('numDVs'='1000','numNulls'='0','lowValue'='1','highValue'='1000');

explain select l.k, l.v, r.w from nn_left l left join nn_right r on l.k = r.k;
