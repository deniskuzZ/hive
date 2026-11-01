-- The rows of a BETWEEN over a column with range statistics and no histogram: with
-- hive.stats.filter.between.range, the overlap of its bounds with the column's range; without, two comparisons each
-- estimated over the whole range and multiplied (a 30-day range of 200 years' dates comes out as a quarter of them).
-- The 10 TB date_dim statistics.
set hive.explain.user=false;
set hive.fetch.task.conversion=none;
create table sb_date_dim (d_date_sk bigint, d_date date, d_month_seq int);
alter table sb_date_dim update statistics set('numRows'='73049','rawDataSize'='3449552');
alter table sb_date_dim update statistics for column d_date_sk set('numDVs'='67850','numNulls'='0','lowValue'='2415022','highValue'='2488070');
alter table sb_date_dim update statistics for column d_date set('numDVs'='76511','numNulls'='0','lowValue'='-25566','highValue'='47482');
alter table sb_date_dim update statistics for column d_month_seq set('numDVs'='2398','numNulls'='0','lowValue'='0','highValue'='2400');

set hive.stats.filter.between.range=false;
explain select d_date_sk from sb_date_dim where d_date between date '2002-08-06' and date '2002-09-05';
explain select d_date_sk from sb_date_dim where d_month_seq between 1200 and 1211;

set hive.stats.filter.between.range=true;
-- 30 of 73,048 days
explain select d_date_sk from sb_date_dim where d_date between date '2002-08-06' and date '2002-09-05';
-- an integer range of 100 values or fewer stays an IN of them: 12 months
explain select d_date_sk from sb_date_dim where d_month_seq between 1200 and 1211;
-- a bound past the column's range is cut to it: 100 of 2,400 months
explain select d_date_sk from sb_date_dim where d_month_seq between 2300 and 9999;
-- and a lower one: 99 of 2,400 months
explain select d_date_sk from sb_date_dim where d_month_seq between -500 and 99;
-- no overlap: no rows
explain select d_date_sk from sb_date_dim where d_month_seq between 5000 and 6000;
-- equal bounds are an equality, estimated as one
explain select d_date_sk from sb_date_dim where d_month_seq between 1200 and 1200;
-- NOT BETWEEN keeps its estimate
explain select d_date_sk from sb_date_dim where d_month_seq not between 1200 and 1211;
