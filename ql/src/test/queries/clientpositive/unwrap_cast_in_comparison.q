-- SORT_QUERY_RESULTS
-- A DATE cast to TIMESTAMP and compared with TIMESTAMP literals is compared as a DATE: midnight literals map to their
-- date, a literal with a time of day moves a bound to the date it falls in, and an equality with it matches no date.
-- Each query runs with the rule off and on; the results must be identical.
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;

create table uc_d (d date, v int);
insert into uc_d select date_add(cast('2002-08-01' as date), pos), pos from (select posexplode(split(space(19), ' '))) t
  union all select null, -1;

-- TPC-DS's form: date + interval is a TIMESTAMP
set hive.optimize.unwrap.cast.in.comparison=true;
explain cbo
select d, v from uc_d where d between cast('2002-08-06' as date) and (cast('2002-08-06' as date) + 3 days);

set hive.optimize.unwrap.cast.in.comparison=false;
select d, v from uc_d where d between cast('2002-08-06' as date) and (cast('2002-08-06' as date) + 3 days);
set hive.optimize.unwrap.cast.in.comparison=true;
select d, v from uc_d where d between cast('2002-08-06' as date) and (cast('2002-08-06' as date) + 3 days);

-- bounds with a time of day, inclusive and exclusive
explain cbo
select d, v from uc_d
where cast(d as timestamp) > timestamp '2002-08-04 12:00:00' and cast(d as timestamp) < timestamp '2002-08-09 12:00:00';

set hive.optimize.unwrap.cast.in.comparison=false;
select d, v from uc_d
where cast(d as timestamp) > timestamp '2002-08-04 12:00:00' and cast(d as timestamp) < timestamp '2002-08-09 12:00:00';
set hive.optimize.unwrap.cast.in.comparison=true;
select d, v from uc_d
where cast(d as timestamp) > timestamp '2002-08-04 12:00:00' and cast(d as timestamp) < timestamp '2002-08-09 12:00:00';

set hive.optimize.unwrap.cast.in.comparison=false;
select d, v from uc_d
where cast(d as timestamp) >= timestamp '2002-08-04 12:00:00' and cast(d as timestamp) <= timestamp '2002-08-09 12:00:00';
set hive.optimize.unwrap.cast.in.comparison=true;
select d, v from uc_d
where cast(d as timestamp) >= timestamp '2002-08-04 12:00:00' and cast(d as timestamp) <= timestamp '2002-08-09 12:00:00';

-- single comparisons, the literal on either side
set hive.optimize.unwrap.cast.in.comparison=false;
select d, v from uc_d where cast(d as timestamp) < timestamp '2002-08-04 00:00:00';
set hive.optimize.unwrap.cast.in.comparison=true;
select d, v from uc_d where cast(d as timestamp) < timestamp '2002-08-04 00:00:00';

set hive.optimize.unwrap.cast.in.comparison=false;
select d, v from uc_d where cast(d as timestamp) < timestamp '2002-08-04 06:00:00';
set hive.optimize.unwrap.cast.in.comparison=true;
select d, v from uc_d where cast(d as timestamp) < timestamp '2002-08-04 06:00:00';

set hive.optimize.unwrap.cast.in.comparison=false;
select d, v from uc_d where timestamp '2002-08-15 06:00:00' <= cast(d as timestamp);
set hive.optimize.unwrap.cast.in.comparison=true;
select d, v from uc_d where timestamp '2002-08-15 06:00:00' <= cast(d as timestamp);

-- an equality: a midnight literal is a date, one with a time of day matches nothing
set hive.optimize.unwrap.cast.in.comparison=false;
select d, v from uc_d where cast(d as timestamp) = timestamp '2002-08-10 00:00:00'
  or cast(d as timestamp) = timestamp '2002-08-11 06:00:00';
set hive.optimize.unwrap.cast.in.comparison=true;
select d, v from uc_d where cast(d as timestamp) = timestamp '2002-08-10 00:00:00'
  or cast(d as timestamp) = timestamp '2002-08-11 06:00:00';

set hive.optimize.unwrap.cast.in.comparison=false;
select d, v from uc_d where cast(d as timestamp) in (timestamp '2002-08-12 00:00:00', timestamp '2002-08-13 06:00:00',
  timestamp '2002-08-14 00:00:00');
set hive.optimize.unwrap.cast.in.comparison=true;
select d, v from uc_d where cast(d as timestamp) in (timestamp '2002-08-12 00:00:00', timestamp '2002-08-13 06:00:00',
  timestamp '2002-08-14 00:00:00');

-- NOT BETWEEN: ranges without an upper or a lower bound; NULL stays out
set hive.optimize.unwrap.cast.in.comparison=false;
select d, v from uc_d where d not between cast('2002-08-06' as date) and (cast('2002-08-06' as date) + 10 days);
set hive.optimize.unwrap.cast.in.comparison=true;
select d, v from uc_d where d not between cast('2002-08-06' as date) and (cast('2002-08-06' as date) + 10 days);

-- ranges inside one date match no date; next to it, a range from noon starts at the next date
set hive.optimize.unwrap.cast.in.comparison=false;
select d, v from uc_d
where cast(d as timestamp) between timestamp '2002-08-10 01:00:00' and timestamp '2002-08-10 02:00:00'
  or cast(d as timestamp) between timestamp '2002-08-16 12:00:00' and timestamp '2002-08-18 00:00:00';
set hive.optimize.unwrap.cast.in.comparison=true;
select d, v from uc_d
where cast(d as timestamp) between timestamp '2002-08-10 01:00:00' and timestamp '2002-08-10 02:00:00'
  or cast(d as timestamp) between timestamp '2002-08-16 12:00:00' and timestamp '2002-08-18 00:00:00';

-- single equalities stay comparisons: a midnight literal is a date, one with a time of day matches nothing
explain cbo
select d, v from uc_d where cast(d as timestamp) = timestamp '2002-08-11 06:00:00';

set hive.optimize.unwrap.cast.in.comparison=false;
select d, v from uc_d where cast(d as timestamp) = timestamp '2002-08-11 06:00:00';
set hive.optimize.unwrap.cast.in.comparison=true;
select d, v from uc_d where cast(d as timestamp) = timestamp '2002-08-11 06:00:00';

set hive.optimize.unwrap.cast.in.comparison=false;
select d, v from uc_d where cast(d as timestamp) = timestamp '2002-08-11 00:00:00';
set hive.optimize.unwrap.cast.in.comparison=true;
select d, v from uc_d where cast(d as timestamp) = timestamp '2002-08-11 00:00:00';

set hive.optimize.unwrap.cast.in.comparison=false;
select d, v from uc_d where cast(d as timestamp) <> timestamp '2002-08-11 06:00:00';
set hive.optimize.unwrap.cast.in.comparison=true;
select d, v from uc_d where cast(d as timestamp) <> timestamp '2002-08-11 06:00:00';
