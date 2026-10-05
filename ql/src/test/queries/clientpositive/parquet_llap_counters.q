set hive.vectorized.execution.enabled=true;
set hive.compute.query.using.stats=false;
set hive.mapred.mode=nonstrict;
set hive.cbo.enable=false;
set hive.llap.io.enabled=true;
set hive.llap.io.parquet.native.enabled=true;
-- MiniLlapLocal reads the warehouse off the local FS, which has no inode ids, so without this
-- the reader gets no file key and caches nothing: every query below would be all-miss.
set hive.llap.cache.allow.synthetic.fileid=true;
set hive.map.aggr=false;
-- disabling map side aggregation as that can lead to different intermediate record counts

CREATE TABLE parquet_llap_staging_n0(t tinyint,
           si smallint,
           i int,
           b bigint,
           f float,
           d double,
           bo boolean,
           s string,
           ts timestamp,
           `dec` decimal(4,2),
           bin binary)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
TBLPROPERTIES ("hive.serialization.decode.binary.as.base64"="false");

LOAD DATA LOCAL INPATH '../../data/files/over1k' OVERWRITE INTO TABLE parquet_llap_staging_n0;
LOAD DATA LOCAL INPATH '../../data/files/over1k' INTO TABLE parquet_llap_staging_n0;

-- st1 is here so one table can drive both readers: a projection that stays in the flat columns
-- takes the native path, one that reaches into the struct declines it and falls back to the
-- vectorized Parquet reader, and both of them write the same file's chunks into the same cache.
CREATE TABLE parquet_llap_n0(t tinyint,
           si smallint,
           i int,
           b bigint,
           f float,
           d double,
           bo boolean,
           s string,
           da date,
           `dec` decimal(4,2),
           st1 struct<c1:int, c2:string>)
STORED AS PARQUET;

-- order by, so this lands as a single file and the byte counters below are the same every run.
insert overwrite table parquet_llap_n0 select t, si, i, b, f, d, bo, s, cast(ts as date) as da,
`dec`, named_struct('c1', i, 'c2', s) from parquet_llap_staging_n0
order by t, si, i, b, f, d, bo, s, da, `dec`;

SET hive.fetch.task.conversion=none;
SET hive.exec.post.hooks=org.apache.hadoop.hive.ql.hooks.PostExecTezSummaryPrinter;

-- Cold: every projected column chunk is a miss, so CACHE_MISS_BYTES is the whole projection and
-- CACHE_HIT_BYTES is 0.
select count(*) from parquet_llap_n0 where t > -100;

-- Warm: the same projection, 100% cache hit. This is the counter that says the chunks were cached
-- under a key the next query could find them by, not just read and dropped.
select count(*) from parquet_llap_n0 where t > -100;

-- A wider flat projection over the same file: t is a hit, the other three were never read before
-- and are misses, so a partially cached file is served part from cache and part from HDFS.
select count(*), sum(i), max(s) from parquet_llap_n0 where t > -100;

-- Reaching into the struct declines the native path: this one reads through LlapCacheAwareFs, which
-- caches the chunks it fetches itself. Both readers are now writing the same file's chunks, which
-- only works because they agree on how a chunk is cut into cache buffers.
select count(st1.c1) from parquet_llap_n0;

-- And back to the native path over a file both readers have written to: the point at which the two
-- of them have to have cut the chunks the same way.
select count(*), sum(i) from parquet_llap_n0 where t > -100;

DROP TABLE parquet_llap_staging_n0;
DROP TABLE parquet_llap_n0;
