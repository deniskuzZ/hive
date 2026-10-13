--! qt:dataset:alltypesorc
-- The native LLAP Parquet reader on splits that carry their file's listed status: same results as the stock reader

set hive.vectorized.execution.enabled=true;
set hive.llap.io.enabled=true;

create table pnls stored as parquet as
select cint, cbigint, cdouble, cstring1, ctimestamp1, cboolean1 from alltypesorc;

set hive.llap.io.parquet.native.enabled=false;
select count(*), sum(hash(cint, cbigint, cdouble, cstring1, ctimestamp1, cboolean1)) from pnls;
select cboolean1, count(*), sum(cbigint), min(cstring1), max(cdouble) from pnls group by cboolean1 order by cboolean1;

set hive.llap.io.parquet.native.enabled=true;
select count(*), sum(hash(cint, cbigint, cdouble, cstring1, ctimestamp1, cboolean1)) from pnls;
select cboolean1, count(*), sum(cbigint), min(cstring1), max(cdouble) from pnls group by cboolean1 order by cboolean1;
-- again, from the footer and data cache
select count(*), sum(hash(cint, cbigint, cdouble, cstring1, ctimestamp1, cboolean1)) from pnls;
