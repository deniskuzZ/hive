--! qt:dataset:src1
--! qt:dataset:src
-- An n-way map join on one key whose last condition is outer, with ON clause filters on both sides (filter
-- tags): its results must match the merge join, vectorized or not.
set hive.merge.nway.joins=true;

-- inner_then_left_outer
set hive.auto.convert.join=true;
set hive.vectorized.execution.enabled=true;
explain
SELECT count(*), sum(hash(src1.key, src1.value, src2.key, src2.value, src3.key, src3.value))
FROM src1 JOIN src src2 ON (src1.key = src2.key)
  LEFT OUTER JOIN src src3 ON (src2.key = src3.key AND src2.key < 200 AND src3.key > 100);
SELECT count(*), sum(hash(src1.key, src1.value, src2.key, src2.value, src3.key, src3.value))
FROM src1 JOIN src src2 ON (src1.key = src2.key)
  LEFT OUTER JOIN src src3 ON (src2.key = src3.key AND src2.key < 200 AND src3.key > 100);
set hive.vectorized.execution.enabled=false;
SELECT count(*), sum(hash(src1.key, src1.value, src2.key, src2.value, src3.key, src3.value))
FROM src1 JOIN src src2 ON (src1.key = src2.key)
  LEFT OUTER JOIN src src3 ON (src2.key = src3.key AND src2.key < 200 AND src3.key > 100);
set hive.auto.convert.join=false;
SELECT count(*), sum(hash(src1.key, src1.value, src2.key, src2.value, src3.key, src3.value))
FROM src1 JOIN src src2 ON (src1.key = src2.key)
  LEFT OUTER JOIN src src3 ON (src2.key = src3.key AND src2.key < 200 AND src3.key > 100);

-- inner_then_right_outer
set hive.auto.convert.join=true;
set hive.vectorized.execution.enabled=true;
explain
SELECT count(*), sum(hash(src1.key, src1.value, src2.key, src2.value, src3.key, src3.value))
FROM src1 JOIN src1 src2 ON (src1.key = src2.key AND src1.key < 300)
  RIGHT OUTER JOIN src src3 ON (src2.key = src3.key AND src3.key < 100 AND src2.key > 10);
SELECT count(*), sum(hash(src1.key, src1.value, src2.key, src2.value, src3.key, src3.value))
FROM src1 JOIN src1 src2 ON (src1.key = src2.key AND src1.key < 300)
  RIGHT OUTER JOIN src src3 ON (src2.key = src3.key AND src3.key < 100 AND src2.key > 10);
set hive.vectorized.execution.enabled=false;
SELECT count(*), sum(hash(src1.key, src1.value, src2.key, src2.value, src3.key, src3.value))
FROM src1 JOIN src1 src2 ON (src1.key = src2.key AND src1.key < 300)
  RIGHT OUTER JOIN src src3 ON (src2.key = src3.key AND src3.key < 100 AND src2.key > 10);
set hive.auto.convert.join=false;
SELECT count(*), sum(hash(src1.key, src1.value, src2.key, src2.value, src3.key, src3.value))
FROM src1 JOIN src1 src2 ON (src1.key = src2.key AND src1.key < 300)
  RIGHT OUTER JOIN src src3 ON (src2.key = src3.key AND src3.key < 100 AND src2.key > 10);

-- left_outer_twice
set hive.auto.convert.join=true;
set hive.vectorized.execution.enabled=true;
explain
SELECT count(*), sum(hash(src1.key, src1.value, src2.key, src2.value, src3.key, src3.value))
FROM src1 LEFT OUTER JOIN src src2 ON (src1.key = src2.key AND src1.key < 200)
  LEFT OUTER JOIN src src3 ON (src1.key = src3.key AND src3.key > 50 AND src1.value <> '');
SELECT count(*), sum(hash(src1.key, src1.value, src2.key, src2.value, src3.key, src3.value))
FROM src1 LEFT OUTER JOIN src src2 ON (src1.key = src2.key AND src1.key < 200)
  LEFT OUTER JOIN src src3 ON (src1.key = src3.key AND src3.key > 50 AND src1.value <> '');
set hive.vectorized.execution.enabled=false;
SELECT count(*), sum(hash(src1.key, src1.value, src2.key, src2.value, src3.key, src3.value))
FROM src1 LEFT OUTER JOIN src src2 ON (src1.key = src2.key AND src1.key < 200)
  LEFT OUTER JOIN src src3 ON (src1.key = src3.key AND src3.key > 50 AND src1.value <> '');
set hive.auto.convert.join=false;
SELECT count(*), sum(hash(src1.key, src1.value, src2.key, src2.value, src3.key, src3.value))
FROM src1 LEFT OUTER JOIN src src2 ON (src1.key = src2.key AND src1.key < 200)
  LEFT OUTER JOIN src src3 ON (src1.key = src3.key AND src3.key > 50 AND src1.value <> '');
