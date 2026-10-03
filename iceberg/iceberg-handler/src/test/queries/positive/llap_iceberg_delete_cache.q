-- Deletes read through LLAP IO are served from the LLAP metadata cache: once cached, a read still applies them after
-- the delete files are gone from storage.
set hive.llap.io.enabled=true;
set hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;

dfs -mkdir -p /tmp/iceberg_llap_delete_cache;
dfs -rm -r -f /tmp/iceberg_llap_delete_cache;

-- V3: deletion vectors
CREATE EXTERNAL TABLE ice_dv (id INT) STORED BY ICEBERG STORED AS ORC
LOCATION '/tmp/iceberg_llap_delete_cache/ice_dv'
TBLPROPERTIES ('format-version'='3');
INSERT INTO ice_dv VALUES (1), (2), (3), (4), (5);
DELETE FROM ice_dv WHERE id IN (2, 4);

SELECT * FROM ice_dv ORDER BY id;
dfs -rm /tmp/iceberg_llap_delete_cache/ice_dv/data/*-pos-deletes*;
SELECT * FROM ice_dv ORDER BY id;

-- V2: position delete files, cached as deletion vectors
CREATE EXTERNAL TABLE ice_pos (id INT) STORED BY ICEBERG STORED AS PARQUET
LOCATION '/tmp/iceberg_llap_delete_cache/ice_pos'
TBLPROPERTIES ('format-version'='2');
INSERT INTO ice_pos VALUES (1), (2), (3);
INSERT INTO ice_pos VALUES (4), (5), (6);
DELETE FROM ice_pos WHERE id IN (2, 5);

SELECT * FROM ice_pos ORDER BY id;
dfs -rm /tmp/iceberg_llap_delete_cache/ice_pos/data/*-pos-deletes*;
SELECT * FROM ice_pos ORDER BY id;

DROP TABLE ice_dv;
DROP TABLE ice_pos;
dfs -rm -r -f /tmp/iceberg_llap_delete_cache;
