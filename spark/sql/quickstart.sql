-- create a Hudi table that is partitioned.
DROP DATABASE IF EXISTS SQL_TESTING CASCADE;
CREATE DATABASE SQL_TESTING;
USE SQL_TESTING;

-- Testing Partitoned COW Table with Record Key
SELECT '****************************************************************************************';
SELECT 'CREATING TABLE';
CREATE TABLE hudi_table (
    ts BIGINT,
    uuid STRING,
    rider STRING,
    driver STRING,
    fare DECIMAL(10,4),
    city STRING
) ${partition}
tblproperties (
${props}
)
location '${path}'
;

SELECT '****************************************************************************************';
SELECT 'INSERTING 8 rows in the table';

INSERT INTO hudi_table
VALUES
(1695159649087,'334e26e9-8355-45cc-97c6-c31daf0df330','rider-A','driver-K',19.10,'san_francisco'),
(1695091554788,'e96c4396-3fad-413a-a942-4cb36106d721','rider-C','driver-M',27.70 ,'san_francisco'),
(1695046462179,'9909a8b1-2d15-4d3d-8ec9-efc48c536a00','rider-D','driver-L',33.90 ,'san_francisco'),
(1695332066204,'1dced545-862b-4ceb-8b43-d2a568f6616b','rider-E','driver-O',93.50,'san_francisco'),
(1695516137016,'e3cf430c-889d-4015-bc98-59bdce1e530c','rider-F','driver-P',34.15,'sao_paulo'    ),
(1695376420876,'7a84095f-737f-40bc-b62f-6b69664712d2','rider-G','driver-Q',43.40 ,'sao_paulo'    ),
(1695173887231,'3eeb61f7-c2b0-4636-99bd-5d7a5a1d2c04','rider-I','driver-S',41.06 ,'chennai'      ),
(1695115999911,'c8abbe79-8d89-47ea-b4ce-4d224bae5bfa','rider-J','driver-T',17.85,'chennai');

SELECT 'Below Count should be 8';
SELECT COUNT(1) FROM hudi_table;

SELECT 'Below Query should return 6 rows';
SELECT ts, fare, rider, driver, city FROM  hudi_table WHERE fare > 20.0;

SELECT 'Updating fare to 25 where rider = rider-D and then checking.';
UPDATE hudi_table SET fare = 25.0 WHERE rider = 'rider-D';
SELECT fare FROM hudi_table WHERE rider = 'rider-D';

SELECT 'Create and load another Source table using Hudi for testing merging into target Hudi table';
CREATE TABLE fare_adjustment (ts BIGINT, uuid STRING, rider STRING, driver STRING, fare DOUBLE, city STRING)
USING HUDI;
INSERT INTO fare_adjustment VALUES
(1695091554788,'e96c4396-3fad-413a-a942-4cb36106d721','rider-C','driver-M',-2.70 ,'san_francisco'),
(1695530237068,'3f3d9565-7261-40e6-9b39-b8aa784f95e2','rider-K','driver-U',64.20 ,'san_francisco'),
(1695241330902,'ea4c36ff-2069-4148-9927-ef8c1a5abd24','rider-H','driver-R',66.60 ,'sao_paulo'    ),
(1695115999911,'c8abbe79-8d89-47ea-b4ce-4d224bae5bfa','rider-J','driver-T',1.85,'chennai'      );

SELECT 'Running Merge Query';

MERGE INTO hudi_table AS target
USING fare_adjustment AS source
ON target.uuid = source.uuid
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
;

SELECT 'After MERGE, Check count of the table. It should return 10. 2 Of them were updates.';
SELECT COUNT(1) FROM hudi_table;

SELECT 'DELETING one of the record. Count after that should return 0';
DELETE FROM hudi_table WHERE uuid = '3f3d9565-7261-40e6-9b39-b8aa784f95e2';

SELECT COUNT(1) FROM hudi_table WHERE uuid = '3f3d9565-7261-40e6-9b39-b8aa784f95e2';

SELECT 'Running some time travel and incremental queries. Should not fail';
SELECT * FROM hudi_table TIMESTAMP AS OF '20220307091628793';
-- time travel based on different timestamp formats
SELECT * FROM hudi_table TIMESTAMP AS OF '2022-03-07 09:16:28.100';
SELECT * FROM hudi_table TIMESTAMP AS OF '2022-03-08';

-- start from earliest available commit, end at latest available commit.
SELECT * FROM hudi_table_changes('SQL_TESTING.hudi_table', 'latest_state', 'earliest');

-- start from earliest, end at 202305160000.
SELECT * FROM hudi_table_changes('SQL_TESTING.hudi_table', 'latest_state', 'earliest', '202305160000');

-- start from 202305150000, end at 202305160000.
SELECT * FROM hudi_table_changes('SQL_TESTING.hudi_table', 'latest_state', '202305150000', '202305160000');

-- CTAS by loading data into Hudi table
CREATE TABLE hudi_table_ctas
${partition}
tblproperties (
${props}
)  location '${path}/hudi_table_ctas' AS SELECT ts, uuid, rider, driver, fare, city  FROM hudi_table;

SELECT COUNT(1) FROM hudi_table_ctas;

SELECT 'SUCCESS';

exit;

