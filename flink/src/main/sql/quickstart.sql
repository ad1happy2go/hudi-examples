set sql-client.execution.result-mode = tableau;

SET 'execution.runtime-mode' = 'streaming';
SET 'state.checkpoints.dir' = 'file:///tmp/hudi_table_checkpoint';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'execution.checkpointing.interval' = '10 s';
SET 'execution.checkpointing.min-pause' = '1 s';
SET 'execution.checkpointing.max-concurrent-checkpoints' = '1';
SET 'execution.checkpointing.prefer-checkpoint-for-recovery' = 'true';

CREATE TABLE hudi_table(
    ts BIGINT,
    uuid VARCHAR(40) PRIMARY KEY NOT ENFORCED,
    rider VARCHAR(20),
    driver VARCHAR(20) NOT NULL,
    fare DOUBLE,
    city VARCHAR(20)
)
PARTITIONED BY (`city`)
WITH (
  'connector' = 'hudi',
  'path' = 'file:///tmp/hudi_table',
  'table.type' = 'MERGE_ON_READ',
  'write.operation' = 'upsert',
  'write.ignore.failed' = 'true'
);

-- insert data using values
INSERT INTO hudi_table/*+ OPTIONS('write.operation'='upsert')*/
VALUES
(16951596490871,'334e26e9-8355-45cc-97c6-c31daf0df336','rider-A','driver-K',19.10,'san_francisco');

-- insert data using values
INSERT INTO hudi_table/*+ OPTIONS('write.operation'='upsert')*/
VALUES
(16951596490871,'334e26e9-8355-45cc-97c6-c31daf0df336','rider-A','driver-K',19.10,'san_francisco');

select * from hudi_table;

select * from hudi_table/*+ OPTIONS('read.start-commit'='earliest', 'read.end-commit'='20231122155636355')*/;



