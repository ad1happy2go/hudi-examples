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

create TABLE parquet_input (
                               ts BIGINT,
                               uuid VARCHAR(40),
                               rider VARCHAR(20),
                               driver VARCHAR(20),
                               fare DOUBLE,
                               city VARCHAR(20)
                           ) WITH ('connector'='filesystem','path'='file:///tmp/parquet_table','format'='parquet');


INSERT INTO hudi_table SELECT * FROM parquet_input;
