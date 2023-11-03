-- Set checkpoint properties
SET 'execution.runtime-mode' = 'streaming';
SET 'state.checkpoints.dir' = 'file:///tmp/flink_consistent_hashing_demo_checkpoint';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'execution.checkpointing.interval' = '10 s';
SET 'execution.checkpointing.min-pause' = '1 s';
SET 'execution.checkpointing.max-concurrent-checkpoints' = '1';
SET 'execution.checkpointing.prefer-checkpoint-for-recovery' = 'true';

-- Create user_source table
CREATE TABLE user_source (
    id INT,
    name STRING,
    age INT,
    sex BOOLEAN,
    birth DATE
)
WITH (
    'connector' = 'datagen',
    'rows-per-second' = '1000'
);

-- Create flink_consistent_hashing_demo table
CREATE TABLE flink_consistent_hashing_demo (
    id INT,
    name STRING,
    age INT,
    sex BOOLEAN,
    birth DATE,
    PRIMARY KEY (id) NOT ENFORCED
) PARTITIONED BY (birth)
WITH (
    'connector' = 'hudi',
    'path' = 'file:///tmp/flink_consistent_hashing_demo',
    'table.type' = 'MERGE_ON_READ',
    'hoodie.datasource.write.hive_style_partitioning' = 'true',
    'hoodie.datasource.write.partitionpath.field' = 'birth',
    'hoodie.datasource.write.recordkey.field' = 'id',
    'hoodie.parquet.small.file.limit' = '0',
    'hoodie.parquet.max.file.size' = '1048576',
    'write.partition.format' = 'yyyy-MM-dd',
    'write.parquet.max.file.size' = '1',
    'read.streaming.enabled' = 'true',
    'hoodie.index.type' = 'BUCKET',
    'hoodie.bucket.index.hash.field' = 'id',
    'hoodie.index.bucket.engine' = 'CONSISTENT_HASHING',
    'hoodie.bucket.index.min.num.buckets' = '1',
    'hoodie.bucket.index.max.num.buckets' = '8',
    'hoodie.bucket.index.split.threshold' = '2',
    'write.operation' = 'upsert',
    'clustering.schedule.enabled' = 'true',
    'clustering.plan.strategy.class' = 'org.apache.hudi.client.clustering.plan.strategy.FlinkConsistentBucketClusteringPlanStrategy',
    'hoodie.clustering.execution.strategy.class' = 'org.apache.hudi.client.clustering.run.strategy.SparkConsistentBucketClusteringExecutionStrategy',
    'hoodie.bucket.index.num.buckets' = '1',
    'compaction.async.enabled' = 'false',
    'clean.async.enabled' = 'false',
    'clustering.delta_commits' = '4'
);

-- Insert data into flink_consistent_hashing_demo from user_source
INSERT INTO flink_consistent_hashing_demo SELECT * FROM user_source;
