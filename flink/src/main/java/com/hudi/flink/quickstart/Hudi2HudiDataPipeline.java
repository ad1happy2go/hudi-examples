package com.hudi.flink.quickstart;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.util.HoodiePipeline;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import java.util.HashMap;
import java.util.Map;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.flink.table.data.*;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.writer.BinaryRowWriter;
import org.apache.flink.table.data.writer.BinaryWriter;
public class Hudi2HudiDataPipeline {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Enable checkpointing
        env.enableCheckpointing(5000); // Checkpoint every 5 seconds

        // Set checkpointing behavior
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        checkpointConfig.setMinPauseBetweenCheckpoints(1000); // Minimum time between checkpoints
        checkpointConfig.setCheckpointTimeout(60000); // Checkpoint timeout in milliseconds
        checkpointConfig.setCheckpointStorage("file:///tmp/hudi_flink_checkpoint");
        String sourceTable = "hudi_table";
        String sourceBasePath = "file:///tmp/hudi_table";

        Map<String, String> sourceOptions = new HashMap<>();
        sourceOptions.put(FlinkOptions.PATH.key(), sourceBasePath);
        sourceOptions.put(FlinkOptions.TABLE_TYPE.key(), HoodieTableType.MERGE_ON_READ.name());
        sourceOptions.put(FlinkOptions.READ_AS_STREAMING.key(), "true"); // this option enable the streaming read
        sourceOptions.put(FlinkOptions.READ_START_COMMIT.key(), "20210316134557"); // specifies the start commit instant time

        HoodiePipeline.Builder sourceBuilder = HoodiePipeline.builder(sourceTable)
                .column("uuid VARCHAR(20)")
                .column("name VARCHAR(10)")
                .column("age INT")
                .column("ts TIMESTAMP(3)")
                .column("`partition` VARCHAR(20)")
                .pk("uuid")
                .partition("partition")
                .options(sourceOptions);

        DataStream<RowData> rowDataDataStream = sourceBuilder.source(env);
        String targetTable = "hudi_table";
        String basePath = "file:///tmp/hudi_table_target";

        Map<String, String> options = new HashMap<>();
        options.put(FlinkOptions.PATH.key(), basePath);
        options.put(FlinkOptions.TABLE_TYPE.key(), HoodieTableType.MERGE_ON_READ.name());
        options.put(FlinkOptions.PRECOMBINE_FIELD.key(), "ts");
        options.put(FlinkOptions.IGNORE_FAILED.key(), "true");
        options.put(FlinkOptions.WRITE_PARQUET_MAX_FILE_SIZE.key(), "-1");
        options.put(HoodieIndexConfig.BUCKET_INDEX_MIN_NUM_BUCKETS.key(), "2");
        options.put(HoodieIndexConfig.BUCKET_INDEX_MIN_NUM_BUCKETS.key(), "8");
        options.put(FlinkOptions.INDEX_TYPE.key(), HoodieIndex.IndexType.BUCKET.name());
        options.put(FlinkOptions.OPERATION.key(), WriteOperationType.UPSERT.name());
        options.put(FlinkOptions.BUCKET_INDEX_NUM_BUCKETS.key(), "4");
        options.put(HoodieClusteringConfig.EXECUTION_STRATEGY_CLASS_NAME.key(), "org.apache.hudi.client.clustering.run.strategy.SparkConsistentBucketClusteringExecutionStrategy");

        HoodiePipeline.Builder builder = HoodiePipeline.builder(targetTable)
                .column("uuid VARCHAR(20)")
                .column("name VARCHAR(10)")
                .column("age INT")
                .column("ts TIMESTAMP(3)")
                .column("`partition` VARCHAR(20)")
                .pk("uuid")
                .partition("partition")
                .options(options);

        builder.sink(rowDataDataStream, false); // The second parameter indicating whether the input data stream is bounded
        try {
            env.execute("Hudi_Source_Sink");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
