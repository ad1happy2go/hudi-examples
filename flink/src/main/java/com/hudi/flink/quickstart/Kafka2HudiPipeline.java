package com.hudi.flink.quickstart;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.writer.BinaryRowWriter;
import org.apache.flink.table.data.writer.BinaryWriter;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.hudi.client.clustering.plan.strategy.FlinkConsistentBucketClusteringPlanStrategy;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.table.types.DataType;
import org.apache.hudi.util.HoodiePipeline;

import java.util.*;

/**
 * A Flink program that ingests data from Kafka and writes it to Apache Hudi.
 */
public class Kafka2HudiPipeline {

    public static void main(String[] args) throws Exception {
        if (args.length < 5) {
            throw new Exception("Please pass 5 parameters - PipelineName HudiBasePath KafkGroupId CheckpointLocation kafkaTopicName");
        }

        // Parse command line arguments
        String pipelineName = args[0];
        String hudiBasePath = args[1];
        String kafkaGroupId = args[2];
        String checkpointLocation = args[3];
        String kafkaTopicName = args[4];

        // Create a Flink execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Configure checkpointing
        configureCheckpointing(env, checkpointLocation);

        // Set up Kafka source
        FlinkKafkaConsumer<String> kafkaConsumer = createKafkaConsumer(kafkaTopicName, kafkaGroupId);

        // Create a Kafka stream
        DataStream<String> kafkaStream = env.addSource(kafkaConsumer);

        // Transform Kafka data to Hudi records
        DataStream<RowData> transformedStream = kafkaStream.map(new HudiDataSource());

        // Define Hudi target table and options
        String targetTable = "hudi_table";
        Map<String, String> options = createHudiOptions(hudiBasePath);

        // Define HoodiePipeline.Builder for configuring the Hudi write
        HoodiePipeline.Builder builder = createHudiPipeline(targetTable, options);

        // Write to Hudi
        builder.sink(transformedStream, false);

        // Execute the Flink job
        env.execute(pipelineName);
    }

    // Configure Flink checkpointing settings
    private static void configureCheckpointing(StreamExecutionEnvironment env, String checkpointLocation) {
        env.enableCheckpointing(10000); // Checkpoint every 50 seconds
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        checkpointConfig.setMinPauseBetweenCheckpoints(1000); // Minimum time between checkpoints
        checkpointConfig.setCheckpointTimeout(60000); // Checkpoint timeout in milliseconds
        checkpointConfig.setCheckpointStorage(checkpointLocation);
    }

    // Create a Kafka consumer with specified properties
    private static FlinkKafkaConsumer<String> createKafkaConsumer(String topicName, String kafkaGroupId) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", kafkaGroupId);

        return new FlinkKafkaConsumer<>(
                topicName,
                new SimpleStringSchema(),
                properties
        );
    }

    // Create Hudi options for the data sink
    private static Map<String, String> createHudiOptions(String basePath) {
        Map<String, String> options = new HashMap<>();
        options.put(FlinkOptions.PATH.key(), basePath);
        options.put(FlinkOptions.TABLE_TYPE.key(), HoodieTableType.MERGE_ON_READ.name());
        options.put(FlinkOptions.PRECOMBINE_FIELD.key(), "ts");
        options.put(FlinkOptions.IGNORE_FAILED.key(), "true");
        options.put(FlinkOptions.WRITE_PARQUET_MAX_FILE_SIZE.key(), "-1");
        options.put(HoodieIndexConfig.BUCKET_INDEX_MIN_NUM_BUCKETS.key(), "1");
        options.put(HoodieIndexConfig.BUCKET_INDEX_MAX_NUM_BUCKETS.key(), "8");
        options.put(HoodieIndexConfig.BUCKET_SPLIT_THRESHOLD.key(), String.valueOf(1 / 1024.0 / 1024.0));
        options.put(FlinkOptions.BUCKET_INDEX_NUM_BUCKETS.key(), "1");
        options.put(FlinkOptions.INDEX_TYPE.key(), HoodieIndex.IndexType.BUCKET.name());
        options.put(FlinkOptions.OPERATION.key(), WriteOperationType.UPSERT.name());
        options.put(FlinkOptions.CLUSTERING_SCHEDULE_ENABLED.key(), "true");
        options.put(FlinkOptions.BUCKET_INDEX_ENGINE_TYPE.key(), HoodieIndex.BucketIndexEngineType.CONSISTENT_HASHING.name());
        options.put(FlinkOptions.CLUSTERING_PLAN_STRATEGY_CLASS.key(), FlinkConsistentBucketClusteringPlanStrategy.class.getName());
        options.put(HoodieClusteringConfig.EXECUTION_STRATEGY_CLASS_NAME.key(), "org.apache.hudi.client.clustering.run.strategy.SparkConsistentBucketClusteringExecutionStrategy");
        return options;
    }

    // Create a HoodiePipeline.Builder with specified target table and options
    private static HoodiePipeline.Builder createHudiPipeline(String targetTable, Map<String, String> options) {
        return HoodiePipeline.builder(targetTable)
                .column("uuid VARCHAR(256)")
                .column("name VARCHAR(10)")
                .column("age INT")
                .column("ts TIMESTAMP(3)")
                .column("`partition` VARCHAR(20)")
                .pk("uuid")
                .partition("partition")
                .options(options);
    }

    // Define the schema for Hudi records
    public static final DataType ROW_DATA_TYPE = DataTypes.ROW(
                    DataTypes.FIELD("uuid", DataTypes.VARCHAR(256)), // record key
                    DataTypes.FIELD("name", DataTypes.VARCHAR(10)),
                    DataTypes.FIELD("age", DataTypes.INT()),
                    DataTypes.FIELD("ts", DataTypes.TIMESTAMP(3)), // precombine field
                    DataTypes.FIELD("partition", DataTypes.VARCHAR(10)))
            .notNull();

    // Create a Hudi record from specified fields
    public static BinaryRowData insertRow(RowType rowType, Object... fields) {
        LogicalType[] types = rowType.getFields().stream().map(RowType.RowField::getType)
                .toArray(LogicalType[]::new);
        BinaryRowData row = new BinaryRowData(fields.length);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        writer.reset();
        for (int i = 0; i < fields.length; i++) {
            Object field = fields[i];
            if (field == null) {
                writer.setNullAt(i);
            } else {
                BinaryWriter.write(writer, i, field, types[i], InternalSerializers.create(types[i]));
            }
        }
        writer.complete();
        return row;
    }

    // Overloaded method for creating a Hudi record using the default schema
    public static BinaryRowData insertRow(Object... fields) {
        return insertRow((RowType) ROW_DATA_TYPE.getLogicalType(), fields);
    }

    // Mapper to convert Kafka data to Hudi records
    static class HudiDataSource implements MapFunction<String, RowData> {
        @Override
        public RowData map(String kafkaRecord) throws Exception {
            return insertRow(StringData.fromString(kafkaRecord), StringData.fromString("Danny"), 23,
                    TimestampData.fromEpochMillis(1), StringData.fromString("par1"));
        }
    }
}
