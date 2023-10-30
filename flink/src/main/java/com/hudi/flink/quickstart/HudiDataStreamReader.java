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

/**
 * This class demonstrates how to use Apache Hudi with Apache Flink for reading data as a streaming source.
 */
public class HudiDataStreamReader {

    /**
     * The entry point of the program.
     *
     * @param args Command-line arguments (targetTable and basePath).
     * @throws Exception If there is an issue with Flink execution.
     */
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: HudiDataStreamReader <targetTable> <basePath>");
            System.exit(1);
        }

        String targetTable = args[0];
        String basePath = args[1];

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        configureCheckpointing(env);

        Map<String, String> options = createHudiOptions(basePath);
        HoodiePipeline.Builder builder = createHudiPipeline(targetTable, options);

        DataStream<RowData> rowDataDataStream = builder.source(env);
        rowDataDataStream.print();

        executeFlinkJob(env, "Api_Source");
    }

    /**
     * Configure Flink checkpointing settings.
     *
     * @param env The Flink StreamExecutionEnvironment.
     */
    private static void configureCheckpointing(StreamExecutionEnvironment env) {
        env.enableCheckpointing(5000); // Checkpoint every 5 seconds
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        checkpointConfig.setMinPauseBetweenCheckpoints(1000);
        checkpointConfig.setCheckpointTimeout(60000);
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
    }

    /**
     * Create Hudi options for the data source.
     *
     * @param basePath The base path for Hudi data.
     * @return A Map containing Hudi options.
     */
    private static Map<String, String> createHudiOptions(String basePath) {
        Map<String, String> options = new HashMap<>();
        options.put(FlinkOptions.PATH.key(), basePath);
        options.put(FlinkOptions.TABLE_TYPE.key(), HoodieTableType.MERGE_ON_READ.name());
        options.put(FlinkOptions.READ_AS_STREAMING.key(), "true");
        options.put(FlinkOptions.READ_START_COMMIT.key(), "20210316134557");
        return options;
    }

    /**
     * Create a HudiPipeline.Builder with the specified target table and options.
     *
     * @param targetTable The name of the Hudi table.
     * @param options     The Hudi options for the data source.
     * @return A HudiPipeline.Builder.
     */
    private static HoodiePipeline.Builder createHudiPipeline(String targetTable, Map<String, String> options) {
        return HoodiePipeline.builder(targetTable)
                .column("uuid VARCHAR(20)")
                .column("name VARCHAR(10)")
                .column("age INT")
                .column("ts TIMESTAMP(3)")
                .column("`partition` VARCHAR(20)")
                .pk("uuid")
                .partition("partition")
                .options(options);
    }

    /**
     * Execute the Flink job.
     *
     * @param env  The Flink StreamExecutionEnvironment.
     * @param name The name of the job.
     * @throws Exception If there is an issue with Flink execution.
     */
    private static void executeFlinkJob(StreamExecutionEnvironment env, String name) throws Exception {
        try {
            env.execute(name);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
