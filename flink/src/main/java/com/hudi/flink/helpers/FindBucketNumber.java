package com.hudi.flink.helpers;

import org.apache.flink.configuration.Configuration;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.client.clustering.plan.strategy.FlinkConsistentBucketClusteringPlanStrategy;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.sink.clustering.FlinkClusteringConfig;
import org.apache.hudi.table.HoodieFlinkTable;
import org.apache.hudi.util.CompactionUtil;
import org.apache.hudi.util.FlinkWriteClients;
import org.apache.hudi.util.StreamerUtil;

import java.util.HashMap;
import java.util.Map;

public class FindBucketNumber {

    public static final String PATH = "file:///tmp/flink_consistent_hashing_demo/";
    private static Map<String, String> getDefaultConsistentHashingOption() {
        Map<String, String> options = new HashMap<>();
        options.put(FlinkOptions.PATH.key(), PATH);
        options.put(FlinkOptions.TABLE_TYPE.key(), HoodieTableType.MERGE_ON_READ.name());
        options.put(FlinkOptions.OPERATION.key(), WriteOperationType.UPSERT.name());
        options.put(FlinkOptions.INDEX_TYPE.key(), HoodieIndex.IndexType.BUCKET.name());
        options.put(FlinkOptions.BUCKET_INDEX_ENGINE_TYPE.key(), HoodieIndex.BucketIndexEngineType.CONSISTENT_HASHING.name());
        options.put(FlinkOptions.BUCKET_INDEX_NUM_BUCKETS.key(), "1");
        options.put(FlinkOptions.CLUSTERING_PLAN_STRATEGY_CLASS.key(), FlinkConsistentBucketClusteringPlanStrategy.class.getName());
        options.put(HoodieClusteringConfig.EXECUTION_STRATEGY_CLASS_NAME.key(), "org.apache.hudi.client.clustering.run.strategy.SparkConsistentBucketClusteringExecutionStrategy");
        options.put(FlinkOptions.CLUSTERING_SCHEDULE_ENABLED.key(), "true");
        options.put(HoodieIndexConfig.BUCKET_SPLIT_THRESHOLD.key(), "2.0");
        options.put("hoodie.parquet.max.file.size", "1048576");
        return options;
    }

    private static HoodieClusteringPlan getLatestClusteringPlan(HoodieFlinkWriteClient writeClient) {
        HoodieFlinkTable<?> table = writeClient.getHoodieTable();
        table.getMetaClient().reloadActiveTimeline();
        Option<Pair<HoodieInstant, HoodieClusteringPlan>> clusteringPlanOption = ClusteringUtils.getClusteringPlan(
                table.getMetaClient(), table.getMetaClient().getActiveTimeline().filterPendingReplaceTimeline().lastInstant().get());
        return clusteringPlanOption.get().getRight();
    }
    private static org.apache.flink.configuration.Configuration getDefaultConfiguration() throws Exception {
        FlinkClusteringConfig cfg = new FlinkClusteringConfig();
        cfg.path = PATH;
        org.apache.flink.configuration.Configuration conf = FlinkClusteringConfig.toFlinkConfig(cfg);

        HoodieTableMetaClient metaClient = StreamerUtil.createMetaClient(conf);

        conf.setString(FlinkOptions.TABLE_NAME, metaClient.getTableConfig().getTableName());
        conf.setString(FlinkOptions.RECORD_KEY_FIELD, metaClient.getTableConfig().getRecordKeyFieldProp());
        conf.setString(FlinkOptions.PARTITION_PATH_FIELD, metaClient.getTableConfig().getPartitionFieldProp());
        for (Map.Entry<String, String> e : getDefaultConsistentHashingOption().entrySet()) {
            conf.setString(e.getKey(), e.getValue());
        }
        CompactionUtil.setAvroSchema(conf, metaClient);

        return conf;
    }
    public static void main(String[] args) throws Exception{
        Configuration conf = getDefaultConfiguration();
        try (HoodieFlinkWriteClient writeClient = FlinkWriteClients.createWriteClient(new Configuration(conf))) {
            try{
                HoodieClusteringPlan clusteringPlan = getLatestClusteringPlan(writeClient);
                System.out.println("NUmber of Buckets - " + clusteringPlan.getInputGroups().get(0).getNumOutputFileGroups());
                System.exit(0);
            }catch(Exception e){
                System.out.println("Clustering not yet scheduled");
                System.exit(0);
            }

        }
    }
}