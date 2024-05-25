package com.hudi.spark
import org.apache.spark.SparkContext
import scala.collection.JavaConverters._
import org.apache.spark.sql.SparkSession
import org.apache.spark.api.java.JavaSparkContext
import org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer
import org.apache.hudi.utilities.deltastreamer.SchedulerConfGenerator
import org.apache.hudi.utilities.UtilHelpers

object DeltaStreamerExample {

  def main(args: Array[String]): Unit = {
    var config = Array(
      "--schemaprovider-class", "org.apache.hudi.utilities.schema.FilebasedSchemaProvider",
      "--source-class", "org.apache.hudi.utilities.sources.JsonKafkaSource",
      "--source-ordering-field", "ts_ux",
      "--target-base-path", "s3://"+ args("TARGET_BUCKET") + "/hudi_test_mor3/" + args("TARGET_TABLE") + "/",
      "--target-table", args("TARGET_TABLE"),
      "--table-type" , "MERGE_ON_READ",
      //"--table-type" , "COPY_ON_WRITE",
      "--enable-hive-sync",
      "--hoodie-conf", "hoodie.deltastreamer.schemaprovider.source.schema.file=s3://schema/artifacts/hudi/hudi-deltastreamer-glue/config/mongodb-userdata-prod-schema-new.avsc",
      "--hoodie-conf", "hoodie.deltastreamer.schemaprovider.target.schema.file=s3://schema/artifacts/hudi/hudi-deltastreamer-glue/config/mongodb-userdata-prod-schema-new.avsc",
      "--hoodie-conf", "hoodie.deltastreamer.source.kafka.topic="+ args("SOURCE_TOPIC") + "",
      //"--hoodie-conf", "hoodie.datasource.hive_sync.table="+ args("TARGET_TABLE") + "",
      "--hoodie-conf", "hoodie.datasource.write.recordkey.field=" +args("HOODIE_RECORDKEY_FIELD") + "",
      "--hoodie-conf", "hoodie.datasource.write.precombine.field=" +args("HOODIE_PRECOMBINE_FIELD") + "",
      "--hoodie-conf", "hoodie.datasource.hive_sync.enable=true",
      "--hoodie-conf", "hoodie.datasource.hive_sync.database="+ args("TARGET_DATABASE") + "",
      "--hoodie-conf", "hoodie.datasource.hive_sync.table="+ args("TARGET_TABLE") + "",
      "--hoodie-conf", "hoodie.datasource.write.operation=UPSERT",
      "--hoodie-conf", "hoodie.datasource.hive_sync.use_jdbc=false",
      "--hoodie-conf", "hoodie.datasource.hive_sync.partition_extractor_class=org.apache.hudi.hive.NonPartitionedExtractor",
      "--hoodie-conf", "hoodie.datasource.write.keygenerator.class=org.apache.hudi.keygen.NonpartitionedKeyGenerator",
      "--hoodie-conf", "security.protocol=PLAINTEXT",
      "--hoodie-conf", "auto.offset.reset=latest",
      "--hoodie-conf", "bootstrap.servers=" + args("KAFKA_BOOTSTRAP_SERVERS"),
      "--hoodie-conf", "group.id=native-hudi-job",
      "--hoodie-conf", "hoodie.kafka.allow.commit.on.errors=true",
      "--hoodie-conf", "hoodie.write.allow_null_updates",
      "--hoodie-conf", "hoodie.index.type=SIMPLE",
      "--hoodie-conf", "hoodie.upsert.shuffle.parallelism=200",
      "--hoodie-conf", "hoodie.finalize.write.parallelism=400",
      "--hoodie-conf", "hoodie.markers.delete.parallelism=200",
      "--hoodie-conf", "hoodie.file.listing.parallelism=400",
      "--hoodie-conf", "hoodie.cleaner.parallelism=400",
      "--hoodie-conf", "hoodie.archive.delete.parallelism=200",
      "--hoodie-conf", "compaction.trigger.strategy=NUM_OR_TIME",
      "--hoodie-conf", "hoodie.compact.inline.trigger.strategy=NUM_OR_TIME",
      "--hoodie-conf", "compaction.schedule.enabled=true",
      "--hoodie-conf", "compaction.async.enabled=true",
      "--hoodie-conf", "compaction.delta_commits=5",
      "--hoodie-conf", "hoodie.compact.inline.max.delta.commits=5",
      "--hoodie-conf", "compaction.delta_seconds=600",
      "--hoodie-conf", "hoodie.compact.inline.max.delta.seconds=600",
      "--hoodie-conf", "hoodie.metrics.on=true",
      "--hoodie-conf", "hoodie.metrics.reporter.type=CLOUDWATCH",
      //"--hoodie-conf", "hoodie.precommit.validators=org.apache.hudi.client.validator.SqlQuerySingleResultPreCommitValidator",
      //"--hoodie-conf", "hoodie.precommit.validators.single.value.sql.queries=select count(*) from <TABLE_NAME> where updtdTm is null#0",
      "--hoodie-conf", "hoodie.deltastreamer.kafka.commit_on_errors=true",
      "--continuous"
      //"--commit-on-errors"
    )
    val cfg = HoodieDeltaStreamer.getConfig(config)
  }

}
