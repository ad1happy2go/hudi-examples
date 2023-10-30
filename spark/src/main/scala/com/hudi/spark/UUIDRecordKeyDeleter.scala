package com.hudi.spark

import org.apache.hudi.client.WriteStatus
import org.apache.hudi.common.model.{HoodieKey, HoodieRecord}
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.sql.{Column, DataFrame, Encoders, SparkSession}

object UUIDRecordKeyDeleter {

  def query(tablePath: String, predicate: Column, queryType: String)(implicit
                                                                     spark: SparkSession
  ): DataFrame = {
    spark.read
      .format("hudi")
      .option(QUERY_TYPE.key(), queryType)
      .option(HoodieMetadataConfig.ENABLE.key(), "false")
      .load(tablePath)
      .where(predicate)
  }

  // Computes record keys of the records matching the predicate
  def computeRecordKeys(
                         tablePath: String,
                         predicate: Column
                       )(implicit
                         spark: SparkSession
                       ): JavaRDD[HoodieKey] = {
    implicit val hoodieKeyEncoder: Encoder[HoodieKey] =
      Encoders.bean(classOf[org.apache.hudi.common.model.HoodieKey])
    query(tablePath, predicate, QUERY_TYPE_SNAPSHOT_OPT_VAL)
      .select(HoodieRecord.RECORD_KEY_METADATA_FIELD, HoodieRecord.PARTITION_PATH_METADATA_FIELD)
      .map { row =>
      {
        val recordKey = row.getString(0)
        val partitionPath = row.getString(1)
        new org.apache.hudi.common.model.HoodieKey(recordKey, partitionPath)
      }
      }
      .toJavaRDD
  }

  // Deletes all records with the provided record keys without triggering compaction.
  def deleteRecords(
                     config: WriteClientConfig,
                     envConfig: EnvConfig,
                     dataLakeRecord: DataLakeRecord[_, _],
                     recordKeys: JavaRDD[HoodieKey]
                   )(implicit
                     spark: SparkSession
                   ): DeleteStats = {
    var stats = DeleteStats()
    val writeClient =
      buildWriteClient(config, envConfig, dataLakeRecord, spark.sparkContext)
    var deleteInstant: String = ""

    try {
      deleteInstant = writeClient.startCommit()
      val statuses: mutable.Seq[WriteStatus] =
        writeClient.delete(recordKeys, deleteInstant).collect().asScala
      stats = stats.copy(
        totalDeleted = getTotalDeletesFromWriteStatuses(statuses),
        totalPartitionsDeleted = getPartitionsFromWriteStatuses(statuses).size
      )
    } catch {
      case t: Throwable =>
        logErrorAndExit(s"Delete operation failed for instant $deleteInstant due to ", t)
    } finally {
      log.info(s"Finished delete operation for instant $deleteInstant")
      writeClient.close()
    }
    stats
  }

  def compactReadOptimizedRecordsMatchingPredicate(
                                                    config: WriteClientConfig,
                                                    envConfig: EnvConfig,
                                                    dataLakeRecord: DataLakeRecord[_, _],
                                                    predicate: Column
                                                  )(implicit spark: SparkSession): DeleteStats = {
    import spark.implicits._
    var stats = DeleteStats()
    if (config.scheduleCompaction) {
      val partitions = query(config.tablePath, predicate, QUERY_TYPE_READ_OPTIMIZED_OPT_VAL)
        .select(col(HoodieRecord.PARTITION_PATH_METADATA_FIELD))
        .distinct()
        .map(r => r.getString(0))
        .collect()
        .toList

      if (partitions.isEmpty) {
        log.warn(
          s"Skipping compaction on table ${dataLakeRecord.tableName} as no records were found"
        )
      } else {
        log.info(
          s"Compacting ${partitions.size} partition paths in table ${dataLakeRecord.tableName}: ${partitions.take(10).mkString(",")}..."
        )

        val writeClient =
          compactingWriteClient(config, dataLakeRecord.tableName, envConfig, partitions)

        // Retry any pending (requested/inflight) compactions targeting overlapping partitions.
        // This is essentially a recovery process - if a previous run failed after scheduling a compaction,
        // the file groups in that compaction plan will be excluded from the new scheduled plan and their compaction won't be retried.
        // The compaction execution uses locks therefore it is safe to execute the same compaction plan concurrently.
        // The lake API limits GDPR deletion jobs to one at a time per env_id, therefore such conflict is unlikely (but still possible with mutations).
        // todo: get stats from this compaction to aggregate into final stats
        if (config.executeCompaction) {
          CompactionHelper.executeCompactionsWithOverlappingPartitions(
            config.tablePath,
            partitions,
            writeClient
          )
        }

        try {
          log.info(s"""Scheduling compaction on table ${dataLakeRecord.tableName}:
                      |                 hoodie.compaction.strategy: ${writeClient.getConfig.getCompactionStrategy}
                      |     "hoodie.compaction.include.partitions": ${writeClient.getConfig
            .getString(
              "hoodie.compaction.include.partitions"
            )
            .take(100)}...
                      |             hoodie.compact.schedule.inline: ${writeClient.getConfig
            .scheduleInlineCompaction()}
                      |                      hoodie.compact.inline: ${writeClient.getConfig
            .inlineCompactionEnabled()}
                      |                 hoodie.write.lock.provider: ${writeClient.getConfig.getLockProviderClass}
                      |                     hoodie.metadata.enable: ${writeClient.getConfig.getMetadataConfig
            .enabled()}
                      |""".stripMargin)
          val instant = writeClient.scheduleCompaction(org.apache.hudi.common.util.Option.empty())
          if (instant.isPresent) {
            if (config.executeCompaction) {
              log.info(
                s"Running compaction on table ${dataLakeRecord.tableName} at instant: ${instant.get}"
              )
              val metadata = writeClient.compact(instant.get).getCommitMetadata
              stats = stats.copy(
                compactionDeletes = metadata.get.getTotalRecordsDeleted,
                compactionTotalFiles = metadata.get.getTotalLogFilesCompacted,
                compactionTotalPartitions = metadata.get.fetchTotalPartitionsWritten
              )
              log.info(
                s"Successfully completed compaction on table ${dataLakeRecord.tableName} at instant: ${instant.get}"
              )
            }
          } else {
            log.warn(
              s"Unable to schedule compaction on table ${dataLakeRecord.tableName}, see Hudi logs for reason."
            )
          }
        } catch {
          case e: Throwable =>
            logErrorAndExit(s"Compaction failed on table ${dataLakeRecord.tableName}!", e)
        } finally {
          writeClient.close()
        }
      }
    }
    stats
  }

  // Deletes all records matching the predicate and runs compaction afterwards.
  // Deletion and compaction are only executed if there are records matching the predicate.
  def deleteRecords(
                     config: WriteClientConfig,
                     envConfig: EnvConfig,
                     dataLakeRecord: DataLakeRecord[_, _],
                     predicate: Column
                   )(implicit
                     spark: SparkSession
                   ): DeleteStats = {
    var stats = DeleteStats()
    val recordCount = query(config.tablePath, predicate, QUERY_TYPE_SNAPSHOT_OPT_VAL).count()
    stats = stats.copy(
      totalFoundBeforeDelete = recordCount
    )
    if (recordCount == 0) {
      log.warn(
        s"Skipping delete operation on table ${dataLakeRecord.tableName} as no records were found"
      )
      DeleteStats()
    } else {
      log.warn(s"Deleting $recordCount records from table ${dataLakeRecord.tableName}")
      val keysToDelete = computeRecordKeys(config.tablePath, predicate)
      val deleteStats = deleteRecords(config, envConfig, dataLakeRecord, keysToDelete)
      stats = stats.copy(
        totalDeleted = deleteStats.totalDeleted,
        totalPartitionsDeleted = deleteStats.totalPartitionsDeleted
      )
    }
    val compactionStats =
      compactReadOptimizedRecordsMatchingPredicate(config, envConfig, dataLakeRecord, predicate)
    stats = stats.copy(
      compactionDeletes = compactionStats.compactionDeletes,
      compactionTotalFiles = compactionStats.compactionTotalFiles,
      compactionTotalPartitions = compactionStats.compactionTotalPartitions
    )
    stats
  }

  def buildWriteClient(
                        config: WriteClientConfig,
                        envConfig: EnvConfig,
                        datalakeRecord: DataLakeRecord[_, _],
                        sparkContext: SparkContext
                      ): SparkRDDWriteClient[_] = {
    val lockProperties = new Properties()
    val lockOptionsMap =
      WriterOptions.lockOptions(datalakeRecord.tableName, config.region, envConfig: EnvConfig)
    lockProperties.putAll(lockOptionsMap.asJava)

    val metricsProperties = new Properties()
    val metricsOptionsMap = metricsOptions(config.statsDHost, envConfig, datalakeRecord.tableName)
    metricsProperties.putAll(metricsOptionsMap.asJava)

    val writerConfig = HoodieWriteConfig
      .newBuilder()
      .withCompactionConfig(
        HoodieCompactionConfig
          .newBuilder()
          .withInlineCompaction(false)
          .withScheduleInlineCompaction(false)
          .withMaxNumDeltaCommitsBeforeCompaction(1)
          .build()
      )
      .withArchivalConfig(HoodieArchivalConfig.newBuilder().withAutoArchive(false).build())
      .withCleanConfig(HoodieCleanConfig.newBuilder().withAutoClean(false).build())
      .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(false).build())
      .withLockConfig(HoodieLockConfig.newBuilder().fromProperties(lockProperties).build())
      .withMetricsConfig(HoodieMetricsConfig.newBuilder().fromProperties(metricsProperties).build())
      .withDeleteParallelism(config.writeParallelism)
      .withPath(config.tablePath)
      .forTable(datalakeRecord.tableName)
      .build()
    val engineContext: HoodieEngineContext = new HoodieSparkEngineContext(
      JavaSparkContext.fromSparkContext(sparkContext)
    )
    new SparkRDDWriteClient(engineContext, writerConfig)
  }

}