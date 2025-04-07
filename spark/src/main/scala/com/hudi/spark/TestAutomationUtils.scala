import org.apache.hudi.QuickstartUtils._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import scala.collection.JavaConversions._
import scala.io.Source
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.HoodieTableVersion
import org.apache.hudi.common.util.StringUtils
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.table.upgrade.SparkUpgradeDowngradeHelper
import org.apache.hudi.table.upgrade.UpgradeDowngrade
import org.apache.spark.api.java.JavaSparkContext
import org.apache.hudi.common.model.HoodieRecord.HOODIE_META_COLUMNS
//import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.client.SparkRDDWriteClient
import org.apache.hudi.common.config.TypedProperties
import scala.collection.JavaConverters._
import scala.collection.{JavaConverters, mutable}
import org.apache.hudi.common.table.timeline.HoodieTimeline

Logger.getLogger("org").setLevel(Level.ERROR)
Logger.getLogger("akka").setLevel(Level.ERROR)

object TestAutomationUtils {
  val INSERT_MODE="INSERT"
  val DELETE_MODE="DELETE"
  val UPDATE_MODE="UPDATE"

  def loadData(spark: SparkSession,basePath:String, tableName: String, s3OrLocal: String = "local", batch_id:String = "0" , numInserts: Int = 100, numUpdates: Int = 0, numDeletes: Int = 0, conf: String = "", upgrade:String = "") {
    val dataGen = new DataGenerator
    var configs = getConfigs(tableName) ++ readConfigs(conf)
    if(upgrade == "true"){
      configs = configs.updated("hoodie.write.auto.upgrade", "true")
    }
    println("Props conf")
    println(readConfigs(conf))
    println("Final conf")
    println(configs)
    val jsc = new JavaSparkContext(spark.sparkContext)
    val inserts = convertToStringList(dataGen.generateInserts(numInserts))
    var df = spark.read.json(spark.sparkContext.parallelize(inserts, (numInserts / 100).toInt)).withColumn("batch_id", lit(batch_id)).withColumn("mode", lit(INSERT_MODE))
    df.write.format("hudi").options(configs).mode("append").save(basePath)
    df.write.format("parquet").save(basePath + "_parquet_" + batch_id + INSERT_MODE)
    assert(spark.read.format("hudi").load(basePath).where(f"batch_id = '${batch_id}'").count() == spark.read.format("parquet").load(basePath + "_parquet_" + batch_id + INSERT_MODE).count())
    if (numUpdates > 0) {
      val updates = convertToStringList(dataGen.generateUpdates(numUpdates))
      val df = spark.read.json(spark.sparkContext.parallelize(updates, (numUpdates / 100).toInt)).withColumn("batch_id", lit(batch_id)).withColumn("mode", lit(UPDATE_MODE))
      df.write.format("hudi").options(configs).mode("append").save(basePath)
//      val metaClient = HoodieTableMetaClient.builder
//        .setConf(HadoopFSUtils.getStorageConfWithCopy(jsc.hadoopConfiguration))
//        .setBasePath(basePath)
//        .build
//      if (false && metaClient.getTableConfig.isMetadataTableAvailable && !HoodieTableMetaClient.builder
//        .setConf(HadoopFSUtils.getStorageConfWithCopy(jsc.hadoopConfiguration))
//        .setBasePath(metaClient.getMetaPath.toString + "/metadata")
//        .build.getActiveTimeline.lastInstant().get().getAction.equals(HoodieTimeline.COMMIT_ACTION)) {
//        rollbackLastInstant(spark, basePath, configs)
//        df.write.format("hudi").options(configs).mode("append").save(basePath)
//        df.write.format("parquet").save(basePath + "_parquet_" + batch_id + UPDATE_MODE)
//        assert(spark.read.format("hudi").load(basePath).where(f"batch_id = '${batch_id}'").count() == spark.read.format("parquet").load(basePath + "_parquet_" + batch_id + INSERT_MODE).count())
//      } else {
        df.write.format("parquet").save(basePath + "_parquet_" + batch_id + UPDATE_MODE)
        assert(spark.read.format("hudi").load(basePath).where(f"batch_id = '${batch_id}'").count() == spark.read.format("parquet").load(basePath + "_parquet_" + batch_id + INSERT_MODE).count())
//      }
    }
    if (numDeletes > 0) {
      val deletes = convertToStringList(dataGen.generateUpdates(numDeletes))
      val df = spark.read.json(spark.sparkContext.parallelize(deletes, (numUpdates / 100).toInt)).withColumn("batch_id", lit(batch_id)).withColumn("mode", lit(DELETE_MODE))
      df.write.format("hudi").options(configs).option("hoodie.datasource.write.operation","delete").mode("append").save(basePath)
      df.write.format("parquet").save(basePath + "_parquet_" + batch_id + DELETE_MODE)
      assert(spark.read.format("hudi").load(basePath).where(f"batch_id = '${batch_id}'").count() == spark.read.format("parquet").load(basePath + "_parquet_" + batch_id + INSERT_MODE).count() - df.count())
    }
    // Saving state of table after this batch
    spark.read.format("hudi").load(basePath).drop(HOODIE_META_COLUMNS.toList:_*).write.format("parquet").save(basePath + "_parquet_" + batch_id)
  }

  def loadDataWithoutValidations(spark: SparkSession,basePath:String, tableName: String, s3OrLocal: String = "local", batch_id:String = "0" , numInserts: Int = 100, numUpdates: Int = 0, numDeletes: Int = 0, conf: String = "", numBatches: Int = 1) {
    val dataGen = new DataGenerator
    val configs = getConfigs(tableName) ++ readConfigs(conf)
    for( w <- 0 to numBatches){
    val inserts = convertToStringList(dataGen.generateInserts(numInserts))
    val df = spark.read.json(spark.sparkContext.parallelize(inserts, (numInserts / 100).toInt)).withColumn("batch_id", lit(batch_id)).withColumn("mode", lit(INSERT_MODE))
    if (numUpdates > 0) {
      val updates = convertToStringList(dataGen.generateUpdates(numUpdates))
      val df_updates = spark.read.json(spark.sparkContext.parallelize(updates, (numUpdates / 100).toInt)).withColumn("batch_id", lit(batch_id)).withColumn("mode", lit(UPDATE_MODE))
      df.union(df_updates).write.format("hudi").options(configs).mode("append").save(basePath)
    }else{
      df.write.format("hudi").options(configs).mode("append").save(basePath)
    }
    if (numDeletes > 0) {
      val deletes = convertToStringList(dataGen.generateUpdates(numDeletes))
      val df = spark.read.json(spark.sparkContext.parallelize(deletes, (numUpdates / 100).toInt)).withColumn("batch_id", lit(batch_id)).withColumn("mode", lit(DELETE_MODE))
      df.write.format("hudi").options(configs).option("hoodie.datasource.write.operation","delete").mode("append").save(basePath)
    }
    }
    // Saving state of table after this batch
    spark.read.format("hudi").load(basePath).drop(HOODIE_META_COLUMNS.toList:_*).write.format("parquet").save(basePath + "_parquet_" + batch_id)
  }

  def getCountByBatch(spark: SparkSession, basePath:String):Map[Int, Int] = {
    spark.read.format("hudi").load(basePath).groupBy("batch_id","mode").agg(count(lit(1))).collect().map(x => (x(0).toString.toInt,x(1).toString.toInt)).toMap[Int,Int]
  }

  def compareData(spark: SparkSession, basePath:String, batch_id:String) = {
    val outputDF = spark.read.format("hudi").load(basePath).drop(HOODIE_META_COLUMNS.toList:_*)
    val cols = outputDF.columns.toList
    val expectedOutput = spark.read.format("parquet").load(basePath + "_parquet_" + batch_id).selectExpr(cols:_*)
    val expectedInserts = spark.read.format("parquet").load(basePath + "_parquet_" + batch_id + INSERT_MODE).selectExpr(cols:_*)
    val expectedUpdates = spark.read.format("parquet").load(basePath + "_parquet_" + batch_id + UPDATE_MODE).selectExpr(cols:_*)
    val expectedDeletes = spark.read.format("parquet").load(basePath + "_parquet_" + batch_id + DELETE_MODE).selectExpr(cols:_*)
    val actualDF = outputDF.where(f"batch_id = '${batch_id}'")
    assert(actualDF.where(f"mode = '${INSERT_MODE}'").except(expectedInserts).count() == 0)
    assert(expectedDeletes.intersect(actualDF).count() == 0)

    assert(expectedOutput.except(outputDF).count() == 0)
    assert(outputDF.except(expectedOutput).count() == 0)
  }


  def compareDataWithoutValidations(spark: SparkSession, basePath:String, batch_id:String) = {
    val outputDF = spark.read.format("hudi").load(basePath).drop(HOODIE_META_COLUMNS.toList:_*)
    val cols = outputDF.columns.toList
    val expectedOutput = spark.read.format("parquet").load(basePath + "_parquet_" + batch_id).selectExpr(cols:_*)
    assert(outputDF.except(expectedOutput).count() == 0)
  }

  def compareOnlyInserts(spark: SparkSession, basePath: String, batch_id: String) = {
    val outputDF = spark.read.format("hudi").load(basePath).drop(HOODIE_META_COLUMNS.toList: _*)
    val cols = outputDF.columns.toList
    val expectedOutput = spark.read.format("parquet").load(basePath + "_parquet_" + batch_id).selectExpr(cols:_*)
    val expectedInserts = spark.read.format("parquet").load(basePath + "_parquet_" + batch_id + INSERT_MODE).selectExpr(cols:_*)
    val actualDF = outputDF.where(f"batch_id = '${batch_id}'")
    assert(actualDF.where(f"mode = '${UPDATE_MODE}'").count() == 0)
    assert(actualDF.where(f"mode = '${INSERT_MODE}'").except(expectedInserts).count() == 0)
    assert(expectedInserts.except(actualDF.where(f"mode = '${INSERT_MODE}'")).count() == 0)
    assert(expectedInserts.except(actualDF).count() == 0)
    assert(actualDF.except(expectedInserts).count() == 0)
  }

//  def rollbackLastInstant(spark: SparkSession, basePath: String, hudiOpts: Map[String, String]): Unit = {
//    val jsc = new JavaSparkContext(spark.sparkContext)
//    val metaClient = HoodieTableMetaClient.builder
//      .setConf(HadoopFSUtils.getStorageConfWithCopy(jsc.hadoopConfiguration))
//      .setBasePath(basePath)
//      .build
//    val writeClient = new SparkRDDWriteClient(new HoodieSparkEngineContext(jsc), getWriteConfig(hudiOpts, basePath))
//      .rollback(metaClient.getActiveTimeline.getCommitsTimeline.lastInstant().get().getTimestamp)
//  }
//
//  protected def getWriteConfig(hudiOpts: Map[String, String], basePath: String): HoodieWriteConfig = {
//    val props = TypedProperties.fromMap(JavaConverters.mapAsJavaMapConverter(hudiOpts).asJava)
//    HoodieWriteConfig.newBuilder()
//      .withProps(props)
//      .withPath(basePath)
//      .build()
//  }

  def getCount(spark: SparkSession, basePath:String):Long = {
    spark.read.format("hudi").load(basePath).count()
  }


  def getConfigs(tableName: String): Map[String, String] = {
    Map(
      "hoodie.datasource.write.operation" -> "upsert",
      "hoodie.datasource.write.recordkey.field" -> "uuid",
      "hoodie.datasource.write.precombine.field " -> "ts",
      "hoodie.datasource.write.hive_style_partitioning" -> "true",
      "hoodie.table.name" -> tableName,
      "hoodie.parquet.compression.codec" -> "snappy",
      "hoodie.keep.min.commits" -> "10",
      "hoodie.keep.max.commits" -> "15",
      "hoodie.cleaner.commits.retained" -> "8",
      "hoodie.clustering.inline" -> "false",
      "hoodie.clustering.inline.max.commits" -> "6",
      "hoodie.compact.inline.max.delta.commits" -> "1"
    )
  }

  def getCustomKeyGenConfigs(): Map[String, String] = {
    Map(
      "hoodie.datasource.write.keygenerator.class" -> "org.apache.hudi.keygen.CustomKeyGenerator",
      "hoodie.deltastreamer.keygen.timebased.timestamp.type" -> "EPOCHMILLISECONDS",
      "hoodie.deltastreamer.keygen.timebased.output.dateformat" -> "yyyy/MM/dd",
      "hoodie.datasource.write.partitionpath.field" -> "partitionpath:timestamp"
    )
  }

  def readConfigs(conf: String): Map[String, String] = {
    if (conf == null || conf == "") return Map[String, String]()
    val configFile = conf
    val lines = Source.fromFile(configFile).getLines()
    val configMap = collection.mutable.Map[String, String]()
    lines.foreach { line =>
      val Array(key, value) = line.split("=")
      configMap.put(key.trim, value.trim)
    }
    configMap.toMap
  }

}