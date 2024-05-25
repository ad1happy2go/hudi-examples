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


Logger.getLogger("org").setLevel(Level.ERROR)
Logger.getLogger("akka").setLevel(Level.ERROR)

object TestAutomationUtils {
  val INSERT_MODE="INSERT"
  val DELETE_MODE="DELETE"
  val UPDATE_MODE="UPDATE"

  def loadData(spark: SparkSession,basePath:String, tableName: String, s3OrLocal: String = "local", batch_id:String = "0" , numInserts: Int = 100, numUpdates: Int = 0, numDeletes: Int = 0, conf: String = "") {
    val dataGen = new DataGenerator
    val configs = getConfigs(tableName) ++ readConfigs(conf)
    val inserts = convertToStringList(dataGen.generateInserts(numInserts))
    val df = spark.read.json(spark.sparkContext.parallelize(inserts, (numInserts / 100).toInt)).withColumn("batch_id", lit(batch_id)).withColumn("mode", lit(INSERT_MODE))
    df.write.format("hudi").options(configs).mode("append").save(basePath)
    df.write.format("parquet").save(basePath + "_parquet_" + batch_id + INSERT_MODE)
    assert(spark.read.format("hudi").load(basePath).where(f"batch_id = '${batch_id}'").count() == spark.read.format("parquet").load(basePath + "_parquet_" + batch_id + INSERT_MODE).count())
    if (numUpdates > 0) {
      val updates = convertToStringList(dataGen.generateUpdates(numUpdates))
      val df = spark.read.json(spark.sparkContext.parallelize(updates, (numUpdates / 100).toInt)).withColumn("batch_id", lit(batch_id)).withColumn("mode", lit(UPDATE_MODE))
      df.write.format("hudi").options(configs).mode("append").save(basePath)
      df.write.format("parquet").save(basePath + "_parquet_" + batch_id + UPDATE_MODE)
      assert(spark.read.format("hudi").load(basePath).where(f"batch_id = '${batch_id}'").count() == spark.read.format("parquet").load(basePath + "_parquet_" + batch_id + INSERT_MODE).count())
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
    assert(actualDF.where(f"mode = '${INSERT_MODE}'").except(expectedInserts.select()).count() == 0)
    assert(expectedDeletes.intersect(actualDF).count() == 0)

    assert(expectedOutput.except(outputDF).count() == 0)
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
      "hoodie.parquet.compression.codec" -> "snappy"
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

  def validateFromAndToVersion(toVersion: HoodieTableVersion, fromVersion: HoodieTableVersion): Unit = {
    if (toVersion.compareTo(fromVersion) >= 0) throw new IllegalArgumentException("Hudi table can not be downgraded from " + fromVersion + " to version " + toVersion)
  }

  def downgradeTable(spark: SparkSession, basePath: String, toVersion: Int) = {
    val jsc = JavaSparkContext.fromSparkContext(spark.sparkContext)
    val hoodieEngineContext = new HoodieSparkEngineContext(jsc)
    val metaClient = HoodieTableMetaClient.builder.setConf(spark.sparkContext.hadoopConfiguration).setBasePath(basePath).build
    val fromVersion = metaClient.getTableConfig.getTableVersion
    validateFromAndToVersion(HoodieTableVersion.versionFromCode(toVersion), fromVersion)
    val writeConf = HoodieWriteConfig.newBuilder.withProps(metaClient.getTableConfig.getProps).withPath(basePath).build
    new UpgradeDowngrade(metaClient, writeConf, hoodieEngineContext, SparkUpgradeDowngradeHelper.getInstance).run(HoodieTableVersion.versionFromCode(toVersion), null)
    val newMetaClient = HoodieTableMetaClient.builder.setConf(spark.sparkContext.hadoopConfiguration).setBasePath(basePath).build
    val newVersion = newMetaClient.getTableConfig.getTableVersion
    println(newVersion)
  }

}