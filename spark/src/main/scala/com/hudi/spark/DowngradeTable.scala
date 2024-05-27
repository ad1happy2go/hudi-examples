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


object DowngradeTable{
  def validateFromAndToVersion(toVersion: HoodieTableVersion, fromVersion: HoodieTableVersion): Boolean = {
    if (toVersion.compareTo(fromVersion) > 0) throw new IllegalArgumentException("Hudi table can not be downgraded from " + fromVersion + " to version " + toVersion)
    else if (toVersion.compareTo(fromVersion) == 0) return false
    else return true
  }

  def downgradeTable(spark: SparkSession, basePath: String, toVersion: Int) = {
    val jsc = JavaSparkContext.fromSparkContext(spark.sparkContext)
    val fs = org.apache.hadoop.fs.FileSystem.getLocal(spark.sparkContext.hadoopConfiguration)
    val conf = new org.apache.hudi.storage.hadoop.HoodieHadoopStorage(fs).getConf().newInstance()
    val hoodieEngineContext = new HoodieSparkEngineContext(jsc)
    val metaClient = HoodieTableMetaClient.builder.setConf(conf).setBasePath(basePath).build
    val fromVersion = metaClient.getTableConfig.getTableVersion
    if(validateFromAndToVersion(HoodieTableVersion.versionFromCode(toVersion), fromVersion)) {
      val writeConf = HoodieWriteConfig.newBuilder.withProps(metaClient.getTableConfig.getProps).withPath(basePath).build
      new UpgradeDowngrade(metaClient, writeConf, hoodieEngineContext, SparkUpgradeDowngradeHelper.getInstance).run(HoodieTableVersion.versionFromCode(toVersion), null)
      val newMetaClient = HoodieTableMetaClient.builder.setConf(conf).setBasePath(basePath).build
      val newVersion = newMetaClient.getTableConfig.getTableVersion
      println(newVersion)
    }
  }
}