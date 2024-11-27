// spark-shell
import scala.collection.JavaConversions._
import org.apache.spark.sql.SaveMode._
import org.apache.hudi.DataSourceReadOptions._
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.common.table.HoodieTableConfig._
import org.apache.hudi.config.HoodieWriteConfig._
import org.apache.hudi.keygen.constant.KeyGeneratorOptions._
import org.apache.hudi.common.model.HoodieRecord
import spark.implicits._

val tableName = "trips_table"
val basePath = "file:///tmp/trips_table"

// spark-shell
val columns = Seq("ts","uuid","rider","driver","fare","city")
val data =
  Seq((1695159649087L,"334e26e9-8355-45cc-97c6-c31daf0df330","rider-A","driver-K",19.10,"san_francisco"),
    (1695091554788L,"e96c4396-3fad-413a-a942-4cb36106d721","rider-C","driver-M",27.70 ,"san_francisco"),
    (1695046462179L,"9909a8b1-2d15-4d3d-8ec9-efc48c536a00","rider-D","driver-L",33.90 ,"san_francisco"),
    (1695516137016L,"e3cf430c-889d-4015-bc98-59bdce1e530c","rider-F","driver-P",34.15,"sao_paulo"    ),
    (1695115999911L,"c8abbe79-8d89-47ea-b4ce-4d224bae5bfa","rider-J","driver-T",17.85,"chennai"));

var inserts = spark.createDataFrame(data).toDF(columns:_*)
inserts.write.format("hudi").
  option("hoodie.datasource.write.partitionpath.field", "city").
  option("hoodie.table.name", tableName).
  option(PRECOMBINE_FIELD_OPT_KEY, "ts").
  option(RECORDKEY_FIELD_OPT_KEY, "uuid").
  option("hoodie.metadata.enable", "false").
  mode(Overwrite).
  save(basePath)

// Query Data
// spark-shell
val tripsDF = spark.read.format("hudi").load(basePath)
tripsDF.createOrReplaceTempView("trips_table")

spark.sql("SELECT uuid, fare, ts, rider, driver, city FROM  trips_table WHERE fare > 20.0").show()
spark.sql("SELECT _hoodie_commit_time, _hoodie_record_key, _hoodie_partition_path, rider, driver, fare FROM  trips_table").show()

// Update Data
// Lets read data from target Hudi table, modify fare column for rider-D and update it.
val updatesDf = spark.read.format("hudi").load(basePath).filter($"rider" === "rider-D").withColumn("fare", col("fare") * 10)

updatesDf.write.format("hudi").
  option("hoodie.datasource.write.operation", "upsert").
  option("hoodie.datasource.write.partitionpath.field", "city").
  option("hoodie.table.name", tableName).
  option(PRECOMBINE_FIELD_OPT_KEY, "ts").
  option(RECORDKEY_FIELD_OPT_KEY, "uuid").
  option("hoodie.metadata.enable", "false").
  mode(Append).
  save(basePath)