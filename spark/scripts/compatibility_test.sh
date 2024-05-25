#!/bin/bash

# Initialize default values
localOrS3="local"
to_version="0.14.0"
from_version="0.13.0"
spark_version="3.2"
test_jar="hudi-spark3.2-bundle_2.12-0.14.0-SNAPSHOT.jar"
conf=""

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -m|--localOrS3)
            localOrS3="$2"
            shift 2
            ;;
        -tv|--to_version)
            to_version="$2"
            shift 2
            ;;
        -fv|--from_version)
            from_version="$2"
            shift 2
            ;;
        -j|--jar)
            test_jar="$2"
            shift 2
            ;;
        -c|--conf)
            conf="$2"
            shift 2
            ;;
        *)
            echo "Unknown parameter: $1"
            exit 1
            ;;
    esac
done

source utils.sh
epoch=`date +%s`
current_date=$(date +%Y%m%d)
tableName=table_comp_test_${from_version//./_}_${to_version//./_}_${epoch}

if [ $localOrS3 == "s3" ]; then
    basePath="s3a://<test_bucket>/output/${current_date}/${tableName}"
else
    basePath="/tmp/output/${current_date}/${tableName}"
fi

echo "SPARK DIR USED - ${SPARK_HOME}"

spark_configs=$(getSparkConfigs "$to_version")

echo ${spark_configs}

${SPARK_HOME}/bin/spark-shell --driver-memory 8g \
--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' --conf 'spark.sql.warehouse.dir=hdfs://localhost:8020/user/hive/warehouse' --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' --conf 'spark.hadoop.spark.sql.legacy.parquet.nanosAsLong=false'  --conf 'spark.hadoop.spark.sql.parquet.binaryAsString=false' --conf 'spark.hadoop.spark.sql.parquet.int96AsTimestamp=true' --conf 'spark.hadoop.spark.sql.caseSensitive=false'  \
--packages org.apache.hudi:hudi-spark${spark_version}-bundle_2.12:${from_version}<< EOF
:load ../src/main/scala/com/hudi/spark/TestAutomationUtils.scala
val batch="1"
TestAutomationUtils.loadData(spark, "${basePath}" ,"${tableName}", conf="${conf}", batch_id = batch, numInserts = 1000, numUpdates = 100, numDeletes = 10)
TestAutomationUtils.compareData(spark, "${basePath}" , batch_id = batch)
assert(TestAutomationUtils.getCount(spark, "${basePath}") ==  990)
EOF

OLD_TABLE_VERSION_PROP=$(cat "${basePath}/.hoodie/hoodie.properties" | grep "hoodie.table.version")
export OLD_TABLE_VERSION="${OLD_TABLE_VERSION_PROP#*=}"

${SPARK_HOME}/bin/spark-shell --driver-memory 8g \
--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' \
--jars ${test_jar} << EOF
:load ../src/main/scala/com/hudi/spark/TestAutomationUtils.scala
TestAutomationUtils.compareData(spark, "${basePath}" ,"1")
val batch="2"
TestAutomationUtils.loadData(spark, "${basePath}" ,"${tableName}", conf="${conf}", batch_id = batch, numInserts = 1000, numUpdates = 100, numDeletes = 10)
assert(TestAutomationUtils.getCount(spark, "${basePath}") ==  1980)
TestAutomationUtils.compareData(spark, "${basePath}" , batch_id = batch)
EOF

echo "Downgrading Table to " ${OLD_TABLE_VERSION}

${SPARK_HOME}/bin/spark-shell --driver-memory 8g \
--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' \
--jars ${test_jar} << EOF
:load ../src/main/scala/com/hudi/spark/TestAutomationUtils.scala
TestAutomationUtils.downgradeTable(spark, "${basePath}", ${OLD_TABLE_VERSION})
EOF

${SPARK_HOME}/bin/spark-shell --driver-memory 8g \
--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' --conf 'spark.sql.warehouse.dir=hdfs://localhost:8020/user/hive/warehouse' --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' --conf 'spark.hadoop.spark.sql.legacy.parquet.nanosAsLong=false'  --conf 'spark.hadoop.spark.sql.parquet.binaryAsString=false' --conf 'spark.hadoop.spark.sql.parquet.int96AsTimestamp=true' --conf 'spark.hadoop.spark.sql.caseSensitive=false'  \
--packages org.apache.hudi:hudi-spark${spark_version}-bundle_2.12:${from_version}<< EOF
:load ../src/main/scala/com/hudi/spark/TestAutomationUtils.scala
assert(TestAutomationUtils.getCount(spark, "${basePath}") ==  1980)
TestAutomationUtils.compareData(spark, "${basePath}" ,"2")
val batch="3"
TestAutomationUtils.loadData(spark, "${basePath}" ,"${tableName}", conf="${conf}", batch_id = batch, numInserts = 1000, numUpdates = 100, numDeletes = 0)
EOF

echo "Forcing Rollback by Deleteing the latest .commit file"
latest_file=$(ls -t ${basePath}/.hoodie/*commit | head -n 1)
rm -f "$latest_file"

${SPARK_HOME}/bin/spark-shell --driver-memory 8g \
--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' --conf 'spark.sql.warehouse.dir=hdfs://localhost:8020/user/hive/warehouse' --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' --conf 'spark.hadoop.spark.sql.legacy.parquet.nanosAsLong=false'  --conf 'spark.hadoop.spark.sql.parquet.binaryAsString=false' --conf 'spark.hadoop.spark.sql.parquet.int96AsTimestamp=true' --conf 'spark.hadoop.spark.sql.caseSensitive=false'  \
--packages org.apache.hudi:hudi-spark${spark_version}-bundle_2.12:${from_version}<< EOF
:load ../src/main/scala/com/hudi/spark/TestAutomationUtils.scala
assert(TestAutomationUtils.getCount(spark, "${basePath}") ==  2980)
TestAutomationUtils.compareOnlyInserts(spark, "${basePath}" , batch_id = "3")
val batch="4"
TestAutomationUtils.loadData(spark, "${basePath}" ,"${tableName}", conf="${conf}", batch_id = batch, numInserts = 1000, numUpdates = 100, numDeletes = 10)
TestAutomationUtils.compareData(spark, "${basePath}" , batch_id = batch)
assert(TestAutomationUtils.getCount(spark, "${basePath}") ==  3970)
EOF

${SPARK_HOME}/bin/spark-shell --driver-memory 8g \
--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' \
--jars ${test_jar} << EOF
:load ../src/main/scala/com/hudi/spark/TestAutomationUtils.scala
assert(TestAutomationUtils.getCount(spark, "${basePath}") ==  3970)
TestAutomationUtils.compareData(spark, "${basePath}" ,"4")
val batch="5"
TestAutomationUtils.loadData(spark, "${basePath}" ,"${tableName}", conf="${conf}", batch_id = batch, numInserts = 1000, numUpdates = 100, numDeletes = 10)
val count = TestAutomationUtils.compareData(spark, "${basePath}" , batch_id = batch)
assert(TestAutomationUtils.getCount(spark, "${basePath}") ==  4960)
TestAutomationUtils.compareData(spark, "${basePath}" ,"5")
EOF