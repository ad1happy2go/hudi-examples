#!/bin/bash

# Initialize default values
localOrS3="local"
to_version=${HUDI_VERSION}
from_version="0.15.0"
spark_version="3.5"
test_jar=""
conf=""
expected_to_version=""
master="local[*]"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -m|--localOrS3)
            localOrS3="$2"
            shift 2
            ;;
        -sv|--spark_version)
            spark_version="$2"
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
        -etv|--expected_to_version)
            expected_to_version="$2"
            shift 2
            ;;
        -mas|--master)
            master="$2"
            shift 2
            ;;
        *)
            echo "Unknown parameter: $1"
            exit 1
            ;;
    esac
done
pwd
source ./utils.sh
epoch=`date +%s`
current_date=$(date +%Y%m%d)
tableName="table_comp_test_$(echo ${from_version//./_}_${to_version//./_}_${epoch} | sed 's/-rc1/_rc1/')"

if [ $localOrS3 == "s3" ]; then
    basePath="s3a://performance-benchmark-datasets-us-west-2/temporary_output/${current_date}/${tableName}"
    master="yarn"
else
    basePath="/Users/rahil/workplace/hudi-examples/spark/scripts/output/${current_date}/${tableName}"
    master="local"
fi

echo "Base path for test table : $basePath, from version : $from_version, to version : $to_version, expected_to_version : $expected_to_version"

echo "SPARK DIR USED - ${SPARK_HOME}"
echo "SPARK VERSION USED - ${spark_version}"

spark_configs=$(getSparkConfigs "$to_version")

echo ${spark_configs}

echo -e "\n\nRunning Spark shell command to load data and compare for batch 1 ======================================"

${SPARK_HOME}/bin/spark-shell --master ${master} --driver-memory 4g \
--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
--conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' \
--conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' \
--executor-cores 3 \
--packages org.apache.hudi:hudi-spark${spark_version}-bundle_2.12:${from_version} << EOF
:load ../src/main/scala/com/hudi/spark/TestAutomationUtils.scala
try {
  val batch = "1"
  TestAutomationUtils.loadData(spark, "${basePath}", "${tableName}", conf="${conf}", batch_id = batch, numInserts = 1000, numUpdates = 100, numDeletes = 10, upgrade = "true")
  TestAutomationUtils.compareData(spark, "${basePath}", batch_id = batch)
  assert(TestAutomationUtils.getCount(spark, "${basePath}") == 990)
  println("Batch 1 completed successfully")
} catch {
  case e: Exception =>
    println(s"Error executing batch1: \${e.getMessage}")
    e.printStackTrace()
    System.exit(1)
}
EOF

rm hoodie.properties

if [[ $basePath == s3a* ]]; then
    aws s3 cp s3://${basePath#s3a://}/.hoodie/hoodie.properties .
else
    cp ${basePath}/.hoodie/hoodie.properties .
fi

OLD_TABLE_VERSION_PROP=$(cat "hoodie.properties" | grep "hoodie.table.version")
export OLD_TABLE_VERSION="${OLD_TABLE_VERSION_PROP#*=}"

cat ${basePath}/.hoodie/hoodie.properties

echo -e "\n\n\nRunning Spark shell command to load data and compare for batch 2 ======================================"

${SPARK_HOME}/bin/spark-shell --master ${master} \
--conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar' \
--driver-memory 4g \
--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
--conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' \
--executor-cores 3 \
--conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' \
--jars ${test_jar} << EOF
:load ../src/main/scala/com/hudi/spark/TestAutomationUtils.scala
try {
  TestAutomationUtils.compareData(spark, "${basePath}", "1")
  val batch = "2"
  TestAutomationUtils.loadData(spark, "${basePath}", "${tableName}", conf="${conf}", batch_id = batch, numInserts = 1000, numUpdates = 100, numDeletes = 10)
  assert(TestAutomationUtils.getCount(spark, "${basePath}") == 1980)
  TestAutomationUtils.compareData(spark, "${basePath}", batch_id = batch)
  println("Batch 2 completed successfully")
} catch {
  case e: Exception =>
    println(s"Error executing batch2: \${e.getMessage}")
    e.printStackTrace()
    System.exit(1)
}
EOF

echo "Validating table version"

if [[ $basePath == s3a* ]]; then
    aws s3 cp s3://${basePath#s3a://}/.hoodie/hoodie.properties .
else
    cp ${basePath}/.hoodie/hoodie.properties .
fi

NEW_TABLE_VERSION_PROP=$(cat "hoodie.properties" | grep "hoodie.table.version")
export NEW_TABLE_VERSION="${NEW_TABLE_VERSION_PROP#*=}"

if [[ $NEW_TABLE_VERSION == $expected_to_version ]]; then
  echo "Table version matched"
else
  echo "Table version mis-match. expected $expected_to_version , but found $NEW_TABLE_VERSION. Exiting"
  exit 1
fi

echo -e "\n\n\nDowngrading Table to ${OLD_TABLE_VERSION} ======================================"

${SPARK_HOME}/bin/spark-shell --master ${master} \
--conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar' \
--driver-memory 4g \
--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
--conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' \
--executor-cores 3 \
--conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' \
--jars ${test_jar} << EOF
:load ../src/main/scala/com/hudi/spark/DowngradeTable.scala
:load ../src/main/scala/com/hudi/spark/TestAutomationUtils.scala
try {
  DowngradeTable.downgradeTable(spark, "${basePath}", ${OLD_TABLE_VERSION})
  TestAutomationUtils.compareData(spark, "${basePath}", "2")
  println("Downgrade completed successfully")
} catch {
  case e: Exception =>
    println(s"Error executing downgrade: \${e.getMessage}")
    e.printStackTrace()
    System.exit(1)
}
EOF

echo -e "\n\n\nRunning Spark shell command to load data and compare for batch 3 ======================================"

${SPARK_HOME}/bin/spark-shell --master ${master} --driver-memory 4g \
--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
--conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' \
--conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' \
--executor-cores 3 \
--packages org.apache.hudi:hudi-spark${spark_version}-bundle_2.12:${from_version} << EOF
:load ../src/main/scala/com/hudi/spark/TestAutomationUtils.scala
try {
  assert(TestAutomationUtils.getCount(spark, "${basePath}") == 1980)
  TestAutomationUtils.compareData(spark, "${basePath}", "2")
  val batch = "3"
  TestAutomationUtils.loadData(spark, "${basePath}", "${tableName}", conf="${conf}", batch_id = batch, numInserts = 1000, numUpdates = 100, numDeletes = 0)
  println("Batch 3 completed successfully")
} catch {
  case e: Exception =>
    println(s"Error executing batch3: \${e.getMessage}")
    e.printStackTrace()
    System.exit(1)
}
EOF

${SPARK_HOME}/bin/spark-shell --master ${master} --driver-memory 4g \
--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
--conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' \
--conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' \
--executor-cores 3 \
--jars ${test_jar} << EOF
:load ../src/main/scala/com/hudi/spark/TestAutomationUtils.scala
try {
  TestAutomationUtils.compareDataWithoutValidations(spark, "${basePath}", "3")
  val batch = "4"
  TestAutomationUtils.loadData(spark, "${basePath}", "${tableName}", conf="${conf}", batch_id = batch, numInserts = 1000, numUpdates = 100, numDeletes = 0, upgrade = "true")
  TestAutomationUtils.compareDataWithoutValidations(spark, "${basePath}", "4")
  println("Batch 4 completed successfully")
} catch {
  case e: Exception =>
    println(s"Error executing batch4: \${e.getMessage}")
    e.printStackTrace()
    System.exit(1)
}
EOF

echo "Downgrading Table to ${OLD_TABLE_VERSION}"

echo -e "\n\n\nDowngrading Table to ${OLD_TABLE_VERSION} ======================================"

${SPARK_HOME}/bin/spark-shell --master ${master} \
--conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar' \
--driver-memory 4g \
--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
--conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' \
--executor-cores 3 \
--conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' \
--jars ${test_jar} << EOF
:load ../src/main/scala/com/hudi/spark/DowngradeTable.scala
:load ../src/main/scala/com/hudi/spark/TestAutomationUtils.scala
try {
  DowngradeTable.downgradeTable(spark, "${basePath}", ${OLD_TABLE_VERSION})
  TestAutomationUtils.compareDataWithoutValidations(spark, "${basePath}", "4")
  println("Second downgrade completed successfully")
} catch {
  case e: Exception =>
    println(s"Error executing downgrade2: \${e.getMessage}")
    e.printStackTrace()
    System.exit(1)
}
EOF

echo -e "\n\n\Validating Table using ${OLD_TABLE_VERSION} ======================================"

${SPARK_HOME}/bin/spark-shell --master ${master} --driver-memory 4g \
--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
--conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' \
--conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' \
--executor-cores 3 \
--packages org.apache.hudi:hudi-spark${spark_version}-bundle_2.12:${from_version} << EOF
:load ../src/main/scala/com/hudi/spark/TestAutomationUtils.scala
try {
  TestAutomationUtils.compareDataWithoutValidations(spark, "${basePath}", "4")
  val batch = "5"
  TestAutomationUtils.loadData(spark, "${basePath}", "${tableName}", conf="${conf}", batch_id = batch, numInserts = 1000, numUpdates = 100, numDeletes = 0)
  println("Batch 5 completed successfully")
} catch {
  case e: Exception =>
    println(s"Error executing batch5: \${e.getMessage}")
    e.printStackTrace()
    System.exit(1)
}
EOF

echo -e "\n\n\nValidation table using toVersion ======================================"

${SPARK_HOME}/bin/spark-shell --master ${master} --driver-memory 4g \
--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
--conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' \
--conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' \
--executor-cores 3 \
--jars ${test_jar} << EOF
:load ../src/main/scala/com/hudi/spark/TestAutomationUtils.scala
try {
  TestAutomationUtils.compareDataWithoutValidations(spark, "${basePath}", "5")
  println("Final validation completed successfully")
} catch {
  case e: Exception =>
    println(s"Error executing validation: \${e.getMessage}")
    e.printStackTrace()
    System.exit(1)
}
EOF
