#!/bin/bash

# Initialize default values
localOrS3="local"
version="jar"
spark_version="3.4"
test_jar="s3a://performance-benchmark-datasets-us-west-2/jars/release_testing_jars/0.15.0-rc1/3.4/hudi-utilities-bundle_2.12-0.15.0-rc1.jar"
conf=""

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -m|--localOrS3)
            localOrS3="$2"
            shift 2
            ;;
        -v|--version)
            version="$2"
            shift 2
            ;;
        -hv|--hudiVersion)
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
current_date=$(date +%Y%m%d%H%M)
dbName=db_sql_test_${epoch}

if [ $localOrS3 == "s3" ]; then
    basePathFolder="s3a://performance-benchmark-datasets-us-west-2/temporary_output/${current_date}"
    MASTER="yarn"
else
    basePathFolder="/tmp/output/${current_date}"
    MASTER="local"
fi

echo "SPARK DIR USED - ${SPARK_HOME}"

echo ${spark_configs}

if [[ $version == "jar" ]]; then
    jar_conf="--jars ${test_jar}"
else
    jar_conf="--packages org.apache.hudi:hudi-spark${spark_version}-bundle_2.12:${from_version}"
fi

function runSQLQuickstart(){
local test=$1
local part=$2
local params=$3
echo props=${part}
echo props=${params}
epoch=`date +%s`
tableName=table_sql_test_${epoch}
basePath=$basePathFolder/${tableName}
${SPARK_HOME}/bin/spark-sql --master ${MASTER} \
--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' --conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar' --conf 'spark.hadoop.spark.sql.legacy.parquet.nanosAsLong=false'  --conf 'spark.hadoop.spark.sql.parquet.binaryAsString=false' --conf 'spark.hadoop.spark.sql.parquet.int96AsTimestamp=true' --conf 'spark.hadoop.spark.sql.caseSensitive=false'  \
${jar_conf} \
-i ../sql/quickstart.sql --hivevar partition="${part}" --hivevar props="${params}" --hivevar path="${basePath}"  > logs/${test}.log 2> logs/error_logs.txt
}

test_name="mor_partitioned_record_key"
echo "Testing mor Table With Partition Key and Record Key. Output at logs/${test_name}.log"
runSQLQuickstart $test_name "USING HUDI PARTITIONED BY (city)" "type = 'mor', primaryKey = 'uuid', preCombineField = 'ts'"

test_name="mor_non_partitioned_record_key"
echo "Testing mor Table With Partition Key and Record Key. Output at logs/${test_name}.log"
runSQLQuickstart $test_name "USING HUDI" "type = 'mor', primaryKey = 'uuid', preCombineField = 'ts'"

test_name="mor_non_partitioned_pkless"
echo "Testing mor Table Without Partition Key and without Record Key. Output at logs/${test_name}.log"
runSQLQuickstart $test_name "USING HUDI" " type = 'mor'"

test_name="mor_partitioned_pkless"
echo "Testing mor Table With Partition Key and Record Key. Output at logs/${test_name}.log"
runSQLQuickstart $test_name "USING HUDI PARTITIONED BY (city)" " type = 'mor'"
test_name="cow_partitioned_record_key"
echo "Testing COW Table With Partition Key and Record Key. Output at logs/${test_name}.log"
runSQLQuickstart $test_name "USING HUDI PARTITIONED BY (city)" "type = 'cow', primaryKey = 'uuid', preCombineField = 'ts'"

test_name="cow_non_partitioned_record_key"
echo "Testing COW Table With Partition Key and Record Key. Output at logs/${test_name}.log"
runSQLQuickstart $test_name "USING HUDI" "type = 'cow', primaryKey = 'uuid', preCombineField = 'ts'"

test_name="cow_non_partitioned_pkless"
echo "Testing COW Table With Partition Key and Record Key. Output at logs/${test_name}.log"
runSQLQuickstart $test_name "USING HUDI" " type = 'cow'"

test_name="cow_partitioned_pkless"
echo "Testing COW Table With Partition Key and Record Key. Output at logs/${test_name}.log"
runSQLQuickstart $test_name "USING HUDI PARTITIONED BY (city)" " type = 'cow'"

