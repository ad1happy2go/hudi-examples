function getSparkConfigs() {
    local hudi_version=$1
    if [[ ${hudi_version} == "0.15."* ]]; then
      echo "--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' --conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar'"
    elif [[ ${hudi_version} == "0.14."* ]]; then
      echo "--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog'"
    elif [[ ${hudi_version} == "0.13."* ]]; then
      echo "--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' --conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar'"
    elif [[ ${hudi_version} == "0.12."* ]]; then
      echo "--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' --conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar'"
    else
      echo "Unsupported Spark Version - ${hudi_version}"
      exit 1
    fi
}