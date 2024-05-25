HOME_DIR=/Users/adityagoenka/
SPARK_HOME=${HOME_DIR}/plain_spark/spark-3.4.1-bin-hadoop3
${SPARK_HOME}/bin/spark-submit --name customer-event-hudideltaStream \
--jars ${HOME_DIR}/jars/0.14.1/spark32/hudi-spark3.4-bundle_2.12-0.14.1.jar \
--class org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer \
${HOME_DIR}/jars/0.14.1/spark32/hudi-utilities-slim-bundle_2.12-0.14.1.jar \
--checkpoint file:///tmp/hudistreamer/test/checkpoint1 \
--target-base-path file:///tmp/hudistreamer/test/output1 \
--target-table customer_profile --table-type COPY_ON_WRITE \
--base-file-format PARQUET \
--props kafka-source.props \
--source-class org.apache.hudi.utilities.sources.JsonKafkaSource --source-ordering-field ts \
--payload-class org.apache.hudi.common.model.DefaultHoodieRecordPayload \
--schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider \
--hoodie-conf hoodie.streamer.schemaprovider.source.schema.file=${HOME_DIR}/docker_demo/conf/schema.avsc \
--hoodie-conf hoodie.streamer.schemaprovider.target.schema.file=${HOME_DIR}/docker_demo/conf/schema.avsc \
--op UPSERT --hoodie-conf hoodie.streamer.source.kafka.topic=stock_ticks \
--hoodie-conf hoodie.datasource.write.partitionpath.field=year \
--continuous

