try:
    import sys, os, uuid
    from pyspark.context import SparkContext
    from pyspark.sql.session import SparkSession
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from pyspark.sql.types import *
    from pyspark.sql.functions import *
    from faker import Faker
except Exception as e:
    print("Modules are missing: {}".format(e))
import time
# Get command-line arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Create a Spark session and Glue context
spark = (SparkSession.builder.config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer') \
         .config('spark.sql.hive.convertMetastoreParquet', 'false') \
         .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.hudi.catalog.HoodieCatalog') \
         .config('spark.sql.extensions', 'org.apache.spark.sql.hudi.HoodieSparkSessionExtension') \
         .config('spark.sql.legacy.pathOptionBehavior.enabled', 'true').getOrCreate())

sc = spark.sparkContext
glueContext = GlueContext(sc)
job = Job(glueContext)
logger = glueContext.get_logger()
job.init(args['JOB_NAME'], args)

# ============================== Settings =======================================
db_name = "hudidb_oss"
table_name = "s3_calls_bench_06112023_0_13_1"
path = "s3://s3-calls-log-bucket/hudi/output/" + table_name
method = 'upsert'
table_type = "COPY_ON_WRITE"
# ====================================================================================
spark.sql(f""" DROP TABLE IF EXISTS {db_name}.{table_name}""")
spark.sql(f"""
CREATE TABLE {db_name}.{table_name}(
            ss_sold_date_sk INT,
            ss_sold_time_sk INT,
            ss_item_sk INT,
            ss_customer_sk INT,
            ss_cdemo_sk INT,
            ss_hdemo_sk INT,
            ss_addr_sk INT,
            ss_store_sk INT,
            ss_promo_sk INT,
            ss_ticket_number BIGINT,
            ss_quantity INT,
            ss_wholesale_cost DECIMAL(
                7,
                2
            ),
            ss_list_price DECIMAL(
                7,
                2
            ),
            ss_sales_price DECIMAL(
                7,
                2
            ),
            ss_ext_discount_amt DECIMAL(
                7,
                2
            ),
            ss_ext_sales_price DECIMAL(
                7,
                2
            ),
            ss_ext_wholesale_cost DECIMAL(
                7,
                2
            ),
            ss_ext_list_price DECIMAL(
                7,
                2
            ),
            ss_ext_tax DECIMAL(
                7,
                2
            ),
            ss_coupon_amt DECIMAL(
                7,
                2
            ),
            ss_net_paid DECIMAL(
                7,
                2
            ),
            ss_net_paid_inc_tax DECIMAL(
                7,
                2
            ),
            ss_net_profit DECIMAL(
                7,
                2
            )
        )
            USING hudi OPTIONS(
            PATH '{path}'
        ) PARTITIONED BY(ss_sold_date_sk) TBLPROPERTIES(
            'primaryKey' = 'ss_item_sk,ss_ticket_number',
            'preCombineField' = 'ss_sold_time_sk',
            'hoodie.metadata.enable' = 'true'
        )
""")

hudi_options = {
    'hoodie.table.name': table_name,
    'hoodie.datasource.write.partitionpath.field': 'ss_sold_date_sk',
    'hoodie.datasource.write.recordkey.field' : 'ss_item_sk,ss_ticket_number',
    'hoodie.datasource.write.precombine.field': 'ss_sold_time_sk'
}

spark.read.table(f"{db_name}.store_sales_external").withColumn("ss_sold_time_sk",lit(1)).write.options(**hudi_options).format("hudi").mode("overwrite").save(path)



spark.read.table(f"{db_name}.store_sales_upsert_2452").withColumn("ss_sold_time_sk",lit(2)).write.options(**hudi_options).format("hudi").mode("append").save(path)

spark.read.table(f"{db_name}.store_sales_merge_2452").withColumn("ss_sold_time_sk",lit(3)).createOrReplaceTempView("source")
spark.read.format("hudi").laod(f"{db_name}.{table_name}").createOrReplaceTempView("target")

spark.sql(f"""MERGE INTO target USING  source ON 
source.ss_sold_date_sk = target.ss_sold_date_sk and source.ss_ticket_number = target.ss_ticket_number and source.ss_item_sk = target.ss_item_sk
WHEN MATCHED THEN UPDATE SET target.ss_list_price = source.ss_list_price""")

spark.read.format("hudi").load(f"{db_name}.{table_name}").createOrReplaceTempView("target")

spark.sql(f"""MERGE INTO target USING source ON 
source.ss_sold_date_sk = target.ss_sold_date_sk and source.ss_ticket_number = target.ss_ticket_number and source.ss_item_sk = target.ss_item_sk
WHEN MATCHED THEN DELETE""")
