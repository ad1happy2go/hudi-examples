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
table_name = "s3_calls_bench_testing_0611"
path = "s3://s3-calls-log-bucket/hudi/output/" + table_name
method = 'upsert'
table_type = "COPY_ON_WRITE"
# ====================================================================================
spark.sql(f""" DROP TABLE {db_name}.{table_name}""")
spark.sql(f"""
CREATE
    TABLE
        {db_name}.{table_name}(
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

spark.read.table(f"{db_name}.store_sales_external").withColumn("ss_sold_time_sk",lit("1")).createOrReplaceTempView("external")

spark.sql(f"""INSERT OVERWRITE {db_name}.{table_name} SELECT * FROM external LIMIT 10""")

spark.read.table(f"{db_name}.store_sales_upsert_2452").withColumn("ss_sold_time_sk",lit("2")).write.format("hudi").mode("append").save(path)

spark.read.table(f"{db_name}.store_sales_merge_2452").withColumn("ss_sold_time_sk",lit("3")).createOrReplaceTempView("store_sales_merge")

spark.sql(f"""MERGE INTO {db_name}.{table_name} target USING store_sales_merge source ON 
source.ss_sold_date_sk = target.ss_sold_date_sk and source.sr_ticket_number = target.ss_ticket_number and source.ss_item_sk = target.ss_item_sk
WHEN MATCHED SET target.ss_list_price = source.ss_list_price""")

spark.sql(f"""MERGE INTO {db_name}.{table_name}
                                USING(
                                SELECT
                                    *
                                FROM (SELECT MIN( d_date_sk ) AS min_date
                                        FROM {db_name}.date_dim
                                        WHERE d_date BETWEEN '2013-01-01' AND '2013-01-02'
                                    ) r
                                JOIN(
                                        SELECT
                                            MAX( d_date_sk ) AS max_date
                                        FROM
                                            {db_name}.date_dim
                                        WHERE d_date BETWEEN '2013-01-01' AND '2013-01-02'
                                    ) s
                            ) SOURCE ON
                            ss_sold_date_sk >= min_date
                            AND ss_sold_date_sk <= max_date
                            WHEN MATCHED THEN DELETE""")


