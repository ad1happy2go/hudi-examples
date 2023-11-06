try:
    import sys, os, uuid
    from pyspark.context import SparkContext
    from pyspark.sql.session import SparkSession
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from pyspark.sql.types import *
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
table_name = "store_sales"
NOW=int(round(time.time()))
path = "s3://s3-calls-log-bucket/hudi/output/" + table_name
method = 'upsert'
table_type = "COPY_ON_WRITE"
# ====================================================================================
spark.sql(f"""DROP TABLE IF EXISTS {db_name}.date_dim""")

spark.sql(f"""CREATE
    TABLE
        {db_name}.date_dim(
            d_date_sk INT,
            d_date_id string,
            d_date DATE,
            d_month_seq INT,
            d_week_seq INT,
            d_quarter_seq INT,
            d_year INT,
            d_dow INT,
            d_moy INT,
            d_dom INT,
            d_qoy INT,
            d_fy_year INT,
            d_fy_quarter_seq INT,
            d_fy_week_seq INT,
            d_day_name string,
            d_quarter_name string,
            d_holiday string,
            d_weekend string,
            d_following_holiday string,
            d_first_dom INT,
            d_last_dom INT,
            d_same_day_ly INT,
            d_same_day_lq INT,
            d_current_day string,
            d_current_week string,
            d_current_month string,
            d_current_quarter string,
            d_current_year string
        ) USING csv OPTIONS(
            PATH = "s3://performance-benchmark-datasets/TPC-DS/100GB/date_dim/" ,sep="|",header="false",emptyValue="",dateFormat="yyyy-MM-dd",timestampFormat="yyyy-MM-dd HH:mm:ss[.SSS]"
        )""")
        
spark.sql(f"""DROP TABLE IF EXISTS  {db_name}.store_sales_external""")
spark.sql(f"""CREATE TABLE {db_name}.store_sales_external(
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
            USING csv OPTIONS(
            PATH = "s3://performance-benchmark-datasets/TPC-DS/100GB/store_sales/" ,sep="|",header="false",emptyValue="",dateFormat="yyyy-MM-dd",timestampFormat="yyyy-MM-dd HH:mm:ss[.SSS]"
        )""")

spark.sql(f"""DROP TABLE IF EXISTS {db_name}.store_sales_upsert_2452""")

spark.sql(f"""CREATE TABLE {db_name}.store_sales_upsert_2452 USING PARQUET LOCATION 's3://performance-benchmark-datasets/TPC-DS/100GB/store_sales_upsert_2452' as SELECT * FROM {db_name}.store_sales_external WHERE ss_sold_date_sk > '2452000' """)

spark.sql(f"""DROP TABLE IF EXISTS {db_name}.store_sales_merge_2452""")
spark.sql(f"""CREATE TABLE {db_name}.store_sales_merge_2452 USING PARQUET LOCATION 's3://performance-benchmark-datasets/TPC-DS/100GB/store_sales_merge_2452' as SELECT ss_sold_date_sk,ss_sold_time_sk,ss_item_sk,ss_ticket_number,ss_list_price * 2 as ss_list_price  FROM {db_name}.store_sales_upsert_2452 """)



