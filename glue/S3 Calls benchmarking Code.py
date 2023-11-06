try:
    import sys, os, uuid
    import re
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import *
    from pyspark.sql.types import StringType
    import os
    import boto3
    from pyspark.sql.types import *
    from pyspark.context import SparkContext
    from pyspark.sql.session import SparkSession
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
except Exception as e:
    print("Modules are missing: {}".format(e))
# Define constants
table_name = "s3_calls_bench_0_13_1_0611"
hudi_table_bucket_name = "s3-calls-log-bucket"
hudi_table_base_path = f"hudi/output/{table_name}"
access_logs_path = "s3a://rxusandbox-us-west-2/s3_access_logs/"
excel_path = "s3a://rxusandbox-us-west-2/s3_calls_bench_results/" + table_name + ".xlsx"

# Create a Spark session
def create_spark_session(isLocalRun = True):
    if(isLocalRun):
        return (SparkSession.builder.config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer') \
            .config('spark.sql.hive.convertMetastoreParquet', 'false') \
            .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.hudi.catalog.HoodieCatalog') \
            .config('spark.sql.extensions', 'org.apache.spark.sql.hudi.HoodieSparkSessionExtension') \
            .config("spark.hadoop.fs.s3a.access.key", os.environ.get("AWS_ACCESS_KEY_ID")) \
            .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("AWS_SECRET_ACCESS_KEY")) \
            .config("spark.hadoop.fs.s3a.session.token", os.environ.get("AWS_SESSION_TOKEN")) \
            .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider') \
            .config("spark.jars.packages", "com.amazonaws:aws-java-sdk-bundle:1.11.375,org.apache.hadoop:hadoop-aws:3.2.0,org.apache.hudi:hudi-spark3.2-bundle_2.12:0.14.0,com.crealytics:spark-excel_2.12:3.5.0_0.20.1") \
            .config('spark.sql.legacy.pathOptionBehavior.enabled', 'true').getOrCreate())
    else:
        args = getResolvedOptions(sys.argv, ['JOB_NAME'])
        return (SparkSession.builder.config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer') \
            .config('spark.sql.hive.convertMetastoreParquet', 'false') \
            .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.hudi.catalog.HoodieCatalog') \
            .config('spark.sql.extensions', 'org.apache.spark.sql.hudi.HoodieSparkSessionExtension') \
            .config('spark.sql.legacy.pathOptionBehavior.enabled', 'true').getOrCreate())

# Define the UDF to extract substrings
def extract_substrings(value):
    pattern = "\\[(.*?)\\]"
    matches = re.findall(pattern, value)
    return [match.replace("[", "").replace("]", "") for match in matches]

def read_s3_logs(spark, access_logs_path, table_name):
    extract_substrings_udf = udf(extract_substrings, StringType())
    df = spark.read.text(access_logs_path) \
        .withColumn("date", extract_substrings_udf(col("value"))) \
        .withColumn("timestamp", from_utc_timestamp(to_timestamp(expr("SUBSTRING(split(date,',')[0],2,length(date)-2)"), "dd/MMM/yyyy:HH:mm:ss Z"), "UTC")) \
        .withColumn("call_type", expr("""split(value,' ')[7]""")) \
        .withColumn("path",  expr("""case when call_type = '''REST.GET.BUCKET''' then split(value,' ')[10] else split(value,' ')[8] end""")) \
        .filter(col("value").like(f"%{table_name}%"))
    df.cache()
    return df

# Find total number of S3 calls
def total_s3_calls(df):
    out = df.groupBy("call_type").agg(count(lit(1)).alias("count")).orderBy(col("count").desc())
    out.write.format("com.crealytics.spark.excel") \
    .option("dataAddress", "'call_type'!B3") \
    .option("header", "true") \
    .option("dateFormat", "yy-mmm-d") \
    .option("timestampFormat", "mm-dd-yyyy hh:mm:ss") \
    .mode("append") \
    .save(excel_path)

# Find 20 paths with most calls
def top_paths_with_calls(df):
    out = df.groupBy("call_type", "path").agg(count(lit(1)).alias("count")).orderBy(col("count").desc()).show(20, False)
    out.write.format("com.crealytics.spark.excel") \
        .option("dataAddress", "'call_type'!B3") \
        .option("header", "true") \
        .option("dateFormat", "yy-mmm-d") \
        .option("timestampFormat", "mm-dd-yyyy hh:mm:ss") \
        .mode("append") \
        .save(excel_path)

# Initialize the S3 client
def init_s3_client():
    return boto3.client(
        's3',
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        aws_session_token=os.environ.get("AWS_SESSION_TOKEN"),
    )

# List objects in S3 bucket
def list_s3_objects(s3, hudi_table_bucket_name, hudi_table_base_path):
    response = s3.list_objects_v2(Bucket=hudi_table_bucket_name, Prefix=f"{hudi_table_base_path}/.hoodie/202")

    commit_range = {}
    s3_contents = response.get('Contents', [])

    for obj in s3_contents:
        instant = obj["Key"].split("/")[-1]
        instant_time = instant.split(".")[0]
        updated_time = obj["LastModified"]
        if instant_time in commit_range and commit_range[instant_time] is not None:
            commit_range[instant_time].append(updated_time)
        else:
            commit_range[instant_time] = [updated_time]

    return commit_range

# Number of calls for each commit
def calls_for_each_commit(df, commit_range):
    df.count()
    commit_counts = {}
    for commit in commit_range:
        print(commit_range[commit])
        commit_counts[commit] = df.filter(col("timestamp") <= commit_range[commit][0]).filter(col("timestamp") >= commit_range[commit][2]).count()

    return commit_counts

# Main function to orchestrate the analysis
def main():
    spark = create_spark_session()
    df = read_s3_logs(spark, access_logs_path, table_name)

    print("Total S3 calls by call type")
    total_s3_calls(df)

    print("20 Paths with maximum Calls")
    top_paths_with_calls(df)

    s3 = init_s3_client()
    commit_range = list_s3_objects(s3, hudi_table_bucket_name, hudi_table_base_path)

    print("Number of calls for each commit")
    commit_counts = calls_for_each_commit(df, commit_range)
    spark.createDataFrame(commit_counts).write.format("com.crealytics.spark.excel") \
        .option("dataAddress", "'call_type'!B3") \
        .option("header", "true") \
        .option("dateFormat", "yy-mmm-d") \
        .option("timestampFormat", "mm-dd-yyyy hh:mm:ss") \
        .mode("append") \
        .save(excel_path)

if __name__ == "__main__":
    main()
