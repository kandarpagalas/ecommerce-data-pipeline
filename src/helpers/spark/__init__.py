import os
import streamlit as st
from pyspark.sql import SparkSession
import findspark

findspark.init()
# Set the path to your Spark installation
# spark_home = "/Users/kandarpagalas/Library/Mobile Documents/com~apple~CloudDocs/MyFolders/HomeLab/spark-3.5.2-bin-hadoop3"

# Set SPARK_HOME environment variable
# os.environ["SPARK_HOME"] = spark_home


# Spark
SPARK_AWAIT_TERMINATION = os.getenv("SPARK_AWAIT_TERMINATION", "60")

# Minio
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minioserver:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "78HevFfBbcX3fL2RbqYm")
MINIO_SECRET_KEY = os.getenv(
    "MINIO_SECRET_KEY", "DS5UZ2DTtwMAoRDCPnYlufktSfgydPVrgW1rDDQ0"
)
# Remote
SPARK_DATA_LOCATION = os.getenv("SPARK_DATA_LOCATION", "s3a://z106/")


# Initialize SparkSession
@st.cache_resource
def init_spark(remote=None, app_name="spark_app"):
    master = "local[*]"
    master = "spark://a47aeaa501aa:7077"
    spark = (
        SparkSession.builder.appName(app_name)
        # .config("spark.master", master)
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        # .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.4.0")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        # .config("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.4")
        .getOrCreate()
    )
    return spark
    # return SparkSession.builder.appName("KafkaSparkStreaming").getOrCreate()
