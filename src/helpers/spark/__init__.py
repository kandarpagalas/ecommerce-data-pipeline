import os
import streamlit as st
from pyspark.sql import SparkSession
import findspark

findspark.init()
# Set the path to your Spark installation
spark_home = "/Users/kandarpagalas/Library/Mobile Documents/com~apple~CloudDocs/MyFolders/HomeLab/spark-3.5.2-bin-hadoop3"

# Set SPARK_HOME environment variable
os.environ["SPARK_HOME"] = spark_home


# Initialize SparkSession
def init_spark(remote=None):
    if remote is None:
        return (
            SparkSession.builder.appName("ParquetReader")
            .config("spark.master", "local[*]")
            .getOrCreate()
        )

    remote = "spark://localhost:7077"
    return (
        SparkSession.builder.appName("KafkaSparkStreaming")
        .config("spark.master", remote)
        .getOrCreate()
    )
