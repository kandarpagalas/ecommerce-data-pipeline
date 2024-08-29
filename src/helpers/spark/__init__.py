import os
import streamlit as st
from pyspark.sql import SparkSession
import findspark

findspark.init()


# Initialize SparkSession
def init_spark(remote=None):
    if remote is None:
        return (
            SparkSession.builder.appName("ParquetReader")
            .config("spark.master", "local[*]")
            .getOrCreate()
        )

    remote = "spark://spark_master:7077"
    return (
        SparkSession.builder.appName("KafkaSparkStreaming")
        .config("spark.master", remote)
        .getOrCreate()
    )
