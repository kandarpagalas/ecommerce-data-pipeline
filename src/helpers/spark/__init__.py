import streamlit as st
from pyspark.sql import SparkSession


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
