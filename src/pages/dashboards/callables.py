import os
import streamlit as st
from dotenv import load_dotenv
from datetime import datetime, timedelta
from pyspark.sql import SparkSession, functions as f

# import matplotlib.pyplot as plt
from pyspark.sql.functions import explode, col, date_format
from pyspark.sql.types import *

load_dotenv(".env")

DOCKER_CONTAINER = os.getenv("DOCKER_CONTAINER", "false")

# Minio
if DOCKER_CONTAINER == "true":
    MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minioserver:9000")
else:
    MINIO_ENDPOINT = "localhost:9000"

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "embuuWvqBMTLPRnbYXxu")
MINIO_SECRET_KEY = os.getenv(
    "MINIO_SECRET_KEY_", "ZApLkWzj71pCNrB0IBGvQ5s5a2x4AJ42XSFZxb39"
)

# Postgres
if DOCKER_CONTAINER == "true":
    POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres_z106:5432")
else:
    POSTGRES_HOST = "localhost:35432"

POSTGRES_DB = os.getenv("POSTGRES_DB", "z106")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "z106pass")

# Remote
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "z106")


@st.cache_resource
def get_spart_session():
    spark = (
        SparkSession.builder.appName("DW_z106")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.4")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .getOrCreate()
    )
    return spark


def query_postgres(spark, query):
    df = (
        spark.read.format("jdbc")
        .option("driver", "org.postgresql.Driver")
        .option("url", f"jdbc:postgresql://{POSTGRES_HOST}/{POSTGRES_DB}")
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("query", query)
        .load()
    )
    return df


# @st.cache_resource()
def get_customers_df(_spark):
    query = "SELECT * FROM customers"
    df_customers = query_postgres(_spark, query)
    # Criando a dimensão de clientes
    df_customers.createOrReplaceTempView("dm_customers")

    return df_customers


# @st.cache_resource()
def get_products_df(_spark):
    query = "SELECT * FROM products"
    df_products = query_postgres(_spark, query)
    # Criando a dimensão de produtos
    df_products.createOrReplaceTempView("dm_products")

    return df_products


def get_dates_df(spark):
    def generate_date_df(spark, start_date, end_date):
        date_list = [
            start_date + timedelta(days=x)
            for x in range(0, (end_date - start_date).days + 1)
        ]
        return spark.createDataFrame(date_list, DateType()).toDF("date")

    # Data de início e fim
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2023, 12, 31)

    # Gerando o DataFrame de datas
    df_dates = generate_date_df(spark, start_date, end_date)

    # Adicionando colunas de tempo
    df_time = (
        df_dates.withColumn("year", f.year("date"))
        .withColumn("month", f.month("date"))
        .withColumn("day", f.dayofmonth("date"))
        .withColumn("weekday", f.date_format("date", "E"))
        .withColumn("week_of_year", f.weekofyear("date"))
    )

    # Criando a dimensão de tempo
    df_time.createOrReplaceTempView("dm_time")

    return df_time


def get_fato_df(spark):
    query = "SELECT * FROM ft_orders"
    ft_orders_df = query_postgres(spark, query)
    ft_orders_df = ft_orders_df.withColumn(
        "year", f.year("charges_paid_at")
    ).withColumn("month", f.month("charges_paid_at"))

    # Criando a dimensão de produtos
    ft_orders_df.createOrReplaceTempView("ft_orders")

    return ft_orders_df
