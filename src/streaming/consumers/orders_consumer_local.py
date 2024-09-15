import os
from time import sleep
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from read import read_order_stream

CONSUMER_NAME = "consumer_local"


# Spark
SPARK_AWAIT_TERMINATION = os.getenv("SPARK_AWAIT_TERMINATION", "60")

# Define o caminho para o arquivo de saída
SPARK_DATA_LOCATION = os.getenv("SPARK_DATA_LOCATION", "/content")

streamed_df = read_order_stream()

# Define / filtra os dados que serão salvos
shipping = streamed_df.select("shipping.*")


# Write Data to parquet
query = (
    shipping.writeStream.outputMode("append")
    .format("parquet")
    .option("failOnDataLoss", "false")
    .option("checkpointLocation", f"{SPARK_DATA_LOCATION}/{CONSUMER_NAME}/checkpoint")
    .option("path", f"{SPARK_DATA_LOCATION}/{CONSUMER_NAME}/data")
    .start()
)


# Esperar alguns segundos para gerar dados
query.awaitTermination(int(SPARK_AWAIT_TERMINATION))

# SUBMIT COMMAND
#docker exec -it spark_worker spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0  /consumer/orders_consumer_local.py
