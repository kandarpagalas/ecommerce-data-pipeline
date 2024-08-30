import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from read import read_order_stream
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    FloatType,
    IntegerType,
    ArrayType,
)

CONSUMER_NAME = "consumer_shipping"

# Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "orders")

# Spark
SPARK_AWAIT_TERMINATION = os.getenv("SPARK_AWAIT_TERMINATION", "60")

# Remote
SPARK_DATA_LOCATION = os.getenv("SPARK_DATA_LOCATION", "s3a://z106/")


# Recebe o stream convertido em DF
streamed_df = read_order_stream()

# Define / filtra os dados que serão salvos
shipping = streamed_df.select("shipping.*")


# Define the query to write DataFrame to MinIO as Parquet
query = (
    shipping.writeStream.outputMode("append")
    .format("parquet")
    .option("checkpointLocation", f"{SPARK_DATA_LOCATION}/{CONSUMER_NAME}/checkpoint")
    .option("path", f"{SPARK_DATA_LOCATION}/{CONSUMER_NAME}/data")
    .start()
)


# Esperar alguns segundos para gerar dados
query.awaitTermination(int(SPARK_AWAIT_TERMINATION))

# SUBMIT COMMAND
# docker exec -it spark_worker spark-submit \
#   --packages org.apache.hadoop:hadoop-aws:3.3.2 \
#   --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
#   /consumer/orders_consumer_minio.py
