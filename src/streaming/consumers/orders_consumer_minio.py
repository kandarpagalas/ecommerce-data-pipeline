import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    FloatType,
    IntegerType,
    ArrayType,
)

CONSUMER_NAME = "consumer_minio"

# Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "orders")

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
spark = (
    SparkSession.builder.appName("Kafka ORDERS Streaming")
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .getOrCreate()
)

# Set Spark log level to WARN
spark.sparkContext.setLogLevel("WARN")

# Ler os dados do Kafka
kafka_stream_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .load()
)

schema = StructType(
    [
        StructField("id", StringType(), nullable=False),
        StructField("reference_id", StringType(), nullable=False),
        StructField("created_at", StringType(), nullable=False),
        StructField(
            "shipping",
            StructType(
                [
                    StructField("street", StringType(), nullable=True),
                    StructField("number", StringType(), nullable=True),
                    StructField("complement", StringType(), nullable=True),
                    StructField("locality", StringType(), nullable=True),
                    StructField("city", StringType(), nullable=True),
                    StructField("region_code", StringType(), nullable=True),
                    StructField("country", StringType(), nullable=True),
                    StructField("postal_code", StringType(), nullable=True),
                ]
            ),
            nullable=False,
        ),
        StructField(
            "items",
            ArrayType(
                StructType(
                    [
                        StructField("name", StringType(), nullable=False),
                        StructField("categoria", StringType(), nullable=False),
                        StructField("reference_id", StringType(), nullable=False),
                        StructField("unit_price", FloatType(), nullable=False),
                        StructField("quantity", IntegerType(), nullable=False),
                    ]
                )
            ),
            nullable=False,
        ),
        StructField(
            "customer",
            StructType(
                [
                    StructField("id", StringType(), nullable=False),
                    StructField("created_at", StringType(), nullable=False),
                    StructField("name", StringType(), nullable=False),
                    StructField("email", StringType(), nullable=False),
                    StructField("tax_id", StringType(), nullable=False),
                    StructField(
                        "phones",
                        ArrayType(
                            StructType(
                                [
                                    StructField(
                                        "country", StringType(), nullable=False
                                    ),
                                    StructField("area", StringType(), nullable=False),
                                    StructField("type", StringType(), nullable=False),
                                ]
                            )
                        ),
                        nullable=True,
                    ),
                ]
            ),
            nullable=False,
        ),
        StructField(
            "charges",
            ArrayType(
                StructType(
                    [
                        StructField("id", StringType(), nullable=False),
                        StructField("reference_id", StringType(), nullable=False),
                        StructField("status", StringType(), nullable=False),
                        StructField("created_at", StringType(), nullable=False),
                        StructField("paid_at", StringType(), nullable=True),
                        StructField("description", StringType(), nullable=True),
                        StructField(
                            "amount",
                            StructType(
                                [
                                    StructField("value", FloatType(), nullable=False),
                                    StructField(
                                        "currency", StringType(), nullable=False
                                    ),
                                    StructField(
                                        "summary",
                                        StructType(
                                            [
                                                StructField(
                                                    "total", FloatType(), nullable=False
                                                ),
                                                StructField(
                                                    "paid", FloatType(), nullable=False
                                                ),
                                                StructField(
                                                    "refunded",
                                                    FloatType(),
                                                    nullable=False,
                                                ),
                                            ]
                                        ),
                                        nullable=False,
                                    ),
                                ]
                            ),
                            nullable=False,
                        ),
                        StructField(
                            "payment_method",
                            StructType(
                                [
                                    StructField("type", StringType(), nullable=False),
                                    StructField(
                                        "pix",
                                        StructType(
                                            [
                                                StructField(
                                                    "notification_id",
                                                    StringType(),
                                                    nullable=False,
                                                ),
                                                StructField(
                                                    "end_to_end_id",
                                                    StringType(),
                                                    nullable=False,
                                                ),
                                                StructField(
                                                    "holder",
                                                    StructType(
                                                        [
                                                            StructField(
                                                                "name",
                                                                StringType(),
                                                                nullable=False,
                                                            ),
                                                            StructField(
                                                                "tax_id",
                                                                StringType(),
                                                                nullable=False,
                                                            ),
                                                        ]
                                                    ),
                                                    nullable=False,
                                                ),
                                            ]
                                        ),
                                        nullable=False,
                                    ),
                                ]
                            ),
                            nullable=False,
                        ),
                    ]
                )
            ),
            nullable=False,
        ),
    ]
)


# Extract the JSON data from Kafka messages
json_df = kafka_stream_df.selectExpr("CAST(value AS STRING)")

# Parse JSON data using from_json function
parsed_df = json_df.select(from_json(col("value"), schema).alias("data")).select(
    "data.*"
)

# Define / filtra os dados que serão salvos
shipping = parsed_df.select("shipping.*")


# Define the query to write DataFrame to MinIO as Parquet
query = (
    parsed_df.writeStream.outputMode("append")
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
