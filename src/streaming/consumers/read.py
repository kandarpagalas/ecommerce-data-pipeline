import os
from time import sleep
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


# Minio
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minioserver:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "78HevFfBbcX3fL2RbqYm")
MINIO_SECRET_KEY = os.getenv(
    "MINIO_SECRET_KEY", "DS5UZ2DTtwMAoRDCPnYlufktSfgydPVrgW1rDDQ0"
)

# Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "orders")


def read_order_stream():

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
        .option("failOnDataLoss", "false")
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
                                        StructField(
                                            "area", StringType(), nullable=False
                                        ),
                                        StructField(
                                            "type", StringType(), nullable=False
                                        ),
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
                                        StructField(
                                            "value", FloatType(), nullable=False
                                        ),
                                        StructField(
                                            "currency", StringType(), nullable=False
                                        ),
                                        StructField(
                                            "summary",
                                            StructType(
                                                [
                                                    StructField(
                                                        "total",
                                                        FloatType(),
                                                        nullable=False,
                                                    ),
                                                    StructField(
                                                        "paid",
                                                        FloatType(),
                                                        nullable=False,
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
                                        StructField(
                                            "type", StringType(), nullable=False
                                        ),
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

    return parsed_df
