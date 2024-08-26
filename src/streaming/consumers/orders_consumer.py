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

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "10.0.0.15:9092")

# Criar a SparkSession com o conector Kafka
spark = (
    SparkSession.builder.appName("Kafka Streaming Demo")
    .master("local[*]")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2")
    .getOrCreate()
)

# Set Spark log level to WARN
spark.sparkContext.setLogLevel("WARN")

topic = "orders"

# Ler os dados do Kafka
kafka_stream_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", topic)
    .load()
)

print("\n\n\n\nSTART -------------------------------------------\n")
print("INPUT SCHEMA:")
kafka_stream_df.printSchema()
print("\nEND -------------------------------------------\n\n\n\n")


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


# Define o caminho para o arquivo de saída
output_path = "/content/output"
checkpointLocation = "/content/checkpoint"

# Write Data to JSON
# query = (
#     parsed_df.writeStream.outputMode("append")
#     .format("json")
#     .option("failOnDataLoss", "false")
#     .option("checkpointLocation", checkpointLocation)
#     .option("path", output_path)
#     .start()
# )

shipping = parsed_df.select("shipping.*")

# Write Data to parquet
query = (
    shipping.writeStream.outputMode("append")
    .format("parquet")
    .option("failOnDataLoss", "false")
    .option("checkpointLocation", checkpointLocation)
    .option("path", output_path)
    .start()
)


# Esperar alguns segundos para gerar dados
query.awaitTermination(40)  # Ajuste o tempo conforme necessário


# docker exec -it spark_worker spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.2 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 /consumer/orders_consumer.py
