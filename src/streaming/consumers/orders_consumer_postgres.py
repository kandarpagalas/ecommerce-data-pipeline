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

# Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "10.0.0.15:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "orders")

# Spark
SPARK_AWAIT_TERMINATION = os.getenv("SPARK_AWAIT_TERMINATION", "60")

# Postgres
POSTGRES_DB = os.getenv("POSTGRES_DB", "z106")
POSTGRES_USER = os.getenv("POSTGRES_PASSWORD", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "PosUniforPass")
POSTGRES_DATABASE_ENDPOINT = os.getenv("POSTGRES_DATABASE_ENDPOINT", "10.0.0.15:35432")

# Criar a SparkSession com o conector Kafka
spark = (
    SparkSession.builder.appName("Kafka ORDERS Streaming")
    .master("local[*]")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2")
    .getOrCreate()
)


# Set Spark log level to WARN
spark.sparkContext.setLogLevel("WARN")

TOPIC = "orders"

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
customer = parsed_df.select(
    [
        "customer.id",
        "customer.created_at",
        "customer.name",
        "customer.email",
        "customer.tax_id",
    ]
)


# Define a tabela onde será salvo
DATABASE_TABLE = "customer"


# Saving batch to Postgres
def foreach_batch_function(batch_df, batch_id):

    batch_df.write.mode("append").format("jdbc").option(
        "driver", "org.postgresql.Driver"
    ).option(
        "url", f"jdbc:postgresql://{POSTGRES_DATABASE_ENDPOINT}/{POSTGRES_DB}"
    ).option(
        "dbtable", DATABASE_TABLE
    ).option(
        "user", POSTGRES_USER
    ).option(
        "password", POSTGRES_PASSWORD
    ).save()


# Define the streaming query
query = (
    customer.writeStream.option("failOnDataLoss", "false")
    .foreachBatch(foreach_batch_function)
    .start()
)

# Esperar alguns segundos para gerar dados
query.awaitTermination(int(SPARK_AWAIT_TERMINATION))

# SUBMIT COMMAND
# docker exec -it spark_worker spark-submit \
#   --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
#   --jars /content/jars/postgresql-42.7.4.jar \
#   /consumer/orders_consumer_postgres.py
