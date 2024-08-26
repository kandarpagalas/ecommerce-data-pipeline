# https://colab.research.google.com/drive/14uewVANFaFdHesZZiO6f2M-McAJVK7_t?usp=sharing#scrollTo=LH7Zs6IyFmRp

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StringType, StructType, StructField

# Read environment variables
kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
kafka_topic = os.getenv("KAFKA_TOPIC", "your_topic")


postgres_db = os.getenv("POSTGRES_DB", "postgres")
postgres_user = os.getenv("POSTGRES_USER", "postgres")
postgres_password = os.getenv("POSTGRES_PASSWORD", "postgres")
postgres_url = os.getenv(
    "POSTGRES_URL", "jdbc:postgresql://localhost:5432/your_database"
)

# Configura a SparkSession
spark = SparkSession.builder.appName("KafkaToPostgres").getOrCreate()

# Configura as propriedades do Kafka
kafka_bootstrap_servers = "localhost:9092"
kafka_topic = "your_topic"

# Define o esquema esperado para os dados
schema = StructType(
    [
        StructField("key", StringType(), True),
        StructField("value", StringType(), True),
        StructField("topic", StringType(), True),
        StructField("partition", StringType(), True),
        StructField("offset", StringType(), True),
        StructField("timestamp", StringType(), True),
    ]
)

# Lê o streaming de Kafka
df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
    .option("subscribe", kafka_topic)
    .load()
)

# Decodifica o conteúdo das mensagens Kafka
df = df.selectExpr("CAST(value AS STRING) as json")

## ADICIONAR LOGICA DA TRANSFORMAÇÃO


postgres_db = os.getenv("POSTGRES_DB", "postgres")
postgres_user = os.getenv("POSTGRES_USER", "postgres")
postgres_password = os.getenv("POSTGRES_PASSWORD", "postgres")
DATABASE_URL = "postgresql://postgres:PosUniforPass@z106-postgres:5432/z106"
postgres_endpoint = "localhost:5432"


# Define o modo de escrita para o PostgreSQL
def write_to_postgres(df, epoch_id):
    df.write.format("jdbc").option(
        "url", f"jdbc:postgresql://{postgres_endpoint}/{postgres_db}"
    ).option("dbtable", "vendas").option("user", postgres_user).option(
        "password", postgres_password
    ).option(
        "diver", "string que define o driver"
    ).mode(
        "append"
    ).save()


# Configura o stream para escrita
query = df.writeStream.foreachBatch(write_to_postgres).outputMode("append").start()

# Espera até que o streaming termine
query.awaitTermination()
