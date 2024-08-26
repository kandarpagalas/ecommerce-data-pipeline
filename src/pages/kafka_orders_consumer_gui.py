import os
import sys
import json
import signal
from datetime import datetime
import streamlit as st
from streamlit import session_state as ss
from kafka import KafkaConsumer

import findspark
from pyspark.sql.functions import col

from src.helpers.spark import init_spark
from src.helpers.kafka.producer import OrderProducer


# !spark-submit --class TP3 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 TweetCount.ipynb

findspark.init()


def kafka_is_up():
    order_producer = OrderProducer(kafka_server=ss.bootstrap_servers)
    status = order_producer.get_status()

    if not status["conn"]:
        st.error(status["message"], icon="❌")

    return status["conn"]


st.title("KAFKA CONSUMER")


with st.sidebar:
    st.subheader("Settings")

    ss.bootstrap_servers = st.selectbox(
        label="SERVER",
        options=("localhost:9092", "10.0.0.15:9092"),
        index=0,
    )
    status = kafka_is_up()

    ss.topic = st.selectbox(
        label="TOPIC",
        options=("orders", "test-topic"),
        index=0,
        placeholder="Select kafka topic...",
    )

    ss.consumer = st.toggle(value=False, label="Listen")

if ss.consumer:
    # Criar um consumer que se conecta ao servidor Kafka em 10.0.0.0:9092
    consumer = KafkaConsumer(
        ss.topic,
        bootstrap_servers=["localhost:9092", ss.bootstrap_servers],
        value_deserializer=json.loads,
        request_timeout_ms=60 * 1000,  # miliseconds
    )

    spark = init_spark(remote=None)

    # Consumir mensagens do tópico "meu_topico"
    for msg in consumer:
        if not ss.consumer:
            break

        data = msg.value
        with st.expander(data["id"]):
            st.write(data)

        print("RECEBIDO:")
        print("\n------\n", msg, "\n------\n")
        print("- timestamp:", msg.timestamp)
        print("- time:", datetime.fromtimestamp(msg.timestamp / 1000))
        print("- topic:", msg.topic)
        print("- payload:", msg.value)
        # print(msg)
        print()


# Initialize SparkSession
# spark = init_spark(remote=None)


# kafka_bootstrap_servers = ss.bootstrap_servers  # Replace with your Kafka broker address
# kafka_topic = ss.topic  # Replace with your Kafka topic

# # Subscribe to 1 topic
# df = (
#     spark.readStream.format("kafka")
#     .option("kafka.bootstrap.servers", ss.bootstrap_servers)
#     .option("subscribe", ss.topic)
#     .load()
# )

# # Cast the 'value' column to string (assuming it's UTF-8 encoded)
# df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")


# query = df.writeStream.outputMode("append").format("console").start()


# def signal_handler(sig, frame):
#     print("Received termination signal. Stopping the streaming query...")
#     query.stop()
#     sys.exit(0)


# # Handle termination signals (e.g., Ctrl+C)
# signal.signal(signal.SIGINT, signal_handler)
# signal.signal(signal.SIGTERM, signal_handler)

# try:
#     # Your streaming logic here
#     query.awaitTermination()
# except KeyboardInterrupt:
#     print("Streaming query interrupted.")
# finally:
#     print("Stopping the Spark session...")
#     spark.stop()
