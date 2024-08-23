import os
import json
import findspark
from datetime import datetime
import streamlit as st
from streamlit import session_state as ss
from kafka import KafkaConsumer

from pyspark.sql import SparkSession

from src.helpers.kafka.producer import OrderProducer


findspark.init()


def kafka_is_up():
    print("Kafka is up")
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

    # if kafka_is_up():
    ss.consumer = st.toggle(value=False, label="Listen")

if ss.consumer:
    # Criar um consumer que se conecta ao servidor Kafka em 10.0.0.0:9092
    consumer = KafkaConsumer(
        ss.topic,
        bootstrap_servers=["localhost:9092", ss.bootstrap_servers],
        value_deserializer=json.loads,
        request_timeout_ms=60 * 1000,  # miliseconds
    )
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


# kafka_server = None
# topic = "orders"
