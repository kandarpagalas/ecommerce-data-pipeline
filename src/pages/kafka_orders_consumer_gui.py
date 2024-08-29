import os
import sys
import json
from datetime import datetime
import streamlit as st
from streamlit import session_state as ss
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable


from src.helpers.kafka.producer import OrderProducer


def kafka_admin(kafka_server):
    return KafkaConsumer(bootstrap_servers=kafka_server)


def kafka_is_up(kafka_server):
    order_producer = OrderProducer(kafka_server=kafka_server)
    status = order_producer.get_status()

    if not status["conn"]:
        st.error(status["message"], icon="❌")
    return status["conn"]


def get_topics(kafka_server):
    try:
        admin = kafka_admin(kafka_server)
        return admin.topics()
    except NoBrokersAvailable as e:
        st.toast(e.__class__.__name__, icon="🚨")
        return []


st.title("KAFKA CONSUMER")


with st.sidebar:
    st.subheader("Settings")

    brokers = [
        {"name": "Docker", "server": "kafka:9092"},
        {"name": "Local", "server": "localhost:9092"},
        {"name": "Remote", "server": "10.0.0.15:9092"},
    ]

    ss.bootstrap_servers = st.selectbox(
        label="SERVER",
        format_func=lambda x: x["name"],
        options=brokers,
        index=0,
    )
    if ss.bootstrap_servers["name"] == "Remote":
        remote_server = st.text_input(
            label="kafka server", value=ss.bootstrap_servers["server"]
        )
        if ss.bootstrap_servers["server"] != remote_server:
            ss.bootstrap_servers["server"] = remote_server

    status = kafka_is_up(ss.bootstrap_servers["server"])

    ss.topic = st.selectbox(
        label="TOPIC",
        options=get_topics(ss.bootstrap_servers["server"]),
        # index=0,
        placeholder="Select kafka topic...",
    )

    ss.consumer = st.toggle(value=False, label="Listen")

if ss.consumer:
    # Criar um consumer que se conecta ao servidor Kafka em 10.0.0.0:9092
    @st.fragment
    def display_messages():
        consumer = KafkaConsumer(
            ss.topic,
            bootstrap_servers=ss.bootstrap_servers["server"],
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

    display_messages()
