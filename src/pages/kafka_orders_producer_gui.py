import multiprocessing as mp
import os
import pendulum
from random import randint
from time import sleep
import streamlit as st
from streamlit import session_state as ss
from src.helpers.gen_order import OrderGen
from src.helpers.kafka.producer import OrderProducer

st.title("KAFKA PRODUCER")

order_generator = OrderGen()
DOCKER_GATEWAY_HOST = os.getenv("DOCKER_GATEWAY_HOST")


def kafka_is_up(kafka_server):
    st.toast(f"verfy {kafka_server}")
    order_producer = OrderProducer(kafka_server=kafka_server)
    status = order_producer.get_status()

    if not status["conn"]:
        st.error(status["message"], icon="❌")

    return status["conn"]


def real_time_orders(
    stream=True, min_interval=1, max_interval=5, kafka_server="localhost:9092"
):
    print("ORDER REAL TIME")
    while stream:
        if not ss.streaming:
            print("END REAL TIME")
            break
        order_producer = OrderProducer(kafka_server=kafka_server)
        order = order_producer.send_order(topic=ss.topic)
        _id = order["id"]

        print(_id)
        yield order
        sleep(randint(min_interval, max_interval))


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
        options=("orders", "test-topic"),
        index=0,
        placeholder="Select kafka topic...",
    )

    min_interval, max_interval = st.slider(
        label="INTERVAL",
        min_value=1,
        max_value=10,
        step=1,
        value=(1, 5),
        help="Define o range de intervao entre envios",
        label_visibility="visible",
        format="%is",
    )

    # if kafka_is_up():
    ss.streaming = st.toggle(value=False, label="Produce ORDERS")

if ss.streaming:
    for order in real_time_orders(
        stream=ss.streaming,
        min_interval=min_interval,
        max_interval=max_interval,
        kafka_server=ss.bootstrap_servers["server"],
    ):
        st.session_state.last_order = order

        with st.expander(order["id"]):
            st.write(order)

else:
    col1 = st.columns(1)
    st.toast("Streaming Stopped", icon="✋")
