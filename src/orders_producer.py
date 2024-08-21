import multiprocessing as mp

import pendulum
from random import randint
from time import sleep
import streamlit as st
from streamlit import session_state as ss
from src.helpers.gen_order import OrderGen
from src.helpers.kafka.producer import OrderProducer

st.set_page_config(
    page_title="Kafka Producer",
    page_icon="✉️",
    initial_sidebar_state="expanded",
    layout="wide",
)
st.title("ORDER GENERATOR")
# st.subheader("Kafka Producer")

order_generator = OrderGen()


def kafka_is_up():
    print("Kafka is up")
    order_producer = OrderProducer(kafka_server=ss.bootstrap_servers)
    status = order_producer.get_status()

    if not status["conn"]:
        st.error(status["message"], icon="❌")
    # else:
    #     st.success(status["message"], icon="✅")

    return status["conn"]


def real_time_orders(stream=True, min_interval=1, max_interval=5):
    print("ORDER REAL TIME")
    while stream:
        if not ss.streaming:
            print("END REAL TIME")
            break
        order_producer = OrderProducer(kafka_server=ss.bootstrap_servers)
        order = order_producer.send_order(topic=ss.topic)
        _id = order["id"]

        print(_id)
        yield order
        sleep(randint(min_interval, max_interval))


with st.sidebar:
    st.title("KAFKA ORDER PRODUCER")
    st.subheader("Settings")
    col1, col2 = st.columns([3, 1])

    ss.bootstrap_servers = st.selectbox(
        label="SERVER",
        options=("10.0.0.15:9092", "kafka.z106.kandarpagalas.com"),
        index=0,
    )
    status = kafka_is_up()

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
    # st.header("ORDERS")
    for order in real_time_orders(
        stream=ss.streaming, min_interval=min_interval, max_interval=max_interval
    ):
        st.session_state.last_order = order

        with st.expander(order["id"]):
            st.write(order)

else:
    col1 = st.columns(1)
    st.toast("Streaming Stopped", icon="✋")
