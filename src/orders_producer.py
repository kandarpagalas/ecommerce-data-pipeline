import multiprocessing as mp

import pendulum
from random import randint
from time import sleep
import streamlit as st
from src.helpers.gen_order import OrderGen

st.set_page_config(
    page_title="Kafka Producer",
    page_icon="✉️",
    initial_sidebar_state="expanded",
    layout="wide"
    )
st.title("ORDER GENERATOR")
st.subheader("Kafka Producer")

order_generator = OrderGen()

def kafka_is_up():
    return True

def real_time_orders(stream = True, min_interval = 1, max_interval = 5):
    print("ORDER REAL TIME")
    while stream:
        order = order_generator.generate(pendulum.now("America/Fortaleza"))
        order_id = order["id"]

        print(order_id)
        yield order
        sleep(randint(min_interval, max_interval))


with st.sidebar:
    st.subheader("Endpoints")
    if kafka_is_up():
        st.success('Kafka!', icon="✅")
    else:
        st.error('Kafka!', icon="⚠️")


    st.subheader("Settings")
    min_interval, max_interval = st.slider(
        label= "Yeld interval", 
        min_value=1, 
        max_value=10,
        step=1, 
        value=(1, 5), 
        help="Define o range de intervao entre envios",
        label_visibility="visible",
        format="%is"
        )
    
    # if kafka_is_up():
    streaming = st.toggle(value=False, label="Streaming")

if streaming:
    col1, col2, = st.columns([2, 4])

    st.header("ORDERS")
    for order in real_time_orders(stream = True, min_interval = min_interval, max_interval = max_interval):
        st.session_state.last_order = order

        with st.expander(order["id"]):
            st.write(order)

else:
    col1= st.columns(1)
    st.toast('Streaming Stopped', icon='✋')



