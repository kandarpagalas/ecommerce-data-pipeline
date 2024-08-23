import streamlit as st
from src.helpers.setup import load_env

load_env()

st.set_page_config(layout="wide")
readme = st.Page(
    title="Readme",
    icon=":material/info:",
    page="src/pages/about.py",
)
consumer = st.Page(
    title="Kafka Consumer",
    icon=":material/download:",
    page="src/pages/kafka_orders_consumer_gui.py",
)
producer = st.Page(
    title="Kafka Producer",
    icon=":material/publish:",
    page="src/pages/kafka_orders_producer_gui.py",
)

products = st.Page(
    title="Produtos",
    icon=":material/inventory:",
    page="src/pages/products.py",
)

pg = st.navigation(
    {"SOBRE": [readme], "ANÁLISES": [products], "INFRAESTRUTURA": [consumer, producer]}
)
pg.run()
