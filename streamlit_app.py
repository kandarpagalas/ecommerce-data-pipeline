import streamlit as st
from src.helpers.setup import load_env

load_env()

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
    page="kafka_orders_producer_gui.py",
)

pg = st.navigation(
    {"SOBRE": [readme], "ANÁLISES": [], "INFRAESTRUTURA": [consumer, producer]}
)
pg.run()
