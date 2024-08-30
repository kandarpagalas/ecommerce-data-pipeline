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

spark_basics = st.Page(
    title="Leitura",
    icon=":material/inventory:",
    page="src/pages/spark_basics.py",
)

pg = st.navigation(
    {
        "SOBRE": [readme],
        "SPARK": [spark_basics],
        "INFRAESTRUTURA": [consumer, producer],
    }
)
pg.run()
