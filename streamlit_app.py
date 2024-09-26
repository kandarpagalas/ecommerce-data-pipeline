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

painel_vendas = st.Page(
    title="Vendas RT",
    icon=":material/monitoring:",
    page="src/pages/painel_vendas/painel_vendas.py",
)


dashboard_01 = st.Page(
    title="Dashboard",
    icon=":material/monitoring:",
    page="src/pages/dashboards/dashboard.py",
)


pg = st.navigation(
    {
        "SOBRE": [readme],
        "DASHBOARDS": [dashboard_01],
        # "SPARK": [spark_basics],
        # "PAINEL": [painel_vendas],
        "TESTE KAFKA": [consumer, producer],
    }
)
pg.run()
