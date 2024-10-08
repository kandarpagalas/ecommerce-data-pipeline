import os
import streamlit as st
import plotly.express as px
import pyspark
from pyspark.sql import SparkSession

from src.helpers.spark import init_spark
import pyspark.sql.functions as f
from pyspark.sql.types import StructType, StructField, StringType, FloatType

st.title("PRODUTOS")
# st.write(os.getenv("JAVA_HOME"))
# st.write(os.getenv("SPARK_HOME"))
ROOT_PATH = os.getenv("STREAMLIT_HOME")


@st.cache_data
def get_parquet_sample(parquet_file_path, num_rows=None):
    spark = init_spark()
    df = spark.read.option("recursiveFileLookup", "true").parquet(parquet_file_path)
    # df = df.select("id")

    if num_rows is None:
        sample_df = df.toPandas()  # Convert to pandas for Streamlit display
    else:
        sample_df = df.limit(
            num_rows
        ).toPandas()  # Convert to pandas for Streamlit display

    df.show()
    spark.stop()
    return sample_df


@st.cache_data
def media_categoria(parquet_file_path):
    spark = init_spark()
    df = spark.read.parquet(parquet_file_path)

    # Agrupar por `categoria` e calcular a média da coluna `preco_pix`
    resultado = df.groupBy("categoria").agg(
        f.count("reference_id").alias("quantidade"),
        f.avg("unit_price").alias("preco_medio"),
    )

    pandas_df = resultado.toPandas()

    spark.stop()
    return df


# Path to the Parquet file
CONSUMER_NAME = "consumer_local"
# local
DATA_FOLDER = "/app/data/volumes/spark/content"
# Remoto
DATA_FOLDER = "/content"
# parquet_file_path = f"{DATA_FOLDER}/{CONSUMER_NAME}/data/*.parquet"


local_files = ROOT_PATH + "/data/volumes/spark/content/consumer_local/data/*.parquet"
local_df = get_parquet_sample(local_files)
with st.expander("Arquivos locais"):
    st.dataframe(data=local_df, use_container_width=True, hide_index=True)

s3_files = "s3a://z106/consumer_minio/data/*.parquet"
s3_df = get_parquet_sample(s3_files)
with st.expander("Arquivos locais"):
    st.dataframe(data=s3_df, use_container_width=True, hide_index=True)


# st.header("Categoria")
# col1, col2 = st.columns(2)
# with col1:
#     st.header("Quantidade")
#     st.bar_chart(
#         data=sample_df[["categoria", "quantidade"]],
#         x="categoria",
#         x_label="Categoria",
#         y="quantidade",
#         y_label="Quantidade",
#     )
# with col2:
#     st.header("Preço médio")
#     st.bar_chart(
#         data=sample_df[["categoria", "preco_medio"]],
#         x="categoria",
#         x_label="Categoria",
#         y="preco_medio",
#         y_label="Preço Médio",
#         stack=False,
#     )
# fig = px.bar(
#     sample_df[["categoria", "preco_medio"]],
#     x="categoria",
#     y="preco_medio",
#     title="Long-Form Input",
#     labels={"preco_medio": "Preço Médio"},
# )

# st.plotly_chart(fig, use_container_width=True)
