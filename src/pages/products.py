import streamlit as st
import plotly.express as px

from src.helpers.spark import init_spark
import pyspark.sql.functions as f
from pyspark.sql.types import StructType, StructField, StringType, FloatType

st.title("PRODUTOS")


@st.cache_data
def get_parquet_sample(parquet_file_path, num_rows=5):
    spark = init_spark()
    df = spark.read.parquet(parquet_file_path)

    if num_rows is None:
        sample_df = df.toPandas()  # Convert to pandas for Streamlit display
    else:
        sample_df = df.limit(
            num_rows
        ).toPandas()  # Convert to pandas for Streamlit display

    # Agrupar por `categoria` e calcular a média da coluna `preco_pix`
    resultado = (
        df.groupBy("categoria")
        .agg(f.avg("preco_pix").alias("media_preco_pix"))
        .withColumn("media_preco_pix", f.format_number("media_preco_pix", 2))
    )
    # Formatar a média com 2 casas decimais
    # resultado = resultado

    resultado.show()
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
    return pandas_df


# Path to the Parquet file
parquet_file_path = "src/data/products.parquet"

# Get a sample from the Parquet file
# sample_df = get_parquet_sample(parquet_file_path, num_rows=None)
sample_df = media_categoria(parquet_file_path)


with st.expander("Data"):
    st.dataframe(data=sample_df, use_container_width=True, hide_index=True)


st.header("Categoria")
col1, col2 = st.columns(2)
with col1:
    st.header("Quantidade")
    st.bar_chart(
        data=sample_df[["categoria", "quantidade"]],
        x="categoria",
        x_label="Categoria",
        y="quantidade",
        y_label="Quantidade",
    )
with col2:
    st.header("Preço médio")
    st.bar_chart(
        data=sample_df[["categoria", "preco_medio"]],
        x="categoria",
        x_label="Categoria",
        y="preco_medio",
        y_label="Preço Médio",
        stack=False,
    )
fig = px.bar(
    sample_df[["categoria", "preco_medio"]],
    x="categoria",
    y="preco_medio",
    title="Long-Form Input",
    labels={"preco_medio": "Preço Médio"},
)

st.plotly_chart(fig, use_container_width=True)
