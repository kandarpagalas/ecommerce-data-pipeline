import streamlit as st
from streamlit import session_state as ss
import pandas as pd
import plotly.express as px
from pyspark.sql import SparkSession, functions as f
from pyspark.sql.functions import explode, col, date_format
from src.pages.dashboards.callables import (
    get_spart_session,
    get_customers_df,
    get_products_df,
    get_dates_df,
    get_fato_df,
)


st.title("Dashboard")


if "ft_orders_df" not in st.session_state:
    st.session_state["ft_orders_df"] = pd.DataFrame()
if "ft_orders_count" not in ss:
    ss.ft_orders_count = 0

ss.min_year = 2019
ss.max_year = 2024
ss.min_year, ss.max_year = st.slider(
    "Select a range of values", 2019, 2024, (ss.min_year, ss.max_year)
)

spark = get_spart_session()


# @st.fragment(run_every="5s")
def update_ft_orders_df(spark):
    ft_orders_df = get_fato_df(spark)

    return ft_orders_df


# @st.fragment(run_every="5s")
def update_data():
    st.toast("Verificando novos dados", icon="😍")
    ft_orders_df = get_fato_df(spark)
    orders_count = ft_orders_df.count()
    if ss.ft_orders_count != orders_count:
        st.toast("Dados atualizados", icon="😍")
        pass


update_data()


def filter_ft_orders_df(_ft_orders_df):
    filtered_by_month_df = _ft_orders_df.withColumn(
        "year", f.year("charges_paid_at")
    ).withColumn("month", f.month("charges_paid_at"))
    filtered_by_month_df = filtered_by_month_df.filter(
        (filtered_by_month_df.year >= ss.min_year)
        & (filtered_by_month_df.year <= ss.max_year)
    )
    return filtered_by_month_df


ft_orders_df = filter_ft_orders_df(get_fato_df(spark))
products_df = get_products_df(spark)
time_df = get_dates_df(spark)


def receita_por_cliente(_ft_orders_df):
    receita_por_cliente_df = (
        _ft_orders_df.groupBy("customer")
        .agg(f.sum("charge_value").alias("total_revenue"))
        .orderBy("total_revenue", ascending=False)
        .limit(10)
    )
    return receita_por_cliente_df.toPandas()


with st.expander("ft_orders_df", expanded=False):
    st.dataframe(ft_orders_df, hide_index=True, use_container_width=True)


## Receita Total por Mês --------------------------------------------------
@st.cache_data()
def receita_total_mes(_ft_orders_df):
    receita_mensal_df = (
        _ft_orders_df.groupBy(["year", "month"])
        .agg(
            f.count_distinct("id").alias("order_count"),
            f.sum("charge_value").alias("total_revenue"),
        )
        .orderBy("year", ascending=True)
    )
    receita_mensal_df = receita_mensal_df.withColumn(
        "period", f.format_string("%s_%02d", f.col("year"), f.col("month"))
    )

    fig_receita_mensal = px.line(
        receita_mensal_df, x="period", y="total_revenue", title="Receita Total por Mês"
    )

    return fig_receita_mensal


st.header("Receita Total por Mês")
fig_receita_mensal = receita_total_mes(ft_orders_df)
st.plotly_chart(fig_receita_mensal, theme="streamlit", use_container_width=True)


col1, col2 = st.columns(2)
col3, col4 = st.columns(2)

# Receita Total por Cliente (Top 10)
with col1:
    st.subheader("Receita Total por Cliente (Top 10)")
    receita_por_cliente = receita_por_cliente(ft_orders_df)
    fig = px.bar(
        receita_por_cliente,
        x="customer",
        y="total_revenue",
    )
    st.plotly_chart(fig, theme="streamlit", use_container_width=True)

# Número de Pedidos por Categoria de Produto
with col2:
    st.subheader("Número de Pedidos por Categoria de Produto")
    # items_df = ft_orders_df.withColumn("item", explode(col("item_references")))
    items_df = ft_orders_df.withColumn("item", col("item_references"))
    items_df = items_df.select("item", "charge_value")
    pedidos_por_produto_df = (
        items_df.join(products_df, items_df.item == products_df.reference_id, "left")
        .groupBy("categoria")
        .agg(f.count("item").alias("order_count"))
    )

    categorias_df = pedidos_por_produto_df.toPandas()
    fig_categorias = px.bar(
        categorias_df,
        x="categoria",
        y="order_count",
        title="Número de Pedidos por Categoria de Produto",
    )
    st.plotly_chart(fig_categorias, theme="streamlit", use_container_width=True)

# Número de Envio de Produtos por Estado
with col3:
    st.subheader("Número de Envio de Produtos por Estado")
    quantidade_pedidos_por_estado_df = ft_orders_df.groupBy("shipping").agg(
        f.count("id").alias("order_count")
    )

    fig_quantidade_pedidos_por_estado = px.bar(
        quantidade_pedidos_por_estado_df,
        x="shipping",
        y="order_count",
        title="Número de Pedidos por Estado de Entrega",
    )
    st.plotly_chart(
        fig_quantidade_pedidos_por_estado,
        theme="streamlit",
        use_container_width=True,
    )

# Receita por metodo de pagamento
with col4:
    st.subheader("Receita por metodo de pagamento")


## ------------------------------------------------
# st.dataframe(receita_por_cliente, hide_index=True)

# st.dataframe(st.session_state["ft_orders_df"], hide_index=True)
