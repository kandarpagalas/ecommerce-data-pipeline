import os
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


# Inicializa todos os DF
df_list = [
    "ft_orders_df",
    "receita_por_cliente_df",
    "receita_total_mes_df",
    "pedidos_por_produto_df",
]
for df_name in df_list:
    if df_name not in ss:
        ss[df_name] = pd.DataFrame()

count_list = [
    "ft_orders_count",
    "ft_orders_last_count",
    "count_customers",
    "count_products",
]
for count_name in count_list:
    if count_name not in ss:
        ss[count_name] = 0


ss.min_year = 2019
ss.max_year = 2024
# ss.min_year, ss.max_year = st.slider(
#     "Select a range of values", 2019, 2024, (ss.min_year, ss.max_year)
# )

# Inicializa o spark
spark = get_spart_session()


def filter_ft_orders_df(_ft_orders_df):
    filtered_by_month_df = _ft_orders_df.withColumn(
        "year", f.year("charges_paid_at")
    ).withColumn("month", f.month("charges_paid_at"))
    filtered_by_month_df = filtered_by_month_df.filter(
        (filtered_by_month_df.year >= ss.min_year)
        & (filtered_by_month_df.year <= ss.max_year)
    )
    return filtered_by_month_df


# ft_orders_df = filter_ft_orders_df(get_fato_df(spark))
# products_df = get_products_df(spark)


@st.cache_data()
def receita_por_cliente():
    spark = get_spart_session()
    _ft_orders_df = get_fato_df(spark)
    receita_por_cliente_df = (
        _ft_orders_df.groupBy("customer")
        .agg(f.sum("charge_value").alias("total_revenue"))
        .orderBy("total_revenue", ascending=False)
        .limit(10)
    )
    return receita_por_cliente_df.toPandas()


@st.cache_data()
def receita_total_mes():
    spark = get_spart_session()
    _ft_orders_df = get_fato_df(spark)

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
    return receita_mensal_df.toPandas()


@st.cache_data()
def pedidos_categoria_produto():
    spark = get_spart_session()
    _ft_orders_df = get_fato_df(spark)
    _products_df = get_products_df(spark)

    items_df = _ft_orders_df.withColumn("item", col("item_references"))
    items_df = items_df.select("item", "charge_value")
    pedidos_por_produto_df = (
        items_df.join(_products_df, items_df.item == _products_df.reference_id, "left")
        .groupBy("categoria")
        .agg(f.count("item").alias("order_count"))
    )
    return pedidos_por_produto_df.toPandas()


@st.cache_data()
def quantidade_pedidos_por_estado():
    spark = get_spart_session()
    _ft_orders_df = get_fato_df(spark)
    quantidade_pedidos_por_estado_df = _ft_orders_df.groupBy("shipping").agg(
        f.count("id").alias("order_count")
    )
    return quantidade_pedidos_por_estado_df.toPandas()


# UPDATE DATA --------------------------------
@st.fragment(run_every="10s")
def update_data():
    ft_orders_df = get_fato_df(spark)
    orders_count = ft_orders_df.count()
    if ss.ft_orders_count != orders_count:

        # Métricas
        ss.ft_orders_last_count = ss.ft_orders_count
        ss.count_customers = get_customers_df(spark).count()
        ss.count_products = get_products_df(spark).count()

        # Gráficos
        st.cache_data.clear()
        ss.receita_por_cliente_df = receita_por_cliente()
        ss.receita_total_mes_df = receita_total_mes()
        ss.pedidos_por_produto_df = pedidos_categoria_produto()
        ss.quantidade_pedidos_por_estado_df = quantidade_pedidos_por_estado()
        st.toast(f"Dados atualizados [count = {orders_count}]", icon="😍")

        ss.ft_orders_count = orders_count

        st.subheader("Métricas")
        col1, col2, col3 = st.columns(3)
        col1.metric("Total Clientes", ss.count_customers)
        col2.metric("Total Produtos", ss.count_products)
        col3.metric(
            "Total Vendas",
            ss.ft_orders_count,
            ss.ft_orders_count - ss.ft_orders_last_count,
        )


update_data()
# UPDATE DATA --------------------------------


# with st.expander("ft_orders_df", expanded=False):
#     st.dataframe(ft_orders_df, hide_index=True, use_container_width=True)


st.subheader("Receita Total por Mês")
col_receita_menu, col_receita_chart = st.columns([1, 4])
with col_receita_menu:
    st.write("FILTROS")
    ano_inicio, ano_final = st.slider("ano", 2019, 2024, (2023, 2024))
with col_receita_chart:
    fig_receita_mensal_filtered = ss.receita_total_mes_df[
        (ss.receita_total_mes_df["year"] >= ano_inicio)
        & (ss.receita_total_mes_df["year"] <= ano_final)
    ]
    fig_receita_mensal = px.line(
        fig_receita_mensal_filtered,
        x="period",
        y="total_revenue",
    )
    st.plotly_chart(fig_receita_mensal, theme="streamlit", use_container_width=True)


col1, col2 = st.columns(2)
col3, col4 = st.columns(2)

# Receita Total por Cliente (Top 10)
with col1:
    st.subheader("Receita Total por Cliente (Top 10)")
    receita_por_cliente_filtered = ss.receita_por_cliente_df
    fig = px.bar(
        receita_por_cliente_filtered,
        x="customer",
        y="total_revenue",
    )
    st.plotly_chart(fig, theme="streamlit", use_container_width=True)


# Número de Pedidos por Categoria de Produto
with col2:
    st.subheader("Número de Pedidos por Categoria de Produto")

    pedidos_por_produto_df_filtered = ss.pedidos_por_produto_df
    categorias_df = pedidos_por_produto_df_filtered
    fig_categorias = px.bar(
        categorias_df,
        x="categoria",
        y="order_count",
    )
    st.plotly_chart(fig_categorias, theme="streamlit", use_container_width=True)

# Número de Envio de Produtos por Estado
with col3:
    st.subheader("Número de Envio de Produtos por Estado")

    quantidade_pedidos_por_estado_df_filtered = ss.quantidade_pedidos_por_estado_df
    fig_quantidade_pedidos_por_estado = px.bar(
        quantidade_pedidos_por_estado_df_filtered,
        x="shipping",
        y="order_count",
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
