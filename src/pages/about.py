import re
import requests
import streamlit as st


@st.cache_data()
def st_markdown(markdown_string):
    parts = re.split(r"!\[(.*?)\]\((.*?)\)", markdown_string)
    for i, part in enumerate(parts):
        if i % 3 == 0:
            st.markdown(part, unsafe_allow_html=True)
        elif i % 3 == 1:
            title = part
        else:
            st.image(part)  # Add caption if you want -> , caption=title)


@st.cache_data(ttl="1h")
def get_readme():
    response = requests.get(
        "https://raw.githubusercontent.com/kandarpagalas/ecommerce-data-pipeline/main/README.md",
        timeout=30,
    )
    return response.text


st_markdown(get_readme())
