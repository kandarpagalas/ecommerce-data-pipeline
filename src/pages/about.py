import streamlit as st
import requests
import re


@st.cache_data(ttl="1h")
def st_markdown(markdown_string):
    parts = re.split(r"!\[(.*?)\]\((.*?)\)", markdown_string)
    for i, part in enumerate(parts):
        if i % 3 == 0:
            st.markdown(part, unsafe_allow_html=True)
        elif i % 3 == 1:
            title = part
        else:
            print("------------", part)
            st.image(part)  # Add caption if you want -> , caption=title)


def get_readme():
    diagram = "https://github.com/kandarpagalas/ecommerce-data-pipeline/blob/main/src/diagrams/ecommerce-data-pipeline-arch.png"

    response = requests.get(
        "https://raw.githubusercontent.com/kandarpagalas/ecommerce-data-pipeline/main/README.md",
        timeout=30,
    )
    content = response.text
    # print(content)
    # content = content.replace("src/diagrams/ecommerce-data-pipeline-arch.png", diagram)
    # print(content)

    return content


# st.markdown(get_readme(), unsafe_allow_html=True)

st_markdown(get_readme())
