FROM bitnami/spark:3.5.1

USER root
ENV STREAMLIT_HOME /opt/bitnami/streamlit
WORKDIR /opt/bitnami/

# Install Python packages
COPY requirements.txt /opt/bitnami/streamlit/
RUN pip install --no-cache-dir -r /opt/bitnami/streamlit/requirements.txt

HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health

ENV PYTHONPATH "$PYTHONPATH:$PWD"

# Set the entry point to your application
ENTRYPOINT [ "streamlit", "run" ]
# ENTRYPOINT ["streamlit", "run", "streamlit_app.py", "--server.port=8501", "--server.address=0.0.0.0"]
CMD ["/opt/bitnami/streamlit/streamlit_app.py", "--server.port=8501", "--server.address=0.0.0.0", "--server.fileWatcherType", "none", "--browser.gatherUsageStats", "false"]


EXPOSE 8501