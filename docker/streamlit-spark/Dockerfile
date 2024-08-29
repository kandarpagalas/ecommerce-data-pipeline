# Use the official Python image as the base image
FROM --platform=linux/arm64/v8 ubuntu:22.04

ENV PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python
ENV TZ America/Fortaleza
ENV DEBIAN_FRONTEND noninteractive

# Setup python and java and base system
RUN apt-get update && \
  apt-get upgrade -y

RUN apt-get install -qq -y ca-certificates-java openjdk-8-jdk-headless python3-pip wget


# Set environment variables for Java
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64
ENV PATH $JAVA_HOME/bin:$PATH

# Define the version of Spark you want to install
ENV SPARK_VERSION 3.5.2

# Download and install Spark
RUN wget https://dlcdn.apache.org/spark/spark-3.5.2/spark-3.5.2-bin-hadoop3.tgz && \
    tar -xzf spark-3.5.2-bin-hadoop3.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop3 /opt/spark && \
    rm spark-${SPARK_VERSION}-bin-hadoop3.tgz

# Set environment variables for Spark
ENV SPARK_HOME /opt/spark
ENV PATH $SPARK_HOME/bin:$PATH

# Install Python packages
COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Copy your application code into the container
# COPY . /app
WORKDIR /app

HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health

# Set the entry point to your application
ENTRYPOINT [ "streamlit", "run" ]
# ENTRYPOINT ["streamlit", "run", "streamlit_app.py", "--server.port=8501", "--server.address=0.0.0.0"]
CMD ["streamlit_app.py", "--server.port=8501", "--server.address=0.0.0.0", "--server.fileWatcherType", "none", "--browser.gatherUsageStats", "false"]


EXPOSE 8501