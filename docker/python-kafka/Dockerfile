# Use the official Python image as the base image
FROM python:3.11

ENV TZ America/Fortaleza

# Install Python packages
COPY docker/python-kafka/requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /app

ENV PYTHONPATH "$PYTHONPATH:$PWD"
