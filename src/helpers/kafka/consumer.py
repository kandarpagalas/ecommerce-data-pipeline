import os
import json
from datetime import datetime
from kafka import KafkaConsumer

from src.helpers.setup import load_env


load_env()
kafka_server = None
kafka_server = kafka_server or os.environ.get("KAFKA_SERVER", "localhost:9092")
topic = "orders"

# Criar um consumer que se conecta ao servidor Kafka em 10.0.0.0:9092
consumer = KafkaConsumer(
    topic,
    bootstrap_servers=["localhost:9092", kafka_server],
    value_deserializer=json.loads,
    request_timeout_ms=60 * 1000,  # miliseconds
)

# Consumir mensagens do tópico "meu_topico"
for msg in consumer:
    print("RECEBIDO:")
    print("\n------\n", msg, "\n------\n")
    print("- timestamp:", msg.timestamp)
    print("- time:", datetime.fromtimestamp(msg.timestamp / 1000))
    print("- topic:", msg.topic)
    print("- payload:", msg.value)
    # print(msg)
    print()
