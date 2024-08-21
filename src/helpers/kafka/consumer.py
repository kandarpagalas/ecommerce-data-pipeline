import json
from datetime import datetime
from kafka import KafkaConsumer

kafka_server = ["10.0.0.15:9092"]
topic = "orders"

# Criar um consumer que se conecta ao servidor Kafka em 10.0.0.0:9092
consumer = KafkaConsumer(
    topic,
    bootstrap_servers=kafka_server,
    value_deserializer=json.loads,
    request_timeout_ms=60 * 1000,  # miliseconds
)

# Consumir mensagens do tópico "meu_topico"
for msg in consumer:
    print("RECEBIDO:")
    print("- timestamp:", msg.timestamp)
    print("- time:", datetime.fromtimestamp(msg.timestamp / 1000))
    print("- topic:", msg.topic)
    print("- payload:", msg.value)
    # print(msg)
    print()
