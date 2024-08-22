import os
import random
import json
import pendulum
from time import sleep
from kafka import KafkaProducer
from src.helpers.gen_order import OrderGen

# Modificador personalisado do load_dotenv
from src.helpers.setup import load_env

load_env()


class OrderProducer:
    def __init__(self, kafka_server=None) -> None:
        self.kafka_server = kafka_server or os.environ.get(
            "KAFKA_SERVER", "localhost:9092"
        )

        self.order_generator = OrderGen()

    def _producer(self):
        # Criar um producer que se conecta ao servidor Kafka em 10.0.0.0:9092
        return KafkaProducer(
            bootstrap_servers=self.kafka_server,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks=0,
        )

    def get_status(self):
        try:
            producer = self._producer()
            status = producer.bootstrap_connected()
            producer.close()
            return {"conn": status, "message": "BrokersAvailable"}
        except Exception as e:
            return {"conn": False, "message": e.__class__.__name__}

    def send_order(self, topic="orders"):
        payload = self.order_generator.generate(pendulum.now("America/Fortaleza"))

        # Connecta o producer
        producer = self._producer()

        producer.send(topic, value=payload)

        # print("ENVIADO:")
        # print("- topic:", topic)
        # print("- payload:", payload)
        # print()
        # Fechar o producer
        producer.close()

        return payload


if __name__ == "__main__":
    print("LOCAL:")
    producer1 = OrderProducer()
    print(" - Broker:", producer1.kafka_server)
    print(" - Response:", producer1.get_status())

    producer3 = OrderProducer(kafka_server="10.0.0.15:9092")
    print()
    print("REMOTE:")
    print(" - Broker:", producer3.kafka_server)
    print(" - Response:", producer3.get_status())
    # order_producer.send_order()
