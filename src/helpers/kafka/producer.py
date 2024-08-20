import random
import json
import pendulum
from time import sleep
from kafka import KafkaProducer
from src.helpers.gen_order import OrderGen


class OrderProducer:
    def __init__(self, kafka_server="10.0.0.15:9092") -> None:
        self.kafka_server = kafka_server
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
            print(e)
            return {"conn": False, "message": e}

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
    order_producer = OrderProducer()
    order_producer.send_order()
