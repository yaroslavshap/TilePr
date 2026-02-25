from __future__ import annotations
import json
import pika
from config import settings


class TileBuildQueue:
    def __init__(self, amqp_url: str):
        self.amqp_url = amqp_url

    def publish_build(self, message: dict) -> None:
        params = pika.URLParameters(self.amqp_url)
        connection = pika.BlockingConnection(params)
        ch = connection.channel()

        # durable queues
        ch.queue_declare(queue=settings.RABBIT_QUEUE, durable=True)
        ch.queue_declare(queue=settings.RABBIT_RETRY_QUEUE, durable=True)
        ch.queue_declare(queue=settings.RABBIT_DLQ, durable=True)

        body = json.dumps(message, ensure_ascii=False).encode("utf-8")
        ch.basic_publish(
            exchange="",
            routing_key=settings.RABBIT_QUEUE,
            body=body,
            properties=pika.BasicProperties(
                delivery_mode=2,  # persistent
                content_type="application/json",
            ),
        )
        connection.close()