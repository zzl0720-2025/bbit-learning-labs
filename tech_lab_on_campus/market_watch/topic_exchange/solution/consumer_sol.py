import os
import pika

from consumer_interface import mqConsumerInterface


class mqConsumer(mqConsumerInterface):
    def __init__(
        self, binding_key: str, exchange_name: str, queue_name: str
    ) -> None:
        self.binding_key = binding_key
        self.exchange_name = exchange_name
        self.queue_name = queue_name
        self.connection = None
        self.channel = None
        self.setupRMQConnection()

    def setupRMQConnection(self) -> None:
        con_params = pika.URLParameters(os.environ["AMQP_URL"])
        self.connection = pika.BlockingConnection(parameters=con_params)
        self.channel = self.connection.channel()

        self.channel.exchange_declare(
            exchange=self.exchange_name,
            exchange_type="topic"
        )
        self.channel.queue_declare(queue=self.queue_name)

        self.channel.queue_bind(
            queue=self.queue_name,
            exchange=self.exchange_name,
            routing_key=self.binding_key,
        )

        self.channel.basic_consume(
            queue=self.queue_name,
            on_message_callback=self.on_message_callback,
            auto_ack=False,
        )

    def bindQueueToExchange(self, queueName: str, topic: str) -> None:
        self.channel.queue_bind(
            queue=queueName,
            exchange=self.exchange_name,
            routing_key=topic,
        )

    def createQueue(self, queueName: str) -> None:
        self.channel.queue_declare(queue=queueName)
        self.channel.basic_consume(
            queue=queueName,
            on_message_callback=self.on_message_callback,
            auto_ack=False,
        )

    def on_message_callback(self, channel, method_frame, header_frame, body):
        channel.basic_ack(method_frame.delivery_tag, False)
        print(body.decode("utf-8"))

    def startConsuming(self) -> None:
        print(" [*] Waiting for messages. To exit press CTRL+C")
        self.channel.start_consuming()