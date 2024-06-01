import pika
from services import stock_command_event_processor
from msgspec import msgpack
class RabbitMQConsumer:

    def declare_queues(self):
        self.channel.queue_declare("stock_queue")
        # self.channel.queue_bind("stock_queue", "ORCHESTRATION_SAGA", "stock_queue")

    def connect(self):
        try:
            connection = pika.BlockingConnection(pika.URLParameters("amqp://guest:guest@so-rabbit:5672"))
            channel = connection.channel()
            self.channel = channel
            self.declare_queues()
        except Exception as e:
            print(e)

    def __init__(self) -> None:
        self.channel = None

    def callback(self, ch, method, properties, body):
        msg = msgpack.decode(body)
        ch.basic_ack(delivery_tag=method.delivery_tag)  # Acknowledge the message
        try:
            if msg['action'] == "create_item":
                key = {"key":"abcd"}
                self.publish_message(properties, key)
        except Exception as e:
            pass


    def start_consuming(self):
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(queue="stock_queue", on_message_callback=self.callback)
        # self.channel.basic_consume(queue=STOCK_SUBTRACT, on_message_callback=self.callback)
        try:
            self.channel.start_consuming()
        except Exception as e:
            print(e)

    def publish_message(self, properties, response):
        self.channel.basic_publish(
            exchange='',
            routing_key=properties.reply_to,
            properties=pika.BasicProperties(
                correlation_id=properties.correlation_id
            ),
            body=msgpack.encode(response)
        )

if __name__ == "__main__":
    consumer = RabbitMQConsumer()
    consumer.connect()
    consumer.start_consuming()
