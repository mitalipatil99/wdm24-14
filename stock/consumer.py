import pika
import redis
from config import STOCK_ADD, STOCK_SUBTRACT, PAYMENT_ADD, PAYMENT_DEDUCT
import os
from services import stock_command_event_processor

class RabbitMQConsumer:

    def declare_queues(self):
        exchange = self.channel.exchange_declare("ORCHESTRATION_SAGA", "direct")
        self.exchange = exchange
        self.channel.queue_declare(STOCK_ADD)
        self.channel.queue_bind(STOCK_ADD, "ORCHESTRATION_SAGA", STOCK_ADD)
        self.channel.queue_declare(STOCK_SUBTRACT)
        self.channel.queue_bind(STOCK_SUBTRACT, "ORCHESTRATION_SAGA", STOCK_SUBTRACT)
        self.channel.queue_declare(PAYMENT_ADD)
        self.channel.queue_bind(PAYMENT_ADD, "ORCHESTRATION_SAGA", PAYMENT_ADD)
        self.channel.queue_declare(PAYMENT_DEDUCT)
        self.channel.queue_bind(PAYMENT_DEDUCT, "ORCHESTRATION_SAGA", PAYMENT_DEDUCT)

    def connect(self):
        try:
            connection = pika.BlockingConnection(pika.URLParameters("amqp://guest:guest@so-rabbit:5672"))
            channel = connection.channel()
            self.channel = channel
            self.declare_queues()
        except Exception as e:
            print(f"wqaosjidahkajdask {str(e)} {e}")
            print(e)

    def __init__(self) -> None:
        self.channel = None

    # {"event": "StockSubtracted", "details": "Stock subtracted for the order", "data": {"664427e1-dca1-46e4-a83e-7ac05b3106d1": 1, "ae25b316-316d-46d4-a2ad-0580acd1c0f2": 1}, "key": "512652df-56e0-435f-9104-f04505ef0c82"}
    def callback(self, ch, method, properties, body):
        print(f"Received {body} {ch} {method} {properties} ")
        try:
            # payment_event_processor(ch, method, properties, body)
            ch.basic_ack(delivery_tag=method.delivery_tag)  # Acknowledge the message
            raise Exception("")
            
        except Exception as e:
            print(f"Error processing message: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag)  # Negative Acknowledge the message


    def start_consuming(self):
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(queue=STOCK_ADD, on_message_callback=self.callback)
        self.channel.basic_consume(queue=STOCK_SUBTRACT, on_message_callback=self.callback)
        try:
            self.channel.start_consuming()
        except Exception as e:
            print(e)

if __name__ == "__main__":
    print("wwwwwwwwwwwwwwwww")
    consumer = RabbitMQConsumer()
    consumer.connect()
    consumer.start_consuming()
