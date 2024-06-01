import pika
import redis
from config import STOCK_ADD, STOCK_SUBTRACT, PAYMENT_ADD, PAYMENT_DEDUCT
import os
from services import payment_event_processor

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

    # {"event": "payment_deduct", "details": "payment_deduct for the order", "data": {"user_id": "b7c31e70-71a9-44a1-a464-cc3a7b1afe16", "total_cost": 10}, "key": "2d59395b-824c-424e-ac67-fabb7d0ffa3c"}
    def callback(self, ch, method, properties, body):
        print(f"Received {body} {ch} {method} {properties} ")
        try:
            ch.basic_ack(delivery_tag=method.delivery_tag)  # Acknowledge the message
        
            raise Exception("")
        except Exception as e:
            print(f"Error processing message: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag)  # Negative Acknowledge the message


    def start_consuming(self):
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(queue=PAYMENT_DEDUCT, on_message_callback=self.callback)
        self.channel.basic_consume(queue=PAYMENT_ADD, on_message_callback=self.callback)
        try:
            self.channel.start_consuming()
        except Exception as e:
            print(e)

if __name__ == "__main__":
    print("wwwwwwwwwwwwwwwww")
    consumer = RabbitMQConsumer()
    consumer.connect()
    consumer.start_consuming()
