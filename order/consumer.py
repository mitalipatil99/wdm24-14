import pika
import time
from msgspec import msgpack
import os
from exceptions import RedisDBError, InsufficientCreditError, RabbitMQError
from services import create_order_db, get_order_by_id_db, batch_init_users_db, add_item_db, confirm_order
from config import *
import logging

def generate_response(status, data={}):
    return {"status":status, "data":data}

class RabbitMQConsumer:

    def declare_queues(self):
        self.channel.queue_declare(ORDER_QUEUE)

    def setup_logger(self):
        self.logger = logging.getLogger("OrderService")
        self.logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

    def connect(self):
        try:
            connection = pika.BlockingConnection(pika.URLParameters(os.environ['RABBITMQ_BROKER_URL']))
            channel = connection.channel()
            self.channel = channel
            self.declare_queues()
        except Exception as e:
            raise RabbitMQError

    def __init__(self) -> None:
        self.channel = None
        self.setup_logger()

    def callback(self, ch, method, properties, body):
        msg = msgpack.decode(body)
        self.logger.info(f"[{properties.reply_to}] : {msg}")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        try:
            if msg['action'] == "find_order":
                entry = get_order_by_id_db(msg['order_id'])
                response = {
                    "order_id": msg['order_id'],
                    "paid": entry.paid,
                    "items": entry.items,
                    "user_id": entry.user_id,
                    "total_cost": entry.total_cost
                }
                self.publish_message(properties, generate_response(STATUS_SUCCESS, response))

            elif msg['action'] == "create_order":
                key = create_order_db(msg['user_id'])
                self.publish_message(properties, generate_response(STATUS_SUCCESS, {"order_id": key}))

            elif msg['action'] == "batch_init_users":
                batch_init_users_db(msg['kv_pairs'])
                self.publish_message(properties, generate_response(STATUS_SUCCESS, {"msg": "Batch init for orders successful"}))

            elif msg['action'] == "add_item":
                add_item_db(msg['order_id'], msg['order_entry'],properties.correlation_id)
                response = {
                    "order_id": msg['order_id'],
                }
                self.publish_message(properties, generate_response(STATUS_SUCCESS, response))

            elif msg['action'] == "confirm_order":
                confirm_order(msg['order_id'], msg['order_entry'],properties.correlation_id)
                self.publish_message(properties, generate_response(STATUS_SUCCESS, "Checkout successful"))


        except RedisDBError as e:
            self.logger.error(f"[{properties.reply_to}] { msg['action']} : {msg} {e}")
            self.publish_message(properties, generate_response(STATUS_SERVER_ERROR, DB_ERROR_STR))
        except InsufficientCreditError as e:
            self.logger.error(f"[{properties.reply_to}] { msg['action']} : {msg} {e}")
            self.publish_message(properties, generate_response(STATUS_CLIENT_ERROR, REQ_ERROR_STR))
        except Exception as e:
            self.logger.error(f"[{properties.reply_to}] { msg['action']} : {msg} {e}")
            self.publish_message(properties, generate_response(STATUS_SERVER_ERROR, SERV_ERROR_STR))

    def start_consuming(self):
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(queue=ORDER_QUEUE, on_message_callback=self.callback)
        try:   
            self.logger.info("Starting conumer...")
            self.channel.start_consuming()
        except Exception as e:
            raise RabbitMQError
        

    def publish_message(self, properties, response):
        self.logger.info(f"[{properties.reply_to}] Response: {response}")
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
    while True:
        try:
            consumer.connect()
            consumer.start_consuming()
        except RabbitMQError:
            time.sleep(5)
        
            