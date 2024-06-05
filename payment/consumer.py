import pika
from msgspec import msgpack

from exceptions import RedisDBError, InsufficientCreditError, RabbitMQError
from services import create_user_db, batch_init_db, get_user_db, add_credit_db, remove_credit_db
from config import *
import os
import logging
import time

def generate_response(status, data={}):
    return {"status":status, "data":data}


class RabbitMQConsumer:

    def declare_queues(self):
        self.channel.queue_declare(PAYMENT_QUEUE)

    def setup_logger(self):
        self.logger = logging.getLogger("PaymentService")
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

        ch.basic_ack(delivery_tag=method.delivery_tag)  # Acknowledge the message
        try:
            if msg['action'] == "create_user":
                key = create_user_db()
                self.publish_message(properties, generate_response(STATUS_SUCCESS, {"user_id": key}))

            elif msg['action'] == "batch_init":
                batch_init_db(msg['n'], msg['starting_money'])
                self.publish_message(properties, generate_response(STATUS_SUCCESS, {"msg": "Batch init for payment successful"}))

            elif msg['action'] == "find_user":
                user_entry = get_user_db(msg['user_id'])
                response = {
                    "credit": user_entry.credit,
                    "last_upd": user_entry.last_upd
                }
                self.publish_message(properties, generate_response(STATUS_SUCCESS, response))

            elif msg['action'] == "add_funds":
                upd_usr = properties.correlation_id
                user_entry = add_credit_db(msg['user_id'], msg['amount'], upd_usr)
                response = {
                    "credit": user_entry.credit,
                    "last_upd": user_entry.last_upd
                }
                self.publish_message(properties, generate_response(STATUS_SUCCESS, response))

            elif msg['action'] == "remove_credit":
                upd_usr = properties.correlation_id
                user_entry = remove_credit_db(msg['user_id'], msg['amount'], upd_usr)
                response = {
                    "credit": user_entry.credit,
                    "last_upd": user_entry.last_upd
                }
                self.publish_message(properties, generate_response(STATUS_SUCCESS, response))

        except RedisDBError as e:
            self.logger.error(f"[{properties.reply_to}] { msg['action']} : {msg} {e}")
            self.publish_message(properties, generate_response(STATUS_SERVER_ERROR, DB_ERROR_STR ))
        except InsufficientCreditError as e:
            self.logger.error(f"[{properties.reply_to}] { msg['action']} : {msg} {e}")
            self.publish_message(properties, generate_response(STATUS_CLIENT_ERROR, REQ_ERROR_STR))
        except Exception as e:
            self.logger.error(f"[{properties.reply_to}] { msg['action']} : {msg} {e}")
            self.publish_message(properties, generate_response(STATUS_SERVER_ERROR, SERV_ERROR_STR) )

    def start_consuming(self):
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(queue=PAYMENT_QUEUE, on_message_callback=self.callback)

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