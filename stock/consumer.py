import pika
from services import set_new_item, set_users, get_item, add_amount, remove_amount, remove_amount_bulk, add_amount_bulk
from msgspec import msgpack
import os
from exceptions import *
from config import *
import logging
import time

def generate_response(status, data={}):
    return {"status":status, "data":data}

class RabbitMQConsumer:

    def declare_queues(self):
        self.channel.queue_declare(STOCK_QUEUE)

    def setup_logger(self):
        self.logger = logging.getLogger("StockService")
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
            if msg['action'] == "create_item":
                key = set_new_item(msg['price'])
                print(properties.reply_to)
                self.publish_message(properties, generate_response(STATUS_SUCCESS, {"item_id":key}))
            
            elif msg['action'] == "batch_init":
                set_users(msg['n'], msg['starting_stock'], msg['item_price'])
                self.publish_message(properties, generate_response(STATUS_SUCCESS,{"msg": "Batch init for stock successful"}))
            
            elif msg['action'] == "find_item":
                item_entry = get_item(msg['item_id'])
                data = {
                    "stock": item_entry.stock,
                    "price": item_entry.price,
                    "last_upd": item_entry.last_upd
                }
                self.publish_message(properties, generate_response(STATUS_SUCCESS, data))



            elif msg['action'] == "add_stock":
                new_stock = add_amount(msg['item_id'], msg['amount'], properties.correlation_id)
                response = {
                    "item_id": msg['item_id'],
                    "stock": new_stock
                }
                self.publish_message(properties, generate_response(STATUS_SUCCESS, response))



            elif msg['action'] == "remove_stock":
                new_stock = remove_amount(msg['item_id'], msg['amount'], properties.correlation_id)
                response = {
                    "item_id": msg['item_id'],
                    "stock": new_stock
                }
                self.publish_message(properties, generate_response(STATUS_SUCCESS, response))


            elif msg['action'] == "remove_stock_bulk":
                remove_amount_bulk(msg['data'], properties.correlation_id)
                response = {
                    "order_id": msg['order_id'],
                }
                self.publish_message(properties, generate_response(STATUS_SUCCESS, response))


            elif msg['action'] == "add_stock_bulk":
                add_amount_bulk(msg['data'], properties.correlation_id)
                self.publish_message(properties, generate_response(STATUS_SUCCESS))

        except RedisDBError as e:
            self.logger.error(f"[{properties.reply_to}] { msg['action']} : {msg} {e}")
            self.publish_message(properties, generate_response(STATUS_SERVER_ERROR, DB_ERROR_STR ))
        except ItemNotFoundError as e:
            self.logger.error(f"[{properties.reply_to}] { msg['action']} : {msg} {e}")
            self.publish_message(properties, generate_response(STATUS_CLIENT_ERROR, REQ_ERROR_STR))
        except InsufficientStockError as e:
            self.logger.error(f"[{properties.reply_to}] { msg['action']} : {msg} {e}")
            self.publish_message(properties, generate_response(STATUS_CLIENT_ERROR, REQ_ERROR_STR))
        except Exception as e:
            self.logger.error(f"[{properties.reply_to}] { msg['action']} : {msg} {e}")
            self.publish_message(properties, generate_response(STATUS_SERVER_ERROR, SERV_ERROR_STR) )


    def start_consuming(self):
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(queue=STOCK_QUEUE, on_message_callback=self.callback)
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
        except Exception:
            time.sleep(5)