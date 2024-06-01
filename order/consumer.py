import pika
from msgspec import msgpack
import os
from exceptions import RedisDBError
from services import create_order_db, get_order_by_id_db, batch_init_users_db, add_item_db


class RabbitMQConsumer:

    def declare_queues(self):
        self.channel.queue_declare("order_queue")

    def connect(self):
        try:
            connection = pika.BlockingConnection(pika.URLParameters(os.environ['RABBITMQ_BROKER_URL']))
            channel = connection.channel()
            self.channel = channel
            self.declare_queues()
        except Exception as e:
            print(e)

    def __init__(self) -> None:
        self.channel = None

    def callback(self, ch, method, properties, body):
        msg = msgpack.decode(body)
        print(msg)
        ch.basic_ack(delivery_tag=method.delivery_tag)
        try:
            if msg['action'] == "find_order":
                entry = get_order_by_id_db(msg['order_id'])
                response = {
                    "paid": entry.paid,
                    "items": entry.items,
                    "user_id": entry.user_id,
                    "total_cost": entry.total_cost
                }
                self.publish_message(properties, response)

            elif msg['action'] == "create_order":
                key = create_order_db(msg['user_id'])
                self.publish_message(properties, {"order_id": key})

            elif msg['action'] == "batch_init_users":
                batch_init_users_db(msg['kv_pairs'])
                self.publish_message(properties, {"msg": "Batch init for orders successful"})

            elif msg['action'] == "add_item":
                add_item_db(msg['order_id'], msg['order_entry'])
                response = {
                    "order_id": msg['order_id'],
                    "status": 200
                }
                self.publish_message(properties, response)

            elif msg['action'] == "order_checkout":
                pass
                # new_stock = remove_amount(msg['item_id'], msg['amount'])
                # response = {
                #     "item_id": msg['item_id'],
                #     "stock": new_stock
                # }
                # self.publish_message(properties, response)



        except RedisDBError:
            print("woaaaaaaaaaaaaaaaaaaaaaaaaaah")

    def start_consuming(self):
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(queue="order_queue", on_message_callback=self.callback)
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
