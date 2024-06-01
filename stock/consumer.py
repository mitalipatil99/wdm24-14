import pika
from services import set_new_item, set_users, get_item
from msgspec import msgpack
import os
from exceptions import RedisDBError

class RabbitMQConsumer:

    def declare_queues(self):
        self.channel.queue_declare("stock_queue")

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
            if msg['action'] == "create_item":
                key = set_new_item(msg['price'])
                print(properties.reply_to)
                self.publish_message(properties, {"item_id":key})
                
            
            elif msg['action'] == "batch_init":
                set_users(msg['n'], msg['starting_stock'], msg['item_price'])
                self.publish_message(properties, {"msg": "Batch init for stock successful"})

            
            elif msg['action'] == "find_item":
                item_entry = get_item(msg['item_id'])
                response = {
                    "stock": item_entry.stock,
                    "price": item_entry.price
                }
                self.publish_message(properties, response)


        except RedisDBError:
            print("woaaaaaaaaaaaaaaaaaaaaaaaaaah")


    def start_consuming(self):
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(queue="stock_queue", on_message_callback=self.callback)
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
