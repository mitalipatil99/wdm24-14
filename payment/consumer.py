import pika
from msgspec import msgpack

from payment.exceptions import RedisDBError
from payment.services import create_user_db, batch_init_db, get_user_db, add_credit_db, remove_credit_db


class RabbitMQConsumer:

    def declare_queues(self):
        self.channel.queue_declare("payment_queue")
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
            if msg['action'] == "create_user":
                key = create_user_db()
                self.publish_message(properties, {"user_id": key})

            elif msg['action'] == "batch_init":
                batch_init_db(msg['n'], msg['starting_money'])
                self.publish_message(properties, {"msg": "Batch init for payment successful"})

            elif msg['action'] == "find_user":
                user_entry = get_user_db(msg['user_id'])
                response = {
                    "credit": user_entry.credit,
                    "last_upd": user_entry.last_upd
                }
                self.publish_message(properties, response)

            elif msg['action'] == "add_funds":
                user_entry = add_credit_db(msg['user_id'], msg['amount'])
                response = {
                    "credit": user_entry.credit,
                    "last_upd": user_entry.last_upd
                }
                self.publish_message(properties, response)

            elif msg['action'] == "remove_credit":
                user_entry = remove_credit_db(msg['user_id'], msg['amount'])
                response = {
                    "credit": user_entry.credit,
                    "last_upd": user_entry.last_upd
                }
                self.publish_message(properties, response)

        except RedisDBError:
            print("AAAAHHHHHHHHHHHHHHHHHHHHHHHH")

    def start_consuming(self):
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(queue="payment_queue", on_message_callback=self.callback)
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
