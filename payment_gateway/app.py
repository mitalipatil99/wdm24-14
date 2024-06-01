import logging
import uuid
import pika
import os
from threading import Thread
from msgspec import msgpack
from flask import Flask, jsonify

app = Flask("payment-gateway")

class RabbitMQClient:
    def __init__(self):
        self.connection = pika.BlockingConnection(pika.URLParameters(os.environ['RABBITMQ_BROKER_URL']))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue='payment_queue')

    def call(self, message):
        corr_id = str(uuid.uuid4())
        response_queue = self.channel.queue_declare(queue='', exclusive=True).method.queue

        def on_response(ch, method, props, body):
            if corr_id == props.correlation_id:
                ch.basic_ack(delivery_tag=method.delivery_tag)
                self.response = msgpack.decode(body)
                ch.stop_consuming()

        self.channel.basic_consume(queue=response_queue, on_message_callback=on_response, auto_ack=False)
        self.response = None
        self.channel.basic_publish(
            exchange='',
            routing_key='payment_queue',
            properties=pika.BasicProperties(
                reply_to=response_queue,
                correlation_id=corr_id,
            ),
            body=msgpack.encode(message)
        )

        while self.response is None:
            self.connection.process_data_events(time_limit=None)

        return self.response


rabbitmq_client = RabbitMQClient()


class ThreadWithReturnValue(Thread):

    def __init__(self, group=None, target=None, name=None,
                 args=(), kwargs={}, Verbose=None):
        Thread.__init__(self, group, target, name, args, kwargs)
        self._return = None

    def run(self):
        if self._target is not None:
            self._return = self._target(*self._args,
                                        **self._kwargs)

    def join(self, *args):
        Thread.join(self, *args)
        return self._return


def threaded(fn):
    def wrapper(*args, **kwargs):
        def callback():
            try:
                return fn(*args, **kwargs)
            except Exception as e:
                app.logger.error(f"Error in thread: {e}")

        thread = ThreadWithReturnValue(target=callback)
        thread.start()
        # handle status code
        ret = thread.join()
        print(f"woahh {ret}")
        app.logger.error(f"woahh {ret}")
        return jsonify(ret)

    wrapper.__name__ = f"{fn.__name__}_wrapper"
    return wrapper

@app.post('/create_user')
@threaded
def create_user():
    response = rabbitmq_client.call({'action': 'create_user'})
    return response

@app.post('/batch_init/<n>/<starting_money>')
@threaded
def batch_init_users(n: int, starting_money: int):
    response = rabbitmq_client.call({'action': 'batch_init','n':n, 'starting_money':starting_money})
    return response


@app.get('/find_user/<user_id>')
@threaded
def find_user(user_id: str):
    response = rabbitmq_client.call({'action': 'find_user','user_id':user_id})
    return response


@app.post('/add_funds/<user_id>/<amount>')
@threaded
def add_credit(user_id: str, amount: int):
    response = rabbitmq_client.call({'action': 'add_funds','user_id':user_id,'amount':amount})
    return response

@app.post('/pay/<user_id>/<amount>')
@threaded
def remove_credit(user_id: str, amount: int):
    response = rabbitmq_client.call({'action': 'remove_credit','user_id':user_id,'amount':amount})
    return response


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)