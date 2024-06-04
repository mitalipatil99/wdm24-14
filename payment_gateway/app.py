import logging
import uuid
import pika
import os
from threading import Thread
from msgspec import msgpack
from flask import Flask, jsonify, abort, request
from datetime import datetime
from config import *
import time

app = Flask("payment-gateway")

class RabbitMQClient:

    def connect(self):
        app.logger.info("Connecting to broker...")
        self.connection = pika.BlockingConnection(pika.URLParameters(os.environ['RABBITMQ_BROKER_URL']))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=ORDER_QUEUE)
        self.channel.queue_declare(queue=STOCK_QUEUE)
        self.channel.queue_declare(queue=PAYMENT_QUEUE)

    def __init__(self):
        self.connect()


    def call(self, message, queue, is_message_sent = False, retry_queue = None, retry_corr_id = None):
        corr_id = None
        response_queue = None
        try:
            if is_message_sent:
                corr_id = retry_corr_id
                response_queue = retry_queue
            else:
                corr_id = str(uuid.uuid4())
                response_queue = self.channel.queue_declare(queue='', exclusive=True).method.queue
            
            def on_response(ch, method, props, body):
                if corr_id == props.correlation_id:
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    self.response = msgpack.decode(body)
                    ch.stop_consuming()

            self.channel.basic_consume(queue=response_queue, on_message_callback=on_response, auto_ack=False)
            self.response = None
            if not is_message_sent:
                self.channel.basic_publish(
                    exchange='',
                    routing_key=queue,
                    properties=pika.BasicProperties(
                        reply_to=response_queue,
                        correlation_id=corr_id,
                    ),
                    body=msgpack.encode(message)
                )
                is_message_sent = True

            while self.response is None:
                self.connection.process_data_events(time_limit=None)

            return self.response
        except Exception as e:
            app.logger.error(e)
            while True:
                try:
                    self.connect()
                    if is_message_sent:
                        return self.call(message, queue, is_message_sent, response_queue, corr_id)
                    else:
                        return self.call(message, queue)
                except Exception as e:
                    time.sleep(5)
                    app.logger.error("Reconnecting...")


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
        endpoint = request.endpoint
        request_data = request.get_json() if request.is_json else request.values.to_dict()
        timestamp = datetime.now()
        app.logger.info(f"[{timestamp}] API: {endpoint} | {request_data}")
            
        def callback():
              
            try:
                  return fn(*args, **kwargs)
            except Exception as e:
                app.logger.error(f"Error in thread: {e}")

        thread = ThreadWithReturnValue(target=callback)
        thread.start()
        response = thread.join()
        app.logger.info(f"Response: {response}")
        if response is not None:
            if response['status'] == STATUS_SUCCESS:
                return jsonify(response['data'])
            else:
                return abort(response['status'], response['data'] )
        else:
            return abort(500, SERV_ERROR_STR)

    wrapper.__name__ = f"{fn.__name__}_wrapper"
    return wrapper

@app.post('/create_user')
@threaded
def create_user():
    response = rabbitmq_client.call({'action': 'create_user'}, PAYMENT_QUEUE)
    return response

@app.post('/batch_init/<n>/<starting_money>')
@threaded
def batch_init_users(n: int, starting_money: int):
    response = rabbitmq_client.call({'action': 'batch_init','n':n, 'starting_money':starting_money}, PAYMENT_QUEUE)
    return response


@app.get('/find_user/<user_id>')
@threaded
def find_user(user_id: str):
    response = rabbitmq_client.call({'action': 'find_user','user_id':user_id}, PAYMENT_QUEUE)
    return response


@app.post('/add_funds/<user_id>/<amount>')
@threaded
def add_credit(user_id: str, amount: int):
    response = rabbitmq_client.call({'action': 'add_funds','user_id':user_id,'amount':amount}, PAYMENT_QUEUE)
    return response

@app.post('/pay/<user_id>/<amount>')
@threaded
def remove_credit(user_id: str, amount: int):
    response = rabbitmq_client.call({'action': 'remove_credit','user_id':user_id,'amount':amount}, PAYMENT_QUEUE)
    return response


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
