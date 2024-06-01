import logging
import threading
import uuid
import pika
import os
from threading import Thread
from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response

from services import get_item, set_new_item, set_users, add_amount, remove_amount
from exceptions import RedisDBError, ItemNotFoundError, InsufficientStockError

DB_ERROR_STR = "DB error"

app = Flask("stock-service")


class StockValue(Struct):
    stock: int
    price: int
    last_upd: str


class RabbitMQClient:
    def __init__(self):
        self.connection = pika.BlockingConnection(pika.URLParameters("amqp://guest:guest@so-rabbit:5672"))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue='stock_queue')

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
            routing_key='stock_queue',
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


@app.post('/item/create/<price>')
@threaded
def create_item(price: int):
    try:
        key = set_new_item(price)
    except RedisDBError:
        return abort(400, DB_ERROR_STR)

    response = rabbitmq_client.call({'action': 'create_item', 'item_id': key, 'price': price})
    return {"item_id": key}


@app.post('/batch_init/<n>/<starting_stock>/<item_price>')
@threaded
def batch_init_users(n: int, starting_stock: int, item_price: int):
    try:
        set_users(n, starting_stock, item_price)
    except RedisDBError:
        return abort(400, DB_ERROR_STR)

    response = rabbitmq_client.call(
        {'action': 'batch_init', 'n': n, 'starting_stock': starting_stock, 'item_price': item_price})
    return jsonify(response)


@app.get('/find/<item_id>')
@threaded
def find_item(item_id: str):
    try:
        item_entry: StockValue = get_item(item_id)
    except RedisDBError:
        return abort(400, DB_ERROR_STR)
    except ItemNotFoundError:
        return abort(400, f"Item not found!")

    response = rabbitmq_client.call({'action': 'find_item', 'item_id': item_id})
    return jsonify(response)


@app.post('/add/<item_id>/<amount>')
@threaded
def add_stock(item_id: str, amount: int):
    try:
        item_entry: StockValue = get_item(item_id)
        item_entry.stock += int(amount)
        add_amount(item_id, amount)
    except RedisDBError:
        return abort(400, DB_ERROR_STR)
    except ItemNotFoundError:
        return abort(400, f"Item not found!")

    response = rabbitmq_client.call({'action': 'add_stock', 'item_id': item_id, 'amount': amount})
    return jsonify(response)


@app.post('/subtract/<item_id>/<amount>')
@threaded
def remove_stock(item_id: str, amount: int):
    try:
        item_entry: StockValue = get_item(item_id)
        remove_amount(item_id, amount)
    except RedisDBError:
        return abort(400, DB_ERROR_STR)
    except InsufficientStockError:
        return abort(400, f"Insufficient funds!")
    except ItemNotFoundError:
        return abort(400, f"Item not found!")

    response = rabbitmq_client.call({'action': 'remove_stock', 'item_id': item_id, 'amount': amount})
    return jsonify(response)


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
