import logging
import threading
import uuid
import pika

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response, request
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
        self.connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue='stock_queue')
        self.callback_queue = self.channel.queue_declare(queue='', exclusive=True).method.queue
        self.channel.basic_consume(queue=self.callback_queue, on_message_callback=self.on_response, auto_ack=True)
        self.response = None
        self.corr_id = None

    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response = msgpack.decode(body)

    def call(self, message):
        self.response = None
        self.corr_id = str(uuid.uuid4())
        self.channel.basic_publish(
            exchange='',
            routing_key='stock_queue',
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.corr_id,
            ),
            body=msgpack.encode(message)
        )
        while self.response is None:
            self.connection.process_data_events()
        return self.response

rabbitmq_client = RabbitMQClient()

def threaded(fn):
    def wrapper(*args, **kwargs):
        def callback():
            return fn(*args, **kwargs)
        thread = threading.Thread(target=callback)
        thread.start()
    return wrapper

@app.post('/item/create/<price>')
@threaded
def create_item(price: int):
    try:
        key = set_new_item(price)
    except RedisDBError:
        return abort(400, DB_ERROR_STR)

    response = rabbitmq_client.call({'action': 'create_item', 'item_id': key, 'price': price})
    return jsonify(response)

@app.post('/batch_init/<n>/<starting_stock>/<item_price>')
@threaded
def batch_init_users(n: int, starting_stock: int, item_price: int):
    try:
        set_users(n, starting_stock, item_price)
    except RedisDBError:
        return abort(400, DB_ERROR_STR)

    response = rabbitmq_client.call({'action': 'batch_init', 'n': n, 'starting_stock': starting_stock, 'item_price': item_price})
    return jsonify(response)

@app.get('/find/<item_id>')
@threaded
def find_item(item_id: str):
    try:898
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
    return Response(f"Item: {item_id} stock updated to: {item_entry.stock}", status=200)

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
    return Response(f"Item: {item_id} stock updated to: {item_entry.stock}", status=200)

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
