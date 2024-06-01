import logging
import threading
import uuid
import pika

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


def publish_to_rabbitmq(message: dict):
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='stock_queue')
    channel.basic_publish(exchange='', routing_key='stock_queue', body=msgpack.encode(message))
    connection.close()


def threaded(fn):
    def wrapper(*args, **kwargs):
        thread = threading.Thread(target=fn, args=args, kwargs=kwargs)
        thread.start()
        return jsonify({"msg": "Processing in background"}), 202

    return wrapper


async def get_item_from_db(item_id: str) -> StockValue | None:
    try:
        entry: bytes = await get_item(item_id)
    except RedisDBError:
        raise RedisDBError
    except ItemNotFoundError:
        raise ItemNotFoundError
    return entry


@app.post('/item/create/<price>')
@threaded
def create_item(price: int):
    try:
        key = set_new_item(price)
    except RedisDBError:
        return abort(400, DB_ERROR_STR)

    publish_to_rabbitmq({'action': 'create_item', 'item_id': key, 'price': price})
    return key


@app.post('/batch_init/<n>/<starting_stock>/<item_price>')
@threaded
def batch_init_users(n: int, starting_stock: int, item_price: int):
    try:
        set_users(n, starting_stock, item_price)
    except RedisDBError:
        return abort(400, DB_ERROR_STR)

    publish_to_rabbitmq({'action': 'batch_init', 'n': n, 'starting_stock': starting_stock, 'item_price': item_price})
    return jsonify({"msg": "Batch init for stock successful"})


@app.get('/find/<item_id>')
@threaded
def find_item(item_id: str):
    try:
        item_entry: StockValue = get_item(item_id)
    except RedisDBError:
        return abort(400, DB_ERROR_STR)
    except ItemNotFoundError:
        return abort(400, f"Item not found!")

    publish_to_rabbitmq({'action': 'find_item', 'item_id': item_id})
    return jsonify(
        {
            "stock": item_entry.stock,
            "price": item_entry.price
        }
    )


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

    publish_to_rabbitmq({'action': 'add_stock', 'item_id': item_id, 'amount': amount})
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

    publish_to_rabbitmq({'action': 'remove_stock', 'item_id': item_id, 'amount': amount})
    return Response(f"Item: {item_id} stock updated to: {item_entry.stock}", status=200)


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
