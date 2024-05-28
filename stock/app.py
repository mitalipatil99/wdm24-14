import logging
import os
import atexit
import uuid

import redis

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

async def get_item_from_db(item_id: str) -> StockValue | None:
    # get serialized data
    try:
        entry: bytes = await get_item(item_id)
    except RedisDBError:
        raise RedisDBError
    except ItemNotFoundError:
        raise ItemNotFoundError
    # deserialize data if it exists else return null
    # entry: StockValue | None = msgpack.decode(entry, type=StockValue) if entry else None
    # if entry is None:
        # if item does not exist in the database; abort
        # abort(400, f"Item: {item_id} not found!")
    return entry


@app.post('/item/create/<price>')
async def create_item(price: int):
    # key = str(uuid.uuid4())
    # app.logger.debug(f"Item: {key} created")
    # value = msgpack.encode(StockValue(stock=0, price=int(price)))
    try:
        key = await set_new_item(price)
    except RedisDBError:
        return abort(400, DB_ERROR_STR)
    return key


@app.post('/batch_init/<n>/<starting_stock>/<item_price>')
async def batch_init_users(n: int, starting_stock: int, item_price: int):
    try:
        await set_users(n, starting_stock, item_price)
    except RedisDBError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for stock successful"})


@app.get('/find/<item_id>')
async def find_item(item_id: str):
    try:
        item_entry: StockValue = await get_item(item_id)
    except RedisDBError:
        return abort(400, DB_ERROR_STR)
    except ItemNotFoundError:
        return abort(400, f"Item not found!")
    return jsonify(
        {
            "stock": item_entry.stock,
            "price": item_entry.price
        }
    )


@app.post('/add/<item_id>/<amount>')
async def add_stock(item_id: str, amount: int):
    try:
        item_entry: StockValue = await get_item(item_id)
        # update stock, serialize and update database
        item_entry.stock += int(amount)
        await add_amount(item_id, amount)
    except RedisDBError:
        return abort(400, DB_ERROR_STR)
    except ItemNotFoundError:
        return abort(400, f"Item not found!")
    return Response(f"Item: {item_id} stock updated to: {item_entry.stock}", status=200)


@app.post('/subtract/<item_id>/<amount>')
async def remove_stock(item_id: str, amount: int):
    try:
        item_entry: StockValue = await get_item(item_id)
        await remove_amount(item_id, amount)
    except RedisDBError:
        return abort(400, DB_ERROR_STR)
    except InsufficientStockError:
        return abort(400, f"Insufficient funds!")
    except ItemNotFoundError:
        return abort(400, f"Item not found!")
    return Response(f"Item: {item_id} stock updated to: {item_entry.stock}", status=200)


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)