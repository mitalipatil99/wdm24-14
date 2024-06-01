import logging

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, request, Response
from services import *
from exceptions import *
from model import StockValue

DB_ERROR_STR = "DB error"

app = Flask("stock-service")


@app.post('/item/create/<price>')
async def create_item(price: int):
    try:
        key = await set_new_item(price)
    except RedisDBError:
        return abort(400, DB_ERROR_STR)
    return jsonify({'item_id': key})


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
        item_entry: StockValue = await add_amount(item_id, amount)
    except RedisDBError:
        return abort(400, DB_ERROR_STR)
    except ItemNotFoundError:
        return abort(400, f"Item not found!")
    return Response(f"Item: {item_id} stock updated to: {item_entry.stock}", status=200)


@app.post('/subtract/<item_id>/<amount>')
async def remove_stock(item_id: str, amount: int):
    try:
        item_entry: StockValue = await remove_amount(item_id, amount)
    except RedisDBError:
        return abort(400, DB_ERROR_STR)
    except InsufficientStockError:
        return abort(400, f"Insufficient funds!")
    except ItemNotFoundError:
        return abort(400, f"Item not found!")
    return Response(f"Item: {item_id} stock updated to: {item_entry.stock}", status=200)


@app.post('/add_bulk/<order_id>')
async def add_stock_bulk(order_id: str):
    stock_add = request.get_json()
    try:
        await add_amount_bulk(stock_add, order_id)
    except RedisDBError:
        return abort(400, DB_ERROR_STR)
    except DuplicateUpdateError:
        return Response(f"Items already added to stock", status=200)
    return Response(f"Items added to stock", status=200)


@app.post('/subtract_bulk/<order_id>')
async def remove_stock_bulk(order_id: str):
    stock_remove = request.get_json()
    try:
        await remove_amount_bulk(stock_remove, order_id)
    except RedisDBError:
        return abort(400, DB_ERROR_STR)
    except InsufficientStockError:
        return abort(405, f"Insufficient stock!")
    except DuplicateUpdateError:
        return Response(f"Items already subtracted from stock", status=200)
    return Response(f"Items subtracted from stock", status=200)



if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)