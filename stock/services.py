import os
import atexit
import uuid
import redis

from msgspec import msgpack
from exceptions import *

from model import StockValue
from amqp_client import AMQPClient

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))

# TODO: Where redis error, retry if db not available

def close_db_connection() -> StockValue | None:
    db.close()

atexit.register(close_db_connection)


async def set_updated_str(last_upd_str: str, new_upd: str):
    if len(last_upd_str.split(',')) < 20:
        return f"{last_upd_str},{new_upd}"
    else:
        last_upd_str = ','.join(last_upd_str.split(',')[1:])
        return f"{last_upd_str},{new_upd}"


async def get_item(item_id: str) -> str:
    try:
        entry = db.get(item_id)
    except redis.exceptions.RedisError:
        raise RedisDBError
    entry: StockValue | None = msgpack.decode(entry, type=StockValue) if entry else None
    if entry is None:
        raise ItemNotFoundError
    return entry


# IMP: Assuming scenario where item always exists (Maybe can add check while adding to order and before checkout)
async def get_item_bulk(item_ids: list) -> list:
    try:
        entries = db.mget(item_ids)
    except redis.exceptions.RedisError:
        raise RedisDBError
    return entries


async def set_new_item(value: int):
    key = str(uuid.uuid4())
    value = msgpack.encode(StockValue(stock=0, price=int(value)))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        raise RedisDBError
    return key


async def set_users(n: int, starting_stock: int, item_price: int):
    n = int(n)
    starting_stock = int(starting_stock)
    item_price = int(item_price)
    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(StockValue(stock=starting_stock, price=item_price))
                                  for i in range(n)}
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        raise RedisDBError


async def add_amount(item_id: str, amount: int):
    item_entry: StockValue = await get_item(item_id)
    item_entry.stock += int(amount)
    try:
        item_entry.last_op = "add"
        db.set(item_id, msgpack.encode(item_entry))
    except redis.exceptions.RedisError:
        raise RedisDBError
    return item_entry


async def remove_amount(item_id: str, amount: int):
    item_entry: StockValue = await get_item(item_id)
    item_entry.stock -= int(amount)
    if item_entry.stock < 0:
        raise InsufficientStockError
    try:
        item_entry.last_op = "sub"
        db.set(item_id, msgpack.encode(item_entry))
    except redis.exceptions.RedisError:
        raise RedisDBError
    return item_entry


async def add_amount_bulk(stock_add: dict, order_id: str):
    item_ids = list(stock_add.keys())
    items = await get_item_bulk(item_ids)
    stocks_upd = dict()
    for i in range(len(items)):
        item : StockValue | None = msgpack.decode(items[i], type=StockValue) if items[i] else None
        # If item has already been updated by same order id
        # if order_id in item.last_upd and item.last_op == "add":
        #     raise DuplicateUpdateError
        item.stock += int(stock_add[item_ids[i]])
        items[i] = item
        stocks_upd[item_ids[i]] = msgpack.encode(
            StockValue(stock=item_ids[i].stock, 
                       price=item_ids[i].price, 
                       last_upd=await set_updated_str(
                           item_ids[i].last_upd, 
                           order_id),
                           last_op="add"))
    try:
        db.mset(stocks_upd)
    except redis.exceptions.RedisError:
        raise RedisDBError


async def remove_amount_bulk(stock_remove: dict, order_id: str):
    item_ids = list(stock_remove.keys())
    items = await get_item_bulk(item_ids)
    stocks_upd = dict()
    for i in range(len(items)):
        item : StockValue | None = msgpack.decode(items[i], type=StockValue) if items[i] else None
        # If item has already been updated by same order id, so not retry: Check if this is fine, same order can be checked out multiple times

        # if order_id in item.last_upd and item.last_op == "sub":
        #     raise DuplicateUpdateError
        item.stock -= int(stock_remove[item_ids[i]])
        if item.stock < 0:
            # TODO: Publish message to rollback containing order_id
            raise InsufficientStockError
        stocks_upd[item_ids[i]] = msgpack.encode(
            StockValue(stock=item.stock, 
                       price=item.price, 
                       last_upd=await set_updated_str(
                           item.last_upd, 
                           order_id),
                           last_op="sub"))
    try:
        db.mset(stocks_upd)
    except redis.exceptions.RedisError:
        raise RedisDBError
    