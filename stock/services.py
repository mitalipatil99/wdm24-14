import os
import time
import atexit
import uuid
import redis
from msgspec import msgpack
import logging

from config import *
from model import StockValue
from exceptions import *

def connect_redis():
    db_conn: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))
    return db_conn

def retry_connection():
    global db 
    for attempt in range(1, MAX_RETRIES):
        try: 
            db = connect_redis()
            db.ping()
        except redis.exceptions.ConnectionError:
            if attempt < MAX_RETRIES:
                time.sleep(SLEEP_TIME)
            else:
                raise RedisDBError
                

def close_db_connection():
    db.close()


db = connect_redis()
atexit.register(close_db_connection)


def set_updated_str(last_upd_str: str, new_upd: str, upd_op: str):
    if len(last_upd_str.split(',')) < LAST_UPD_LIMIT:
        return f"{last_upd_str},{new_upd}_{upd_op}"
    else:
        last_upd_str = ','.join(last_upd_str.split(',')[1:])
        return f"{last_upd_str},{new_upd}_{upd_op}"


def is_duplicate_operation(last_upd_str: str, new_upd: str, upd_op: str):
    updates = last_upd_str.split(',')
    order_id_ops = [s for s in updates if new_upd in s]
    if order_id_ops:
        last_op = order_id_ops[-1].split('_')[1]
        if last_op == upd_op:
            return True
        else:
            return False
    else:
        return False
        

def get_item(item_id: str) -> str:
    try:
        entry = db.get(item_id)
    except redis.exceptions.ConnectionError:
        retry_connection()
        entry = db.get(item_id)
    except redis.exceptions.RedisError:
        raise RedisDBError
    entry: StockValue | None = msgpack.decode(entry, type=StockValue) if entry else None
    if entry is None:
        # if item does not exist in the database; abort
        raise ItemNotFoundError
    return entry


def get_item_bulk(item_ids: list) -> list:
    try:
        entries = db.mget(item_ids)
    except redis.exceptions.ConnectionError:
        retry_connection()
        entries = db.mget(item_ids)
    except redis.exceptions.RedisError:
        raise RedisDBError
    return entries


def set_new_item(value: int):
    key = str(uuid.uuid4())
    value = msgpack.encode(StockValue(stock=0, price=int(value), last_upd='admin_add'))
    try:
        db.set(key, value)
    except redis.exceptions.ConnectionError:
        retry_connection()
        db.set(key, value)
    except redis.exceptions.RedisError:
        raise RedisDBError
    return key


def set_users(n: int, starting_stock: int, item_price: int, item_upd: str = 'admin_add'):
    n = int(n)
    starting_stock = int(starting_stock)
    item_price = int(item_price)
    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(StockValue(stock=starting_stock, price=item_price, last_upd=item_upd))
                                  for i in range(n)}
    try:
        db.mset(kv_pairs)
    except redis.exceptions.ConnectionError:
        retry_connection()
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        raise RedisDBError


def add_amount(item_id: str, amount: int, item_upd: str = 'api'):
    item_entry: StockValue =  get_item(item_id)
    # update stock, serialize and update database
    item_entry.stock += int(amount)
    item_entry.last_upd =  set_updated_str(item_entry.last_upd, item_upd, upd_op="add")
    try:
        db.set(item_id, msgpack.encode(item_entry))
    except redis.exceptions.ConnectionError:
        retry_connection()
        db.set(item_id, msgpack.encode(item_entry))
    except redis.exceptions.RedisError:
        raise RedisDBError
    return item_entry.stock


def remove_amount(item_id: str, amount: int, item_upd: str = 'api'):
    item_entry: StockValue = get_item(item_id)
    item_entry.stock -= int(amount)
    item_entry.last_upd = set_updated_str(item_entry.last_upd, item_upd, upd_op="sub")
    if item_entry.stock < 0:
        raise InsufficientStockError
    try:
        db.set(item_id, msgpack.encode(item_entry))
    except redis.exceptions.ConnectionError:
        retry_connection()
        db.set(item_id, msgpack.encode(item_entry))
    except redis.exceptions.RedisError:
        raise RedisDBError
    return item_entry.stock


def add_amount_bulk(message: dict, order_id: str):
    item_ids = list(message.keys())
    items = get_item_bulk(item_ids)
    stocks_upd = dict()
    retry_flag = False
    for i in range(len(items)):
        item : StockValue | None = msgpack.decode(items[i], type=StockValue) if items[i] else None
        if is_duplicate_operation(item.last_upd, order_id, "add"):
            retry_flag = True
            break
        item.stock += int(message[item_ids[i]])
        stocks_upd[item_ids[i]] = msgpack.encode(
            StockValue(stock=item.stock, 
                    price=item.price, 
                    last_upd=set_updated_str(item.last_upd,
                                                order_id,
                                                "add")
                    ))
    if not retry_flag:
        try:
            db.mset(stocks_upd)
        except redis.exceptions.ConnectionError:
            retry_connection()
            db.mset(stocks_upd)
        except redis.exceptions.RedisError:
            raise RedisDBError


def remove_amount_bulk(stock_remove: dict, order_id: str):
    item_ids = list(stock_remove.keys())
    items = get_item_bulk(item_ids)
    stocks_upd = dict()
    retry_flag = False
    for i in range(len(items)):
        item : StockValue | None = msgpack.decode(items[i], type=StockValue) if items[i] else None
        if is_duplicate_operation(item.last_upd, order_id, "sub"):
            retry_flag = True
            break
        item.stock -= int(stock_remove[item_ids[i]])
        if item.stock < 0:
            raise InsufficientStockError
        stocks_upd[item_ids[i]] = msgpack.encode(
            StockValue(stock=item.stock, 
                       price=item.price, 
                       last_upd=set_updated_str(
                           item.last_upd, 
                           order_id,
                           "sub"),
                           ))
    if not retry_flag:
        try:
            db.mset(stocks_upd)
        except redis.exceptions.ConnectionError:
            retry_connection()
            db.mset(stocks_upd)
        except redis.exceptions.RedisError:
            raise RedisDBError

