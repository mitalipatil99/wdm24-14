import atexit
import os
from typing import List
from uuid import uuid4
import uuid
import redis
from msgspec import msgpack, Struct
import redis.exceptions

from exceptions import RedisDBError, InsufficientCreditError

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))

def close_db_connection():
    db.close()

atexit.register(close_db_connection)


class OrderValue(Struct):
    paid: bool
    items: list[tuple[str, int]]
    user_id: str
    total_cost: int

def get_order_by_id_db(order_id: str) :
    try:
        entry: bytes = db.get(order_id)
    except redis.exceptions.RedisError:
        raise RedisDBError(Exception)
    entry: OrderValue | None = msgpack.decode(entry, type=OrderValue) if entry else None
    return entry


def create_order_db(user_id: str):
    key = str(uuid.uuid4())
    value = msgpack.encode(OrderValue(paid=False, items=[], user_id=user_id, total_cost=0))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        raise RedisDBError(Exception)
    return key

def batch_init_users_db(kv_pairs):
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        raise RedisDBError(Exception)
    
def add_item_db(order_id, order_entry):
    try:
        db.set(order_id, msgpack.encode(order_entry))
    except redis.exceptions.RedisError:
        raise RedisDBError(Exception)
    

def confirm_order(order_id, order_entry):
    try:
        db.set(order_id, msgpack.encode(order_entry))   
    except redis.exceptions.RedisError:
        raise RedisDBError(Exception)