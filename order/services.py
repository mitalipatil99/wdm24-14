import atexit
import os
import time
import uuid
import redis
import socket
from msgspec import msgpack
from model import OrderValue
from config import *

from exceptions import RedisDBError


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

def get_order_by_id_db(order_id: str) :
    try:
        entry: bytes = db.get(order_id)
    except redis.exceptions.ConnectionError:
        retry_connection()
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
    except redis.exceptions.ConnectionError:
        retry_connection()
        db.set(key, value)
    except redis.exceptions.RedisError:
        raise RedisDBError(Exception)
    return key

def batch_init_users_db(kv_pairs):
    try:
        db.mset(kv_pairs)
    except redis.exceptions.ConnectionError:
        retry_connection()
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        raise RedisDBError(Exception)
    
def add_item_db(order_id, order_entry):
    try:
        db.set(order_id, msgpack.encode(order_entry))
    except redis.exceptions.ConnectionError:
        retry_connection()
        db.set(order_id, msgpack.encode(order_entry))
    except redis.exceptions.RedisError:
        raise RedisDBError(Exception)
    

def confirm_order(order_id, order_entry):
    try:
        db.set(order_id, msgpack.encode(order_entry))   
    except redis.exceptions.ConnectionError:
        retry_connection()
        db.set(order_id, msgpack.encode(order_entry))
    except redis.exceptions.RedisError:
        raise RedisDBError(Exception)