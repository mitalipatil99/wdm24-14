import redis
import time
import os
import json
import atexit
import uuid
from msgspec import msgpack
from config import *

from model import UserValue
from exceptions import RedisDBError, InsufficientCreditError

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


def get_user_db(user_id: str) -> UserValue | None:
    try:
        # get serialized data
        entry: bytes = db.get(user_id)
    except redis.exceptions.ConnectionError:
        retry_connection()
        entry: bytes = db.get(user_id)
    except redis.exceptions.RedisError:
        raise RedisDBError(Exception)
    # deserialize data if it exists else return null
    entry: UserValue | None = msgpack.decode(entry, type=UserValue) if entry else None
    return entry

def create_user_db():
    key = str(uuid.uuid4())
    value = msgpack.encode(UserValue(credit=0, last_upd="api_add"))
    try:
        db.set(key, value)
    except redis.exceptions.ConnectionError:
        retry_connection()
        db.set(key, value)
    except redis.exceptions.RedisError:
        raise RedisDBError(Exception)
    return key
    

def batch_init_db(n: int, starting_money: int):
    n = int(n)
    starting_money = int(starting_money)
    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(UserValue(credit=starting_money, last_upd="api_add"))
                                  for i in range(n)}
    try:
        db.mset(kv_pairs)
    except redis.exceptions.ConnectionError:
        retry_connection()
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        raise RedisDBError(Exception)


def add_credit_db(user_id: str, amount: int, new_upd: str) -> UserValue:
    user_entry: UserValue =  get_user_db(user_id)
    if user_entry.last_upd != new_upd:
        user_entry.credit += int(amount)
        user_entry.last_upd = new_upd
        try:
            db.set(user_id, msgpack.encode(user_entry))
        except redis.exceptions.ConnectionError:
            retry_connection()
            db.set(user_id, msgpack.encode(user_entry))
        except redis.exceptions.RedisError:
            raise RedisDBError(Exception)
    return user_entry


def remove_credit_db(user_id: str, amount: int, new_upd: str):
    user_entry: UserValue = get_user_db(user_id)
    if user_entry.last_upd != new_upd:
        user_entry.credit -= int(amount)
        if user_entry.credit < 0:
            raise InsufficientCreditError(Exception)
        try:
            user_entry.last_upd = new_upd
            db.set(user_id, msgpack.encode(user_entry))
        except redis.exceptions.ConnectionError:
            retry_connection()
            db.set(user_id, msgpack.encode(user_entry))
        except redis.exceptions.RedisError:
            raise RedisDBError(Exception)  
    return user_entry
