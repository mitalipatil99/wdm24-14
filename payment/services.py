import redis
import os
import json
import atexit
import uuid

from msgspec import msgpack, Struct
from amqp_client import AMQPClient
from model import UserValue
from exceptions import RedisDBError, InsufficientCreditError

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))

def close_db_connection():
    db.close()

atexit.register(close_db_connection)

# TODO: Where redis error, retry if db not available

async def get_user_db(user_id: str) -> UserValue | None:
    try:
        entry: bytes = db.get(user_id)
    except redis.exceptions.RedisError:
        raise RedisDBError(Exception)
    entry: UserValue | None = msgpack.decode(entry, type=UserValue) if entry else None
    return entry

async def create_user_db():
    key = str(uuid.uuid4())
    value = msgpack.encode(UserValue(credit=0, last_upd="admin"))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        raise RedisDBError(Exception)
    return key
    

async def batch_init_db(n: int, starting_money: int):
    n = int(n)
    starting_money = int(starting_money)
    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(UserValue(credit=starting_money, last_upd="admin"))
                                  for i in range(n)}
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        raise RedisDBError(Exception)


async def add_credit_db(user_id: str, amount: int, order_id: str = "") -> UserValue:
    user_entry: UserValue = await get_user_db(user_id)
    # update credit, serialize and update database
    user_entry.credit += int(amount)
    user_entry.last_upd = order_id if order_id else "api"
    try:
        db.set(user_id, msgpack.encode(user_entry))
    except redis.exceptions.RedisError:
        raise RedisDBError(Exception)
    return user_entry


async def remove_credit_db(user_id: str, amount: int, order_id: str = ""):
    user_entry: UserValue = await get_user_db(user_id)
    if user_entry.last_upd == order_id:
        # payment was already deducted for this order
        return user_entry
    user_entry.credit -= int(amount)
    if user_entry.credit < 0:
        # TODO: Publish message to rollback containing order_id
        raise InsufficientCreditError(Exception)
    try:
        user_entry.last_upd = order_id if order_id else "api"
        db.set(user_id, msgpack.encode(user_entry))
    except redis.exceptions.RedisError:
        raise RedisDBError(Exception)  
    return user_entry
