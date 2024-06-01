import atexit
import os
from typing import List
import uuid
import random

import redis
from msgspec import msgpack, Struct
import redis.exceptions

from model import OrderValue
from amqp_client import AMQPClient
from exceptions import *

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))

def close_db_connection():
    db.close()

atexit.register(close_db_connection)

# async def order_details(session: Session, id: str) -> Booking:
#     return session.query(Order).filter(Order.id == id).one()


# class Order:
#     pass


# async def order_details_by_order_ref_no(session: Session, parking_slot_ref_no: str) -> Order:
#     return session.query(Order).filter(Order.order_ref_no == order_ref_no).one()


# async def order_list(session: Session) -> List[Order]:
#     return session.query(Order).all()


# async def create_order(order_uuid: str) -> Order:
#     # Since customers may happen to book the same parking slot,
#     # we need to include unique booking identifier (uuid4) to parking_slot_ref_no.
#     # The booking identifier will be used throughout the services to identify
#     # transaction.
#     order = Order(
#         order_ref_no=f'{order_uuid}:{uuid4()}',
#         status='pending'
#     )
#     session.add(order)
#     session.commit()
#     session.refresh(order)

#     return order


# async def update_order(session: Session, order: Order) -> Order:
#     session.commit()
#     session.refresh(order)
#     return order

async def get_order_by_id_db(order_id: str) :
    try:
        entry: bytes = db.get(order_id)
    except redis.exceptions.RedisError:
        raise RedisDBError(Exception)
    entry: OrderValue | None = msgpack.decode(entry, type=OrderValue) if entry else None
    return entry


async def create_order_db(user_id: str):
    key = str(uuid.uuid4())
    value = msgpack.encode(OrderValue(paid=False, items=[], user_id=user_id, total_cost=0))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        raise RedisDBError(Exception)
    return key


async def batch_init_users_db(n: int, n_items: int, n_users: int, item_price: int):
    try:
        n = int(n)
        n_items = int(n_items)
        n_users = int(n_users)
        item_price = int(item_price)

        def generate_entry() -> OrderValue:
            user_id = random.randint(0, n_users - 1)
            item1_id = random.randint(0, n_items - 1)
            item2_id = random.randint(0, n_items - 1)
            value = OrderValue(paid=False,
                            items=[(f"{item1_id}", 1), (f"{item2_id}", 1)],
                            user_id=f"{user_id}",
                            total_cost=2*item_price)
            return value

        kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(generate_entry())
                                    for i in range(n)}
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        raise RedisDBError(Exception)


async def add_item_db(order_id, order_entry):
    try:
        db.set(order_id, msgpack.encode(order_entry))
    except redis.exceptions.RedisError:
        raise RedisDBError(Exception)


async def confirm_order(order_id, order_entry):
    try:
        db.set(order_id, msgpack.encode(order_entry))   
    except redis.exceptions.RedisError:
        raise RedisDBError(Exception)