import redis
import os
import json
import atexit
import uuid

from aio_pika import IncomingMessage
from msgspec import msgpack, Struct

from model import AMQPMessage
from amqp_client import AMQPClient
from exceptions import RedisDBError, InsufficientCreditError

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))

def close_db_connection():
    db.close()

atexit.register(close_db_connection)

class UserValue(Struct):
    credit: int
    last_upd: str


async def get_user_db(user_id: str) -> UserValue | None:
    try:
        # get serialized data
        entry: bytes = db.get(user_id)
    except redis.exceptions.RedisError:
        raise RedisDBError(Exception)
    # deserialize data if it exists else return null
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


async def add_credit_db(user_id: str, amount: int, user_upd: str = "api") -> UserValue:
    user_entry: UserValue = await get_user_db(user_id)
    # update credit, serialize and update database
    user_entry.credit += int(amount)
    user_entry.last_upd = user_upd
    try:
        db.set(user_id, msgpack.encode(user_entry))
    except redis.exceptions.RedisError:
        raise RedisDBError(Exception)
    return user_entry


async def remove_credit_db(user_id: str, amount: int, user_upd: str = "api"):
    user_entry: UserValue = await get_user_db(user_id)
    # update credit, serialize and update database
    user_entry.credit -= int(amount)
    if user_entry.credit < 0:
        raise InsufficientCreditError(Exception)
    try:
        user_entry.last_upd = user_upd
        db.set(user_id, msgpack.encode(user_entry))
    except redis.exceptions.RedisError:
        raise RedisDBError(Exception)  
    return user_entry


async def payment_event_processor(message: IncomingMessage):
    async with message.process(ignore_processed=True):
        command = message.headers.get('COMMAND')
        client = message.headers.get('CLIENT')

        payment_message = json.loads(str(message.body.decode('utf-8')))
        response_obj: AMQPMessage = None
        if client == 'ORDER_REQUEST_ORCHESTRATOR' and command == 'PAYMENT_DEDUCT':
            try:
                await remove_credit_db(payment_message['data'].get('user_id'), payment_message['data'].get('total_cost'), payment_message.get('key'))
                funds_available = True
            except InsufficientCreditError:
                funds_available = False
            except RedisDBError:
                #TODO: To be replaced by timeout? Or check types of error and then 
                funds_available = False

            await message.ack()
            response_obj = AMQPMessage(
                id=message.correlation_id,
                content=None,
                reply_state=('PAYMENT_UNSUCCESSFUL','PAYMENT_SUCCESSFUL')[funds_available]
            )

        if client == 'ORDER_REQUEST_ORCHESTRATOR' and command == 'PAYMENT_ADD':
            try:
                await add_credit_db(payment_message['data'].get('user_id'), payment_message['data'].get('total_cost'), payment_message.get('key'))
                refund_success = True
            except RedisDBError:
                refund_success = False
            
            await message.ack()
            response_obj = AMQPMessage(
                id=message.correlation_id,
                content=None,
                reply_state=('PAYMENT_UNSUCCESSFUL','PAYMENT_SUCCESSFUL')[refund_success]
            )

        # There must be a response object to signal orchestrator of
        # the outcome of the request.
        assert response_obj is not None

        amqp_client: AMQPClient = await AMQPClient().init()
        await amqp_client.event_producer(
            'ORDER_TX_EVENT_STORE',
            message.reply_to,
            message.correlation_id,
            response_obj
        )
        await amqp_client.connection.close()
