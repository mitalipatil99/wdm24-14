import os
import atexit
import uuid
import redis

from msgspec import msgpack, Struct
from exceptions import RedisDBError, ItemNotFoundError, InsufficientStockError


class StockValue(Struct):
    stock: int
    price: int
    last_upd: str

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))


def close_db_connection() -> StockValue | None:
    db.close()

atexit.register(close_db_connection)


def set_updated_str(last_upd_str: str, new_upd: str):
    if len(last_upd_str.split(',')) < 20:
        return f"{last_upd_str},{new_upd}"
    else:
        last_upd_str = ','.join(last_upd_str.split(',')[1:])
        return f"{last_upd_str},{new_upd}"


def get_item(item_id: str) -> str:
    try:
        entry = db.get(item_id)
    except redis.exceptions.RedisError:
        raise RedisDBError
    entry: StockValue | None = msgpack.decode(entry, type=StockValue) if entry else None
    if entry is None:
        # if item does not exist in the database; abort
        raise ItemNotFoundError
    return entry


# IMP: Assuming scenario where item always exists (Maybe can add check while adding to order and before checkout)
def get_item_bulk(item_ids: list) -> list:
    try:
        entries = db.mget(item_ids)
    except redis.exceptions.RedisError:
        raise RedisDBError
    return entries


def set_new_item(value: int):
    key = str(uuid.uuid4())
    value = msgpack.encode(StockValue(stock=0, price=int(value), last_upd='admin'))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        raise RedisDBError
    return key

# Check functionality: We are setting same price and stock amount for each item??
def set_users(n: int, starting_stock: int, item_price: int, item_upd: str = 'admin'):
    n = int(n)
    starting_stock = int(starting_stock)
    item_price = int(item_price)
    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(StockValue(stock=starting_stock, price=item_price, last_upd=item_upd))
                                  for i in range(n)}
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        raise RedisDBError
    


def add_amount(item_id: str, amount: int, item_upd: str = 'api'):
    item_entry: StockValue =  get_item(item_id)
    # update stock, serialize and update database
    item_entry.stock += int(amount)
    item_entry.last_upd =  set_updated_str(item_entry.last_upd, item_upd)
    try:
        db.set(item_id, msgpack.encode(item_entry))
    except redis.exceptions.RedisError:
        raise RedisDBError
    return item_entry.stock


def remove_amount(item_id: str, amount: int, item_upd: str = 'api'):
    item_entry: StockValue = get_item(item_id)
    item_entry.stock -= int(amount)
    if item_entry.stock < 0:
        raise InsufficientStockError
    try:
        item_entry.last_upd = set_updated_str(item_entry.last_upd, item_upd)
        db.set(item_id, msgpack.encode(item_entry))
    except redis.exceptions.RedisError:
        raise RedisDBError
    return item_entry.stock


def add_amount_bulk(message: dict):
    item_ids = list(message.keys())
    items = get_item_bulk(item_ids)
    stocks_upd = dict()
    for i in range(len(items)):

        item : StockValue | None = msgpack.decode(items[i], type=StockValue) if items[i] else None
        item.stock += int(message[item_ids[i]])
        items[i] = item
        # TODO: What should be the last_upd here
        stocks_upd[item_ids[i]] = msgpack.encode(
            StockValue(stock=items[i].stock, 
                       price=items[i].price, 
                       last_upd=set_updated_str(items[i].last_upd, "")
                      ))
    try:
        db.mset(stocks_upd)
    except redis.exceptions.RedisError:
        raise RedisDBError


def remove_amount_bulk(stock_remove: dict, order_id: str):
    item_ids = list(stock_remove.keys())
    items = get_item_bulk(item_ids)
    stocks_upd = dict()
    for i in range(len(items)):
        item : StockValue | None = msgpack.decode(items[i], type=StockValue) if items[i] else None
        # If item has already been updated by same order id, so not retry: Check if this is fine, same order can be checked out multiple times

        # if order_id in item.last_upd and item.last_op == "sub":
        #     raise DuplicateUpdateError
        item.stock -= int(stock_remove[item_ids[i]])
        if item.stock < 0:
            # TODO: Error here
            # stock_insuf = {""}
            # AMQPClient.publish_event()
            raise InsufficientStockError
        stocks_upd[item_ids[i]] = msgpack.encode(
            StockValue(stock=item.stock, 
                       price=item.price, 
                       last_upd=set_updated_str(
                           item.last_upd, 
                           order_id),
                           ))
    try:
        db.mset(stocks_upd)
    except redis.exceptions.RedisError:
        raise RedisDBError


# def stock_command_event_processor(message: IncomingMessage):
#     async with message.process(ignore_processed=True):
#         command = message.headers.get('COMMAND')
#         client = message.headers.get('CLIENT')

#         stock_order = json.loads(str(message.body.decode('utf-8')))
#         response_obj: AMQPMessage = None

#         if client == 'ORDER_REQUEST_ORCHESTRATOR' and command == 'STOCK_ADD':
#             reply_state="SUCCESSFUL"
#             try:
#                 await add_amount_bulk(stock_order)  
#             except RedisDBError:
#                 reply_state="UNSUCCESSFUL"
#             except ItemNotFoundError:
#                 reply_state="UNSUCCESSFUL"
#             await message.ack()
#             response_obj = AMQPMessage(
#                 id=message.correlation_id,
#                 reply_state=reply_state
#             )

#         if client == 'ORDER_REQUEST_ORCHESTRATOR' and command == 'STOCK_SUBTRACT':
#             reply_state = "SUCCESSFUL"
#             try:
#                 await remove_amount_bulk(stock_order)
#             except RedisDBError:
#                 reply_state="UNSUCCESSFUL"
#             except InsufficientStockError:
#                 reply_state = "UNSUCCESSFUL"
#             await message.ack()
#             response_obj = AMQPMessage(
#                 id=message.correlation_id,
#                 reply_state=reply_state
#             )

#         assert response_obj is not None

#         amqp_client: AMQPClient = await AMQPClient().init()
#         await amqp_client.event_producer(
#             'STOCK_EVENT_STORE',
#             message.reply_to,
#             message.correlation_id,
#             response_obj
#         )
#         await amqp_client.connection.close()