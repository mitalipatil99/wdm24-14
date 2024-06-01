import asyncio
import os

from saga import OrchestrationBuilder
from msgspec import msgpack, Struct
# from amqp_client import AMQPClient
from config import STOCK_ADD, STOCK_SUBTRACT, PAYMENT_ADD, PAYMENT_DEDUCT
from logging import Logger
import uuid


class OrderValue(Struct):
    paid: bool
    items: list[tuple[str, int]]
    user_id: str
    total_cost: int


# broker = AMQPClient(Logger)


async def start_orch():
    event = {'event': 'order_created', 'details': 'Order process started', 'key': str(uuid.uuid4())}
    broker.publish_event('order_created', event)  # publish rabbitMQ event
    return 'orchestration started'

async def stock_subtract(result, items: OrderValue):
    try:
        print('Inside stock_subtract()...')
        data = {}
        for item in items.items:
            data[item[0]] = item[1]
        event = {'event': 'stock_subtract', 'details': 'Stock subtracted for the order', 'data': data,'key': str(uuid.uuid4())}
        # broker.publish_event(STOCK_SUBTRACT, event)  # publish rabbitMQ event
        return "SUC"
    except Exception as ex:
        raise ex


async def stock_add(result, items: OrderValue):
    try:
        print('Inside stock_add()...')
        data = {}
        for item in items.items:
            data[item[0]] = item[1]
        event = {'event': 'stock_add', 'details': 'Stock added for the order', 'data': data,'key': str(uuid.uuid4())}
        # broker.publish_event(STOCK_ADD, event)  # publish rabbitMQ event
        return "SUC"
    except Exception as ex:
        raise ex


async def payment_deduct(res, user_id, total_cost):
    try:
        print('Inside payment_deduct()...')
        data = {'user_id': user_id, 'total_cost': total_cost}
        event = {'event': 'payment_deduct', 'details': 'payment_deduct for the order', 'data': data,'key': str(uuid.uuid4())}
        # broker.publish_event(PAYMENT_DEDUCT, event)  # publish rabbitMQ event
        return "SUC"
    except Exception as ex:
        raise ex


async def payment_add(res, user_id, total_cost):
    try:
        print('Inside payment_add()...')
        data = {'user_id': user_id, 'total_cost': total_cost}
        event = {'event': 'payment_add', 'details': 'payment_add for the order', 'data': data,'key': str(uuid.uuid4())}
        # broker.publish_event(PAYMENT_ADD, event)  # publish rabbitMQ event
        return "SUC"
    except Exception as ex:
        raise ex


async def CreateOrderRequestSaga(order_entry: OrderValue, total_cost=0, user_id=0):
    builder = (
        OrchestrationBuilder()
        .add_step(start_orch, lambda curr_act_res, items=order_entry: stock_add(curr_act_res, items))
        .add_step(lambda prev_act_res, items=order_entry: stock_subtract(prev_act_res, items),
                  lambda curr_act_res, user_id=user_id, total_cost=total_cost: payment_add(curr_act_res, user_id,
                                                                                           total_cost))
        .add_step(lambda prev_act_res, user_id=user_id, total_cost=total_cost: payment_deduct(prev_act_res, user_id,
                                                                                              total_cost),
                  lambda: None)
    )
    await builder.execute()
