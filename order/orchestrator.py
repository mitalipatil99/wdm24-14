import asyncio
import os

from saga import OrchestrationBuilder
from msgspec import msgpack, Struct

import pika
import json


class OrderValue(Struct):
    paid: bool
    items: list[tuple[str, int]]
    user_id: str
    total_cost: int


async def start_orch():
    event = {'event': 'OrderCreated', 'details': 'Order process started'}
    publish_event('order_created', event) #publish rabbitMQ event
    return 'orchestration started'


async def stock_subtract(items: OrderValue):
    try:
        print('Inside stock_subtract()...')
        data = {}
        for item in items:
            data[item[0]] = item[1]
        event = {'event': 'StockSubtracted', 'details': 'Stock subtracted for the order', 'data': data}
        publish_event('stock_subtracted', event) #publish rabbitMQ event
    except Exception as ex:
        raise ex


async def stock_add(items: OrderValue):
    try:
        print('Inside stock_add()...')
        data = {}
        for item in items:
            data[item[0]] = item[1]
        event = {'event': 'StockAdd', 'details': 'Stock added for the order', 'data': data}
        publish_event('stock_add', event) #publish rabbitMQ event
    except Exception as ex:
        raise ex


async def payment_deduct(user_id, total_cost):
    try:
        print('Inside payment_deduct()...')
        data = {'user_id': user_id, 'total_cost': total_cost}
        event = {'event': 'payment_deduct', 'details': 'payment_deduct for the order', 'data': data}
        publish_event('payment_deduct', event) #publish rabbitMQ event
    except Exception as ex:
        raise ex


async def payment_add( user_id, total_cost):
    try:
        print('Inside payment_add()...')
        data = {'user_id': user_id, 'total_cost': total_cost}
        event = {'event': 'payment_add', 'details': 'payment_add for the order', 'data': data}
        publish_event('payment_add', event) #publish rabbitMQ event
    except Exception as ex:
        raise ex

def CreateOrderRequestSaga(order_entry: OrderValue, order_id=0, items_quantities=0, total_cost=0, user_id=0):
    builder = (
        OrchestrationBuilder()
        .add_step(start_orch, lambda curr_act_res, items=order_entry: stock_add(curr_act_res, items))
        .add_step(lambda prev_act_res, items=order_entry: stock_subtract(items),
                  lambda curr_act_res, user_id=user_id, total_cost=total_cost: payment_add(curr_act_res, user_id,
                                                                                           total_cost))
        .add_step(lambda prev_act_res, user_id=user_id, total_cost=total_cost: payment_deduct(user_id, total_cost),
                  lambda: None)
    )
    asyncio.run(builder.execute())

order_entry = OrderValue(paid=False,
                   items=[("1", 1), ("2", 1)],
                   user_id="123",
                   total_cost=200)

CreateOrderRequestSaga(order_entry=order_entry)