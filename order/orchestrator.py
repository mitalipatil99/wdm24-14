import asyncio
from saga import OrchestrationBuilder
from msgspec import msgpack, Struct

class OrderValue(Struct):
    paid: bool
    items: list[tuple[str, int]]
    user_id: str
    total_cost: int


async def start_orch():
    print('orchestration started')
    return 'orchestration started'

async def stock_subtract(items: OrderValue): 
    print('Inside stock_subtract()...')
    print([x for x in items.items])
    # raise RuntimeError('test')
    return 'result_1'


async def stock_add(result, items):
    print(f'Inside stock_add()...')
    print([x for x in items.items])


async def payment_deduct(user_id, total_cost):
    print('Inside payment_deduct()...')
    print(user_id, total_cost)
    # raise RuntimeError('test')

async def payment_add(result, user_id, total_cost):
    print(f'Inside payment_add()...')

def CreateOrderRequestSaga(order_id, items_quantities, total_cost, user_id, order_entry: OrderValue):
    builder = (
        OrchestrationBuilder()
        .add_step(start_orch, lambda curr_act_res, items = order_entry: stock_add(curr_act_res, items))
        .add_step(lambda prev_act_res, items = order_entry: stock_subtract(items), lambda curr_act_res, user_id = user_id, total_cost = total_cost: payment_add(curr_act_res, user_id, total_cost))
        .add_step(lambda prev_act_res, user_id = user_id, total_cost = total_cost : payment_deduct(user_id, total_cost), lambda: None)
    )
    asyncio.run(builder.execute())

# order_entry = OrderValue(paid=False,
#                    items=[("1", 1), ("2", 1)],
#                    user_id="123",
#                    total_cost=200)

# CreateOrderRequestSaga(order_entry = order_entry)
