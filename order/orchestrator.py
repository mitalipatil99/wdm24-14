import asyncio

from order.saga import OrchestrationBuilder

async def start_orch():
    print('orchestration started')
    return 'orchestration started'
async def stock_subtract(): 
    print('async_action_1()')
    return 'result_1'


async def stock_add(result):
    print(f'async_compensation_1({result})')


async def payment_deduct():
    print('async_action_2()')
    raise RuntimeError('test')

async def payment_add(result):
    print(f'async_compensation_1({result})')


builder = (
    OrchestrationBuilder()
    .add_step(start_orch, lambda curr_act_res: stock_add(curr_act_res))
    .add_step(stock_subtract, lambda curr_act_res: payment_add(curr_act_res))
    .add_step(payment_deduct, lambda: None)
)
asyncio.run(builder.execute())
