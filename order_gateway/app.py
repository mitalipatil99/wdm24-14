import logging
import uuid
from random import random

import pika
import os
from threading import Thread
from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response

DB_ERROR_STR = "DB error"

app = Flask("order-gateway")

class OrderValue(Struct):
    paid: bool
    items: list[tuple[str, int]]
    user_id: str
    total_cost: int

class RabbitMQClient:
    def __init__(self):
        self.connection = pika.BlockingConnection(pika.URLParameters(os.environ['RABBITMQ_BROKER_URL']))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue='order_queue')
        self.channel.queue_declare(queue='stock_queue')
        self.channel.queue_declare(queue='payment_queue')


    def call(self, message, queue):
        corr_id = str(uuid.uuid4())
        response_queue = self.channel.queue_declare(queue='', exclusive=True).method.queue

        def on_response(ch, method, props, body):
            if corr_id == props.correlation_id:
                ch.basic_ack(delivery_tag=method.delivery_tag)
                self.response = msgpack.decode(body)
                ch.stop_consuming()

        self.channel.basic_consume(queue=response_queue, on_message_callback=on_response, auto_ack=False)
        self.response = None
        self.channel.basic_publish(
            exchange='',
            routing_key=queue,
            properties=pika.BasicProperties(
                reply_to=response_queue,
                correlation_id=corr_id,
            ),
            body=msgpack.encode(message)
        )

        while self.response is None:
            self.connection.process_data_events(time_limit=None)

        return self.response


rabbitmq_client = RabbitMQClient()

class ThreadWithReturnValue(Thread):
    
    def __init__(self, group=None, target=None, name=None,
                 args=(), kwargs={}, Verbose=None):
        Thread.__init__(self, group, target, name, args, kwargs)
        self._return = None

    def run(self):
        if self._target is not None:
            self._return = self._target(*self._args,
                                                **self._kwargs)
    def join(self, *args):
        Thread.join(self, *args)
        return self._return


def threaded(fn):
    def wrapper(*args, **kwargs):
        def callback():
            try:
                return fn(*args, **kwargs)
            except Exception as e:
                app.logger.error(f"Error in thread: {e}")
        
        thread = ThreadWithReturnValue(target=callback)
        thread.start()
        # handle status code
        ret = thread.join()
        app.logger.debug(f"woahh {ret}")
        if ret['status'] == 200:
            return Response(ret['msg'], ret['status'])
        return jsonify(ret)

    wrapper.__name__ = f"{fn.__name__}_wrapper"
    return wrapper


@app.post('/create/<user_id>')
@threaded
def create_order(user_id: str):
    response = rabbitmq_client.call({'action': 'create_order','user_id':user_id}, 'order_queue')
    return response



@app.post('/batch_init/<n>/<n_items>/<n_users>/<item_price>')
@threaded
def batch_init_users(n: int, n_items: int, n_users: int, item_price: int):

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

    response = rabbitmq_client.call({'action': 'batch_init_users','kv_pairs':kv_pairs}, 'order_queue')
    return response


@app.get('/find/<order_id>')
@threaded
def find_order(order_id: str):
    response = rabbitmq_client.call({'action': 'find_order','order_id':order_id}, 'order_queue')
    return response


@app.post('/addItem/<order_id>/<item_id>/<quantity>')
@threaded
def add_item(order_id: str, item_id: str, quantity: int):
    
    order_details = rabbitmq_client.call({'action': 'find_order','order_id':order_id}, 'order_queue')
    item_details = rabbitmq_client.call({'action':'find_item', 'item_id': item_id}, 'stock_queue')
    # check status
    order_details['items'].append((item_id, int(quantity)))
    order_details['total_cost'] += int(quantity) * item_details['price']

    response = rabbitmq_client.call({'action': 'add_item','order_id':order_id,'order_entry': order_details}, 'order_queue')
    return response

@app.post('/checkout/<order_id>')
@threaded
def checkout(order_id: str):
    app.logger.debug(f"Checking out {order_id}")
    order_details = rabbitmq_client.call({'action': 'find_order','order_id':order_id}, 'order_queue')
    item_data = {}
    for item in order_details['items']:
        item_data[item[0]] = item[1]
    stock_sub_response = rabbitmq_client.call({'action': 'remove_stock_bulk','data':item_data, 'order_id': order_id}, 'stock_queue')
    # if stock_sub_response['status'] != 200:
    #     stock_add_response = rabbitmq_client.call({'action':'add_stock_bulk', 'data':item}, 'stock_queue')
    #     return {}

    payment = rabbitmq_client.call({'action': 'remove_credit','user_id':order_details['user_id'], 'amount': order_details['total_cost']}, 'payment_queue')
    # print(order_details_response)
    app.logger.info(payment)
    


    app.logger.debug("Checkout successful")
    return {"msg": "checkout successful", "status":200}
    # return Response("Checkout successful", status=200)


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
