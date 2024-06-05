import logging
import uuid
import random
import pika
import os
from threading import Thread
from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response, request
from config import *
from datetime import datetime

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
        self.channel.queue_declare(queue=ORDER_QUEUE)
        self.channel.queue_declare(queue=STOCK_QUEUE)
        self.channel.queue_declare(queue=PAYMENT_QUEUE)


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
        endpoint = request.endpoint
        request_data = request.get_json() if request.is_json else request.get_data()
        timestamp = datetime.now()
        app.logger.info(f"[{timestamp}] API: {endpoint} | {request_data}")        
        
        def callback():
            try:
                # endpoint = request.endpoint
                # request_data = request.get_json() if request.is_json else request.values.to_dict()
                # timestamp = datetime.utcnow().isoformat()
                # app.logger.info(f"[{timestamp}] API: {endpoint} | {request_data}")
                return fn(*args, **kwargs)
            except Exception as e:
                app.logger.error(f"Error in thread: {e}")
                return {"status":500, "data":f"{e}"}
        
        thread = ThreadWithReturnValue(target=callback)
        thread.start()
        response = thread.join()
        app.logger.info(f"Response: {response}")
        if response['status'] == STATUS_SUCCESS:
            return jsonify(response['data'])
        else:
            return abort(response['status'], response['data'] )


    wrapper.__name__ = f"{fn.__name__}_wrapper"
    return wrapper


@app.post('/create/<user_id>')
@threaded
def create_order(user_id: str):
    response = rabbitmq_client.call({'action': 'create_order','user_id':user_id}, ORDER_QUEUE)
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

    response = rabbitmq_client.call({'action': 'batch_init_users','kv_pairs':kv_pairs}, ORDER_QUEUE)
    return response


@app.get('/find/<order_id>')
@threaded
def find_order(order_id: str):
    response = rabbitmq_client.call({'action': 'find_order','order_id':order_id}, ORDER_QUEUE)
    return response


@app.post('/addItem/<order_id>/<item_id>/<quantity>')
@threaded
def add_item(order_id: str, item_id: str, quantity: int):
    
    order_details = rabbitmq_client.call({'action': 'find_order','order_id':order_id}, ORDER_QUEUE)
    if order_details['status'] != 200:
        return order_details
    order_details = order_details['data']

    item_details = rabbitmq_client.call({'action':'find_item', 'item_id': item_id}, STOCK_QUEUE)
    if item_details['status'] != 200:
        return item_details
    item_details = item_details['data']

    order_details['items'].append((item_id, int(quantity)))
    order_details['total_cost'] += int(quantity) * item_details['price']

    response = rabbitmq_client.call({'action': 'add_item','order_id':order_id,'order_entry': order_details}, ORDER_QUEUE)
    if response['status']!=200:
        return response
    response['data'] = f"Item: {item_id} added to: {order_id} price updated to: {order_details["total_cost"]}"
    return response

@app.post('/checkout/<order_id>')
@threaded
def checkout(order_id: str):
    # TODO: While fetching order details check if paid, check if success already
    app.logger.debug(f"Checking out {order_id}")
    order_details = rabbitmq_client.call({'action': 'find_order','order_id':order_id}, ORDER_QUEUE)
    if order_details['status']!= 200:
        return order_details
    order_details = order_details['data']
    
    item_data = {}
    for item in order_details['items']:
        item_data[item[0]] = item[1]
    
    stock_sub_response = rabbitmq_client.call({'action': 'remove_stock_bulk','data':item_data, 'order_id': order_id}, STOCK_QUEUE)
    if stock_sub_response['status'] != 200:
        if stock_sub_response['status'] == 500:
            stock_add_response = rabbitmq_client.call({'action':'add_stock_bulk', 'data':item_data}, STOCK_QUEUE)
        return stock_sub_response
    
    payment_sub_response = rabbitmq_client.call({'action': 'remove_credit','user_id':order_details['user_id'], 'amount': order_details['total_cost']}, PAYMENT_QUEUE)
    if payment_sub_response['status'] != 200:
        # Fix response
        if payment_sub_response['status'] == 500:
            payment_add_response = rabbitmq_client.call({'action': 'add_funds','user_id':order_details['user_id'], 'amount': order_details['total_cost']}, PAYMENT_QUEUE)
        stock_add_response = rabbitmq_client.call({'action':'add_stock_bulk', 'data':item_data, 'order_id': order_id}, STOCK_QUEUE)
        return payment_sub_response
    order_details['paid'] = True
    confirm_order_response = rabbitmq_client.call({'action':'confirm_order', 'order_id': order_id, 'order_entry': order_details}, ORDER_QUEUE)
    return confirm_order_response

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
