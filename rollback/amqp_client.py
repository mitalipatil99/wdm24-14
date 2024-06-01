import os
import json
from logging import Logger
import pika
from config import STOCK_ADD,STOCK_SUBTRACT,PAYMENT_ADD,PAYMENT_DEDUCT


class Singleton(type):
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]
    
class AMQPClient(metaclass = Singleton):

    def declare_queues(self):
        
        exchange =  self.channel.exchange_declare("ORCHESTRATION_SAGA", "direct")
        self.exchange = exchange
        self.channel.queue_declare(STOCK_ADD)
        self.channel.queue_bind(STOCK_ADD, "ORCHESTRATION_SAGA", STOCK_ADD)
        self.channel.queue_declare(STOCK_SUBTRACT)
        self.channel.queue_bind(STOCK_SUBTRACT, "ORCHESTRATION_SAGA", STOCK_SUBTRACT)
        self.channel.queue_declare(PAYMENT_ADD)
        self.channel.queue_bind(PAYMENT_ADD, "ORCHESTRATION_SAGA", PAYMENT_ADD)
        self.channel.queue_declare(PAYMENT_DEDUCT)
        self.channel.queue_bind(PAYMENT_DEDUCT, "ORCHESTRATION_SAGA", PAYMENT_DEDUCT)


    def connect(self):
        try:
            connection = pika.BlockingConnection(pika.URLParameters(os.environ['RABBITMQ_BROKER_URL']))
            channel = connection.channel()
            self.channel = channel
            self.declare_queues()
        except Exception as e:
            print(e)
    

    def __init__(self, logger: Logger, is_consumer=False) -> None:
        self.logger = logger       
        self.channel = None
        self.is_consumer = is_consumer


    def publish_event(self, queue, data):
        try:
           self.channel.basic_publish(exchange="ORCHESTRATION_SAGA", routing_key= queue, body=json.dumps(data))
        except Exception as e:
            print(e)

