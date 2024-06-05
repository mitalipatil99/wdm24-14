class RedisDBError(Exception):
	pass

class InsufficientStockError(Exception):
	pass

class ItemNotFoundError(Exception):
	pass

class RabbitMQError(Exception):
    pass