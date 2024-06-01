class RedisDBError(Exception):
	pass

class InsufficientStockError(Exception):
	pass

class ItemNotFoundError(Exception):
	pass

class DuplicateUpdateError(Exception):
	pass