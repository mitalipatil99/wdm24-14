from msgspec import Struct

class StockValue(Struct):
    stock: int
    price: int
    last_upd: str