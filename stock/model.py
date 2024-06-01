from msgspec import Struct
from typing import Optional

class StockValue(Struct):
    stock: int
    price: int
    last_upd: Optional[str] = "api"
    last_op: Optional[str] = "add"