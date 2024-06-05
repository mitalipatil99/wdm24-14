from msgspec import Struct
from typing import Optional

class OrderValue(Struct):
    paid: bool
    items: list[tuple[str, int]]
    user_id: str
    total_cost: int
    last_upd: str = "admin"