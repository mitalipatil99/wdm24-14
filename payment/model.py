from msgspec import Struct

class UserValue(Struct):
    credit: int
    last_upd: str