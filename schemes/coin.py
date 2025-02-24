from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel


class Types(StrEnum):
    spot = "spot"
    linear = "linear"
    inverse = "inverse"
    option = "option"


class Coin(BaseModel):
    symbol: str
    market_name: int | str | None
    type: Types
    price: float
    spread: float | None
    index_price: float | None
    volume_24h: float
    open_interest: float | None
    funding_rate: float | None
    ts: datetime

class ExtendedCoin(Coin):
    volume_perc:float|None = None

class CoinDepth(BaseModel):
    coin_id: int
    plus_depth: float
    minus_depth: float


class Market(BaseModel):
    name: str
    id: str | None
    coins: list[Coin]
