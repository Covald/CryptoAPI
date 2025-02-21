from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel


class Types(StrEnum):
    spot = "spot"
    linear = "linear"
    inverse = "inverse"


class Coin(BaseModel):
    market_id: int | None
    symbol: str
    type: Types
    price: float
    index_price: float | None
    volume: float
    spread: float | None
    open_interest: float | None
    funding_rate: float | None
    ts: datetime


class CoinDepth(BaseModel):
    coin_id: int
    plus_depth: float
    minus_depth: float


class Market(BaseModel):
    name: str
    id: str | None
    coins: list[Coin]
