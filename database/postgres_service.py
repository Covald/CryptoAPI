import asyncio

from schemes import Coin
from database import postgres

async def _get_market_id(market_name:str)->int:
    select_market_id, insert_market_id = await asyncio.gather(
        postgres.fetch(f"SELECT id from markets where name='{market_name}'"),
        postgres.fetch(
            f"INSERT INTO markets (name) VALUES ('{market_name}') ON CONFLICT DO NOTHING RETURNING id;")
    )

    if select_market_id:
        return select_market_id[0]['id']
    else:
        return insert_market_id[0]['id']

async def upsert_coins(coins:list[Coin]):
    sql = """
    INSERT INTO coins (symbol, market_id, type, price, spread, index_price, volume_24h, open_interest, funding_rate, ts)
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
    ON CONFLICT (symbol,market_id,type) DO UPDATE SET
    price=excluded.price,
    spread=excluded.spread,
    index_price=excluded.index_price,
    volume_24h=excluded.volume_24h,
    open_interest=excluded.open_interest,
    funding_rate=excluded.funding_rate,
    ts=excluded.ts
    """
    params = [list(coin.model_dump().values()) for coin in coins]
    await postgres.executemany(sql,params)