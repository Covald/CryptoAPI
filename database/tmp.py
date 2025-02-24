import asyncio

from schemes import Coin
from database.postgres import postgres
from database.clickhouse_service import insert_coins


async def get_market_id(market_name: str) -> int:
    select_market_id, insert_market_id = await asyncio.gather(
        postgres.fetch(f"SELECT id from markets where name='{market_name}'"),
        postgres.fetch(
            f"INSERT INTO markets (name) VALUES ('{market_name}') ON CONFLICT DO NOTHING RETURNING id;")
    )

    if select_market_id:
        return select_market_id[0]['id']
    else:
        return insert_market_id[0]['id']

async def upload_data(market_name: str, coins: list[Coin]) -> None:
    market_id = await get_market_id(market_name)

    for coin in coins:
        coin.market_name = market_id

    tasks = [
        upsert_coins(coins),
        insert_coins(market_name,coins)
    ]

    await asyncio.gather(*tasks)


async def upsert_coins(coins: list[Coin]):
    sql = f"""
    INSERT INTO coins (symbol, market_id, type, price, index_price, volume_24h, spread, open_interest, funding_rate, ts)
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
    ON CONFLICT (symbol, market_id, type) DO UPDATE SET
    price=EXCLUDED.price,
    index_price=EXCLUDED.index_price,
    volume_24h=EXCLUDED.volume_24h,
    spread=EXCLUDED.spread,
    open_interest=EXCLUDED.open_interest,
    funding_rate=EXCLUDED.funding_rate,
    ts=EXCLUDED.ts
    """
    params = [
        (coin.symbol, coin.market_name, coin.type, coin.price,
         coin.index_price,
         coin.volume_24h, coin.spread,
         coin.open_interest, coin.funding_rate, coin.ts.replace(tzinfo=None))
        for coin in coins
    ]
    await postgres.executemany(sql, params)

