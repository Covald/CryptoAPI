import asyncio

from schemes import Coin
from database.postgres import postgres
from database.clickhouse import clickhouse


async def get_market_id(market_name: str) -> int:
    select_market_id, insert_market_id = await asyncio.gather(
        postgres.execute(f"SELECT id from markets where name='{market_name}'"),
        postgres.execute(f"INSERT INTO markets (name) VALUES ('{market_name}') ON CONFLICT DO NOTHING RETURNING id;")
    )

    if select_market_id:
        return select_market_id[0]['id']
    else:
        return insert_market_id[0]['id']


async def clickhouse_connect():
    await clickhouse.connect()
    await clickhouse.create_table()


async def upload_data(market_name: str, coins: list[Coin]) -> None:
    await clickhouse.connect()
    await clickhouse.create_table()

    market_id = await get_market_id(market_name)

    for coin in coins:
        coin.market_id = market_id

    tasks = [
        upload_coins(coins),
        upload_coins_to_ch(market_name,coins)
    ]

    await asyncio.gather(*tasks)


async def upload_coins(coins: list[Coin]):
    sql = f"""
    INSERT INTO coins (symbol, market_id, type, price, index_price, volume, spread, open_interest, funding_rate, ts)
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
    
    ON CONFLICT (symbol, market_id, type) DO UPDATE SET
    price=EXCLUDED.price,
    index_price=EXCLUDED.index_price,
    volume=EXCLUDED.volume,
    spread=EXCLUDED.spread,
    open_interest=EXCLUDED.open_interest,
    funding_rate=EXCLUDED.funding_rate,
    ts=EXCLUDED.ts
    """
    params = [
        (coin.symbol, coin.market_id, coin.type, coin.price,
         coin.index_price,
         coin.volume, coin.spread,
         coin.open_interest, coin.funding_rate, coin.ts)
        for coin in coins
    ]
    await postgres.executemany(sql, params)


async def upload_coins_to_ch(market_name: str, coins: list[Coin]):
    sql = f"""
        INSERT INTO coins (symbol, market_name, type, price, index_price, volume, spread, open_interest, funding_rate, ts)
        VALUES 
        {',\n'.join([f'({dump_coin(coin, market_name)})' for coin in coins])}
        """
    await clickhouse.execute(sql)


def dump_coin(coin: Coin, market_name: str | None = None) -> str:
    return ", ".join(map(str,
                         [f"'{coin.symbol}'", f"'{market_name}'" if market_name else coin.market_id, f"'{coin.type}'",
                          coin.price,
                          coin.index_price,
                          coin.volume, coin.spread,
                          coin.open_interest, coin.funding_rate, f"'{coin.ts.strftime('%Y-%m-%d %H:%M:%S')}'"]))
