from database.clickhouse import clickhouse
from schemes import Coin


async def insert_coins(market_name,coins:list[Coin]):
    columns = [
        'symbol',
        'market_name',
        'type',
        'price',
        'spread',
        'index_price',
        'volume_24h',
        'open_interest',
        'funding_rate',
        'ts'
    ]
    data = [(
        coin.symbol,
        market_name,
        coin.type,
        coin.price,
        coin.spread,
        coin.index_price,
        coin.volume_24h,
        coin.open_interest,
        coin.funding_rate,
        coin.ts
    ) for coin in coins]
    await clickhouse.conn.insert('coins', data, columns)