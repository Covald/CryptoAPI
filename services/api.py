from datetime import datetime, timedelta

from database import postgres, clickhouse
from schemes import Types, Coin
from schemes.coin import ExtendedCoin


async def get_extended_coin(market: str | None = None, symbol: str | None = None, type: Types | None = None,
                      limit: int | None = 100):
    base_sql = """
           SELECT m.name AS market_name, symbol, type, price, spread, index_price, volume_24h,  open_interest, funding_rate, ts
           FROM coins c
           RIGHT JOIN markets m ON m.id = c.market_id
       """

    conditions = []
    if market:
        conditions.append(f"m.name='{market}'")
    if symbol:
        conditions.append(f"symbol='{symbol}'")
    if type:
        conditions.append(f"type='{type}'")

    if conditions:
        base_sql += " WHERE " + " AND ".join(conditions)

    pg_coins = await postgres.fetch(base_sql)
    coins = []
    for row in pg_coins:
        coins.append(ExtendedCoin(
            symbol=row['symbol'],
            market_name=row['market_name'],
            type=row['type'],
            price=row['price'],
            spread=row['spread'],
            index_price=row['index_price'],
            volume_24h=row['volume_24h'],
            open_interest=row['open_interest'],
            funding_rate=row['funding_rate'],
            ts=row['ts']
        ))

    current_time = datetime.now().replace(microsecond=0) - timedelta(hours=3)
    yesterday = current_time - timedelta(days=0)
    start_time = yesterday - timedelta(minutes=30)
    end_time = yesterday + timedelta(minutes=30)
    print(f"select ts from coins where ts between '{start_time}' and '{end_time}' limit 1")
    res = await clickhouse.execute(f"select ts from coins where ts between '{start_time}' and '{end_time}' limit 1")
    print(res.result_rows)
    ch_time = res.first_row[0]

    conditions = []
    conditions.append(f"ts='{ch_time}'")
    if market:
        conditions.append(f"market_name='{market}'")
    if symbol:
        conditions.append(f"symbol='{symbol}'")
    if type:
        conditions.append(f"type='{type}'")

    if conditions:
        base_sql += " WHERE " + " AND ".join(conditions)
    ch_sql = "select symbol, volume_24h from coins" + " WHERE " + " AND ".join(conditions)
    ch_coins = await clickhouse.execute(ch_sql)
    ch_coins = dict(ch_coins.result_rows)

    for coin in coins:
        yesterday_volume = ch_coins.get(coin.symbol, coin.volume_24h)
        print(yesterday_volume,coin.volume_24h)
        coin.volume_perc = round(((coin.volume_24h - yesterday_volume) / yesterday_volume * 100), 7)

    return coins
